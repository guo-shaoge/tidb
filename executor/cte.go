// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
)

// Following diagram describes how CTEExec works.
//
// `iterInTbl` is shared by `CTEExec` and `CTETableReaderExec`.
// `CTETableReaderExec` reads data from `iterInTbl`.
// Output will be stored into `iterOutTbl` by `CTEExec`.
// When an iteration ends, `CTEExec` will move all data from `iterOutTbl` into `iterInTbl`,
// which will be the input for new iteration.
// At the end of each iteration, data in `iterOutTbl` will also be added into `resTbl`.
// `resTbl` stores data of all iteration.
//                                   +----------+
//                                   |iterOutTbl|
//       CTEExec ------------------->|          |
//          |                        +----+-----+
//    -------------                       |
//    |           |                       v
// other op     other op             +----------+
// (seed)       (recursive)          |  resTbl  |
//                  ^                |          |
//                  |                +----------+
//            CTETableReaderExec
//                   ^
//                   |               +----------+
//                   +---------------+iterInTbl |
//                                   |          |
//                                   +----------+
type CTEExec struct {
	baseExecutor

	seedExec      Executor
	recursiveExec Executor

	// resTbl and iterInTbl are shared by all CTEExec which reference to same CTE.
	// iterInTbl is also shared by CTETableReaderExec.
	resTbl     CTEStorage
	iterInTbl  CTEStorage
	iterOutTbl CTEStorage

	chkIdx     int
	isDistinct bool
	curIter    int
}

func (e *CTEExec) Open(ctx context.Context) (err error) {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	memTracker := e.resTbl.GetMemTracker()
	memTracker.SetLabel(memory.LabelForCTEStorage)
	memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	diskTracker := e.resTbl.GetDiskTracker()
	diskTracker.SetLabel(memory.LabelForCTEStorage)
	diskTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.DiskTracker)

	if config.GetGlobalConfig().OOMUseTmpStorage {
		// TODO: also record memory usage of iterInTbl and iterOutTbl
		actionSpill := e.resTbl.ActionSpill()
		e.ctx.GetSessionVars().StmtCtx.MemTracker.FallbackOldAndSetNewAction(actionSpill)
	}

	return nil
}

func (e *CTEExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.Reset()
	e.resTbl.Lock()
	if !e.resTbl.Done() {
		// Compute seed part
		for {
			chk := newFirstChunk(e.seedExec)
			if err = Next(ctx, e.seedExec, chk); err != nil {
				return err
			}
			if chk.NumRows() == 0 {
				break
			}
			if err = e.iterInTbl.Add(chk); err != nil {
				return err
			}
			if err = e.resTbl.Add(chk); err != nil {
				return err
			}
		}

		if e.iterInTbl.NumChunks() == 0 {
			return nil
		}

		// Start to compute recursive part. Iteration 1 begins.
		e.curIter = 1
		for {
			chk := newFirstChunk(e.recursiveExec)
			if err = Next(ctx, e.recursiveExec, chk); err != nil {
				return err
			}
			if chk.NumRows() == 0 {
				// TODO: other iteration stop condition: max iter num
				if e.iterOutTbl.NumChunks() == 0 {
					break
				} else {
					// Next iteration begins. Need use iterOutTbl as input of next iteration.
					for i := 0; i < e.iterOutTbl.NumChunks(); i++ {
						if chk, err = e.iterOutTbl.GetChunk(i); err != nil {
							return err
						}
						if err = e.resTbl.Add(chk); err != nil {
							return err
						}
					}
					if err = e.iterInTbl.SwapData(e.iterOutTbl); err != nil {
						return err
					}
					if err = e.iterOutTbl.ResetData(); err != nil {
						return err
					}
					e.curIter++
					e.iterInTbl.SetIter(e.curIter)
				}
			} else {
				if err = e.iterOutTbl.Add(chk); err != nil {
					return err
				}
			}
		}

		e.resTbl.SetDone()
	}
	e.resTbl.Unlock()

	if e.chkIdx < e.resTbl.NumChunks() {
		res, err := e.resTbl.GetChunk(e.chkIdx)
		if err != nil {
			return err
		}
		// Need to copy chunk to make sure upper operator will not change chunk in resTbl.
		req.SwapColumns(res.CopyConstruct())
		e.chkIdx++
	}
	return nil
}

func (e *CTEExec) Close() (err error) {
	if err = e.resTbl.DerefAndClose(); err != nil {
		return err
	}
	if err = e.iterInTbl.DerefAndClose(); err != nil {
		return err
	}
	if err = e.iterOutTbl.DerefAndClose(); err != nil {
		return err
	}

	return e.baseExecutor.Close()
}
