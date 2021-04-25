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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/chunk"
)

type CTETableReaderExec struct {
	baseExecutor

	iterInTbl CTEStorage
	chkIdx    int
	curIter   int
}

func (e *CTETableReaderExec) Open(ctx context.Context) error {
	return e.baseExecutor.Open(ctx)
}

func (e *CTETableReaderExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.Reset()
	if e.curIter != e.iterInTbl.GetIter() {
		if e.curIter > e.iterInTbl.GetIter() {
			return errors.Errorf("invalid iteration for CTETableReaderExec(e.curIter: %d, e.iterInTbl.GetIter(): %d", e.curIter, e.iterInTbl.GetIter())
		}
		e.chkIdx = 0
		e.curIter = e.iterInTbl.GetIter()
	}
	if e.chkIdx < e.iterInTbl.NumChunks() {
		res, err := e.iterInTbl.GetChunk(e.chkIdx)
		if err != nil {
			return err
		}
		// Need to copy chunk to make sure upper operator will not change chunk in iterInTbl.
		req.SwapColumns(res.CopyConstruct())
		e.chkIdx++
	}
	return nil
}

func (e *CTETableReaderExec) Close() (err error) {
	if err = e.iterInTbl.DerefAndClose(); err != nil {
		return err
	}
	return e.baseExecutor.Close()
}
