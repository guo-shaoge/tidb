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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"fmt"
	"context"
	"sync"

	"github.com/pingcap/tidb/executor/tidb_velox_wrapper"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
)

var _ Executor = &VeloxExec{}

type VeloxExec struct {
	baseExecutor

	prepared     bool
	tableReaders []*TableReaderExecutor
	workerWg     sync.WaitGroup

	veloxQueryCtx tidb_velox_wrapper.VeloxQueryCtx
}

func (e *VeloxExec) Open(ctx context.Context) (err error) {
	logutil.BgLogger().Info("Velox log VeloxExec Open beg")
	for _, r := range e.tableReaders {
		if err = r.Open(ctx); err != nil {
			return err
		}
	}
	logutil.BgLogger().Info("Velox log VeloxExec Open done")
	return err
}

func (e *VeloxExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	logutil.BgLogger().Info("Velox log VeloxExec Next beg")
	if !e.prepared {
		e.startWorkers(ctx)
		e.prepared = true
	}
	tidb_velox_wrapper.FetchVeloxOutput(e.veloxQueryCtx)
	logutil.BgLogger().Info("Velox log VeloxExec Next done")
	return nil
}

func (e *VeloxExec) Close() error {
	var firstErr error
	for _, r := range e.tableReaders {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	e.workerWg.Wait()
	tidb_velox_wrapper.DestroyVeloxQueryCtx(e.veloxQueryCtx)
	e.ctx.GetSessionVars().StmtCtx.VeloxQueryCtx = nil
	return firstErr
}

type veloxWorker struct {
	tableReader *TableReaderExecutor

	// Each tableReader correspond one.
	veloxDS *tidb_velox_wrapper.CGoVeloxDataSource

	workerWg sync.WaitGroup
}

// For each tableReader, start a worker to:
// 1. call tableReader.Next() to read chunk
// 2. call ChunkConvertor to convert chunk to Velox::RowVectorPtr
// 3. push Velox::RowVectorPtr to veloxDataSource
func (e *VeloxExec) startWorkers(ctx context.Context) {
	for _, r := range e.tableReaders {
		startTS := e.ctx.GetSessionVars().TxnCtx.StartTS
		// gjt todo: maybe use string as id?
		ds := tidb_velox_wrapper.NewCGoVeloxDataSource(int64(startTS + uint64(r.id)))
		worker := &veloxWorker{
			tableReader: r,
			veloxDS:     ds,
			workerWg:    e.workerWg,
		}
		e.workerWg.Add(1)
		worker.run(ctx)
	}
}

func (w *veloxWorker) run(ctx context.Context) {
	for {
		// gjt todo: how to calcel?
		// select {
		// case <-ctx.Done():
		// 	w.workerWg.Done()
		// }

		req := newFirstChunk(w.tableReader)
		w.tableReader.Next(ctx, req)
		logutil.BgLogger().Info(fmt.Sprintf("Velox log VeloxExec tableReader one chunk req.Num(): %d\n", req.NumRows()))
		if req.NumRows() == 0 {
			// w.veloxDS.noMoreInput()
			break
		}

		var arrow tidb_velox_wrapper.CGoRowVector
		// arrow := chunkConvertor.convertToArrow(req)
		// may block. gjt todo: how to cancel
		w.veloxDS.Enqueue(arrow)
	}
}
