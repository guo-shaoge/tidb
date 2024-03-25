// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tidbworker

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	workercli "github.com/tidbcloud/aws-shared-provider/pkg/tidbworker/client"
	"go.uber.org/zap"
)

var _ Manager = &manager{}

type manager struct {
	client workercli.Client
	role   string
}

const (
	// WorkerTypeDDL is the type of ddl background task.
	WorkerTypeDDL = "ddl"
	// WorkerTypeBatch is the type of batch background task.
	WorkerTypeBatch = "batch"
	// WorkerTypeGCV2 is the type of GCV2 background task.
	WorkerTypeGCV2 = "gcv2"
	// WorkerTypeGC is the type of GC background task.
	WorkerTypeGC = "gc"
)

// TaskWorkerType converts the task type in global to the type of TiDB worker.
func TaskWorkerType(taskType string) string {
	switch taskType {
	case "backfill":
		return WorkerTypeDDL
	case "batch":
		return WorkerTypeBatch
	default:
		return ""
	}
}

// InitManager initialize the global TiDB worker manager with the given backend.
func InitManager(ctx context.Context, keyspaceName string, cfg config.TiDBWorker) (err error) {
	once.Do(func() {
		if cfg.LocalMode.Enable {
			GlobalTiDBWorkerManager = &manager{
				client: &localClient{},
				role:   cfg.Role,
			}
			return
		}
		var c workercli.Client
		c, err = workercli.NewClientWithContext(ctx, keyspaceName, cfg.TidbPool, cfg.RegistryAddr, cfg.APIServerAddr)
		if err != nil {
			log.Error("[tidb worker] failed to connect to tidb worker service", zap.Error(err))
			return
		}
		GlobalTiDBWorkerManager = &manager{
			client: c,
			role:   cfg.Role,
		}
	})
	return err
}

func (m *manager) InitializeGC(ctx context.Context, sctx sessionctx.Context) error {
	log.Info("[tidb-worker] initialize GC tasks")
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeGC, metrics.InitializeWorkerTasks, "").Inc()
	tasks, err := ddlutil.LoadDeleteRanges(ctx, sctx, math.MaxUint64)
	if err != nil {
		return errors.Trace(err)
	}
	for _, task := range tasks {
		err := m.RegisterGC(ctx, task.Ts)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (m *manager) InitializeGCV2(ctx context.Context) error {
	log.Info("[tidb-worker] initialize GCV2 tasks")
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeGCV2, metrics.InitializeWorkerTasks, "").Inc()
	// Use 0 as the timestamp to make sure this task can be cleaned by the completion of any other GCV2 task.
	err := m.RegisterGCV2(ctx, time.Now().Unix(), 0)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *manager) RegisterGC(ctx context.Context, ts uint64) error {
	log.Info("[tidb-worker] register a GC task to worker service", zap.Uint64("ts", ts))
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeGC, metrics.RegisterWorkerTask, "").Inc()
	return m.client.RegisterGC(ctx, ts)
}

func (m *manager) RecycleGC(ctx context.Context, safePoint uint64) error {
	log.Info("[tidb-worker] notify worker service to recycle a GC task", zap.Uint64("safe-point", safePoint))
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeGC, metrics.RecycleWorkerTask, "").Inc()
	return m.client.RecycleGC(ctx, safePoint)
}

func (m *manager) RegisterGCV2(ctx context.Context, gcLastRunTime int64, ts uint64) error {
	log.Info("[tidb-worker] register a GCV2 task to worker service",
		zap.Int64("gc-last-run-time", gcLastRunTime),
		zap.Uint64("ts", ts),
	)
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeGCV2, metrics.RegisterWorkerTask, "").Inc()
	return m.client.RegisterGCV2(ctx, gcLastRunTime, ts)
}

func (m *manager) RecycleGCV2(ctx context.Context, safePoint uint64) error {
	log.Info("[tidb-worker] register a GCV2 task to worker service", zap.Uint64("safe-point", safePoint))
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeGCV2, metrics.RecycleWorkerTask, "").Inc()
	return m.client.RecycleGCV2(ctx, safePoint)
}

func (m *manager) AbortGCV2(ctx context.Context) error {
	log.Info("[tidb-worker] abort all GCV2 tasks")
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeGCV2, metrics.AbortWorkerTask, "").Inc()
	return m.client.RecycleGCV2(ctx, math.MaxUint64)
}

func (m *manager) Role() string {
	return m.role
}

func (m *manager) RegisterBgTask(ctx context.Context, taskType, taskKey string, gTaskID, subTaskID int64, execID string) error {
	log.Info("[tidb-worker] register a background task to worker service",
		zap.String("task-key", taskKey),
		zap.String("task-type", taskType),
		zap.Int64("global-task-id", gTaskID),
		zap.Int64("subtask-id", subTaskID),
		zap.String("exec-id", execID),
	)
	metrics.WorkerTaskCounter.WithLabelValues(taskType, metrics.RegisterWorkerTask, taskKey).Inc()
	return m.client.RegisterBgTask(ctx, taskType, taskKey, gTaskID, subTaskID, execID)
}

func (m *manager) RecycleBgTask(ctx context.Context, gTaskID int64, taskKey string) error {
	log.Info("[tidb-worker] recycle a background task from worker service", zap.Int64("global-task-id", gTaskID))
	metrics.WorkerTaskCounter.WithLabelValues("", metrics.RecycleWorkerTask, taskKey).Inc()
	return m.client.RecycleBgTask(ctx, gTaskID)
}

func (m *manager) RegisterRemoteQuery(ctx context.Context, queryID, queryAddr string) error {
	log.Info("[tidb-worker] register a remote query to worker service", zap.String("query-id", queryID))
	return m.client.RegisterRemoteQuery(ctx, queryID, queryAddr)
}
