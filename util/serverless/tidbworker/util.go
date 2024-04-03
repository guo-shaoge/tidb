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
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	workerIDPrefix = "tidb-worker-"
)

// IsMaster returns whether the current TiDB is of role master.
func IsMaster() bool {
	return GlobalTiDBWorkerManager != nil && GlobalTiDBWorkerManager.Role() == config.RoleMaster
}

// IsBgTaskMaster returns whether the current TiDB is of role bg job master.
func IsBgTaskMaster(taskType string) bool {
	// Check if the current TiDB has worker enabled.
	if GlobalTiDBWorkerManager == nil {
		return false
	}
	enabled, _, err := loadBgTaskConfig(TaskWorkerType(taskType))
	if err != nil {
		logutil.BgLogger().Warn("[tidb-worker] failed to load worker config", zap.Error(err))
		return false
	}
	return enabled
}

// IsGCWorker returns whether the current TiDB is a GC worker.
func IsGCWorker() bool {
	return GlobalTiDBWorkerManager != nil && GlobalTiDBWorkerManager.Role() == config.RoleGCWorker
}

// IsGCV2Worker returns whether the current TiDB is a GCV2 worker.
func IsGCV2Worker() bool {
	return GlobalTiDBWorkerManager != nil && GlobalTiDBWorkerManager.Role() == config.RoleGCV2Worker
}

// IsTTLTaskWorker returns whether the current TiDB is a GCV2 worker.
func IsTTLTaskWorker() bool {
	return GlobalTiDBWorkerManager != nil && GlobalTiDBWorkerManager.Role() == config.RoleTTLTaskWorker
}

// IsUseTiDBWorker returns whether the current TiDB use TiDB worker.
func IsUseTiDBWorker() bool {
	return GlobalTiDBWorkerManager != nil
}

// SchedulerNodes generate scheduler nodes according to tidb worker config instead of current
// cluster topology.
func SchedulerNodes(workerType string, gTaskID int64) []*infosync.ServerInfo {
	enabled, nodeCount, err := loadBgTaskConfig(workerType)
	if err != nil {
		logutil.BgLogger().Warn("[tidb-worker] failed to load worker config", zap.Error(err))
		return nil
	}
	if !enabled || nodeCount == 0 {
		return nil
	}
	nodeIP := workerIDPrefix + workerType
	// If not in local mode, append the global task ID to the node IP.
	if !config.GetGlobalConfig().TiDBWorker.LocalMode.Enable || !config.GetGlobalConfig().TiDBWorker.LocalMode.StaticExecID {
		nodeIP += "-" + strconv.FormatInt(gTaskID, 10)
	}
	nodes := make([]*infosync.ServerInfo, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodes[i] = &infosync.ServerInfo{
			IP:   nodeIP,
			Port: uint(i),
		}
	}
	return nodes
}

// IsWorkerExecID checks whether the execID belongs to a tidb worker.
func IsWorkerExecID(execID, workerType string) bool {
	prefix := workerIDPrefix + workerType + "-"
	return len(execID) > len(prefix) && execID[:len(prefix)] == prefix
}

func loadBgTaskConfig(workerType string) (enabled bool, workerCount int, err error) {
	if config.GetGlobalConfig().TiDBWorker.LocalMode.Enable {
		cfg, ok := config.GetGlobalConfig().TiDBWorker.LocalMode.BgTaskConfig[workerType]
		if !ok {
			return false, 0, nil
		}
		return !cfg.Paused, cfg.WorkerCount, nil
	}

	addr, err := url.JoinPath(config.GetGlobalConfig().TiDBWorker.APIServerAddr, "scaler/api/v1/bgtask", workerType+"-worker")
	if err != nil {
		return false, 0, err
	}
	res, err := http.Get(addr)
	if err != nil {
		return false, 0, err
	}
	defer res.Body.Close()

	var cfg config.BgTaskConfig
	err = json.NewDecoder(res.Body).Decode(&cfg)
	if err != nil {
		return false, 0, err
	}
	return !cfg.Paused, cfg.WorkerCount, nil
}
