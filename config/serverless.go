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

package config

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/pingcap/tidb/util/logutil"
)

const (
	// EnvVarExecID is the overwritten execID of the ddl worker.
	EnvVarExecID = "EXEC_ID"
)

// BootstrapControl contains ratelimit configuration options.
type BootstrapControl struct {
	SkipServerlessVariables bool `toml:"skip-serverless-variables" json:"skip-serverless-variables"`
	SkipRootPriv            bool `toml:"skip-root-priv" json:"skip-root-priv"`
	SkipCloudAdminPriv      bool `toml:"skip-cloud-admin-priv" json:"skip-cloud-admin-priv"`
	SkipRoleAdminPriv       bool `toml:"skip-role-admin-priv" json:"skip-role-admin-priv"`
	SkipPushdownBlacklist   bool `toml:"skip-pushdown-blacklist" json:"skip-pushdown-blacklist"`
}

// defaultBootstrapControl creates a new BootstrapControl.
func defaultBootstrapControl() BootstrapControl {
	return BootstrapControl{
		SkipServerlessVariables: false,
		SkipRootPriv:            false,
		SkipCloudAdminPriv:      false,
		SkipRoleAdminPriv:       false,
		SkipPushdownBlacklist:   false,
	}
}

// DefaultResourceGroup is the default resource group name for all txns and snapshots.
var DefaultResourceGroup string

// TiDBWorker is the config for TiDB worker.
type TiDBWorker struct {
	// Enable indicates whether to start the TiDB worker manager.
	Enable bool `toml:"enable" json:"enable"`
	// Role indicates the role of the TiDB worker.
	Role string `toml:"role" json:"role"`
	// TidbPool specifies pool of the tidb worker.
	TidbPool string `toml:"tidb-pool" json:"tidb-pool"`
	// RegistryAddr specifies the address of the TiDB worker service.
	RegistryAddr string `toml:"registry-addr" json:"registry-addr"`
	// APIServerAddr specifies the address of the TiDB worker API server.
	APIServerAddr string `toml:"api-server" json:"api-server"`
	// ExecID specifies execID when TiDB is running as ddl worker.
	ExecID string `toml:"exec-id" json:"exec-id"`

	// LocalMode allows tidb to skip the tidb-worker registry and return a local bg task config.
	LocalMode LocalMode `toml:"local-mode" json:"local-mode"`
}

// LocalMode is the config for local mode.
type LocalMode struct {
	// Enable indicates whether to start the TiDB worker manager in local mode.
	Enable bool `toml:"enable" json:"enable"`
	// BgTaskConfig specifies the response of the TiDB worker API server in local mode.
	BgTaskConfig map[string]BgTaskConfig `toml:"bg-task-config" json:"bg-task-config"`
	// StaticExecID fixes the execID of the task generated in local mode.
	StaticExecID bool `toml:"static-exec-id" json:"static-exec-id"`
}

// BgTaskConfig is the config for background task.
type BgTaskConfig struct {
	// Paused indicates whether the specific background task is paused.
	Paused bool `toml:"paused" json:"paused"`
	// WorkerCount indicates the number of workers for the specific background task.
	WorkerCount int `toml:"worker-count" json:"worker-count"`
}

const (
	// RoleMaster is the role for user tidb.
	RoleMaster = "master"
	// RoleGCWorker is the role for GC worker.
	RoleGCWorker = "gc"
	// RoleGCV2Worker is the role for GCV2 worker.
	RoleGCV2Worker = "gcv2"
	// RoleDDLWorker is the role for DDL worker.
	RoleDDLWorker = "ddl"
	// RoleBatchWorker is the role for batch worker.
	RoleBatchWorker = "batch"
	// RoleRemoteQueryWorker is the role for remote query worker.
	RoleRemoteQueryWorker = "remote-query"
	// RoleTTLTaskWorker is the role for batch worker.
	RoleTTLTaskWorker = "ttl"
)

// defaultTiDBWorker creates a new TiDBWorker.
func defaultTiDBWorker() TiDBWorker {
	return TiDBWorker{
		Enable:        false,
		Role:          RoleMaster,
		TidbPool:      "tidb-pool",
		RegistryAddr:  "root:@tcp(serverless-cluster-tidb.tidb-serverless.svc:4000)/serverless",
		APIServerAddr: "http://scaler-svc.tidb-worker:9080",
		LocalMode:     defaultLocalMode(),
	}
}

// defaultLocalMode creates a new LocalMode.
func defaultLocalMode() LocalMode {
	return LocalMode{
		Enable:       false,
		BgTaskConfig: make(map[string]BgTaskConfig),
		StaticExecID: false,
	}
}

func tryConnect(dsn string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		return err
	}
	return nil
}

// Valid validates the TiDBWorker config.
func (w *TiDBWorker) Valid(c *Config) error {
	// Skip validation if TiDB worker is disabled.
	if !w.Enable {
		return nil
	}
	w.Role = strings.ToLower(w.Role)
	switch w.Role {
	case RoleMaster:
		if !c.EnableSafePointV2 {
			// When running as master without enabling SafePointV2, need to disable GC drop table.
			c.SkipGCWorker = true
		}
		if !w.LocalMode.Enable && tryConnect(w.RegistryAddr) != nil {
			logutil.BgLogger().Error("failed to connect to tidb worker service, fallback to standalone mode")
			w.Enable = false
			return nil
		}
	case RoleGCWorker:
	case RoleGCV2Worker:
		// GCV2 worker must have SafePointV2 enabled.
		if !c.EnableSafePointV2 {
			return fmt.Errorf("must enable safe point V2 to run as GCV2 worker")
		}
	case RoleDDLWorker, RoleBatchWorker:
		// Skip running GC worker on DDL worker.
		c.SkipGCWorker = true
		// Overwrite execID if its set in env.
		if execID := os.Getenv(EnvVarExecID); execID != "" {
			w.ExecID = execID
		}
	case RoleRemoteQueryWorker:
		// Overwrite execID if its set in env.
		if execID := os.Getenv(EnvVarExecID); execID != "" {
			w.ExecID = execID
		}
	case RoleTTLTaskWorker:
	default:
		return fmt.Errorf("invalid tidb worker role %s", w.Role)
	}
	return nil
}

const (
	// LogFormatText is stardard log format surrounded with []
	LogFormatText = "TEXT"
	// LogFormatJSON is json format
	LogFormatJSON = "JSON"
)

// AuditLog is the config for serverless audit log
type AuditLog struct {
	// Enable indicates whether audit log will be recorded
	Enable bool `toml:"enable" json:"enable"`
	// Path is the audit log output path
	Path string `toml:"path" json:"path"`
	// Format is the output format of audit log, both text and json are supported
	Format string `toml:"format" json:"format"`
	// MaxFilesize is the maximum file size before the log file be rotated, unit is MB
	MaxFilesize int64 `toml:"max-filesize" json:"max-filesize"`
	// MaxLifetime is the maximum time before the log file be rotated, unit is second
	MaxLifetime int64 `toml:"max-lifetime" json:"max-lifetime"`
	// ReservedBackups is the number of rotated log files to reserve.
	ReservedBackups int `toml:"reserved-backups" json:"reserved-backups"`
	// ReservedDays is the day to reseve rotated log files.
	ReservedDays int `toml:"reserved-days" json:"reserved-days"`
	// Redacted indicates whether audit log redaction is enabled. If it set to true, user data will be replaced with `?`
	Redacted bool `toml:"redacted" json:"redacted"`
	// EncryptKey is used to encrypt sensitive informations in audit log. This key should be 32 bytes, we use AES-256 for encryption
	EncryptKey string `toml:"encrypt-key" json:"encrypt-key"`
}

func defaultAuditLog() AuditLog {
	return AuditLog{
		Enable:          false,
		Path:            "tidb-audit.log",
		Format:          LogFormatText,
		MaxFilesize:     1,
		MaxLifetime:     24 * 60 * 60,
		ReservedBackups: 1,
		ReservedDays:    0,
		Redacted:        true,
		EncryptKey:      "",
	}
}
