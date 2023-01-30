// Copyright 2022 PingCAP, Inc.
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

package session

import (
	"context"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	serverlessVersionVar = "serverless_version"
	// serverlessVersion2 added support for Disaggregated TiFlash, as a result, we can now safely enable mpp
	// and two previously forbidden executor push-down.
	serverlessVersion2 = 2
)

// currentServerlessVersion is defined as a variable, so we can modify its value for testing.
// please make sure this is the largest version
var currentServerlessVersion int64 = serverlessVersion2

var bootstrapServerlessVersion = []func(Session, int64){
	upgradeToServerlessVer2,
}

// updateServerlessVersion updates serverless version variable in mysql.TiDB table.
func updateServerlessVersion(s Session) {
	// Update serverless version.
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "Serverless bootstrap version. Do not delete.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, serverlessVersionVar, currentServerlessVersion, currentServerlessVersion,
	)
}

// getServerlessVersion gets serverless version from mysql.tidb table.
func getServerlessVersion(s Session) (int64, error) {
	sVal, isNull, err := getTiDBVar(s, serverlessVersionVar)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if isNull {
		return 0, nil
	}
	return strconv.ParseInt(sVal, 10, 64)
}

// runServerlessUpgrade check and runs upgradeServerless functions on given store if necessary.
func runServerlessUpgrade(store kv.Storage) {
	s, err := createSession(store)
	if err != nil {
		// Bootstrap fail will cause program exit.
		logutil.BgLogger().Fatal("createSession error", zap.Error(err))
	}
	s.sessionVars.EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	s.SetValue(sessionctx.Initing, true)
	upgradeServerless(s)
	s.ClearValue(sessionctx.Initing)
}

// upgradeServerless execute some upgrade work if system is bootstrapped by tidb with lower serverless version.
func upgradeServerless(s Session) {
	ver, err := getServerlessVersion(s)
	terror.MustNil(err)

	if ver >= currentServerlessVersion {
		return
	}

	// Do upgrade works then update bootstrap version.
	for _, upgradeFunc := range bootstrapServerlessVersion {
		upgradeFunc(s, ver)
	}
	updateServerlessVersion(s)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	_, err = s.ExecuteInternal(ctx, "COMMIT")

	if err != nil {
		sleepTime := 1 * time.Second
		logutil.BgLogger().Info("upgrade serverless version failed",
			zap.Error(err), zap.Duration("sleeping time", sleepTime))
		time.Sleep(sleepTime)
		// Check if serverless version is already upgraded.
		v, err1 := getServerlessVersion(s)
		if err1 != nil {
			logutil.BgLogger().Fatal("upgrade serverless version failed", zap.Error(err1))
		}
		if v >= currentServerlessVersion {
			// It is already bootstrapped/upgraded by a TiDB server with higher serverless version.
			return
		}
		logutil.BgLogger().Fatal("[Upgrade] upgrade serverless version failed",
			zap.Int64("from", ver),
			zap.Int64("to", currentServerlessVersion),
			zap.Error(err))
	}
}

func upgradeToServerlessVer2(s Session, ver int64) {
	if ver >= serverlessVersion2 {
		return
	}

	// Enable mpp.
	mustExecute(s, "INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?;",
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBAllowMPPExecution, variable.On, variable.On)

	// Remove lead/lag from pushdown_blacklist.
	mustExecute(s, "DELETE FROM mysql.expr_pushdown_blacklist where name in "+
		"(\"Lead\", \"Lag\") and store_type = \"tiflash\"")
}
