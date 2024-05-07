// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package standby

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/keyspace"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/signal"
	"go.uber.org/zap"
)

const (
	standbyState   = "standby"
	activatedState = "activated"

	connNormalClosed         = "normal closed"
	tidbNormalRestartLogPath = "/tmp/tidb-normal-restart.log"
)

// ActivateRequest is the request body for activating the tidb server.
type ActivateRequest struct {
	KeyspaceName   string          `json:"keyspace_name"`
	AuditLog       *AuditLogConfig `json:"audit_log,omitempty"`
	ExportID       string          `json:"export_id"`
	MaxIdleSeconds uint            `json:"max_idle_seconds"`
}

func (r *ActivateRequest) auditLogEnabled() bool {
	if r.AuditLog == nil {
		return false
	}
	return r.AuditLog.Enable
}

// AuditLogConfig is the configuration about audit log when activating the tidb server.
type AuditLogConfig struct {
	Enable     bool   `json:"enable"`
	EncryptKey string `json:"encrypt_key"`
}

type sessionManager interface {
	ConnectionCount() int
	GetUserProcessList() map[uint64]*util.ProcessInfo
	GetClientCapabilityList() map[uint64]uint32
	GetNormalClosedConn(keyspaceName, connID string) string
	ForceShutdown()
}

var (
	mu              sync.RWMutex
	state           = standbyState
	activateRequest ActivateRequest

	// activationTimeout specifies the maximum allowed time for tidb to activate from standby mode.
	activationTimeout uint

	preTidbNormalRestartKeyspaceName, preTidbNormalRestartMsg string
)

var activateCh = make(chan struct{}, 1)

// KeyspaceMismatch is the response body when the keyspace name in http request
// does not match the local keyspace name.
type KeyspaceMismatch struct {
	Remote string `json:"remote"`
	Local  string `json:"local"`
}

func keyspaceChecker(next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		remote := r.URL.Query().Get("keyspace")
		local := config.GetGlobalKeyspaceName()
		if remote != local {
			w.WriteHeader(http.StatusPreconditionFailed)
			mismatch := KeyspaceMismatch{
				Remote: remote,
				Local:  local,
			}
			body, err := json.Marshal(mismatch)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Write(body)
			return
		}
		next.ServeHTTP(w, r)
	}
}

func loadTiDBNormalRestartInfoAndRemove() {
	data, err := os.ReadFile(tidbNormalRestartLogPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logutil.BgLogger().Error("failed to read tidb normal restart log file", zap.Error(err))
		}
		return
	}

	parts := strings.SplitN(string(data), ":", 2)
	if len(parts) < 2 {
		logutil.BgLogger().Error("invalid tidb normal restart log file")
		return
	}

	preTidbNormalRestartKeyspaceName = parts[0]
	preTidbNormalRestartMsg = parts[1]
	logutil.BgLogger().Info("load tidb normal restart log file",
		zap.String("preTidbNormalRestartKeyspaceName", preTidbNormalRestartKeyspaceName),
		zap.String("preTidbNormalRestartMsg", preTidbNormalRestartMsg))

	if err := os.Remove(tidbNormalRestartLogPath); err != nil {
		logutil.BgLogger().Error("failed to remove tidb normal restart log file", zap.Error(err))
	}
}

// SaveTidbNormalRestartInfo saves tidb normal restart info to file.
func SaveTidbNormalRestartInfo(msg string) {
	keyspaceName := keyspace.GetKeyspaceNameBySettings()
	if keyspaceName == "" {
		return
	}

	if err := os.WriteFile(tidbNormalRestartLogPath, []byte(keyspaceName+":"+msg), 0644); err != nil {
		logutil.BgLogger().Error("failed to write tidb normal restart log file", zap.Error(err))
	}
}

// IsPreTidbNormalRestart returns whether tidb is restarted normally before.
func IsPreTidbNormalRestart(keyspaceName string) (bool, string) {
	if keyspaceName == "" || preTidbNormalRestartKeyspaceName != keyspaceName {
		return false, ""
	}

	return true, preTidbNormalRestartMsg
}

// Handler returns a handler to query tidb pool status or activate or exit the tidb server.
func Handler(sm sessionManager) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/tidb-pool/status", statusHandler)
	mux.HandleFunc("/tidb-pool/activate", func(w http.ResponseWriter, r *http.Request) {
		var req ActivateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.KeyspaceName == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.AuditLog != nil && req.AuditLog.Enable {
			if len(req.AuditLog.EncryptKey) != 32 {
				logutil.BgLogger().Error("bad audit log encrypt key", zap.String("encrypt_key", req.AuditLog.EncryptKey))
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			logutil.BgLogger().Info("activate with audit log enabled", zap.String("keyspaceName", req.KeyspaceName))
		}

		mu.Lock()
		if state == standbyState {
			state = activatedState
			activateRequest = req
			activateCh <- struct{}{}
		} else if activateRequest.KeyspaceName != req.KeyspaceName {
			mu.Unlock()
			w.WriteHeader(http.StatusPreconditionFailed)
			w.Write([]byte("server is not in standby mode"))
			return
		} else if activateRequest.auditLogEnabled() != req.auditLogEnabled() {
			mu.Unlock()
			w.WriteHeader(http.StatusPreconditionFailed)
			w.Write([]byte("server audit log status has changed"))
			return
		}
		// if client tries to activate with same keyspace name, wait for ready signal and return 200.
		mu.Unlock()

		timeout := make(<-chan time.Time)
		if activationTimeout > 0 {
			timeout = time.After(time.Duration(activationTimeout) * time.Second)
		}

		select {
		case <-r.Context().Done(): // client closed connection.
			go func() {
				EndStandby(errors.New("client closed connection"))
				signal.TiDBExit()
			}()
		case <-timeout: // reach hardlimit timeout from config.
			logutil.BgLogger().Warn("timeout waiting for activation")
			w.WriteHeader(http.StatusRequestTimeout)
			w.Write([]byte("timeout waiting for activation"))
			go func() {
				EndStandby(errors.New("timeout waiting for activation"))
				signal.TiDBExit()
			}()
		case <-serverStartCh:
			if startServerErr != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(startServerErr.Error()))
				return
			}
			statusHandler(w, r)
		}
	})
	mux.HandleFunc("/tidb-pool/exit", keyspaceChecker(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logutil.BgLogger().Info("receiving exit request")
		if sm != nil {
			sm.ForceShutdown()
			SaveTidbNormalRestartInfo("received exit request")
		}
		w.WriteHeader(http.StatusOK)
		signal.TiDBExit()
	})))
	mux.HandleFunc("/tidb-pool/checkconn", func(w http.ResponseWriter, r *http.Request) {
		keyspaceName, connID := r.URL.Query().Get("keyspace_name"), r.URL.Query().Get("conn_id")
		if keyspaceName == "" || connID == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("keyspace_name or conn_id is empty"))
			return
		}
		logger := logutil.BgLogger().With(zap.String("keyspace_name", keyspaceName), zap.String("conn_id", connID))
		logger.Info("check connection")
		if sm != nil {
			if msg := sm.GetNormalClosedConn(keyspaceName, connID); msg != "" {
				logger.Info("connection is normal closed", zap.String("msg", msg))
				w.Write([]byte(connNormalClosed))
				return
			}
		}
		if ok, msg := IsPreTidbNormalRestart(keyspaceName); ok {
			logger.Info("connection is normal closed", zap.String("msg", msg))
			w.Write([]byte(connNormalClosed))
			return
		}
		logger.Info("connection is unconfirmed")
		w.Write([]byte(`unconfirmed`))
	})
	return mux
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	mu.RLock()
	defer mu.RUnlock()
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	if activateRequest.ExportID != "" {
		fmt.Fprintf(w, `{"state": "%s", "keyspace_name": "%s","export_id": "%s"}`,
			state, activateRequest.KeyspaceName, activateRequest.ExportID)
	} else {
		fmt.Fprintf(w, `{"state": "%s", "keyspace_name": "%s"}`, state, activateRequest.KeyspaceName)
	}
}

var server *http.Server

// StartStandby starts a http server to listen and wait for activation signal.
func StartStandby(host string, port uint, timeout uint) ActivateRequest {
	mux := Handler(nil)
	// handle liveness probe.
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	// handle health
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"status":"standby"}`)) })
	server = &http.Server{
		Handler: mux,
	}
	activationTimeout = timeout
	loadTiDBNormalRestartInfoAndRemove()
	logutil.BgLogger().Info("tidb-server is now running as standby, waiting for activation...", zap.String("addr", server.Addr))
	go func() {
		addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
		l, err := net.Listen("tcp", addr)
		if err != nil {
			logutil.BgLogger().Warn("failed to listen", zap.Error(err))
			os.Exit(1)
		}
		clusterSecurity := config.GetGlobalConfig().Security.ClusterSecurity()
		tlsConfig, err := clusterSecurity.ToTLSConfig()
		if err != nil {
			logutil.BgLogger().Warn("failed to get tls config", zap.Error(err))
			os.Exit(1)
		}
		if tlsConfig != nil {
			l = tls.NewListener(l, tlsConfig)
		}
		if err := server.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logutil.BgLogger().Warn("failed to start tidb-server as standby", zap.Error(err))
			os.Exit(1)
		}
	}()

	<-activateCh

	mu.RLock()
	defer mu.RUnlock()
	return activateRequest
}

var (
	serverStartCh  = make(chan struct{})
	startServerErr error
	endOnce        sync.Once
)

// EndStandby is used to notify the temp http server that the tidb server is ready or failed to init.
func EndStandby(err error) {
	endOnce.Do(func() {
		startServerErr = err
		close(serverStartCh)
		if server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			server.Shutdown(ctx)
		}
	})
}
