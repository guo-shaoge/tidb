// Copyright 2024 PingCAP, Inc.
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

package remotequery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const executorPingInterval = 30 * time.Second

// SQLSession is a session that can execute SQL.
type SQLSession interface {
	Execute(context.Context, string) ([]sqlexec.RecordSet, error) // Execute a sql statement.
	GetSessionVars() *variable.SessionVars
}

// Executor is a remote query executor.
type Executor struct {
	masterAddr string
	done       chan struct{}

	QueryAddr string
	Query     string `json:"query"`
	User      string `json:"user"`
	UserHost  string `json:"userHost"`
	DB        string `json:"db"`
}

// NewExecutor creates a new remote query executor.
func NewExecutor(queryAddr string) (*Executor, error) {
	e := &Executor{
		QueryAddr: queryAddr,
		done:      make(chan struct{}),
	}
	err := e.loadQuery()
	if err != nil {
		return nil, err
	}
	go e.start()
	return e, nil
}

func (e *Executor) loadQuery() error {
	res, err := http.Get(e.QueryAddr)
	if err != nil {
		logutil.BgLogger().Error("failed to load query", zap.Error(err))
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		logutil.BgLogger().Error("failed to load query", zap.String("status", res.Status))
		return errors.New("failed to load query")
	}
	err = json.NewDecoder(res.Body).Decode(e)
	if err != nil {
		logutil.BgLogger().Error("failed to decode query", zap.Error(err))
		return err
	}
	logutil.BgLogger().Info("query loaded", zap.String("queryAddr", e.QueryAddr))
	return nil
}

func (e *Executor) start() {
	ticker := time.NewTicker(executorPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.done:
			return
		case <-ticker.C:
			e.ping()
		}
	}
}

func (e *Executor) ping() {
	res, err := http.Get(e.QueryAddr + "/ping")
	if err != nil {
		logutil.BgLogger().Error("failed to ping server", zap.Error(err))
	}
	res.Body.Close()
}

// Close closes the executor.
func (e *Executor) Close() {
	close(e.done)
}

// Execute executes the query.
func (e *Executor) Execute(ctx context.Context, se SQLSession) error {
	se.GetSessionVars().CurrentDB = e.DB
	se.GetSessionVars().User = &auth.UserIdentity{Username: e.User, Hostname: e.UserHost}
	// TODO: other variables
	rss, err := se.Execute(ctx, e.Query)
	if err != nil {
		logutil.BgLogger().Error("failed to execute query", zap.Error(err))
		return err
	}
	rs := rss[0]
	defer rs.Close()
	firstChunk := true
	var codec *chunk.Codec
	for {
		chk := rs.NewChunk(nil)
		err := rs.Next(ctx, chk)
		if err != nil {
			logutil.BgLogger().Error("failed to fetch result", zap.Error(err))
			return err
		}
		if firstChunk {
			firstChunk = false
			fields := make([]*ColumnField, 0, len(rs.Fields()))
			fieldTypes := make([]*types.FieldType, 0, len(rs.Fields()))
			for _, field := range rs.Fields() {
				fields = append(fields, &ColumnField{
					Column:       field.Column,
					ColumnAsName: field.ColumnAsName,
					Table:        field.Table,
					TableAsName:  field.TableAsName,
					DBName:       field.DBName,
				})
				fieldTypes = append(fieldTypes, &field.Column.FieldType)
			}
			codec = chunk.NewCodec(fieldTypes)
			data, err := json.Marshal(fields)
			if err != nil {
				logutil.BgLogger().Error("failed to marshal meta", zap.Error(err))
				return err
			}
			err = e.postData(data)
			if err != nil {
				logutil.BgLogger().Error("failed to post meta chunk", zap.Error(err))
				return err
			}
		}
		var chunkData []byte
		if chk.NumRows() > 0 {
			chunkData = codec.Encode(chk)
		}
		err = e.postData(chunkData)
		if err != nil {
			logutil.BgLogger().Error("failed to post data chunk", zap.Error(err))
			return err
		}
		if chk.NumRows() == 0 {
			break
		}
	}
	return nil

}

func (e *Executor) postData(data []byte) error {
	res, err := http.Post(e.QueryAddr, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		logutil.BgLogger().Error("failed to post query result", zap.Error(err))
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		logutil.BgLogger().Error("failed to post query result", zap.String("status", res.Status))
		return errors.New("failed to post query result")
	}
	return nil
}
