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
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// RecordSet is a record set proxy of remote executor.
type RecordSet struct {
	start          time.Time
	chunkInitCap   int
	chunkMaxSize   int
	fieldsRecieved chan struct{}
	fields         []*ast.ResultField
	fieldTypes     []*types.FieldType
	codec          *chunk.Codec
	recieveCh      chan []byte
	closed         atomic.Bool
	quit           chan struct{}
}

// NewRecordSet creates a new RecordSet.
func NewRecordSet(chunkInitCap, chunkMaxSize int) *RecordSet {
	rs := &RecordSet{
		start:          time.Now(),
		chunkInitCap:   chunkInitCap,
		chunkMaxSize:   chunkMaxSize,
		fieldsRecieved: make(chan struct{}),
		recieveCh:      make(chan []byte),
		quit:           make(chan struct{}),
	}
	go rs.recvFieldsMeta()
	return rs
}

func (rs *RecordSet) waitForMeta() bool {
	start := time.Now()
	select {
	case <-rs.quit:
		logutil.BgLogger().Error("timeout waiting for meta data")
		metrics.RemoteQueryRecordSetCounter.WithLabelValues("wait_meta", "timeout").Inc()
		return false
	case <-rs.fieldsRecieved:
		metrics.RemoteQueryRecordSetDuration.WithLabelValues("wait_meta").Observe(time.Since(start).Seconds())
		return true
	}
}

// Fields implements the sqlexec.RecordSet Fields interface.
func (rs *RecordSet) Fields() []*ast.ResultField {
	if rs.waitForMeta() {
		return rs.fields
	}
	return nil
}

// NewChunk implements the sqlexec.RecordSet NewChunk interface.
func (rs *RecordSet) NewChunk(allocator chunk.Allocator) *chunk.Chunk {
	if rs.waitForMeta() {
		return allocator.Alloc(rs.fieldTypes, rs.chunkInitCap, rs.chunkMaxSize)
	}
	return nil
}

func (rs *RecordSet) recvFieldsMeta() {
	select {
	case <-rs.quit:
	case data, ok := <-rs.recieveCh:
		if !ok {
			logutil.BgLogger().Error("failed to recieve fields meta, recieve channel is closed")
			metrics.RemoteQueryRecordSetCounter.WithLabelValues("recv_meta", "failed").Inc()
			return
		}
		var fields []*ColumnField
		err := json.Unmarshal(data, &fields)
		if err != nil {
			logutil.BgLogger().Error("failed to unmarshal fields meta", zap.Error(err))
			metrics.RemoteQueryRecordSetCounter.WithLabelValues("recv_meta", "failed").Inc()
			return
		}
		rs.fields = make([]*ast.ResultField, 0, len(fields))
		rs.fieldTypes = make([]*types.FieldType, 0, len(fields))
		for _, field := range fields {
			rs.fields = append(rs.fields, &ast.ResultField{
				Column:       field.Column,
				ColumnAsName: field.ColumnAsName,
				Table:        field.Table,
				TableAsName:  field.TableAsName,
				DBName:       field.DBName,
			})
			rs.fieldTypes = append(rs.fieldTypes, &field.Column.FieldType)
		}
		rs.codec = chunk.NewCodec(rs.fieldTypes)
		close(rs.fieldsRecieved)
		metrics.RemoteQueryRecordSetCounter.WithLabelValues("recv_meta", "ok").Inc()
		metrics.RemoteQueryRecordSetDuration.WithLabelValues("recv_meta").Observe(time.Since(rs.start).Seconds())
	}
}

// Next implements the sqlexec.RecordSet Next interface.
func (rs *RecordSet) Next(ctx context.Context, req *chunk.Chunk) error {
	if !rs.waitForMeta() {
		return errors.New("failed to get fields meta")
	}

	startWaitChunk := time.Now()

	if req != nil {
		req.Reset()
	}
	select {
	case <-rs.quit:
		return errors.New("record set is closed")
	case data, ok := <-rs.recieveCh:
		if !ok {
			logutil.BgLogger().Error("no more chunks to recieve")
			metrics.RemoteQueryRecordSetCounter.WithLabelValues("next_chunk", "failed").Inc()
			return errors.New("no more chunks")
		}
		if len(data) > 0 {
			remains := rs.codec.DecodeToChunk(data, req)
			if len(remains) > 0 {
				logutil.BgLogger().Error("failed to decode chunk from recieved data")
				metrics.RemoteQueryRecordSetCounter.WithLabelValues("next_chunk", "failed").Inc()
				return errors.New("remains data after decode")
			}
		}
		metrics.RemoteQueryRecordSetCounter.WithLabelValues("next_chunk", "ok").Inc()
		metrics.RemoteQueryRecordSetDuration.WithLabelValues("wait_chunk").Observe(time.Since(startWaitChunk).Seconds())
		return nil
	}
}

// Close implements the sqlexec.RecordSet Close interface.
func (rs *RecordSet) Close() error {
	if rs.closed.CompareAndSwap(false, true) {
		close(rs.quit)
		metrics.RemoteQueryRecordSetDuration.WithLabelValues("close").Observe(time.Since(rs.start).Seconds())
	}
	return nil
}

// RecvData recieves data from remote executor.
func (rs *RecordSet) RecvData(data []byte) {
	start := time.Now()
	select {
	case <-rs.quit:
	case rs.recieveCh <- data:
		metrics.RemoteQueryRecordSetDuration.WithLabelValues("recv_chunk").Observe(time.Since(start).Seconds())
	}
}

// ColumnField is the result field of a query.
type ColumnField struct {
	Column       *model.ColumnInfo `json:"column"`
	ColumnAsName model.CIStr       `json:"columnAsName"`
	Table        *model.TableInfo  `json:"table"`
	TableAsName  model.CIStr       `json:"tableAsName"`
	DBName       model.CIStr       `json:"dbName"`
}
