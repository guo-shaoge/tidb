// Copyright 2023 PingCAP, Inc.
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

package tiflash

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	pdclient "github.com/tikv/pd/client"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// RUConsumption record tiflash ru usage.
type RUConsumption struct {
	readRU    *atomic.Float64
	readBytes *atomic.Float64
	cpuTimeMs *atomic.Float64
}

// NewRUConsumption return empty ru consumption.
func NewRUConsumption() *RUConsumption {
	return &RUConsumption{
		readRU:    atomic.NewFloat64(0.0),
		readBytes: atomic.NewFloat64(0.0),
		cpuTimeMs: atomic.NewFloat64(0.0),
	}
}

// GetAndClear return ru info and set it empty.
func (r *RUConsumption) GetAndClear() (readRU float64, readBytes float64, cpuTime float64) {
	readRU = r.readRU.Swap(0.0)
	readBytes = r.readBytes.Swap(0.0)
	cpuTime = r.cpuTimeMs.Swap(0.0)
	return
}

// MergeAndClear merge other into self and clear other.
func (r *RUConsumption) MergeAndClear(other *RUConsumption) {
	if other == nil {
		return
	}
	readRU, readBytes, cpuTime := other.GetAndClear()
	r.readRU.Add(readRU)
	r.readBytes.Add(readBytes)
	r.cpuTimeMs.Add(cpuTime)
}

// MergeExecutionSummary merge execution summaries from selectResponse into ruDetails.
func (r *RUConsumption) MergeExecutionSummary(execSummaries []*tipb.ExecutorExecutionSummary) error {
	for _, summary := range execSummaries {
		if summary != nil && summary.GetRuConsumption() != nil {
			tiflashRU := new(resource_manager.Consumption)
			if err := tiflashRU.Unmarshal(summary.GetRuConsumption()); err != nil {
				return err
			}
			other := &RUConsumption{
				readRU:    atomic.NewFloat64(tiflashRU.RRU),
				readBytes: atomic.NewFloat64(tiflashRU.ReadBytes),
				cpuTimeMs: atomic.NewFloat64(tiflashRU.TotalCpuTimeMs),
			}
			r.MergeAndClear(other)
		}
	}
	return nil
}

// RUReporter starts background goroutine to report ru consumption every n seconds.
type RUReporter struct {
	ctx    context.Context
	cancel context.CancelFunc
	pdCli  pdclient.Client
	rus    []*RUConsumption
	ticker *time.Ticker
	exitCh chan struct{}
}

// NewRUReporter returns new RUReporter.
func NewRUReporter(pdCli pdclient.Client, exitCh chan struct{}) (*RUReporter, error) {
	if pdCli == nil {
		return nil, errors.New("pd client is nil when init tiflash ru reporter")
	}
	ruCap := 128
	rus := make([]*RUConsumption, 0, 128)
	for i := 0; i < ruCap; i++ {
		rus = append(rus, NewRUConsumption())
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &RUReporter{
		ctx:    ctx,
		cancel: cancel,
		pdCli:  pdCli,
		// Report every 5 seconds.
		ticker: time.NewTicker(5 * time.Second),
		exitCh: exitCh,
		rus:    rus,
	}
	go r.run()
	return r, nil
}

// MergeAndClear merge ru into rus[id] and clear ru.
func (r *RUReporter) MergeAndClear(id uint64, ru *RUConsumption) {
	r.rus[id%uint64(len(r.rus))].MergeAndClear(ru)
}

func (r *RUReporter) run() {
	for {
		select {
		case <-r.exitCh:
			r.cancel()
			return
		case <-r.ticker.C:
		}

		var readRU, readBytes, cpu float64

		for _, ru := range r.rus {
			tmpReadRU, tmpReadBytes, tmpCPU := ru.GetAndClear()
			readRU += tmpReadRU
			readBytes += tmpReadBytes
			cpu += tmpCPU
		}

		if readRU == 0.0 && readBytes == 0.0 && cpu == 0.0 {
			continue
		}

		tokenBucketReq := &resource_manager.TokenBucketRequest{
			ConsumptionSinceLastRequest: &resource_manager.Consumption{
				RRU:            readRU,
				ReadBytes:      readBytes,
				TotalCpuTimeMs: cpu,
			},
		}
		req := &resource_manager.TokenBucketsRequest{
			Requests: []*resource_manager.TokenBucketRequest{tokenBucketReq},
		}

		// Only report ru consumption, so no need to take care of resp.
		if _, err := r.pdCli.AcquireTokenBuckets(r.ctx, req); err != nil {
			logutil.BgLogger().Error("report ru consumption failed", zap.Any("err", err))
		}
	}
}
