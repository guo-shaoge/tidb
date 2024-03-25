// Copyright 2024 PingCAP, Inc.
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

	workercli "github.com/tidbcloud/aws-shared-provider/pkg/tidbworker/client"
)

type localClient struct {
}

func (c localClient) RegisterGC(ctx context.Context, ts uint64) error {
	return nil
}

func (c localClient) RecycleGC(ctx context.Context, safePoint uint64) error {
	return nil
}

func (c localClient) RegisterGCV2(ctx context.Context, gcLastRunTime int64, ts uint64) error {
	return nil
}

func (c localClient) RecycleGCV2(ctx context.Context, safePoint uint64) error {
	return nil
}

func (c localClient) RegisterBgTask(ctx context.Context, taskType, taskKey string, gTaskID, subTaskID int64, execID string) error {
	return nil
}

func (c localClient) RecycleBgTask(ctx context.Context, gTaskID int64) error {
	return nil
}

func (c localClient) RegisterRemoteQuery(ctx context.Context, queryID, queryAddr string) error {
	return nil
}

var _ workercli.Client = &localClient{}
