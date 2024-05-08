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

package remote

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/keyspace"
	"github.com/pingcap/tidb/store/pdtypes"
	"github.com/tikv/client-go/v2/oracle"
	tikvclient "github.com/tikv/client-go/v2/tikv"
	rm "github.com/tikv/pd/client/resource_group/controller"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

/*
	Remote load data worker API:

	1. init task:
		POST /load_data?cluster_id=%d&task_id=%s&start_ts=%d&commit_ts=%d

	2. put chunk:
		PUT /load_data?cluster_id=%d&task_id=%s&writer_id=%d&chunk_id=%d

		key_len(2) + key(key_len) + val_len(4) + value(val_len)
		key_len(2) + key(key_len) + val_len(4) + value(val_len)
		...

	3. flush:
		POST /load_data?cluster_id=%d&task_id=%s&flush=true

	4. build task:
		POST /load_data?cluster_id=%d&task_id=%s&build=true&compression=zstd&split_size=%d&split_keys=%d

	5. get task states:
		GET /load_data?cluster_id=%d&task_id=%s

		{"canceled": false, "finished": false, "error": "", "created-files": 10, "ingested-regions": 3}

	6. clean up task:
		DELETE /load_data?cluster_id=%d&task_id=%s
*/

const (
	maxDuplicateBatchSize = 4 << 20
	taskExitsMsg          = "task exists"
	sleepDuration         = 1 * time.Second
	retryCount            = 60
)

// LoadDataStates is json data that returned by remote server GET API.
type LoadDataStates struct {
	TaskID          string `json:"task-id"`
	Canceled        bool   `json:"canceled"`
	Finished        bool   `json:"finished"`
	Error           string `json:"error"`
	CreatedFiles    int    `json:"created-files"`
	IngestedRegions int    `json:"ingested-regions"`

	DuplicateEntries []duplicateEntry `json:"duplicated-entries"`
}

// PutChunkResult is json data that returned by remote server PUT API.
type PutChunkResult struct {
	FlushedChunkID uint64 `json:"flushed-chunk-id"`
	HandledChunkID uint64 `json:"handled-chunk-id"`
	Canceled       bool   `json:"canceled"`
	Finished       bool   `json:"finished"`
	Error          string `json:"error"`
}

// FlushResult is json data that returned by remote server POST API.
type FlushResult struct {
	FlushedChunkIDs map[uint64]uint64 `json:"flushed-chunk-ids"`
	Canceled        bool              `json:"canceled"`
	Finished        bool              `json:"finished"`
	Error           string            `json:"error"`
}

type writerState struct {
	FlushedChunkID uint64 `json:"flushed-chunk-id"`
	HandledChunkID uint64 `json:"handled-chunk-id"`
}

type duplicateEntry struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

func (s *LoadDataStates) hasDuplicateEntries() bool {
	return len(s.DuplicateEntries) > 0
}

// NewRemoteBackend creates a new remote backend instance.
func NewRemoteBackend(
	ctx context.Context,
	tls *common.TLS,
	cfg *config.Config,
	keyspaceName string,
) (backend.Backend, error) {
	localFile := cfg.TikvImporter.SortedKVDir

	shouldCreate := true
	if cfg.Checkpoint.Enable {
		if info, err := os.Stat(localFile); err != nil {
			if !os.IsNotExist(err) {
				return nil, err
			}
		} else if info.IsDir() {
			shouldCreate = false
		}
	}
	if shouldCreate {
		err := os.Mkdir(localFile, 0o700)
		if err != nil {
			return nil, common.ErrInvalidSortedKVDir.Wrap(err).GenWithStackByArgs(localFile)
		}
	}
	var (
		duplicateDB *pebble.DB
		err         error
	)
	keyAdapter := local.KeyAdapter(local.NoopKeyAdapter{})
	duplicateDetection := cfg.TikvImporter.DuplicateResolution != config.DupeResAlgNone
	if duplicateDetection {
		duplicateDB, err = local.OpenDuplicateDB(localFile)
		if err != nil {
			return nil, common.ErrOpenDuplicateDB.Wrap(err).GenWithStackByArgs()
		}
		keyAdapter = local.DupDetectKeyAdapter{}
	}
	reportErrOnDup := cfg.TikvImporter.DuplicateResolution == config.DupeResAlgErr

	pdCtl, err := pdutil.NewPdController(ctx, keyspaceName, cfg.TiDB.PdAddr, tls.TLSConfig(), tls.ToPDSecurityOption())
	if err != nil {
		return nil, common.NormalizeOrWrapErr(common.ErrCreatePDClient, err)
	}

	var pdCliForTiKV *tikvclient.CodecPDClient
	if keyspaceName == "" {
		pdCliForTiKV = tikvclient.NewCodecPDClient(tikvclient.ModeTxn, pdCtl.GetPDClient())
	} else {
		pdCliForTiKV, err = tikvclient.NewCodecPDClientWithKeyspace(tikvclient.ModeTxn, pdCtl.GetPDClient(), keyspaceName)
		if err != nil {
			return nil, common.ErrCreatePDClient.Wrap(err).GenWithStackByArgs()
		}
	}

	tikvCodec := pdCliForTiKV.GetCodec()
	spkv, err := keyspace.NewEtcdSafePointKV(strings.Split(cfg.TiDB.PdAddr, ","), tikvCodec, tls.TLSConfig())
	if err != nil {
		return nil, common.ErrCreateKVClient.Wrap(err).GenWithStackByArgs()
	}
	rpcCli := tikvclient.NewRPCClient(tikvclient.WithSecurity(tls.ToTiKVSecurityConfig()), tikvclient.WithCodec(tikvCodec))
	tikvCli, err := tikvclient.NewKVStore("lightning-remote-backend", pdCliForTiKV, spkv, rpcCli)
	if err != nil {
		return nil, common.ErrCreateKVClient.Wrap(err).GenWithStackByArgs()
	}

	httpClient := http.DefaultClient
	if tlsConfig := tls.TLSConfig(); tlsConfig != nil {
		httpClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		}
	}

	ks := pdCliForTiKV.GetCodec().GetKeyspace()
	remote := &Backend{
		workerAddr:       cfg.TikvImporter.Addr,
		pdCtl:            pdCtl,
		tls:              tls,
		pdAddr:           cfg.TiDB.PdAddr,
		keyspace:         ks,
		logger:           log.FromContext(ctx),
		reportWriteBytes: func(int64) {},
		httpClient:       httpClient,

		tikvCodec:          tikvCodec,
		tikvCli:            tikvCli,
		duplicateDB:        duplicateDB,
		keyAdapter:         keyAdapter,
		dupeConcurrency:    cfg.TikvImporter.RangeConcurrency * 2,
		duplicateDetection: duplicateDetection,
		reportErrOnDup:     reportErrOnDup,
		checkpointEnabled:  cfg.Checkpoint.Enable,
		localStoreDir:      localFile,
	}

	if m, ok := metric.FromContext(ctx); ok {
		remote.metrics = m
	}

	if cfg.RUConfig.ReportWRU && remote.metrics != nil {
		keyspaceID := uint32(pdCliForTiKV.GetCodec().GetKeyspaceID())
		err = remote.setupReportWRU(ctx, keyspaceID, keyspaceName, cfg)
		if err != nil {
			remote.logger.Warn("failed to setup report wru", zap.Error(err))
			return nil, err
		}
	}

	return remote, nil
}

// Backend is a remote backend that sends KV pairs to remote worker.
type Backend struct {
	workerAddr       string
	pdCtl            *pdutil.PdController
	tls              *common.TLS
	pdAddr           string
	keyspace         []byte
	metrics          *metric.Metrics
	engines          sync.Map
	logger           log.Logger
	reportWriteBytes func(int64)
	httpClient       *http.Client

	tikvCodec          tikvclient.Codec
	duplicateDB        *pebble.DB
	tikvCli            *tikvclient.KVStore
	keyAdapter         local.KeyAdapter
	dupeConcurrency    int
	duplicateDetection bool
	reportErrOnDup     bool
	checkpointEnabled  bool
	localStoreDir      string
}

// Close the connection to the backend.
func (b *Backend) Close() {
	if b.duplicateDB != nil {
		local.CloseDuplicateDB(b.duplicateDB, b.checkpointEnabled, b.localStoreDir, b.logger)
		b.duplicateDB = nil
	}

	// if checkpoint is disable or we finish load all data successfully, then files in this
	// dir will be useless, so we clean up this dir and all files in it.
	if !b.checkpointEnabled || common.IsEmptyDir(b.localStoreDir) {
		err := os.RemoveAll(b.localStoreDir)
		if err != nil {
			b.logger.Warn("remove local db file failed", zap.Error(err))
		}
	}
	_ = b.tikvCli.Close()
	b.pdCtl.Close()
}

// RetryImportDelay returns the duration to sleep when retrying an import
func (b *Backend) RetryImportDelay() time.Duration {
	return 0
}

// ShouldPostProcess returns whether KV-specific post-processing should be
// performed for this backend. Post-processing includes checksum and analyze.
func (b *Backend) ShouldPostProcess() bool {
	return true
}

// OpenEngine opens an engine for writing.
func (b *Backend) OpenEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	physical, logical, err := b.pdCtl.GetPDClient().GetTS(ctx)
	if err != nil {
		return err
	}
	ts := oracle.ComposeTS(physical, logical)

	loadDataTaskID := genLoadDataTaskID(cfg)
	e, _ := b.engines.LoadOrStore(engineUUID, &engine{
		ctx:            ctx,
		logger:         log.With(zap.String("loadDataTaskID", loadDataTaskID)),
		loadDataTaskID: loadDataTaskID,
		ts:             ts,
		tbl:            cfg.TableInfo,
		addr:           b.workerAddr,
		clusterID:      b.pdCtl.GetPDClient().GetClusterID(ctx),
		writers:        map[uint64]*client{},
		httpClient:     b.httpClient,
		backend:        b,
	})
	engine := e.(*engine)
	if engine.loadDataTaskID == loadDataTaskID {
		// newly created engine.
		err := b.loadDataInit(ctx, engine, cfg.EstimatedDataSize)
		if err != nil {
			b.engines.Delete(engineUUID)
			return err
		}
	}
	return nil
}

func (b *Backend) loadDataInit(ctx context.Context, engine *engine, dataSize int64) error {
	for {
		url := fmt.Sprintf(
			"%s/load_data?cluster_id=%d&task_id=%s&start_ts=%d&commit_ts=%d&data_size=%d",
			engine.addr,
			engine.clusterID,
			engine.loadDataTaskID,
			engine.ts,
			engine.ts+1,
			dataSize,
		)
		client := *b.httpClient
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
		var (
			resp *http.Response
			err  error
		)
		for i := 0; i < retryCount; i++ {
			resp, err = client.Post(url, "application/json", nil)
			if err != nil || retryableHTTPStatusCode(resp.StatusCode) {
				time.Sleep(sleepDuration)
				continue
			}
			break
		}
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusFound {
			engine.addr = strings.TrimSuffix(resp.Header.Get("Location"), "/load_data")
			b.logger.Info("redirect to loadData worker",
				zap.String("loadDataTaskID", engine.loadDataTaskID),
				zap.String("worker addr", engine.addr))
			continue
		}
		if resp.StatusCode != http.StatusOK {
			msg, err := io.ReadAll(resp.Body)
			// If the task exists, we can continue to import data.
			if err == nil && strings.TrimSpace(string(msg)) == taskExitsMsg {
				b.logger.Info("loadData task has inited in load_data worker",
					zap.String("loadDataTask", engine.loadDataTaskID),
					zap.String("worker addr", engine.addr))
				return nil
			}
			return errors.Errorf("failed to open engine %s, msg: %s", resp.Status, string(msg))
		}
		return nil
	}
}

// CloseEngine closes backend engine by uuid.
func (b *Backend) CloseEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	engine, err := b.getEngine(engineUUID)
	if err != nil {
		exist, err := b.checkLoadDataTask(ctx, cfg)
		if err != nil {
			return err
		}
		if !exist {
			return fmt.Errorf("engine %s not found", genLoadDataTaskID(cfg))
		}
		b.logger.Info("not found engine, open a new one")
		err = b.OpenEngine(ctx, cfg, engineUUID)
		if err != nil {
			return err
		}
		engine, err = b.getEngine(engineUUID)
		if err != nil {
			b.logger.Warn("failed to open a new engine", zap.Error(err))
			return err
		}
	}

	return engine.flush(ctx)
}

func (b *Backend) checkLoadDataTask(ctx context.Context, cfg *backend.EngineConfig) (bool, error) {
	clusterID := b.pdCtl.GetPDClient().GetClusterID(ctx)
	taskID := genLoadDataTaskID(cfg)
	addr := b.workerAddr
	for {
		url := fmt.Sprintf("%s/load_data?cluster_id=%d&task_id=%s", addr, clusterID, taskID)
		client := *b.httpClient
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
		var (
			resp *http.Response
			err  error
		)
		for i := 0; i < retryCount; i++ {
			resp, err = client.Get(url)
			if err != nil || retryableHTTPStatusCode(resp.StatusCode) {
				time.Sleep(sleepDuration)
				continue
			}
			break
		}
		if err != nil {
			return false, err
		}
		if resp.StatusCode == http.StatusFound {
			addr = strings.TrimSuffix(resp.Header.Get("Location"), "/load_data")
			b.logger.Info("redirect to loadData worker",
				zap.String("loadDataTaskID", taskID),
				zap.String("worker addr", addr))
			continue
		}
		if resp.StatusCode != http.StatusOK {
			msg, err := io.ReadAll(resp.Body)
			if err != nil {
				return false, err
			}
			return false, errors.Errorf("failed to get task, status: %s, msg: %s", resp.Status, string(msg))
		}
		return true, nil
	}
}

// ImportEngine imports an engine to TiKV.
func (b *Backend) ImportEngine(ctx context.Context, engineUUID uuid.UUID, regionSplitSize, regionSplitKeys int64) error {
	engine, err := b.getEngine(engineUUID)
	if err != nil {
		return err
	}
	err = b.loadDataBuild(ctx, engine, regionSplitSize, regionSplitKeys)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			states, err := b.loadDataGetStates(ctx, engine)
			if err != nil {
				return err
			}
			if states.Canceled {
				b.logger.Error("loadData canceled", zap.String("loadDataTaskID", engine.loadDataTaskID), zap.String("error", states.Error))
				return errors.Errorf("load data canceled, loadDataTaskID:%s, error:%s", engine.loadDataTaskID, states.Error)
			} else if states.Finished {
				if states.hasDuplicateEntries() {
					err = b.handleDuplicateEntries(ctx, engine, states)
					if err != nil {
						return err
					}
				}
				writeBytes := engine.writeBytes.Load()
				b.reportWriteBytes(writeBytes)
				b.logger.Info("loadData finished",
					zap.String("db", engine.tbl.DB),
					zap.String("table", engine.tbl.Name),
					zap.String("loadDataTaskID", engine.loadDataTaskID),
					zap.Int64("write_bytes", writeBytes))
				return nil
			} else {
				b.logger.Info(
					"loadData states",
					zap.String("db", engine.tbl.DB),
					zap.String("table", engine.tbl.Name),
					zap.String("loadDataTaskID", engine.loadDataTaskID),
					zap.Int("created_files", states.CreatedFiles),
					zap.Int("ingested_regions", states.IngestedRegions),
				)
			}
		}
	}
}

func (b *Backend) handleDuplicateEntries(ctx context.Context, engine *engine, states *LoadDataStates) error {
	if !b.duplicateDetection {
		return nil
	}

	b.logger.Info("handling duplicate entries",
		zap.String("db", engine.tbl.DB),
		zap.String("table", engine.tbl.Name),
		zap.Int("duplicateEntries", len(states.DuplicateEntries)))

	if b.reportErrOnDup {
		dupKey, err := hex.DecodeString(states.DuplicateEntries[0].Key)
		dupKey, err = b.tikvCodec.DecodeKey(dupKey)
		if err != nil {
			b.logger.Warn("failed to decode key", zap.String("key", states.DuplicateEntries[0].Key), zap.Error(err))
		}

		dupVal, err := hex.DecodeString(states.DuplicateEntries[0].Values[0])
		if err != nil {
			b.logger.Warn("failed to decode value", zap.String("value", states.DuplicateEntries[0].Values[0]), zap.Error(err))
		}

		return common.ErrFoundDuplicateKeys.FastGenByArgs(dupKey, dupVal)
	}

	writeBatch := b.duplicateDB.NewBatch()
	writeBatchSize := int64(0)
	for _, entry := range states.DuplicateEntries {
		if len(entry.Values) == 0 {
			continue
		}
		rawKey, err := hex.DecodeString(entry.Key)
		if err != nil {
			b.logger.Warn("failed to decode key", zap.String("key", entry.Key), zap.Error(err))
			return err
		}
		for i, value := range entry.Values {
			// append index to rawKey to make it unique.
			encodedRawKey := b.keyAdapter.Encode(nil, rawKey, common.EncodeIntRowID(int64(i)))

			rawValue, err := hex.DecodeString(value)
			if err != nil {
				b.logger.Warn("failed to decode value", zap.String("value", value), zap.Error(err))
				return err
			}
			writeBatch.Set(encodedRawKey, rawValue, nil)
			writeBatchSize += int64(len(encodedRawKey) + len(rawValue))
			if writeBatchSize >= maxDuplicateBatchSize {
				err = writeBatch.Commit(nil)
				if err != nil {
					b.logger.Warn("failed to write duplicate entries", zap.Error(err))
					return err
				}
				writeBatch = b.duplicateDB.NewBatch()
				writeBatchSize = 0
			}
		}
	}

	if writeBatchSize > 0 {
		err := writeBatch.Commit(nil)
		if err != nil {
			b.logger.Warn("failed to write duplicate entries", zap.Error(err))
			return err
		}
	}
	return nil
}

func sendRequest(ctx context.Context, httpClient *http.Client, method, url string, data []byte) ([]byte, error) {
	var (
		resp *http.Response
		err  error
	)
	retry := retryCount
	for retry > 0 {
		resp, err = func() (*http.Response, error) {
			body := bytes.NewReader(data)
			req, err := http.NewRequestWithContext(ctx, method, url, body)
			if err != nil {
				return nil, errors.Errorf("failed to create request %s", url)
			}
			resp, err := httpClient.Do(req)
			if err != nil {
				return nil, errors.Errorf("failed to send request to remote worker, %s", url)
			}
			if resp.StatusCode != http.StatusOK {
				return nil, errors.Errorf("failed to send request to remote worker, %s, status %s", url, resp.Status)
			}
			return resp, nil
		}()
		if err == nil {
			defer resp.Body.Close()
			break
		}

		// sleep for a while between each retry
		time.Sleep(sleepDuration)
		retry -= 1
	}
	if err != nil {
		return nil, err
	}
	return io.ReadAll(resp.Body)
}

func (b *Backend) loadDataBuild(ctx context.Context, engine *engine, splitSize, splitKeys int64) error {
	url := fmt.Sprintf("%s/load_data?cluster_id=%d&task_id=%s&build=true&compression=zstd&split_size=%d&split_keys=%d",
		engine.addr, engine.clusterID, engine.loadDataTaskID, splitSize, splitKeys)

	_, err := sendRequest(ctx, b.httpClient, "POST", url, nil)
	return err
}

func (b *Backend) loadDataGetStates(ctx context.Context, engine *engine) (*LoadDataStates, error) {
	url := fmt.Sprintf("%s/load_data?cluster_id=%d&task_id=%s", engine.addr, engine.clusterID, engine.loadDataTaskID)
	data, err := sendRequest(ctx, b.httpClient, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	state := new(LoadDataStates)
	err = json.Unmarshal(data, state)
	if err != nil {
		return nil, err
	}
	return state, nil
}

// CleanupEngine cleanup the engine and reclaim the space.
func (b *Backend) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	engine, err := b.getEngine(engineUUID)
	if err != nil {
		return err
	}
	return b.loadDataCleanUp(ctx, engine)
}

func (b *Backend) loadDataCleanUp(ctx context.Context, engine *engine) error {
	url := fmt.Sprintf("%s/load_data?cluster_id=%d&task_id=%s", engine.addr, engine.clusterID, engine.loadDataTaskID)
	_, err := sendRequest(ctx, b.httpClient, "DELETE", url, nil)
	return err
}

// FlushEngine ensures all KV pairs written to an open engine has been
// synchronized, such that kill-9'ing Lightning afterwards and resuming from
// checkpoint can recover the exact same content.
//
// This method is only relevant for local backend, and is no-op for all
// other backends.
func (b *Backend) FlushEngine(ctx context.Context, engineUUID uuid.UUID) error {
	engine, err := b.getEngine(engineUUID)
	if err != nil {
		return err
	}
	return engine.flush(ctx)
}

// FlushAllEngines performs FlushEngine on all opened engines. This is a
// very expensive operation and should only be used in some rare situation
// (e.g. preparing to resolve a disk quota violation).
// This method is only relevant for local backend, and is no-op for remote backend.
func (b *Backend) FlushAllEngines(ctx context.Context) error {
	return nil
}

// EngineFileSizes obtains the size occupied locally of all engines managed
// by this backend. This method is used to compute disk quota.
// It can return nil if the content are all stored remotely.
func (b *Backend) EngineFileSizes() []backend.EngineFileSize {
	return nil
}

// ResetEngine clears all written KV pairs in this opened engine.
func (b *Backend) ResetEngine(ctx context.Context, engineUUID uuid.UUID) error {
	return errors.New("cannot reset an engine in Remote backend")
}

// LocalWriter obtains a thread-local EngineWriter for writing rows into the given engine.
func (b *Backend) LocalWriter(_ context.Context, cfg *backend.LocalWriterConfig, engineUUID uuid.UUID) (backend.EngineWriter, error) {
	engine, err := b.getEngine(engineUUID)
	if err != nil {
		return nil, err
	}
	localWriterID := uint64(cfg.LocalWriterID)
	if client, ok := engine.writers[localWriterID]; ok {
		return client, nil
	}

	chunksCache, err := newChunkCache(engine.loadDataTaskID, localWriterID)
	if err != nil {
		return nil, err
	}
	client := &client{
		e:           engine,
		writerID:    localWriterID,
		keyspace:    b.keyspace,
		nextChunkID: 1,
		httpClient:  b.httpClient,
		state:       &writerState{},
		chunksCache: chunksCache,
	}
	engine.writers[localWriterID] = client
	return client, nil
}

func (b *Backend) getEngine(engineUUID uuid.UUID) (*engine, error) {
	v, ok := b.engines.Load(engineUUID)
	if !ok {
		return nil, errors.Errorf("could not found engine for %s", engineUUID.String())
	}
	return v.(*engine), nil
}

type engine struct {
	ctx            context.Context
	logger         log.Logger
	loadDataTaskID string
	ts             uint64
	tbl            *checkpoints.TidbTableInfo
	writers        map[uint64]*client
	addr           string
	clusterID      uint64
	chunkID        atomic.Uint64
	writeBytes     atomic.Int64
	httpClient     *http.Client
	backend        *Backend
}

func (e *engine) flush(ctx context.Context) error {
	for _, writer := range e.writers {
		err := writer.addChunk(ctx)
		if err != nil {
			return err
		}
	}

	for {
		url := fmt.Sprintf("%s/load_data?cluster_id=%d&task_id=%s&flush=true",
			e.addr, e.clusterID, e.loadDataTaskID)

		data, err := sendRequest(ctx, e.httpClient, "POST", url, nil)
		if err != nil {
			e.logger.Error("failed to flush", zap.Error(err))
			return err
		}

		flushRes := new(FlushResult)
		err = json.Unmarshal(data, &flushRes)
		if err != nil {
			e.logger.Error("failed to unmarshal flushed chunks", zap.Error(err))
			return err
		}

		if flushRes.Canceled || flushRes.Error != "" {
			e.logger.Error("failed to flush",
				zap.Bool("canceled", flushRes.Canceled),
				zap.String("error", flushRes.Error))
			return errors.Errorf("failed to flush, canceled: %t, error: %s",
				flushRes.Canceled, flushRes.Error)
		}

		if flushRes.Finished {
			e.logger.Info("loadData finished")
			return nil
		}

		succeed := true
		// make sure all chunks are flushed
		for id, writer := range e.writers {
			flushedChunkID := flushRes.FlushedChunkIDs[id]
			if flushedChunkID == writer.lastChunkID() {
				// writer's all chunks are flushed
				continue
			}

			succeed = false
			// writer's chunks are not flushed, chunks were lost after the remote worker restarting, so put again.
			e.logger.Warn("writer's chunks are not flushed, retry to put chunk",
				zap.Uint64("writerID", id),
				zap.Uint64("flushedChunkID", flushRes.FlushedChunkIDs[id]),
				zap.Uint64("lastChunkID", writer.lastChunkID()),
				zap.Any("state", writer.state),
			)
			_, err := writer.flushed(ctx)
			if err != nil {
				return err
			}
		}
		if succeed {
			break
		}
	}

	for writerID, writer := range e.writers {
		err := writer.chunksCache.cleanAll()
		if err != nil {
			e.logger.Warn("failed to clean chunks cache", zap.Error(err), zap.Uint64("writerID", writerID))
		}
	}
	return nil
}

// chunk is a unit of data that send to remote worker.
type chunk struct {
	id   uint64
	data []byte
}

// client define a local writer that send KV pairs to remote worker.
type client struct {
	e           *engine
	writerID    uint64
	buf         []byte
	keyspace    []byte
	state       *writerState
	nextChunkID uint64
	httpClient  *http.Client
	chunksCache *chunkCache
}

const batchSize int = 8 * 1024 * 1024

func (w *client) AppendRows(
	ctx context.Context,
	columnNames []string,
	rows encode.Rows,
) error {
	kvs := kv.Rows2KvPairs(rows)
	if len(kvs) == 0 {
		return nil
	}
	lenBuf := make([]byte, 4)
	for _, pair := range kvs {
		binary.LittleEndian.PutUint16(lenBuf[:2], uint16(len(pair.Key)+len(w.keyspace)))
		w.buf = append(w.buf, lenBuf[:2]...)
		w.buf = append(w.buf, w.keyspace...)
		w.buf = append(w.buf, pair.Key...)
		binary.LittleEndian.PutUint32(lenBuf, uint32(len(pair.Val)))
		w.buf = append(w.buf, lenBuf...)
		w.buf = append(w.buf, pair.Val...)
	}
	if len(w.buf) > batchSize {
		return w.addChunk(ctx)
	}
	return nil
}

func (w *client) handlePutChunkResult(ctx context.Context, putChunkRes *PutChunkResult, expectChunkID uint64) error {
	if putChunkRes.Canceled {
		return errors.Errorf("failed to put chunk, canceled: %t, finished: %t, error: %s",
			putChunkRes.Canceled, putChunkRes.Finished, putChunkRes.Error)
	}

	if putChunkRes.HandledChunkID != expectChunkID {
		w.e.logger.Info("remote worker may restart, retry to put chunk",
			zap.Uint64("writerID", w.writerID),
			zap.Uint64("expectChunkID", expectChunkID),
			zap.Uint64("handledChunkID", putChunkRes.HandledChunkID),
			zap.Uint64("flushedChunkID", putChunkRes.FlushedChunkID),
			zap.String("error", putChunkRes.Error))

		nextChunkID := putChunkRes.HandledChunkID + 1
		for nextChunkID <= expectChunkID {
			url := fmt.Sprintf("%s/load_data?cluster_id=%d&task_id=%s&writer_id=%d&chunk_id=%d",
				w.e.addr, w.e.clusterID, w.e.loadDataTaskID, w.writerID, nextChunkID)

			buf, err := w.chunksCache.get(nextChunkID)
			if err != nil {
				return err
			}
			w.e.logger.Info("retry to put chunk",
				zap.Uint64("writerID", w.writerID),
				zap.Uint64("chunkID", nextChunkID))
			data, err := sendRequest(ctx, w.httpClient, "PUT", url, buf)
			if err != nil {
				return err
			}
			putChunkRes = new(PutChunkResult)
			err = json.Unmarshal(data, putChunkRes)
			if err != nil {
				return err
			}
			nextChunkID = putChunkRes.HandledChunkID + 1
		}
	}

	lastFlushedChunkID := w.state.FlushedChunkID + 1
	for lastFlushedChunkID <= putChunkRes.FlushedChunkID {
		err := w.chunksCache.clean(lastFlushedChunkID)
		if err != nil {
			w.e.logger.Info("failed to clean chunk cache", zap.Error(err), zap.Uint64("chunkID", lastFlushedChunkID), zap.Uint64("writerID", w.writerID))
		}
		lastFlushedChunkID += 1
	}

	w.state = &writerState{
		FlushedChunkID: putChunkRes.FlushedChunkID,
		HandledChunkID: putChunkRes.HandledChunkID,
	}
	return nil
}

func (w *client) lastChunkID() uint64 {
	return w.nextChunkID - 1
}

func (w *client) addChunk(ctx context.Context) error {
	if len(w.buf) == 0 {
		// nothing to do
		return nil
	}

	// The chunkID must be unique in a task, it should be autoincrement.
	newChunkID := w.nextChunkID
	w.nextChunkID += 1

	data := w.buf
	w.buf = w.buf[:0]

	if newChunkID <= w.state.FlushedChunkID {
		w.e.logger.Info("skip to put chunk", zap.Uint64("chunkID", newChunkID),
			zap.Uint64("handledChunkID", w.state.HandledChunkID),
			zap.Uint64("flushedChunkID", w.state.FlushedChunkID),
			zap.Uint64("writerID", w.writerID))
		return nil
	}

	// cache chunk data
	err := w.chunksCache.put(newChunkID, data)
	if err != nil {
		return err
	}
	if newChunkID <= w.state.HandledChunkID {
		w.e.logger.Info("skip to put chunk", zap.Uint64("chunkID", newChunkID),
			zap.Uint64("handledChunkID", w.state.HandledChunkID),
			zap.Uint64("flushedChunkID", w.state.FlushedChunkID),
			zap.Uint64("writerID", w.writerID))
		return nil
	}
	w.e.writeBytes.Add(int64(len(data)))

	url := fmt.Sprintf("%s/load_data?cluster_id=%d&task_id=%s&writer_id=%d&chunk_id=%d",
		w.e.addr, w.e.clusterID, w.e.loadDataTaskID, w.writerID, newChunkID)

	data, err = sendRequest(ctx, w.httpClient, "PUT", url, data)
	if err != nil {
		return err
	}
	putChunkRes := new(PutChunkResult)
	err = json.Unmarshal(data, putChunkRes)
	if err != nil {
		return err
	}

	return w.handlePutChunkResult(ctx, putChunkRes, newChunkID)
}

func (w *client) IsSynced() bool {
	return false
}

func (w *client) Close(ctx context.Context) (backend.ChunkFlushStatus, error) {
	if len(w.buf) > 0 {
		err := w.addChunk(ctx)
		if err != nil {
			return w, err
		}
	}
	w.buf = nil
	return w, nil
}

func (w *client) Flushed() bool {
	flushed, err := w.flushed(w.e.ctx)
	if err != nil {
		w.e.logger.Error("failed to check flushed", zap.Error(err))
	}
	return flushed
}

func (w *client) flushed(ctx context.Context) (bool, error) {
	if w.state.FlushedChunkID == w.nextChunkID-1 {
		return true, nil
	}

	lastChunkID := w.nextChunkID - 1
	url := fmt.Sprintf("%s/load_data?cluster_id=%d&task_id=%s&writer_id=%d&chunk_id=%d",
		w.e.addr, w.e.clusterID, w.e.loadDataTaskID, w.writerID, lastChunkID)

	data, err := sendRequest(ctx, w.httpClient, "PUT", url, nil)
	if err != nil {
		return false, err
	}
	state := new(PutChunkResult)
	err = json.Unmarshal(data, state)
	if err != nil {
		return false, err
	}

	err = w.handlePutChunkResult(ctx, state, lastChunkID)
	if err != nil {
		return false, err
	}

	return w.state.FlushedChunkID == w.nextChunkID-1, nil
}

// TODO: pushdown remote worker
func newRemoteRequestInfo(writeBytes, replicaNumber int64) remoteRequestInfo {
	return remoteRequestInfo{
		writeBytes:    writeBytes,
		replicaNumber: replicaNumber,
	}
}

type remoteRequestInfo struct {
	writeBytes    int64
	replicaNumber int64
}

func (r remoteRequestInfo) IsWrite() bool {
	return true
}

func (r remoteRequestInfo) WriteBytes() uint64 {
	return uint64(r.writeBytes)
}

func (r remoteRequestInfo) ReplicaNumber() int64 {
	return r.replicaNumber
}

func (r remoteRequestInfo) StoreID() uint64 {
	return 0
}

func (b *Backend) setupReportWRU(ctx context.Context, keyspaceID uint32, keyspaceName string, cfg *config.Config) error {
	// remote backend used to import new table data,
	// so we can use default placement rule as repliace count.
	placementRules, err := pdutil.GetPlacementRules(ctx, b.pdAddr, b.tls.TLSConfig())
	if err != nil {
		b.logger.Warn("failed to get placement rules", zap.Error(err))
		return err
	}
	var defaultRule *pdtypes.Rule
	for i := range placementRules {
		if placementRules[i].ID == "default" {
			defaultRule = &placementRules[i]
			break
		}
	}
	if defaultRule == nil {
		b.logger.Warn("failed to get default placement rule")
		return errors.New("failed to get default placement rule")
	}

	kvCalculator := rm.KVCalculator{
		RUConfig: &rm.RUConfig{
			WriteBaseCost:         rm.RequestUnit(cfg.RUConfig.WriteBaseCost),
			WritePerBatchBaseCost: rm.RequestUnit(cfg.RUConfig.WritePerBatchBaseCost),
			WriteBytesCost:        rm.RequestUnit(cfg.RUConfig.WriteCostPerByte),
		},
	}

	wruMetrics := b.metrics.WRUCostCounter.WithLabelValues(fmt.Sprintf("%d", keyspaceID))
	b.reportWriteBytes = func(bytes int64) {
		reqInfo := newRemoteRequestInfo(bytes, int64(defaultRule.Count))
		consumption := &rmpb.Consumption{}
		kvCalculator.BeforeKVRequest(consumption, reqInfo)
		wruMetrics.Add(consumption.WRU)
	}

	b.logger.Info("setup report WRU", zap.Uint32("keyspaceId", keyspaceID),
		zap.String("keyspaceName", keyspaceName),
		zap.Int("replicaNumber", defaultRule.Count),
		zap.Float64("writeBaseCost", cfg.RUConfig.WriteBaseCost),
		zap.Float64("writePerBatchBaseCost", cfg.RUConfig.WritePerBatchBaseCost),
		zap.Float64("WriteCostPerByte", cfg.RUConfig.WriteCostPerByte),
	)
	return nil
}

// GetDupeController returns a new dupe controller.
func (b *Backend) GetDupeController(dupeConcurrency int, errorMgr *errormanager.ErrorManager) *local.DupeController {
	return local.NewDupeController(dupeConcurrency, errorMgr, nil, b.tikvCli, b.tikvCodec, b.duplicateDB, b.keyAdapter, nil, false)
}

func genLoadDataTaskID(cfg *backend.EngineConfig) string {
	return fmt.Sprintf("%d-%d-%d", cfg.TaskID, cfg.TableInfo.ID, cfg.EngineID)
}

func retryableHTTPStatusCode(statusCode int) bool {
	return statusCode == http.StatusServiceUnavailable || // 503
		statusCode == http.StatusInternalServerError || // 500
		statusCode == http.StatusRequestTimeout // 408
}
