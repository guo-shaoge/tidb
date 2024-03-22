package importer

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/precheck"
	tikvclient "github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

type precheckContextKey string

const taskManagerKey precheckContextKey = "PRECHECK/TASK_MANAGER"

// WithPrecheckKey returns a new context with the given key and value.
func WithPrecheckKey(ctx context.Context, key precheckContextKey, val any) context.Context {
	return context.WithValue(ctx, key, val)
}

// PrecheckItemBuilder is used to build precheck items
type PrecheckItemBuilder struct {
	cfg                *config.Config
	dbMetas            []*mydump.MDDatabaseMeta
	preInfoGetter      PreImportInfoGetter
	checkpointsDB      checkpoints.DB
	pdLeaderAddrGetter func() string
	kvCodec            tikvclient.Codec
}

// NewPrecheckItemBuilder creates a new PrecheckItemBuilder
func NewPrecheckItemBuilder(
	cfg *config.Config,
	dbMetas []*mydump.MDDatabaseMeta,
	preInfoGetter PreImportInfoGetter,
	checkpointsDB checkpoints.DB,
	pdCli pd.Client,
	kvCodec tikvclient.Codec,
) *PrecheckItemBuilder {
	leaderAddrGetter := func() string {
		return cfg.TiDB.PdAddr
	}
	// in tests we may not have a pdCli
	if pdCli != nil {
		leaderAddrGetter = pdCli.GetLeaderURL
	}
	return &PrecheckItemBuilder{
		cfg:                cfg,
		dbMetas:            dbMetas,
		preInfoGetter:      preInfoGetter,
		checkpointsDB:      checkpointsDB,
		pdLeaderAddrGetter: leaderAddrGetter,
		kvCodec:            kvCodec,
	}
}

// BuildPrecheckItem builds a Checker by the given checkID
func (b *PrecheckItemBuilder) BuildPrecheckItem(checkID precheck.CheckItemID) (precheck.Checker, error) {
	switch checkID {
	case precheck.CheckLargeDataFile:
		return NewLargeFileCheckItem(b.cfg, b.dbMetas), nil
	case precheck.CheckSourcePermission:
		return NewStoragePermissionCheckItem(b.cfg), nil
	case precheck.CheckTargetTableEmpty:
		return NewTableEmptyCheckItem(b.cfg, b.preInfoGetter, b.dbMetas, b.checkpointsDB), nil
	case precheck.CheckSourceSchemaValid:
		return NewSchemaCheckItem(b.cfg, b.preInfoGetter, b.dbMetas, b.checkpointsDB), nil
	case precheck.CheckSourceDataSize:
		return NewSourceDataSizeCheckItem(b.cfg, b.preInfoGetter), nil
	case precheck.CheckCheckpoints:
		return NewCheckpointCheckItem(b.cfg, b.preInfoGetter, b.dbMetas, b.checkpointsDB), nil
	case precheck.CheckCSVHeader:
		return NewCSVHeaderCheckItem(b.cfg, b.preInfoGetter, b.dbMetas), nil
	case precheck.CheckTargetClusterSize:
		return NewClusterResourceCheckItem(b.preInfoGetter), nil
	case precheck.CheckTargetClusterEmptyRegion:
		return NewEmptyRegionCheckItem(b.preInfoGetter, b.dbMetas), nil
	case precheck.CheckTargetClusterRegionDist:
		return NewRegionDistributionCheckItem(b.preInfoGetter, b.dbMetas), nil
	case precheck.CheckTargetClusterVersion:
		return NewClusterVersionCheckItem(b.preInfoGetter, b.dbMetas), nil
	case precheck.CheckLocalDiskPlacement:
		return NewLocalDiskPlacementCheckItem(b.cfg), nil
	case precheck.CheckLocalTempKVDir:
		return NewLocalTempKVDirCheckItem(b.cfg, b.preInfoGetter, b.dbMetas), nil
	case precheck.CheckTargetUsingCDCPITR:
		return NewCDCPITRCheckItem(b.cfg, b.pdLeaderAddrGetter, b.kvCodec), nil
	default:
		return nil, errors.Errorf("unsupported check item: %v", checkID)
	}
}

// GetPreInfoGetter gets the pre restore info getter from the builder.
func (b *PrecheckItemBuilder) GetPreInfoGetter() PreImportInfoGetter {
	return b.preInfoGetter
}
