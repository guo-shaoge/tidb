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

package keyspace

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// tidbKeyspaceEtcdPathPrefix is the keyspace prefix for etcd namespace
	tidbKeyspaceEtcdPathPrefix = "/keyspaces/tidb/"

	// EnvVarKeyspaceName is the system env name for keyspace name.
	EnvVarKeyspaceName = "KEYSPACE_NAME"
)

// CodecV1 represents api v1 codec.
var CodecV1 = tikv.NewCodecV1(tikv.ModeTxn)

// EtcdNamespace return the keyspace prefix path for etcd namespace
func EtcdNamespace(c tikv.Codec) string {
	if c == nil || c.GetAPIVersion() == kvrpcpb.APIVersion_V1 {
		return ""
	}
	return fmt.Sprintf(tidbKeyspaceEtcdPathPrefix+"%d", c.GetKeyspaceID())
}

// GetKeyspaceNameBySettings is used to get Keyspace name setting.
func GetKeyspaceNameBySettings() (keyspaceName string) {
	keyspaceName = config.GetGlobalConfig().KeyspaceName
	if !IsKeyspaceNameEmpty(keyspaceName) {
		return keyspaceName
	}

	keyspaceName = os.Getenv(EnvVarKeyspaceName)
	config.UpdateGlobal(func(c *config.Config) {
		c.KeyspaceName = keyspaceName
	})
	return keyspaceName
}

// IsKeyspaceNameEmpty is used to determine whether keyspaceName is set.
func IsKeyspaceNameEmpty(keyspaceName string) bool {
	return keyspaceName == ""
}

// WrapZapcoreWithKeyspace is used to wrap zapcore.Core.
func WrapZapcoreWithKeyspace(keyspaceID uint32) zap.Option {
	return zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		keyspaceName := GetKeyspaceNameBySettings()
		if !IsKeyspaceNameEmpty(keyspaceName) {
			core = core.With([]zap.Field{zap.String("keyspaceName", keyspaceName)})
			core = core.With([]zap.Field{zap.Uint32("keyspaceID", keyspaceID)})
		}
		return core
	})
}

// GetKeyspaceTxnPrefix return the keyspace txn prefix
func GetKeyspaceTxnPrefix(keyspaceID uint32) []byte {
	keyspaceIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyspaceIDBytes, keyspaceID)
	txnLeftBound := codec.EncodeBytes(nil, append([]byte{'x'}, keyspaceIDBytes[1:]...))
	return txnLeftBound
}

// GetKeyspaceTxnRange return the keyspace range prefix
func GetKeyspaceTxnRange(keyspaceID uint32) ([]byte, []byte) {

	// Get keyspace range
	txnLeftBound := GetKeyspaceTxnPrefix(keyspaceID)

	var txnRightBound []byte
	if keyspaceID == 0xffffff {
		var end [4]byte
		binary.BigEndian.PutUint32(end[:], keyspaceID+1)
		end[0] = 'x' + 1 // handle overflow for max keyspace id.
		txnRightBound = codec.EncodeBytes(nil, end[:])
	} else {
		txnRightBound = GetKeyspaceTxnPrefix(keyspaceID + 1)
	}

	return txnLeftBound, txnRightBound
}

// CheckKeyspaceName checks whether the keyspace name is equal to the name in the configuration.
func CheckKeyspaceName(keyspaceName string) error {
	configKeyspaceName := GetKeyspaceNameBySettings()
	// If the keyspace name is not set in the configuration, it is not checked.
	if keyspaceName == configKeyspaceName {
		return nil
	}
	return errors.Errorf("keyspace name: %s is not equal setting: %s", keyspaceName, configKeyspaceName)
}

// BuildAPIContext is used to build APIContext.
func BuildAPIContext(keyspaceName string) (apiContext pd.APIContext) {
	if len(keyspaceName) == 0 {
		apiContext = pd.NewAPIContextV1()
	} else {
		apiContext = pd.NewAPIContextV2(keyspaceName)
	}
	return
}

// NewEtcdSafePointKV is used to add prefix when set keyspace.
func NewEtcdSafePointKV(etcdAddrs []string, codec tikv.Codec, tlsConfig *tls.Config) (*tikv.EtcdSafePointKV, error) {
	var etcdNameSpace string
	if config.GetGlobalConfig().EnableSafePointV2 {
		etcdNameSpace = EtcdNamespace(codec)
	}
	return tikv.NewEtcdSafePointKV(etcdAddrs, tlsConfig, tikv.WithPrefix(etcdNameSpace))
}

// CodecFromName is used to get the corresponding codec according to the keyspace name.
func CodecFromName(ctx context.Context, pdCli pd.Client, keyspaceName string) (tikv.Codec, error) {
	if len(keyspaceName) == 0 {
		return tikv.NewCodecV1(tikv.ModeTxn), nil
	}

	meta, err := pdCli.LoadKeyspace(ctx, keyspaceName)
	if err != nil {
		return nil, err
	}

	return tikv.NewCodecV2(tikv.ModeTxn, meta.Id)
}
