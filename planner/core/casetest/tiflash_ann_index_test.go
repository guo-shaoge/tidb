// Copyright 2024 PingCAP, Inc.
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

package casetest

import (
	"testing"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner/core/internal"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestVectorIndexProtobufMatch(t *testing.T) {
	require.EqualValues(t, tipb.VectorDistanceMetric_INNER_PRODUCT.String(), model.DistanceMetricInnerProduct)
	require.EqualValues(t, tipb.VectorIndexKind_HNSW.String(), model.VectorIndexKindHNSW)
}

func TestTiFlashANNIndex(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set @@global.tidb_enable_vector_type=1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`
		create table t1 (
			vec vector(3) comment 'hnsw(distance=cosine)',
			a int,
			b int,
			c vector(3),
			d vector
		)
	`)
	tk.MustExec(`
		insert into t1 values
			('[1,1,1]', 1, 1, '[1,1,1]', '[1,1,1]'),
			('[2,2,2]', 2, 2, '[2,2,2]', '[2,2,2]'),
			('[3,3,3]', 3, 3, '[3,3,3]', '[3,3,3]')
	`)
	for i := 0; i < 14; i++ {
		tk.MustExec("insert into t1(vec, a, b, c, d) select vec, a, b, c, d from t1")
	}
	tk.MustExec("analyze table t1")
	internal.SetTiFlashReplica(t, dom, "test", "t1")

	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	var input Input
	var output Output
	suiteData := GetANNIndexSuiteData()
	suiteData.LoadTestCases(t, &input, &output)
	testWithData(t, tk, input, output)
}

func TestTiFlashANNIndexForPartition(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set @@global.tidb_enable_vector_type=1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`
		create table t1 (
			vec vector(3) comment 'hnsw(distance=cosine)',
			a int, b int,
			store_id int
		) PARTITION BY RANGE COLUMNS(store_id) (
			PARTITION p0 VALUES LESS THAN (100),
			PARTITION p1 VALUES LESS THAN (200),
			PARTITION p2 VALUES LESS THAN (MAXVALUE)
		);
	`)
	tk.MustExec("insert into t1 values('[1,1,1]', 1, 1, 50), ('[2,2,2]', 2, 2, 150), ('[3,3,3]', 3, 3, 250)")
	for i := 0; i < 14; i++ {
		tk.MustExec("insert into t1(vec, a, b, store_id) select vec, a, b, store_id from t1")
	}
	tk.MustExec("analyze table t1")
	internal.SetTiFlashReplica(t, dom, "test", "t1")

	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	var input Input
	var output Output
	suiteData := GetANNIndexSuiteData()
	suiteData.LoadTestCases(t, &input, &output)
	testWithData(t, tk, input, output)
}
