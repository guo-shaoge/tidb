// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"reflect"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

var _ = check.Suite(&CTEStorageRCTestSuite{})

type CTEStorageRCTestSuite struct {
	store   kv.Storage
	session session.Session
}

func (test *CTEStorageRCTestSuite) SetUpSuite(c *check.C) {
	var err error
	test.store, err = mockstore.NewMockStore()
	c.Assert(err, check.IsNil)

	test.session, err = session.CreateSession4Test(test.store)
	c.Assert(err, check.IsNil)
	test.session.SetConnectionID(0)
}

func (test *CTEStorageRCTestSuite) TestCTEStorageBasic(c *check.C) {
	storage := executor.NewCTEStorageRC(nil, false)
	c.Assert(storage, check.NotNil)

	// close before open
	err := storage.DerefAndClose()
	c.Assert(err, check.NotNil)

	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	chkSize := 1
	err = storage.OpenAndRef(fields, chkSize)
	c.Assert(err, check.IsNil)

	err = storage.DerefAndClose()
	c.Assert(err, check.IsNil)

	err = storage.DerefAndClose()
	c.Assert(err, check.NotNil)

	// open twice
	err = storage.OpenAndRef(fields, chkSize)
	c.Assert(err, check.IsNil)
	err = storage.OpenAndRef(fields, chkSize)
	c.Assert(err, check.IsNil)
	err = storage.DerefAndClose()
	c.Assert(err, check.IsNil)
	err = storage.DerefAndClose()
	c.Assert(err, check.IsNil)
	err = storage.DerefAndClose()
	c.Assert(err, check.NotNil)
}

func (test *CTEStorageRCTestSuite) TestOpenAndClose(c *check.C) {
	storage := executor.NewCTEStorageRC(nil, false)

	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	chkSize := 1
	for i := 0; i < 10; i++ {
		err := storage.OpenAndRef(fields, chkSize)
		c.Assert(err, check.IsNil)
	}

	for i := 0; i < 9; i++ {
		err := storage.DerefAndClose()
		c.Assert(err, check.IsNil)
	}
	err := storage.DerefAndClose()
	c.Assert(err, check.IsNil)

	err = storage.DerefAndClose()
	c.Assert(err, check.NotNil)

}

func (test *CTEStorageRCTestSuite) TestAddAndGetChunk(c *check.C) {
	storage := executor.NewCTEStorageRC(test.session.GetSessionVars().StmtCtx, false)

	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	chkSize := 10

	inChk := chunk.NewChunkWithCapacity(fields, chkSize)
	for i := 0; i < chkSize; i++ {
		inChk.AppendInt64(0, int64(i))
	}

	err := storage.Add(inChk)
	c.Assert(err, check.NotNil)

	err = storage.OpenAndRef(fields, chkSize)
	c.Assert(err, check.IsNil)

	err = storage.Add(inChk)
	c.Assert(err, check.IsNil)

	outChk, err1 := storage.GetChunk(0)
	c.Assert(err1, check.IsNil)

	in64s := inChk.Column(0).Int64s()
	out64s := outChk.Column(0).Int64s()

	c.Assert(reflect.DeepEqual(in64s, out64s), check.IsTrue)
}

func testFilterDuplicated(c *check.C, storage executor.CTEStorage) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	chkSize := 10

	inChk := chunk.NewChunkWithCapacity(fields, chkSize)
	// all zeros
	for i := 0; i < chkSize; i++ {
		inChk.AppendInt64(0, int64(0))
	}

	err := storage.Add(inChk)
	c.Assert(err, check.IsNil)

	outChk, err := storage.GetChunk(0)
	c.Assert(err, check.IsNil)

	res64s := []int64{0}
	out64s := outChk.Column(0).Int64s()

	c.Assert(reflect.DeepEqual(res64s, out64s), check.IsTrue)

	// all ones
	inChk.Reset()
	for i := 0; i < chkSize; i++ {
		inChk.AppendInt64(0, int64(1))
	}

	err = storage.Add(inChk)
	outChk, err = storage.GetChunk(1)
	c.Assert(err, check.IsNil)
	tmpOut64s := outChk.Column(0).Int64s()
	res64s = []int64{0, 1}
	out64s = append(out64s, tmpOut64s...)

	c.Assert(reflect.DeepEqual(res64s, out64s), check.IsTrue)

	// zeros, ones, twos mixed
	inChk.Reset()
	for i := 0; i < 3; i++ {
		inChk.AppendInt64(0, int64(0))
	}
	for i := 0; i < 3; i++ {
		inChk.AppendInt64(0, int64(1))
	}
	for i := 0; i < 4; i++ {
		inChk.AppendInt64(0, int64(2))
	}

	err = storage.Add(inChk)
	outChk, err = storage.GetChunk(2)
	c.Assert(err, check.IsNil)
	tmpOut64s = outChk.Column(0).Int64s()
	res64s = []int64{0, 1, 2}
	out64s = append(out64s, tmpOut64s...)

	c.Assert(reflect.DeepEqual(res64s, out64s), check.IsTrue)
}

func (test *CTEStorageRCTestSuite) TestFilterDuplicated(c *check.C) {
	storage := executor.NewCTEStorageRC(test.session.GetSessionVars().StmtCtx, true)
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	chkSize := 10
	err := storage.OpenAndRef(fields, chkSize)
	c.Assert(err, check.IsNil)
	testFilterDuplicated(c, storage)
}

func (test *CTEStorageRCTestSuite) TestSpillToDisk(c *check.C) {
	storage := executor.NewCTEStorageRC(test.session.GetSessionVars().StmtCtx, false)
	var tmp interface{} = storage

	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	chkSize := 10

	inChk := chunk.NewChunkWithCapacity(fields, chkSize)
	for i := 0; i < chkSize; i++ {
		inChk.AppendInt64(0, int64(i))
	}

	err := storage.OpenAndRef(fields, chkSize)
	c.Assert(err, check.IsNil)

	memTracker := storage.GetMemTracker()
	memTracker.SetBytesLimit(inChk.MemoryUsage() + 1)
	memTracker.FallbackOldAndSetNewAction(tmp.(*executor.CTEStorageRC).ActionSpillForTest())
	diskTracker := storage.GetDiskTracker()

	// all in memory
	err = storage.Add(inChk)
	c.Assert(err, check.IsNil)
	outChk, err1 := storage.GetChunk(0)
	c.Assert(err1, check.IsNil)
	in64s := inChk.Column(0).Int64s()
	out64s := outChk.Column(0).Int64s()
	c.Assert(reflect.DeepEqual(in64s, out64s), check.IsTrue)

	c.Assert(memTracker.BytesConsumed(), check.Greater, int64(0))
	c.Assert(memTracker.MaxConsumed(), check.Greater, int64(0))
	c.Assert(diskTracker.BytesConsumed(), check.Equals, int64(0))
	c.Assert(diskTracker.MaxConsumed(), check.Equals, int64(0))

	// add again, will trigger spill to disk
	err = storage.Add(inChk)
	c.Assert(err, check.IsNil)
	tmp.(*executor.CTEStorageRC).GetRCForTest().GetActionSpillForTest().WaitForTest()
	c.Assert(memTracker.BytesConsumed(), check.Equals, int64(0))
	c.Assert(memTracker.MaxConsumed(), check.Greater, int64(0))
	c.Assert(diskTracker.BytesConsumed(), check.Greater, int64(0))
	c.Assert(diskTracker.MaxConsumed(), check.Greater, int64(0))

	outChk, err = storage.GetChunk(0)
	c.Assert(err, check.IsNil)
	out64s = outChk.Column(0).Int64s()
	c.Assert(reflect.DeepEqual(in64s, out64s), check.IsTrue)

	outChk, err = storage.GetChunk(1)
	c.Assert(err, check.IsNil)
	out64s = outChk.Column(0).Int64s()
	c.Assert(reflect.DeepEqual(in64s, out64s), check.IsTrue)
}