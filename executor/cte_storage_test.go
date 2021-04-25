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
	_ "reflect"

	"github.com/pingcap/check"
	_ "github.com/pingcap/parser/mysql"
	_ "github.com/pingcap/tidb/executor"
	_ "github.com/pingcap/tidb/types"
	_ "github.com/pingcap/tidb/util/chunk"
)

var _ = check.Suite(&CTEStorageRCTestSuite{})

type CTEStorageRCTestSuite struct{}

func (test *CTEStorageRCTestSuite) BasicTest(c *check.C) {
	// storage := executor.NewCTEStorageRC()
	// c.Assert(storage, check.NotNil)

	// err := storage.DerefAndClose()
	// c.Assert(err, check.NotNil)

	// fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	// chkSize := 1
	// err = storage.OpenAndRef(fields, chkSize, 100)
	// c.Assert(err, check.NotNil)

	// err = storage.DerefAndClose()
	// c.Assert(err, check.IsNil)

	// err = storage.DerefAndClose()
	// c.Assert(err, check.NotNil)
}

func (test *CTEStorageRCTestSuite) TestOpenAndClose(c *check.C) {
	// storage := executor.NewCTEStorageRC()

	// fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	// chkSize := 1
	// var bytesLimit int64 = 100
	// for i := 0; i < 10; i++ {
	// 	err := storage.OpenAndRef(fields, chkSize, bytesLimit)
	// 	c.Assert(err, check.IsNil)
	// }

	// for i := 0; i < 9; i++ {
	// 	err := storage.DerefAndClose()
	// 	c.Assert(err, check.IsNil)
	// }
	// err := storage.DerefAndClose()
	// c.Assert(err, check.IsNil)

	// err = storage.DerefAndClose()
	// c.Assert(err, check.NotNil)

}

func (test *CTEStorageRCTestSuite) TestAddAndGetChunk(c *check.C) {
	// storage := executor.NewCTEStorageRC()

	// fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	// var bytesLimit int64 = 100
	// chkSize := 10

	// inChk := chunk.NewChunkWithCapacity(fields, chkSize)
	// for i := 0; i < chkSize; i++ {
	// 	inChk.AppendInt64(0, int64(i))
	// }

	// err := storage.Add(inChk)
	// c.Assert(err, check.NotNil)

	// err = storage.OpenAndRef(fields, chkSize, bytesLimit)
	// c.Assert(err, check.IsNil)

	// err = storage.Add(inChk)
	// c.Assert(err, check.IsNil)

	// outChk, err1 := storage.GetChunk(0)
	// c.Assert(err1, check.IsNil)

	// in64s := inChk.Column(0).Int64s()
	// out64s := outChk.Column(0).Int64s()

	// c.Assert(reflect.DeepEqual(in64s, out64s), check.IsTrue)
}

// TODO: here!!!
func (test *CTEStorageRCTestSuite) TestSpillToDisk(c *check.C) {
	// storage := executor.NewCTEStorageRC()

	// fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	// chkSize := 10

	// inChk := chunk.NewChunkWithCapacity(fields, chkSize)
	// for i := 0; i < chkSize; i++ {
	// 	inChk.AppendInt64(0, int64(i))
	// }

	// var bytesLimit int64 = inChk.MemoryUsage()

	// err := storage.OpenAndRef(fields, chkSize, bytesLimit)
	// c.Assert(err, check.IsNil)

	// // all in memory
	// err = storage.Add(inChk)
	// c.Assert(err, check.IsNil)
	// outChk, err1 := storage.GetChunk(0)
	// c.Assert(err1, check.IsNil)
	// in64s := inChk.Column(0).Int64s()
	// out64s := outChk.Column(0).Int64s()
	// c.Assert(reflect.DeepEqual(in64s, out64s), check.IsTrue)

	// // add again, will trigger spill to disk
	// err = storage.Add(inChk)
	// c.Assert(err, check.IsNil)

	// outChk, err1 = storage.GetChunk(0)
	// c.Assert(err1, check.IsNil)
	// out64s = outChk.Column(0).Int64s()
	// c.Assert(reflect.DeepEqual(in64s, out64s), check.IsTrue)

	// outChk, err1 = storage.GetChunk(1)
	// c.Assert(err1, check.IsNil)
	// out64s = outChk.Column(0).Int64s()
	// c.Assert(reflect.DeepEqual(in64s, out64s), check.IsTrue)
}
