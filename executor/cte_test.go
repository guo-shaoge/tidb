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
	"context"
	_ "fmt"

	"github.com/pingcap/check"

	"github.com/pingcap/tidb/domain"
	_ "github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"

	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = check.Suite(&CTETestSuite{})

type CTETestSuite struct {
	store      kv.Storage
	dom        *domain.Domain
	sessionCtx sessionctx.Context
	session    session.Session
	ctx        context.Context
}

func (test *CTETestSuite) SetUpSuite(c *check.C) {
	var err error
	test.store, err = mockstore.NewMockStore()
	c.Assert(err, check.IsNil)

	test.dom, err = session.BootstrapSession(test.store)
	c.Assert(err, check.IsNil)

	test.sessionCtx = mock.NewContext()

	test.session, err = session.CreateSession4Test(test.store)
	c.Assert(err, check.IsNil)
	test.session.SetConnectionID(0)

	test.ctx = context.Background()
}

func (test *CTETestSuite) TearDownSuite(c *check.C) {
	test.dom.Close()
	test.store.Close()
}

func (test *CTETestSuite) TestBasicCTE(c *check.C) {
	tk := testkit.NewTestKit(c, test.store)
	tk.MustExec("use test")

	rows := tk.MustQuery("with recursive cte1 as (" +
		"select 1 c1 " +
		"union all " +
		"select c1 + 1 c1 from cte1 where c1 < 5) " +
		"select * from cte1")
	rows.Check(testkit.Rows("1", "2", "3", "4", "5"))

	// two seed part
	rows = tk.MustQuery("with recursive cte1 as (" +
		"select 1 c1 " +
		"union all " +
		"select 2 c1 " +
		"union all " +
		"select c1 + 1 c1 from cte1 where c1 < 10) " +
		"select * from cte1 order by c1")
	rows.Check(testkit.Rows("1", "2", "2", "3", "3", "4", "4", "5", "5", "6", "6", "7", "7", "8", "8", "9", "9", "10", "10"))

	// two recursive part
	rows = tk.MustQuery("with recursive cte1 as (" +
		"select 1 c1 " +
		"union all " +
		"select 2 c1 " +
		"union all " +
		"select c1 + 1 c1 from cte1 where c1 < 3) " +
		"union all " +
		"select c1 + 2 c1 from cte1 where c1 < 5) " +
		"select * from cte1 order by c1")
	rows.Check(testkit.Rows("1", "2", "2", "3", "3", "3", "4", "4", "5", "5", "5", "6", "6"))
}

func (test *CTETestSuite) TestSpillToDisk(c *check.C) {
}

func (test *CTETestSuite) TestUnionDistinct(c *check.C) {
}

// test invalid use of CTE
func (test *CTETestSuite) TestInvalidUsage(c *check.C) {
}

func (test *CTETestSuite) TestMultiReference(c *check.C) {
}

func (test *CTETestSuite) TestFailPoint(c *check.C) {
}
