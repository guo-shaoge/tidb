// Copyright 2017 PingCAP, Inc.
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

package core

import (
	substraitgo "github.com/AilinKid/substrait-go/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/telemetry"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// ToSubstraitPB implements PhysicalPlan ToSubstraitPB interface.
func (p *basePhysicalPlan) ToSubstraitPB(ctx sessionctx.Context, ssHandler *SubstraitHandler) (_ *substraitgo.Rel, err error) {
	return nil, errors.Errorf("plan %s fails converts to PB", p.basePlan.ExplainID())
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *basePhysicalPlan) ToPB(_ sessionctx.Context, _ kv.StoreType) (*tipb.Executor, error) {
	return nil, errors.Errorf("plan %s fails converts to PB", p.basePlan.ExplainID())
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalHashAgg) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	groupByExprs, err := expression.ExpressionsToPBList(sc, p.GroupByItems, client)
	if err != nil {
		return nil, err
	}
	aggExec := &tipb.Aggregation{
		GroupBy: groupByExprs,
	}
	for _, aggFunc := range p.AggFuncs {
		agg, err := aggregation.AggFuncToPBExpr(ctx, client, aggFunc, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		aggExec.AggFunc = append(aggExec.AggFunc, agg)
	}
	executorID := ""
	if storeType == kv.TiFlash {
		var err error
		aggExec.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeAggregation, Aggregation: aggExec, ExecutorId: &executorID}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalStreamAgg) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	groupByExprs, err := expression.ExpressionsToPBList(sc, p.GroupByItems, client)
	if err != nil {
		return nil, err
	}
	aggExec := &tipb.Aggregation{
		GroupBy: groupByExprs,
	}
	for _, aggFunc := range p.AggFuncs {
		agg, err := aggregation.AggFuncToPBExpr(ctx, client, aggFunc, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		aggExec.AggFunc = append(aggExec.AggFunc, agg)
	}
	executorID := ""
	if storeType == kv.TiFlash {
		var err error
		aggExec.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeStreamAgg, Aggregation: aggExec, ExecutorId: &executorID}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalSelection) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	conditions, err := expression.ExpressionsToPBList(sc, p.Conditions, client)
	if err != nil {
		return nil, err
	}
	selExec := &tipb.Selection{
		Conditions: conditions,
	}
	executorID := ""
	if storeType == kv.TiFlash {
		var err error
		selExec.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeSelection, Selection: selExec, ExecutorId: &executorID}, nil
}
func getSubStraitType(tp byte) string {
	if tp == mysql.TypeLong {
		return "i32"
	}
	if tp == mysql.TypeLonglong {
		return "i64"
	}
	if tp == mysql.TypeDouble {
		return "fp64"
	}
	panic("not supproted type")
}

func getSubstraitPBFunctionArguments(offsets []int32) (funcArgs []*substraitgo.FunctionArgument) {
	getFunctionArgument := func(offset int32) *substraitgo.FunctionArgument {
		return &substraitgo.FunctionArgument{
			ArgType: &substraitgo.FunctionArgument_Value{
				Value: &substraitgo.Expression{
					RexType: &substraitgo.Expression_Selection{
						Selection: &substraitgo.Expression_FieldReference{
							ReferenceType: &substraitgo.Expression_FieldReference_DirectReference{
								DirectReference: &substraitgo.Expression_ReferenceSegment{
									ReferenceType: &substraitgo.Expression_ReferenceSegment_StructField_{
										StructField: &substraitgo.Expression_ReferenceSegment_StructField{
											// col offset from child chunk
											Field: offset,
										},
									},
								},
							},
						},
					},
				},
			},
		}
	}
	for _, off := range offsets {
		funcArgs = append(funcArgs, getFunctionArgument(off))
	}
	return
}

type SubstraitHandler struct {
	autoIncId uint32
	SigMap    map[string]uint32
}

func NewSubstraitHandler() *SubstraitHandler {
	return &SubstraitHandler{
		autoIncId: uint32(1),
		SigMap:    map[string]uint32{},
	}
}

func (h *SubstraitHandler) insertSig(sigName string) uint32 {
	if _, ok := h.SigMap[sigName]; !ok {
		h.SigMap[sigName] = h.autoIncId
		h.autoIncId++
	}
	return h.getSigID(sigName)
}

func (h *SubstraitHandler) getSigID(sigName string) uint32 {
	return h.SigMap[sigName]
}

func scalarFuncFieldTypeToSubstraitOutputType(sf *expression.ScalarFunction) (outputType *substraitgo.Type) {
	switch sf.GetType().EvalType() {
	case types.ETInt:
		outputType = &substraitgo.Type{
			Kind: &substraitgo.Type_I64_{
				I64: &substraitgo.Type_I64{},
			},
		}
	case types.ETReal:
		outputType = &substraitgo.Type{
			Kind: &substraitgo.Type_Fp64{
				Fp64: &substraitgo.Type_FP64{},
			},
		}
	}
	switch sf.FuncName.L {
	case "lt", "lte", "gt", "gte", "and", "or", "eq", "not":
		outputType = &substraitgo.Type{
			Kind: &substraitgo.Type_Bool{
				Bool: &substraitgo.Type_Boolean{
					Nullability: substraitgo.Type_NULLABILITY_NULLABLE,
				},
			},
		}
	}
	return
}

func sigNameAdjustor(name string) string {
	switch name {
	case "plus":
		return "plus:opt_"
	case "multiply":
		return "multiply:opt_"
	case "minus":
		return "minus:opt_"
	default:
		return name + ":"
	}
}

func (h *SubstraitHandler) scalarFuncToSubstraitgoExpr(sf *expression.ScalarFunction) (*substraitgo.Expression, error) {
	funcSig := tiDBFuncNameToVeloxFuncName[sf.FuncName.L]
	var offsets []int32
	for _, arg := range sf.GetArgs() {
		col, ok := arg.(*expression.Column)
		if !ok {
			return nil, errors.Errorf("fail to convert proj to substrait, only support arg of column type")
		}
		offsets = append(offsets, int32(col.Index))
	}
	funcSig = sigNameAdjustor(funcSig) + getSubStraitType(sf.GetArgs()[0].GetType().GetType()) + "_" + getSubStraitType(sf.GetArgs()[1].GetType().GetType())

	sExpr := &substraitgo.Expression{
		RexType: &substraitgo.Expression_ScalarFunction_{
			ScalarFunction: &substraitgo.Expression_ScalarFunction{
				// function anchor
				FunctionReference: h.insertSig(funcSig),
				// function args
				Arguments:  getSubstraitPBFunctionArguments(offsets),
				OutputType: scalarFuncFieldTypeToSubstraitOutputType(sf),
			},
		},
	}
	return sExpr, nil
}

func (h *SubstraitHandler) columnToSubstraitgoExpr(col *expression.Column) *substraitgo.Expression {
	sExpr := &substraitgo.Expression{
		RexType: &substraitgo.Expression_Selection{
			Selection: &substraitgo.Expression_FieldReference{
				ReferenceType: &substraitgo.Expression_FieldReference_DirectReference{
					DirectReference: &substraitgo.Expression_ReferenceSegment{
						ReferenceType: &substraitgo.Expression_ReferenceSegment_StructField_{
							StructField: &substraitgo.Expression_ReferenceSegment_StructField{
								Field: int32(col.Index),
							},
						},
					},
				},
			},
		},
	}
	return sExpr
}

func tidbConstToVeloxExpressionLiteral(cont *expression.Constant) (el *substraitgo.Expression_Literal) {
	switch cont.GetType().GetType() {
	case mysql.TypeLonglong:
		el = &substraitgo.Expression_Literal{
			LiteralType: &substraitgo.Expression_Literal_I64{
				I64: cont.Value.GetInt64(),
			},
		}
	case mysql.TypeFloat:
		el = &substraitgo.Expression_Literal{
			LiteralType: &substraitgo.Expression_Literal_Fp32{
				Fp32: cont.Value.GetFloat32(),
			},
		}
	case mysql.TypeDouble:
		el = &substraitgo.Expression_Literal{
			LiteralType: &substraitgo.Expression_Literal_Fp64{
				Fp64: cont.Value.GetFloat64(),
			},
		}
	}
	return
}

func (h *SubstraitHandler) constToSubstraitgoExpr(cont *expression.Constant) *substraitgo.Expression {
	sExpr := &substraitgo.Expression{
		RexType: &substraitgo.Expression_Literal_{
			Literal: tidbConstToVeloxExpressionLiteral(cont),
		},
	}
	return sExpr
}

func (h *SubstraitHandler) buildSubstraitProjExpression(tidbExpr []expression.Expression) (substraitgoExprs []*substraitgo.Expression, err error) {
	for _, expr := range tidbExpr {
		var sExpr *substraitgo.Expression
		var err error
		switch x := expr.(type) {
		case *expression.Column:
			sExpr = h.columnToSubstraitgoExpr(x)
		case *expression.Constant:
			sExpr = h.constToSubstraitgoExpr(x)
		case *expression.ScalarFunction:
			sExpr, err = h.scalarFuncToSubstraitgoExpr(x)
			if err != nil {
				return nil, err
			}
		}
		substraitgoExprs = append(substraitgoExprs, sExpr)
	}
	return substraitgoExprs, nil
}

var tiDBFuncNameToVeloxFuncName = map[string]string{
	ast.LE:       "lte",
	ast.LT:       "lt",
	ast.GE:       "gte",
	ast.GT:       "gt",
	ast.Plus:     "plus",
	ast.Minus:    "minus",
	ast.Mul:      "multiply",
	ast.Mod:      "mod",
	ast.EQ:       "eq",
	ast.UnaryNot: "not",
	ast.And:      "and",
	ast.LogicOr:  "or",
}

// ToSubstraitPB implements the substraitgo interface.
func (p *PhysicalProjection) ToSubstraitPB(ctx sessionctx.Context, ssHandler *SubstraitHandler) (*substraitgo.Rel, error) {
	childRel, err := p.children[0].ToSubstraitPB(ctx, ssHandler)
	if err != nil {
		return nil, err
	}
	sspb := &substraitgo.ProjectRel{
		Common: &substraitgo.RelCommon{
			EmitKind: &substraitgo.RelCommon_Direct_{
				Direct: &substraitgo.RelCommon_Direct{},
			},
		},
		Input: childRel,
	}
	sspbExpression, err := ssHandler.buildSubstraitProjExpression(p.Exprs)
	if err != nil {
		logutil.BgLogger().Error("error", zap.Error(err))
		return nil, err
	}
	sspb.Expressions = sspbExpression
	logutil.BgLogger().Warn("expression", zap.Int("len", len(sspb.Expressions)))
	// extract function map (assume there are all simple functions)
	return &substraitgo.Rel{
		RelType: &substraitgo.Rel_Project{Project: sspb},
	}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalProjection) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	exprs, err := expression.ExpressionsToPBList(sc, p.Exprs, client)
	if err != nil {
		return nil, err
	}
	projExec := &tipb.Projection{
		Exprs: exprs,
	}
	executorID := ""
	if storeType == kv.TiFlash || storeType == kv.TiKV {
		var err error
		projExec.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	} else {
		return nil, errors.Errorf("the projection can only be pushed down to TiFlash or TiKV now, not %s", storeType.Name())
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeProjection, Projection: projExec, ExecutorId: &executorID}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalTopN) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	topNExec := &tipb.TopN{
		Limit: p.Count,
	}
	for _, item := range p.ByItems {
		topNExec.OrderBy = append(topNExec.OrderBy, expression.SortByItemToPB(sc, client, item.Expr, item.Desc))
	}
	executorID := ""
	if storeType == kv.TiFlash {
		var err error
		topNExec.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeTopN, TopN: topNExec, ExecutorId: &executorID}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalLimit) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	limitExec := &tipb.Limit{
		Limit: p.Count,
	}
	executorID := ""
	if storeType == kv.TiFlash {
		var err error
		limitExec.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeLimit, Limit: limitExec, ExecutorId: &executorID}, nil
}

func (p *PhysicalTableReader) ToSubstraitPB(ctx sessionctx.Context, ssHandler *SubstraitHandler) (rel *substraitgo.Rel, err error) {
	tableScan, ok := p.TablePlans[0].(*PhysicalTableScan)
	if !ok {
		return nil, nil
	}
	readRel := &substraitgo.ReadRel{}
	var baseSchemaNames []string
	var baseSchemaTypeStruct []*substraitgo.Type
	for _, col := range tableScan.Table.Cols() {
		// Velox is case-sensitive. We suppose all the table name and col name is lowercase.
		baseSchemaNames = append(baseSchemaNames, col.Name.L)
		colFT := types.TiDBFieldTypeToSubstraitType(&col.FieldType)
		if colFT == nil {
			return nil, nil
		}
		baseSchemaTypeStruct = append(baseSchemaTypeStruct, colFT)
	}
	readRel.BaseSchema = &substraitgo.NamedStruct{Names: baseSchemaNames, Struct: &substraitgo.Type_Struct{Types: baseSchemaTypeStruct}}
	readRel.ReadType = &substraitgo.ReadRel_NamedTable_{
		NamedTable: &substraitgo.ReadRel_NamedTable{
			Names: []string{p.GetVeloxDataSourceID()},
		}}
	return &substraitgo.Rel{RelType: &substraitgo.Rel_Read{Read: readRel}}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalTableScan) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	if storeType == kv.TiFlash && p.Table.GetPartitionInfo() != nil && p.IsMPPOrBatchCop && p.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return p.partitionTableScanToPBForFlash(ctx)
	}
	tsExec := tables.BuildTableScanFromInfos(p.Table, p.Columns)
	tsExec.Desc = p.Desc
	keepOrder := p.KeepOrder
	tsExec.KeepOrder = &keepOrder
	tsExec.IsFastScan = &(ctx.GetSessionVars().TiFlashFastScan)

	if p.isPartition {
		tsExec.TableId = p.physicalTableID
	}
	executorID := ""
	if storeType == kv.TiFlash {
		executorID = p.ExplainID().String()

		telemetry.CurrentTiflashTableScanCount.Inc()
		if *(tsExec.IsFastScan) {
			telemetry.CurrentTiflashTableScanWithFastScanCount.Inc()
		}
	}
	err := SetPBColumnsDefaultValue(ctx, tsExec.Columns, p.Columns)
	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tsExec, ExecutorId: &executorID}, err
}

func (p *PhysicalTableScan) partitionTableScanToPBForFlash(ctx sessionctx.Context) (*tipb.Executor, error) {
	ptsExec := tables.BuildPartitionTableScanFromInfos(p.Table, p.Columns, ctx.GetSessionVars().TiFlashFastScan)
	telemetry.CurrentTiflashTableScanCount.Inc()
	if *(ptsExec.IsFastScan) {
		telemetry.CurrentTiflashTableScanWithFastScanCount.Inc()
	}
	ptsExec.Desc = p.Desc
	executorID := p.ExplainID().String()
	err := SetPBColumnsDefaultValue(ctx, ptsExec.Columns, p.Columns)
	return &tipb.Executor{Tp: tipb.ExecType_TypePartitionTableScan, PartitionTableScan: ptsExec, ExecutorId: &executorID}, err
}

// checkCoverIndex checks whether we can pass unique info to TiKV. We should push it if and only if the length of
// range and index are equal.
func checkCoverIndex(idx *model.IndexInfo, ranges []*ranger.Range) bool {
	// If the index is (c1, c2) but the query range only contains c1, it is not a unique get.
	if !idx.Unique {
		return false
	}
	for _, rg := range ranges {
		if len(rg.LowVal) != len(idx.Columns) {
			return false
		}
		for _, v := range rg.LowVal {
			if v.IsNull() {
				// a unique index may have duplicated rows with NULLs, so we cannot set the unique attribute to true when the range has NULL
				// please see https://github.com/pingcap/tidb/issues/29650 for more details
				return false
			}
		}
		for _, v := range rg.HighVal {
			if v.IsNull() {
				return false
			}
		}
	}
	return true
}

// FindColumnInfoByID finds ColumnInfo in cols by ID.
func FindColumnInfoByID(colInfos []*model.ColumnInfo, id int64) *model.ColumnInfo {
	for _, info := range colInfos {
		if info.ID == id {
			return info
		}
	}
	return nil
}

// ToPB generates the pb structure.
func (e *PhysicalExchangeSender) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	child, err := e.Children()[0].ToPB(ctx, kv.TiFlash)
	if err != nil {
		return nil, errors.Trace(err)
	}

	encodedTask := make([][]byte, 0, len(e.TargetTasks))

	for _, task := range e.TargetTasks {
		encodedStr, err := task.ToPB().Marshal()
		if err != nil {
			return nil, errors.Trace(err)
		}
		encodedTask = append(encodedTask, encodedStr)
	}

	hashCols := make([]expression.Expression, 0, len(e.HashCols))
	hashColTypes := make([]*tipb.FieldType, 0, len(e.HashCols))
	for _, col := range e.HashCols {
		hashCols = append(hashCols, col.Col)
		tp, err := expression.ToPBFieldTypeWithCheck(col.Col.RetType, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tp.Collate = col.CollateID
		hashColTypes = append(hashColTypes, tp)
	}
	allFieldTypes := make([]*tipb.FieldType, 0, len(e.Schema().Columns))
	for _, column := range e.Schema().Columns {
		pbType, err := expression.ToPBFieldTypeWithCheck(column.RetType, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		allFieldTypes = append(allFieldTypes, pbType)
	}
	hashColPb, err := expression.ExpressionsToPBList(ctx.GetSessionVars().StmtCtx, hashCols, ctx.GetClient())
	if err != nil {
		return nil, errors.Trace(err)
	}
	ecExec := &tipb.ExchangeSender{
		Tp:              e.ExchangeType,
		EncodedTaskMeta: encodedTask,
		PartitionKeys:   hashColPb,
		Child:           child,
		Types:           hashColTypes,
		AllFieldTypes:   allFieldTypes,
	}
	executorID := e.ExplainID().String()
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeExchangeSender,
		ExchangeSender:                ecExec,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: e.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.GetSessionVars().TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

// ToPB generates the pb structure.
func (e *PhysicalExchangeReceiver) ToPB(ctx sessionctx.Context, _ kv.StoreType) (*tipb.Executor, error) {
	encodedTask := make([][]byte, 0, len(e.Tasks))

	for _, task := range e.Tasks {
		encodedStr, err := task.ToPB().Marshal()
		if err != nil {
			return nil, errors.Trace(err)
		}
		encodedTask = append(encodedTask, encodedStr)
	}

	fieldTypes := make([]*tipb.FieldType, 0, len(e.Schema().Columns))
	for _, column := range e.Schema().Columns {
		pbType, err := expression.ToPBFieldTypeWithCheck(column.RetType, kv.TiFlash)
		if err != nil {
			return nil, errors.Trace(err)
		}
		fieldTypes = append(fieldTypes, pbType)
	}
	ecExec := &tipb.ExchangeReceiver{
		EncodedTaskMeta: encodedTask,
		FieldTypes:      fieldTypes,
	}
	executorID := e.ExplainID().String()
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeExchangeReceiver,
		ExchangeReceiver:              ecExec,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: e.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.GetSessionVars().TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalIndexScan) ToPB(_ sessionctx.Context, _ kv.StoreType) (*tipb.Executor, error) {
	columns := make([]*model.ColumnInfo, 0, p.schema.Len())
	tableColumns := p.Table.Cols()
	for _, col := range p.schema.Columns {
		if col.ID == model.ExtraHandleID {
			columns = append(columns, model.NewExtraHandleColInfo())
		} else if col.ID == model.ExtraPhysTblID {
			columns = append(columns, model.NewExtraPhysTblIDColInfo())
		} else if col.ID == model.ExtraPidColID {
			columns = append(columns, model.NewExtraPartitionIDColInfo())
		} else {
			columns = append(columns, FindColumnInfoByID(tableColumns, col.ID))
		}
	}
	var pkColIds []int64
	if p.NeedCommonHandle {
		pkColIds = tables.TryGetCommonPkColumnIds(p.Table)
	}
	idxExec := &tipb.IndexScan{
		TableId:          p.Table.ID,
		IndexId:          p.Index.ID,
		Columns:          util.ColumnsToProto(columns, p.Table.PKIsHandle),
		Desc:             p.Desc,
		PrimaryColumnIds: pkColIds,
	}
	if p.isPartition {
		idxExec.TableId = p.physicalTableID
	}
	unique := checkCoverIndex(p.Index, p.Ranges)
	idxExec.Unique = &unique
	return &tipb.Executor{Tp: tipb.ExecType_TypeIndexScan, IdxScan: idxExec}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalHashJoin) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()
	// todo: mpp na-key toPB.
	leftJoinKeys := make([]expression.Expression, 0, len(p.LeftJoinKeys))
	rightJoinKeys := make([]expression.Expression, 0, len(p.RightJoinKeys))
	for _, leftKey := range p.LeftJoinKeys {
		leftJoinKeys = append(leftJoinKeys, leftKey)
	}
	for _, rightKey := range p.RightJoinKeys {
		rightJoinKeys = append(rightJoinKeys, rightKey)
	}
	lChildren, err := p.children[0].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rChildren, err := p.children[1].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}

	left, err := expression.ExpressionsToPBList(sc, leftJoinKeys, client)
	if err != nil {
		return nil, err
	}
	right, err := expression.ExpressionsToPBList(sc, rightJoinKeys, client)
	if err != nil {
		return nil, err
	}

	leftConditions, err := expression.ExpressionsToPBList(sc, p.LeftConditions, client)
	if err != nil {
		return nil, err
	}
	rightConditions, err := expression.ExpressionsToPBList(sc, p.RightConditions, client)
	if err != nil {
		return nil, err
	}

	var otherConditionsInJoin expression.CNFExprs
	var otherEqConditionsFromIn expression.CNFExprs
	/// For anti join, equal conditions from `in` clause requires additional processing,
	/// for example, treat `null` as true.
	if p.JoinType == AntiSemiJoin || p.JoinType == AntiLeftOuterSemiJoin || p.JoinType == LeftOuterSemiJoin {
		for _, condition := range p.OtherConditions {
			if expression.IsEQCondFromIn(condition) {
				otherEqConditionsFromIn = append(otherEqConditionsFromIn, condition)
			} else {
				otherConditionsInJoin = append(otherConditionsInJoin, condition)
			}
		}
	} else {
		otherConditionsInJoin = p.OtherConditions
	}
	otherConditions, err := expression.ExpressionsToPBList(sc, otherConditionsInJoin, client)
	if err != nil {
		return nil, err
	}
	otherEqConditions, err := expression.ExpressionsToPBList(sc, otherEqConditionsFromIn, client)
	if err != nil {
		return nil, err
	}

	pbJoinType := tipb.JoinType_TypeInnerJoin
	switch p.JoinType {
	case LeftOuterJoin:
		pbJoinType = tipb.JoinType_TypeLeftOuterJoin
	case RightOuterJoin:
		pbJoinType = tipb.JoinType_TypeRightOuterJoin
	case SemiJoin:
		pbJoinType = tipb.JoinType_TypeSemiJoin
	case AntiSemiJoin:
		pbJoinType = tipb.JoinType_TypeAntiSemiJoin
	case LeftOuterSemiJoin:
		pbJoinType = tipb.JoinType_TypeLeftOuterSemiJoin
	case AntiLeftOuterSemiJoin:
		pbJoinType = tipb.JoinType_TypeAntiLeftOuterSemiJoin
	}
	probeFiledTypes := make([]*tipb.FieldType, 0, len(p.EqualConditions))
	buildFiledTypes := make([]*tipb.FieldType, 0, len(p.EqualConditions))
	for _, equalCondition := range p.EqualConditions {
		retType := equalCondition.RetType.Clone()
		chs, coll := equalCondition.CharsetAndCollation()
		retType.SetCharset(chs)
		retType.SetCollate(coll)
		ty, err := expression.ToPBFieldTypeWithCheck(retType, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		probeFiledTypes = append(probeFiledTypes, ty)
		buildFiledTypes = append(buildFiledTypes, ty)
	}
	// todo: arenatlx, push down hash join
	join := &tipb.Join{
		JoinType:                pbJoinType,
		JoinExecType:            tipb.JoinExecType_TypeHashJoin,
		InnerIdx:                int64(p.InnerChildIdx),
		LeftJoinKeys:            left,
		RightJoinKeys:           right,
		ProbeTypes:              probeFiledTypes,
		BuildTypes:              buildFiledTypes,
		LeftConditions:          leftConditions,
		RightConditions:         rightConditions,
		OtherConditions:         otherConditions,
		OtherEqConditionsFromIn: otherEqConditions,
		Children:                []*tipb.Executor{lChildren, rChildren},
	}

	executorID := p.ExplainID().String()
	return &tipb.Executor{Tp: tipb.ExecType_TypeJoin, Join: join, ExecutorId: &executorID}, nil
}

// ToPB converts FrameBound to tipb structure.
func (fb *FrameBound) ToPB(ctx sessionctx.Context) (*tipb.WindowFrameBound, error) {
	pbBound := &tipb.WindowFrameBound{
		Type:      tipb.WindowBoundType(fb.Type),
		Unbounded: fb.UnBounded,
	}
	offset := fb.Num
	pbBound.Offset = &offset

	calcFuncs, err := expression.ExpressionsToPBList(ctx.GetSessionVars().StmtCtx, fb.CalcFuncs, ctx.GetClient())
	if err != nil {
		return nil, err
	}

	pbBound.CalcFuncs = calcFuncs
	return pbBound, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalWindow) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()

	windowExec := &tipb.Window{}

	windowExec.FuncDesc = make([]*tipb.Expr, 0, len(p.WindowFuncDescs))
	for _, desc := range p.WindowFuncDescs {
		windowExec.FuncDesc = append(windowExec.FuncDesc, aggregation.WindowFuncToPBExpr(ctx, client, desc))
	}
	for _, item := range p.PartitionBy {
		windowExec.PartitionBy = append(windowExec.PartitionBy, expression.SortByItemToPB(sc, client, item.Col.Clone(), item.Desc))
	}
	for _, item := range p.OrderBy {
		windowExec.OrderBy = append(windowExec.OrderBy, expression.SortByItemToPB(sc, client, item.Col.Clone(), item.Desc))
	}

	if p.Frame != nil {
		windowExec.Frame = &tipb.WindowFrame{
			Type: tipb.WindowFrameType(p.Frame.Type),
		}
		if p.Frame.Start != nil {
			start, err := p.Frame.Start.ToPB(ctx)
			if err != nil {
				return nil, err
			}
			windowExec.Frame.Start = start
		}
		if p.Frame.End != nil {
			end, err := p.Frame.End.ToPB(ctx)
			if err != nil {
				return nil, err
			}
			windowExec.Frame.End = end
		}
	}

	var err error
	windowExec.Child, err = p.children[0].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	executorID := p.ExplainID().String()
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeWindow,
		Window:                        windowExec,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: p.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.GetSessionVars().TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalSort) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	if !p.IsPartialSort {
		return nil, errors.Errorf("sort %s can't convert to pb, because it isn't a partial sort", p.basePlan.ExplainID())
	}

	sc := ctx.GetSessionVars().StmtCtx
	client := ctx.GetClient()

	sortExec := &tipb.Sort{}
	for _, item := range p.ByItems {
		sortExec.ByItems = append(sortExec.ByItems, expression.SortByItemToPB(sc, client, item.Expr, item.Desc))
	}
	isPartialSort := p.IsPartialSort
	sortExec.IsPartialSort = &isPartialSort

	var err error
	sortExec.Child, err = p.children[0].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	executorID := p.ExplainID().String()
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeSort,
		Sort:                          sortExec,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: p.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.GetSessionVars().TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

// SetPBColumnsDefaultValue sets the default values of tipb.ColumnInfos.
func SetPBColumnsDefaultValue(ctx sessionctx.Context, pbColumns []*tipb.ColumnInfo, columns []*model.ColumnInfo) error {
	for i, c := range columns {
		// For virtual columns, we set their default values to NULL so that TiKV will return NULL properly,
		// They real values will be compute later.
		if c.IsGenerated() && !c.GeneratedStored {
			pbColumns[i].DefaultVal = []byte{codec.NilFlag}
		}
		if c.GetOriginDefaultValue() == nil {
			continue
		}

		sessVars := ctx.GetSessionVars()
		originStrict := sessVars.StrictSQLMode
		sessVars.StrictSQLMode = false
		d, err := table.GetColOriginDefaultValue(ctx, c)
		sessVars.StrictSQLMode = originStrict
		if err != nil {
			return err
		}

		pbColumns[i].DefaultVal, err = tablecodec.EncodeValue(sessVars.StmtCtx, nil, d)
		if err != nil {
			return err
		}
	}
	return nil
}
