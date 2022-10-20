package tidb_velox_wrapper

//#cgo CXXFLAGS: -std=c++17
//#cgo CFLAGS: -I/home/guojiangtao/code/velox
//#cgo LDFLAGS: -L${SRCDIR} -ltidb_velox -lvelox_vector -lstdc++ -lvelox_exception -lglog -lgflags -lfolly -lm -lunwind -ldouble-conversion -lfmt -liberty -lprotobuf -lz -llz4 -lsnappy -latomic -lboost_fiber -lboost_context -levent
//
//#include <velox/connectors/tidb/tidb_velox_wrapper.h>
//#include <velox/substrait/tidb_query_adapter_wrapper.h>
import "C"
import substraitgo "github.com/AilinKid/substrait-go/proto"
import "github.com/golang/protobuf/proto"
import "github.com/pingcap/tidb/util/chunk"
import "github.com/pingcap/tidb/types"
import "unsafe"
import "github.com/pingcap/tidb/parser/mysql"

type VeloxQueryCtx C.CGoTiDBQueryCtx

func MakeVeloxQueryCtx() VeloxQueryCtx {
	return VeloxQueryCtx(C.make_tidb_query_ctx())
}

func MakeVeloxTaskCursor(ctx VeloxQueryCtx, planPB string) {
	C.make_velox_task_cursor(C.CGoTiDBQueryCtx(ctx), C.CString(planPB), C.size_t(len(planPB)))
}

// gjt todo: return RowVector directly
func FetchVeloxOutput(ctx VeloxQueryCtx) {
	// return C.fetch_velox_output(ctx)
	C.fetch_velox_output(C.CGoTiDBQueryCtx(ctx))
}

func DestroyVeloxQueryCtx(ctx VeloxQueryCtx) {
	C.destroy_tidb_query_ctx(C.CGoTiDBQueryCtx(ctx))
}


/////////////////////////
// For VeloxExec, to transfer input data to Velox.
type CGoRowVector C.CGoRowVector

type CGoVeloxDataSource struct {
	tidbDataSource *C.CGoTiDBDataSource
	id             string
}

// Make sure this id unique in one tidb-server in all operators of all queries.
func NewCGoVeloxDataSource(ctx VeloxQueryCtx, id string) *CGoVeloxDataSource {
	s := C.get_tidb_data_source(C.CGoTiDBQueryCtx(ctx), C.CString(id), C.size_t(len(id)))
	return &CGoVeloxDataSource{
		tidbDataSource: &s,
		id:             id,
	}
}

func getVeloxVectorType(t *types.FieldType) C.TiDBColumnType {
	switch t.GetType() {
	case mysql.TypeLong:
		return C.kTiDBColumnTypeInt32
	case mysql.TypeLonglong:
		return C.kTiDBColumnTypeInt64
	case mysql.TypeFloat:
		return C.kTiDBColumnTypeFloat
	case mysql.TypeDouble:
		return C.kTiDBColumnTypeDouble
	default:
		panic("not supprt other types")
	}
}

func (s *CGoVeloxDataSource) EnqueueTiDBChunk(ctx VeloxQueryCtx, chk *chunk.Chunk, types []*types.FieldType) {
	numCol := chk.NumCols()
	var vecs []C.CGoStdVector
	var vecTypes []C.TiDBColumnType
	for i := 0; i < numCol; i++ {
		col := chk.Column(i)
		t := getVeloxVectorType(types[i])
		// gjt todo: col.offsets is ignored, because we only support fixed length type for now.
		vec := C.tidb_chunk_column_to_velox_vector(
			C.CGoTiDBQueryCtx(ctx), unsafe.Pointer(&col.Data()[0]), unsafe.Pointer(&col.NullBitmap()[0]),
			unsafe.Pointer(nil), C.size_t(col.Length()), t)
		vecs = append(vecs, vec)
		vecTypes = append(vecTypes, t)
	}

	C.enqueue_std_vectors(
		C.CGoTiDBQueryCtx(ctx), (*C.CGoStdVector)(&vecs[0]), 
		(*C.TiDBColumnType)(&vecTypes[0]), 
		C.size_t(len(vecs)), C.CString(s.id), C.size_t(len(s.id)))
}

func (s *CGoVeloxDataSource) Enqueue(ctx VeloxQueryCtx, data CGoRowVector) {
	C.enqueue_tidb_data_source(C.CGoTiDBQueryCtx(ctx), C.CString(s.id), C.size_t(len(s.id)), C.CGoRowVector(data))
}

func (s *CGoVeloxDataSource) Destroy() {
}

// For velox::runTiDBQuery, to transfer Substrait plan to Velox.
func RunTiDBQuery(planPB *substraitgo.Plan) {
	_, err := proto.Marshal(planPB)
	if err != nil {
		panic("123 Marshal planPB failed")
	}
	// gjt todo: remove this
	// C.run_tidb_query(C.CString(string(data)), C.size_t(len(data)))
}
