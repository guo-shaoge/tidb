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
	id             int64
}

// Make sure this id unique in one tidb-server in all operators of all queries.
func NewCGoVeloxDataSource(id int64) *CGoVeloxDataSource {
	s := C.get_tidb_data_source(C.long(id))
	return &CGoVeloxDataSource{
		tidbDataSource: &s,
		id:             id,
	}
}

func (s *CGoVeloxDataSource) Enqueue(data CGoRowVector) {
	C.enqueue_tidb_data_source(C.long(s.id), C.CGoRowVector(data))
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
