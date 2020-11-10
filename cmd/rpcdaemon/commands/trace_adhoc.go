package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// TraceCallParam (see SendTxArgs -- this allows optional prams plus don't use MixedcaseAddress
type TraceCallParam struct {
	_ *common.Address // from
	_ *common.Address // to
	_ *hexutil.Uint64 // gas
	_ *hexutil.Big    // gasPrice
	_ *hexutil.Big    // value
	_ *hexutil.Uint64 // nonce
	// We accept "data" and "input" for backwards-compatibility reasons.
	_ *hexutil.Bytes // data
	_ *hexutil.Bytes // input
}

// TraceCallParams array of callMany structs
type TraceCallParams []TraceCallParam

// Call implements trace_call.
func (api *TraceAPIImpl) Call(ctx context.Context, call TraceCallParam, traceTypes []string, blockNr *rpc.BlockNumberOrHash) ([]interface{}, error) {
	var stub []interface{}
	return stub, fmt.Errorf(NotImplemented, "trace_call")
}

// TODO(tjayrush) - try to use a concrete type here
// TraceCallManyParam array of callMany structs
// type TraceCallManyParam struct {
// 	obj        TraceCallParam
// 	traceTypes []string
// }

// TraceCallManyParams array of callMany structs
// type TraceCallManyParams struct {
// 	things []TraceCallManyParam
// }

// CallMany implements trace_callMany.
func (api *TraceAPIImpl) CallMany(ctx context.Context, calls []interface{}, blockNr *rpc.BlockNumberOrHash) ([]interface{}, error) {
	var stub []interface{}
	return stub, fmt.Errorf(NotImplemented, "trace_callMany")
}

// RawTransaction implements trace_rawTransaction.
func (api *TraceAPIImpl) RawTransaction(ctx context.Context, txHash common.Hash, traceTypes []string) ([]interface{}, error) {
	var stub []interface{}
	return stub, fmt.Errorf(NotImplemented, "trace_rawTransaction")
}

// ReplayBlockTransactions implements trace_replayBlockTransactions.
func (api *TraceAPIImpl) ReplayBlockTransactions(ctx context.Context, blockNr rpc.BlockNumber, traceTypes []string) ([]interface{}, error) {
	var stub []interface{}
	return stub, fmt.Errorf(NotImplemented, "trace_replayBlockTransactions")
}

// ReplayTransaction implements trace_replayTransaction.
func (api *TraceAPIImpl) ReplayTransaction(ctx context.Context, txHash common.Hash, traceTypes []string) ([]interface{}, error) {
	var stub []interface{}
	return stub, fmt.Errorf(NotImplemented, "trace_replayTransaction")
}
