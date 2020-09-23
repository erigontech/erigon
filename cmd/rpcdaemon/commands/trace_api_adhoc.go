package commands

import (
	"context"
)

// Call Implements trace_call
func (api *TraceAPIImpl) Call(ctx context.Context, req TraceFilterRequest) ([]interface{}, error) {
	var stub []interface{}
	return stub, nil
}

// CallMany Implements trace_call
func (api *TraceAPIImpl) CallMany(ctx context.Context, req TraceFilterRequest) ([]interface{}, error) {
	var stub []interface{}
	return stub, nil
}

// RawTransaction Implements trace_rawtransaction
func (api *TraceAPIImpl) RawTransaction(ctx context.Context, req TraceFilterRequest) ([]interface{}, error) {
	var stub []interface{}
	return stub, nil
}

// ReplayBlockTransactions Implements trace_replayBlockTransactions
func (api *TraceAPIImpl) ReplayBlockTransactions(ctx context.Context, req TraceFilterRequest) ([]interface{}, error) {
	var stub []interface{}
	return stub, nil
}

// ReplayTransaction Implements trace_replaytransactions
func (api *TraceAPIImpl) ReplayTransaction(ctx context.Context, req TraceFilterRequest) ([]interface{}, error) {
	var stub []interface{}
	return stub, nil
}
