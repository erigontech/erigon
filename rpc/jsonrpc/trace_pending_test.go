// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// requirePendingRejected checks that err rejects "pending" as an invalid parameter (-32602).
func requirePendingRejected(t *testing.T, err error) {
	t.Helper()
	require.ErrorIs(t, err, errPendingNotSupported)
	var rpcErr rpc.Error
	require.ErrorAs(t, err, &rpcErr)
	require.Equal(t, rpc.ErrCodeInvalidParams, rpcErr.ErrorCode())
}

// Tracing methods with a block selector reject "pending": they replay on the
// committed view, which has no pending block. Accepting the tag would answer
// for the latest executed block and report it as pending.
func TestTracingRejectsPendingTag(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := context.Background()
	pendingNrOrHash := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)

	debugAPI := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, &rpccfg.DebugApiConfig{})
	traceAPI := newTraceApiForTest(m)

	t.Run("debug_traceCall", func(t *testing.T) {
		err := debugAPI.TraceCall(ctx, ethapi.CallArgs{}, &pendingNrOrHash, nil, jsonstream.New(io.Discard))
		requirePendingRejected(t, err)
	})

	t.Run("debug_traceCallMany", func(t *testing.T) {
		err := debugAPI.TraceCallMany(ctx, nil, StateContext{BlockNumber: pendingNrOrHash}, nil, jsonstream.New(io.Discard))
		requirePendingRejected(t, err)
	})

	t.Run("trace_call", func(t *testing.T) {
		_, err := traceAPI.Call(ctx, TraceCallParam{}, []string{TraceTypeTrace}, &pendingNrOrHash, nil)
		requirePendingRejected(t, err)
	})

	t.Run("trace_callMany", func(t *testing.T) {
		_, err := traceAPI.CallMany(ctx, json.RawMessage("[]"), &pendingNrOrHash, nil)
		requirePendingRejected(t, err)
	})

	t.Run("debug_traceBlockByNumber", func(t *testing.T) {
		err := debugAPI.TraceBlockByNumber(ctx, rpc.PendingBlockNumber, nil, jsonstream.New(io.Discard))
		requirePendingRejected(t, err)
	})

	t.Run("trace_block", func(t *testing.T) {
		_, err := traceAPI.Block(ctx, rpc.PendingBlockNumber, nil, nil)
		requirePendingRejected(t, err)
	})

	t.Run("trace_replayBlockTransactions", func(t *testing.T) {
		_, err := traceAPI.ReplayBlockTransactions(ctx, pendingNrOrHash, []string{TraceTypeTrace}, nil, nil)
		requirePendingRejected(t, err)
	})

	pending := rpc.PendingBlockNumber
	t.Run("trace_filter fromBlock", func(t *testing.T) {
		err := traceAPI.Filter(ctx, TraceFilterRequest{FromBlock: &pending}, nil, nil, jsonstream.New(io.Discard))
		requirePendingRejected(t, err)
	})

	t.Run("trace_filter toBlock", func(t *testing.T) {
		err := traceAPI.Filter(ctx, TraceFilterRequest{ToBlock: &pending}, nil, nil, jsonstream.New(io.Discard))
		requirePendingRejected(t, err)
	})
}
