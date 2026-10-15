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
package l1_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/l1test"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// TestRPCConformance is T-L1-1 against an Erigon L1 serving JSON-RPC.
func TestRPCConformance(t *testing.T) {
	m, model := l1test.ErigonL1(t)
	logger := log.New()
	api := jsonrpc.NewEthAPI(jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}),
		m.DB, nil, nil, nil, &rpccfg.EthApiConfig{
			GasCap:                  5_000_000,
			FeeCap:                  ethconfig.Defaults.RPCTxFeeCap,
			ReturnDataLimit:         100_000,
			RpcTxSyncDefaultTimeout: time.Second,
			RpcTxSyncMaxTimeout:     time.Second,
		}, logger)
	server := rpc.NewServer(50, false, false, true, logger, 100)
	require.NoError(t, server.RegisterName("eth", api))
	t.Cleanup(server.Stop)
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)

	r, err := l1.DialRPC(context.Background(), httpServer.URL, logger)
	require.NoError(t, err)
	t.Cleanup(r.Close)
	l1test.Conformance(t, r, model, l1test.SampleFilters)
}

func TestRPCErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		msg  string
		want error
	}{
		{"query returned more than 10000 results", l1.ErrRangeTooWide},
		{"Log response size exceeded. You can make eth_getLogs requests with up to a 2K block range", l1.ErrRangeTooWide},
		{"finalized block not found", l1.ErrNotFound},
		{"unknown block", l1.ErrNotFound},
	} {
		require.ErrorIs(t, l1.MapRPCError(errors.New(tc.msg), "call"), tc.want, tc.msg)
	}
	err := l1.MapRPCError(errors.New("connection refused"), "call")
	require.False(t, errors.Is(err, l1.ErrRangeTooWide) || errors.Is(err, l1.ErrNotFound))
}
