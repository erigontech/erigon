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

package engineapi

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/rpc"
)

// newEngineInProcClient serves e over the same registration path production uses.
func newEngineInProcClient(t *testing.T, e *EngineServer) *rpc.Client {
	t.Helper()
	srv := rpc.NewServer(1, false, false, false, e.logger, 0)
	t.Cleanup(srv.Stop)
	require.NoError(t, srv.RegisterAPI(e.engineAPI()))
	client := rpc.DialInProc(srv, e.logger)
	t.Cleanup(client.Close)
	return client
}

func TestEngineNamespaceServesOnlyTheSpecMethods(t *testing.T) {
	e := NewEngineServer(log.New(), &chain.Config{ChainName: "mainnet"}, nil, nil, false, true, false, false, nil, nil, 0, 0)
	client := newEngineInProcClient(t, e)

	var capabilities []string
	require.NoError(t, client.Call(&capabilities, "engine_exchangeCapabilities", []string{}))
	require.NotEmpty(t, capabilities)

	for _, method := range []string{
		"engine_setConsuming",
		"engine_setBeaconChainConfig",
		"engine_start",
		"engine_handleNewPayload",
		"engine_handleForkChoice",
		"engine_sSZRESTHandler",
	} {
		err := client.Call(new(any), method, false)
		require.ErrorContains(t, err, "does not exist/is not available", method)
	}
}
