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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// debug_traceCall takes the block selector as an optional parameter defaulting
// to latest, like the eth_ state methods.
func TestDebugTraceCallBlockParamDefaultsToLatest(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	debugAPI := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, &rpccfg.DebugApiConfig{})
	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	require.NoError(t, server.RegisterName("debug", PrivateDebugAPI(debugAPI)))
	client := rpc.DialInProc(server, log.New())
	t.Cleanup(func() {
		client.Close()
		server.Stop()
	})

	callArgs := map[string]any{
		"from":     "0x71562b71999873db5b286df957af199ec94617f7",
		"to":       "0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e",
		"gas":      "0x5208",
		"gasPrice": "0x0",
		"value":    "0x1",
	}

	var atLatest json.RawMessage
	require.NoError(t, client.CallContext(t.Context(), &atLatest, "debug_traceCall", callArgs, "latest"))
	require.NotEmpty(t, atLatest)

	for _, tc := range []struct {
		name   string
		params []any
	}{
		{name: "omitted", params: []any{callArgs}},
		{name: "null", params: []any{callArgs, nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got json.RawMessage
			require.NoError(t, client.CallContext(t.Context(), &got, "debug_traceCall", tc.params...))
			require.JSONEq(t, string(atLatest), string(got))
		})
	}
}
