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

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/rpc"
)

// fakeNode answers the four calls fetchTx makes and records their params.
func fakeNode(t *testing.T, results map[string]string) (*httptest.Server, map[string]json.RawMessage) {
	t.Helper()
	params := map[string]json.RawMessage{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		params[req.Method] = req.Params
		res, ok := results[req.Method]
		if !ok {
			res = "null"
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":` + res + `}`))
	}))
	t.Cleanup(srv.Close)
	return srv, params
}

func dial(t *testing.T, url string) *rpc.Client {
	c, err := rpc.DialHTTP(url, log.Root())
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

func TestFetchTxKeepsTheFourResults(t *testing.T) {
	const hash = "0xabc"
	srv, params := fakeNode(t, map[string]string{
		"eth_getTransactionByHash":  `{"hash":"0xabc","blockNumber":"0x10"}`,
		"eth_getTransactionReceipt": `{"gasUsed":"0x5208"}`,
		"eth_getBlockByNumber":      `{"number":"0x10"}`,
		"debug_traceTransaction":    `{"0x01":{"balance":"0x1"}}`,
	})

	raw, err := fetchTx(t.Context(), dial(t, srv.URL), hash)
	require.NoError(t, err)
	var got map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &got))
	require.JSONEq(t, `{"hash":"0xabc","blockNumber":"0x10"}`, string(got["tx"]))
	require.JSONEq(t, `{"gasUsed":"0x5208"}`, string(got["receipt"]))
	require.JSONEq(t, `{"number":"0x10"}`, string(got["block"]))
	require.JSONEq(t, `{"0x01":{"balance":"0x1"}}`, string(got["prestate"]))

	require.JSONEq(t, `["0x10",false]`, string(params["eth_getBlockByNumber"]), "the tx's own block")
	require.JSONEq(t, `["0xabc",{"tracer":"prestateTracer"}]`, string(params["debug_traceTransaction"]))
}

func TestFetchTxFailsOnAMissingTx(t *testing.T) {
	srv, _ := fakeNode(t, map[string]string{})
	_, err := fetchTx(t.Context(), dial(t, srv.URL), "0xabc")
	require.ErrorContains(t, err, "eth_getTransactionByHash: not found")
}

func TestFetchTxNames(t *testing.T) {
	list := filepath.Join(t.TempDir(), "txs.txt")
	require.NoError(t, os.WriteFile(list, []byte("# top rows\n6-cc33e185-chi-mint 0xcc33\n\n0xdead # unnamed\n"), 0o644))

	got, err := fetchTxNames([]string{"0xbeef"}, list)
	require.NoError(t, err)
	require.Equal(t, []txName{{"0xbeef", "0xbeef"}, {"6-cc33e185-chi-mint", "0xcc33"}, {"0xdead", "0xdead"}}, got)

	require.NoError(t, os.WriteFile(list, []byte("a b c\n"), 0o644))
	_, err = fetchTxNames(nil, list)
	require.Error(t, err)
}

// One tx the node cannot serve must not cost the rest of the list.
func TestFetchTxsWritesTheRestPastAFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
			Params []any  `json:"params"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		res := `{"blockNumber":"0x1"}`
		if req.Params[0] == "0xbad" {
			res = "null"
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":` + res + `}`))
	}))
	t.Cleanup(srv.Close)
	out := t.TempDir()

	err := fetchTxs(t.Context(), dial(t, srv.URL), []txName{{"0xbad", "0xbad"}, {"0xgood", "0xgood"}}, out)
	require.ErrorContains(t, err, "0xbad")
	require.FileExists(t, filepath.Join(out, "0xgood.json"))
	require.NoFileExists(t, filepath.Join(out, "0xbad.json"))
}
