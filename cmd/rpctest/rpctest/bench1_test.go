// Copyright 2024 The Erigon Authors
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

package rpctest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWritePatternWritesOneRequestPerLine(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp) // os.TempDir reads TMPDIR on Unix, TMP and TEMP on Windows
	t.Setenv("TMP", tmp)
	t.Setenv("TEMP", tmp)
	setRoutes("http://localhost:8545", "http://localhost:8546")
	resultsCh := make(chan CallResult)
	done := make(chan struct{})
	go func() {
		writePattern(true, []string{"eth_call"}, resultsCh)
		close(done)
	}()
	resultsCh <- CallResult{Target: Geth, Method: "eth_call", RequestID: 1, RequestBody: `{ "jsonrpc": "2.0", "method": "eth_call", "params": [{"to":"0x02"},"0x10"], "id":1}`}
	resultsCh <- CallResult{Target: Geth, Method: "eth_call", RequestID: 2, RequestBody: `{ "jsonrpc": "2.0", "method": "eth_call", "params": [{"to":"0x03"},"0x11"], "id":2}`}
	close(resultsCh)
	<-done

	dir := filepath.Join(tmp, "erigon_stress_test")
	data, err := os.ReadFile(filepath.Join(dir, "eth_call.jsonl"))
	require.NoError(t, err)
	require.Equal(t, []string{
		`{"jsonrpc":"2.0","method":"eth_call","params":[{"to":"0x02"},"0x10"],"id":1}`,
		`{"jsonrpc":"2.0","method":"eth_call","params":[{"to":"0x03"},"0x11"],"id":2}`,
	}, strings.Split(strings.TrimSuffix(string(data), "\n"), "\n"))

	for _, target := range []string{Geth, Erigon} {
		data, err := os.ReadFile(filepath.Join(dir, "vegeta_"+target+"_eth_call.txt"))
		require.NoError(t, err)
		targets := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
		require.Len(t, targets, 2)
		var vt struct {
			Method string `json:"method"`
			URL    string `json:"url"`
			Body   []byte `json:"body"`
		}
		require.NoError(t, json.Unmarshal([]byte(targets[0]), &vt))
		require.Equal(t, "POST", vt.Method)
		require.Equal(t, routes[target], vt.URL)
		require.JSONEq(t, `{"jsonrpc":"2.0","method":"eth_call","params":[{"to":"0x02"},"0x10"],"id":1}`, string(vt.Body))
	}
	require.FileExists(t, filepath.Join(dir, "results_"+Geth+"_eth_call.csv"))
}
