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

package rpc

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

// TestBatchLimit_HTTP_Exceeded pins the answer to a batch over the limit: one error that
// reports the batch size and carries the id of the first call.
func TestBatchLimit_HTTP_Exceeded(t *testing.T) {
	srv := newTestServer(log.New())
	defer srv.Stop()
	srv.SetBatchLimit(2)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	batch := `[{"jsonrpc":"2.0","method":"test_echo","params":["a",1]},` +
		`{"jsonrpc":"2.0","id":7,"method":"test_echo","params":["b",2]},` +
		`{"jsonrpc":"2.0","id":8,"method":"test_echo","params":["c",3]}]`
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL, strings.NewReader(batch))
	require.NoError(t, err)
	req.Header.Set("Content-Type", contentType)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.JSONEq(t, `[{"jsonrpc":"2.0","id":7,"error":{"code":-32600,"message":"batch limit 2 exceeded (can increase by --rpc.batch.limit). Requested batch of size: 3"}}]`, string(body))
}

// TestParseMessageOverBatchLimitDoesNotAllocateElements pins that a batch over the limit is
// rejected without a message per element, so a large body of small elements stays cheap.
func TestParseMessageOverBatchLimitDoesNotAllocateElements(t *testing.T) {
	raw := json.RawMessage("[" + strings.Repeat(`{"jsonrpc":"2.0","method":"test_echo"},`, 10_000) + `{"jsonrpc":"2.0","id":7,"method":"test_echo"}]`)
	allocs := testing.AllocsPerRun(10, func() {
		_, _, err := parseMessage(raw, 100)
		require.ErrorAs(t, err, new(*batchTooLargeError))
	})
	require.LessOrEqual(t, allocs, 10.0)
}
