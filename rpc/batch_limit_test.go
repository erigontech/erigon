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
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

// postBatch sends batch to a server whose batch limit is 2 and returns the answer.
func postBatch(t *testing.T, batch string) string {
	srv := newTestServer(log.New())
	defer srv.Stop()
	srv.SetBatchLimit(2)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL, strings.NewReader(batch))
	require.NoError(t, err)
	req.Header.Set("Content-Type", contentType)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(body)
}

// TestBatchLimit_HTTP_Exceeded pins the answer to a batch over the limit: one error that
// reports the batch size and carries the id of the first call.
func TestBatchLimit_HTTP_Exceeded(t *testing.T) {
	body := postBatch(t, `[{"jsonrpc":"2.0","method":"test_echo","params":["a",1]},`+
		`{"jsonrpc":"2.0","id":7,"method":"test_echo","params":["b",2]},`+
		`{"jsonrpc":"2.0","id":8,"method":"test_echo","params":["c",3]}]`)
	require.JSONEq(t, `[{"jsonrpc":"2.0","id":7,"error":{"code":-32600,"message":"batch limit 2 exceeded (can increase by --rpc.batch.limit). Requested batch of size: 3"}}]`, body)
}

// TestBatchLimit_HTTP_ExceededWithoutCall pins that the error to a batch with no call
// still has an id, a null one.
func TestBatchLimit_HTTP_ExceededWithoutCall(t *testing.T) {
	body := postBatch(t, `[{"jsonrpc":"2.0","method":"test_echo"},{"jsonrpc":"2.0","method":"test_echo"},{"jsonrpc":"2.0","method":"test_echo"}]`)
	require.JSONEq(t, `[{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"batch limit 2 exceeded (can increase by --rpc.batch.limit). Requested batch of size: 3"}}]`, body)
}

// TestBatchLimitWebsocketBudgetHeldUntilAnswered pins that a websocket batch over the
// limit stays charged to the ingress budget until its error, which carries the batch's id,
// is written.
func TestBatchLimitWebsocketBudgetHeldUntilAnswered(t *testing.T) {
	logger := log.New()
	srv := newTestServer(logger)
	defer srv.Stop()
	srv.SetBatchLimit(1)
	httpsrv := httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
	defer httpsrv.Close()

	conn, resp, err := websocket.Dial(t.Context(), "ws:"+strings.TrimPrefix(httpsrv.URL, "http:"), nil)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		t.Fatalf("can't dial: %v", err)
	}
	defer func() { _ = conn.CloseNow() }()
	conn.SetReadLimit(-1)
	// The id is larger than the socket buffers, so the error is not written while the
	// test does not read.
	id := `"` + strings.Repeat("x", 24<<20) + `"`
	msg := `[{"jsonrpc":"2.0","id":` + id + `,"method":"test_echo"},{"jsonrpc":"2.0","id":2,"method":"test_echo"}]`
	require.NoError(t, conn.Write(t.Context(), websocket.MessageText, []byte(msg)))

	charged := int64(len(msg)) - smallBodyLimit
	require.Eventually(t, func() bool { return srv.ingress.used.Load() == charged }, 5*time.Second, time.Millisecond)
	require.Never(t, func() bool { return srv.ingress.used.Load() != charged }, 200*time.Millisecond, time.Millisecond)

	_, answer, err := conn.Read(t.Context())
	require.NoError(t, err)
	require.Contains(t, string(answer), id)
	require.Eventually(t, func() bool { return srv.ingress.used.Load() == 0 }, 5*time.Second, time.Millisecond)
}

// TestParseMessageOverBatchLimitDoesNotAllocateElements pins that a batch over the limit is
// rejected without a message per element, so a large body of small elements stays cheap.
func TestParseMessageOverBatchLimitDoesNotAllocateElements(t *testing.T) {
	for name, elem := range map[string]string{
		"plain keys":   `{"jsonrpc":"2.0","method":"test_echo"},`,
		"escaped keys": `{"jsonrpc":"2.0","\u006dethod":"test_echo","\u0069d":{}},`,
	} {
		t.Run(name, func(t *testing.T) {
			raw := json.RawMessage("[" + strings.Repeat(elem, 10_000) + `{"jsonrpc":"2.0","id":7,"method":"test_echo"}]`)
			allocs := testing.AllocsPerRun(10, func() {
				_, _, err := parseMessage(raw, 100)
				require.ErrorAs(t, err, new(*batchTooLargeError))
			})
			require.LessOrEqual(t, allocs, 10.0)
		})
	}
}

// FuzzFirstCallID pins that the id the batch-too-large error carries is the one of the
// first element the handler would take for a call.
func FuzzFirstCallID(f *testing.F) {
	for _, seed := range []string{
		`[{"id":1,"method":"a"}]`,
		`[{"method":"a"},{"id":2,"method":"b"}]`,
		`[{"id":1,"method":"a","method":null},{"id":2,"method":"b"}]`,
		`[{"id":1,"method":"a","method":5},{"id":2,"method":"b"}]`,
		`[{"id":1,"method":""},{"id":2,"method":"\u0000"}]`,
		`[{"\u0069d":1,"method":"a"}]`,
		`[{"id":1,"\u006dethod":"a","method":null}]`,
		`[{"id":{},"method":"a"},{"id":null,"method":"b"},{"id":[1],"method":"c"}]`,
		`[null,1,"x",{"id":3,"method":"a"}]`,
		`[{"id":1,"id":null,"method":"a"}]`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if !json.Valid([]byte(raw)) || !isBatch([]byte(raw)) {
			return
		}
		msgs, _, err := parseMessage(json.RawMessage(raw), 0)
		require.NoError(t, err)
		var want json.RawMessage
		for _, msg := range msgs {
			if msg != nil && msg.isCall() {
				want = msg.ID
				break
			}
		}
		require.Equal(t, string(want), string(firstCallID([]byte(raw))))
	})
}
