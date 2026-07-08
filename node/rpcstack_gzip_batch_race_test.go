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

package node

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/rpc/jsonstream"
)

// gzipBatchStreamingService exposes a streamable method (last arg jsonstream.Stream) so
// that runMethod invokes the gzip-streaming hook, per rpc/service.go's streamable detection.
type gzipBatchStreamingService struct{}

func (gzipBatchStreamingService) Echo(s string, stream jsonstream.Stream) error {
	stream.WriteString(s)
	return nil
}

// TestGzipHandlerBatchConcurrentStreamableFlush reproduces a batch of streamable calls each
// running on its own goroutine (rpc/handler.go handleBatch), where every goroutine invokes
// the gzip-streaming flush hook installed on the shared request context. gzipResponseWriter.
// Flush is not safe for concurrent use, so calling it from multiple goroutines races on the
// underlying gzip.Writer and can dereference a nil flate compressor. Run with -race to
// observe the race (or a direct panic/crash from an unrecovered panic in a batch goroutine).
func TestGzipHandlerBatchConcurrentStreamableFlush(t *testing.T) {
	srv := newTestRPCServer(t)
	require.NoError(t, srv.RegisterName("test", gzipBatchStreamingService{}))

	handler := newGzipHandler(srv)

	const n = 8
	calls := make([]string, n)
	for i := range calls {
		calls[i] = fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"test_echo","params":["x"]}`, i+1)
	}
	body := "[" + strings.Join(calls, ",") + "]"

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
}
