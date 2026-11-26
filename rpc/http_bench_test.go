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
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
)

type discardResponseWriter struct{ header http.Header }

func (w *discardResponseWriter) Header() http.Header         { return w.header }
func (w *discardResponseWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *discardResponseWriter) WriteHeader(int)             {}

func BenchmarkServeHTTPSingleCall(b *testing.B) {
	srv := NewServer(50, false, false, false, log.New(), 0)
	defer srv.Stop()
	if err := srv.RegisterName("test", new(testService)); err != nil {
		b.Fatal(err)
	}
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"test_noArgsRets","params":[]}`)
	req := httptest.NewRequestWithContext(b.Context(), http.MethodPost, "/", nil)
	req.Header.Set("Content-Type", "application/json")
	reader := bytes.NewReader(body)
	w := &discardResponseWriter{header: http.Header{}}
	b.ReportAllocs()
	for b.Loop() {
		reader.Reset(body)
		req.Body = io.NopCloser(reader)
		clear(w.header)
		srv.ServeHTTP(w, req)
	}
}
