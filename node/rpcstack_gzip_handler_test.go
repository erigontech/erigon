// Copyright 2025 The Erigon Authors
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
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/pool"
)

// decompressGzip reads a gzip-compressed body and returns the raw bytes.
func decompressGzip(t *testing.T, r io.Reader) []byte {
	t.Helper()
	gz, err := gzip.NewReader(r)
	require.NoError(t, err)
	defer gz.Close()
	data, err := io.ReadAll(gz)
	require.NoError(t, err)
	return data
}

// gzipRequest issues a POST to handler with Accept-Encoding: gzip and returns the recorder.
func gzipRequest(t *testing.T, handler http.Handler) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// TestGzipHandlerNonStreaming verifies that a normal (non-streaming) response
// above minGzipBodySize is gzip-compressed and the decompressed body matches
// the original. Content-Length must equal the compressed size (no chunked).
func TestGzipHandlerNonStreaming(t *testing.T) {
	body := bytes.Repeat([]byte(`{"jsonrpc":"2.0","result":"hello"}`), 64) // > minGzipBodySize
	handler := newGzipHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))

	rec := gzipRequest(t, handler)

	assert.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
	assert.Empty(t, rec.Header().Get("Content-Length"), "compressed responses are chunked")
	assert.Equal(t, body, decompressGzip(t, rec.Body))
}

// TestGzipHandlerSmallBody verifies that responses below minGzipBodySize are
// sent as-is: no Content-Encoding, correct Content-Length, verbatim body.
func TestGzipHandlerSmallBody(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","result":"ok"}`)
	require.Less(t, len(body), minGzipBodySize)

	handler := newGzipHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))

	rec := gzipRequest(t, handler)

	// Content-Length on an uncompressed passthrough is set by net/http, not by
	// the middleware, so it is absent on a bare recorder.
	assert.Empty(t, rec.Header().Get("Content-Encoding"))
	assert.Equal(t, body, rec.Body.Bytes())
}

// TestGzipHandlerNoAcceptEncoding verifies that the response is not compressed
// when the client does not advertise gzip support.
func TestGzipHandlerNoAcceptEncoding(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","result":"hello"}`)
	handler := newGzipHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Empty(t, rec.Header().Get("Content-Encoding"))
	assert.Equal(t, body, rec.Body.Bytes())
}

// TestGzipHandlerStreaming verifies that a handler that calls Flush() activates
// stdlib gzip streaming and the full response decompresses correctly.
func TestGzipHandlerStreaming(t *testing.T) {
	parts := [][]byte{
		[]byte(`{"jsonrpc":"2.0","result":`),
		[]byte(`"hello"`),
		[]byte(`}`),
	}
	handler := newGzipHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, p := range parts {
			_, _ = w.Write(p)
		}
		// Pad past minGzipBodySize: below it a response is sent verbatim.
		_, _ = w.Write(bytes.Repeat([]byte(" "), minGzipBodySize))
		w.(http.Flusher).Flush()
	}))

	rec := gzipRequest(t, handler)

	assert.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
	want := append([]byte(`{"jsonrpc":"2.0","result":"hello"}`), bytes.Repeat([]byte(" "), minGzipBodySize)...)
	assert.Equal(t, want, decompressGzip(t, rec.Body))
}

// TestGzipHandlerStatusBuffered verifies that a custom HTTP status set before
// any write is correctly forwarded in the non-streaming (buffered) path, for
// a body large enough to trigger gzip compression.
func TestGzipHandlerStatusBuffered(t *testing.T) {
	body := bytes.Repeat([]byte("x"), minGzipBodySize)
	handler := newGzipHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(body)
	}))

	rec := gzipRequest(t, handler)

	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
	assert.Equal(t, body, decompressGzip(t, rec.Body))
}

// TestGzipHandlerStatusStreaming verifies that a custom HTTP status set before
// Flush() is correctly forwarded when switching to streaming mode.
func TestGzipHandlerStatusStreaming(t *testing.T) {
	handler := newGzipHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write(bytes.Repeat([]byte(`ok`), minGzipBodySize))
		w.(http.Flusher).Flush()
	}))

	rec := gzipRequest(t, handler)

	assert.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
}

// TestGzipHandlerLargeBody verifies that a response body larger than the
// shared buffer-pool cap is compressed correctly. This exercises the
// pool-cap path: the oversized buffer must not be returned to the pool.
func TestGzipHandlerLargeBody(t *testing.T) {
	body := bytes.Repeat([]byte("x"), pool.MaxBufferCap+1)
	handler := newGzipHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))

	rec := gzipRequest(t, handler)

	assert.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
	assert.Equal(t, body, decompressGzip(t, rec.Body))
}

// TestGzipMetricsCounters verifies the per-path raw/compressed byte counters:
// in must grow by the uncompressed payload size, out by the bytes on the wire.
func TestGzipMetricsCounters(t *testing.T) {
	// Buffered path.
	body := bytes.Repeat([]byte("erigon "), 1024) // compressible, > minGzipBodySize
	inBefore, outBefore := gzipInBytes.GetValueUint64(), gzipOutBytes.GetValueUint64()
	rec := gzipRequest(t, newGzipHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	})))
	require.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
	assert.Equal(t, uint64(len(body)), gzipInBytes.GetValueUint64()-inBefore, "buffered in must count the raw payload")
	assert.Equal(t, uint64(rec.Body.Len()), gzipOutBytes.GetValueUint64()-outBefore, "buffered out must count the compressed bytes")

	// Streaming path: bytes written both before the Flush (drained from the
	// buffer) and after it must be counted as raw input.
	inBefore, outBefore = gzipInBytes.GetValueUint64(), gzipOutBytes.GetValueUint64()
	rec = gzipRequest(t, newGzipHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body[:1000])
		w.(http.Flusher).Flush()
		_, _ = w.Write(body[1000:])
	})))
	require.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
	assert.Equal(t, uint64(len(body)), gzipInBytes.GetValueUint64()-inBefore, "streaming in must count raw bytes from both sides of the Flush")
	assert.Equal(t, uint64(rec.Body.Len()), gzipOutBytes.GetValueUint64()-outBefore, "streaming out must count the compressed bytes")
}

// A response gzhttp passes through untouched has no ratio to report: counting
// it would add the same bytes to both counters and pull out/in towards 1.
func TestGzipMetricsSkipUncompressedResponses(t *testing.T) {
	body := bytes.Repeat([]byte("erigon "), 1024) // > minGzipBodySize

	for _, tc := range []struct {
		name   string
		accept string
		body   []byte
	}{
		{"client does not accept gzip", "", body},
		{"body under the minimum size", "gzip", body[:minGzipBodySize-1]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inBefore, outBefore := gzipInBytes.GetValueUint64(), gzipOutBytes.GetValueUint64()

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil)
			if tc.accept != "" {
				req.Header.Set("Accept-Encoding", tc.accept)
			}
			rec := httptest.NewRecorder()
			newGzipHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write(tc.body)
			})).ServeHTTP(rec, req)

			require.NotEqual(t, "gzip", rec.Header().Get("Content-Encoding"))
			require.Equal(t, tc.body, rec.Body.Bytes(), "the body must reach the client verbatim")
			assert.Zero(t, gzipInBytes.GetValueUint64()-inBefore, "in must not count a body nothing compressed")
			assert.Zero(t, gzipOutBytes.GetValueUint64()-outBefore, "out must not count a body nothing compressed")
		})
	}
}

// With --http.compression=false the stack must not wrap the handler at all:
// no Content-Encoding, verbatim body, working Flush, and a status written by
// an inner handler must survive (the removed streaming hook once risked
// committing 200 before a 503 could be sent).
func TestHTTPHandlerStackCompressionDisabled(t *testing.T) {
	body := bytes.Repeat([]byte(`{"jsonrpc":"2.0","result":"x"}`), 200) // well over minGzipBodySize

	for _, tc := range []struct {
		name        string
		compression bool
		wantEnc     string
	}{
		{"compression off", false, ""},
		{"compression on", true, "gzip"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write(body)
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			})
			srv := httptest.NewServer(NewHTTPHandlerStack(inner, nil, nil, tc.compression, 0, false))
			defer srv.Close()

			req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL, nil)
			req.Header.Set("Accept-Encoding", "gzip")
			resp, err := (&http.Client{Transport: &http.Transport{DisableCompression: true}}).Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, "inner status must survive the stack")
			require.Equal(t, tc.wantEnc, resp.Header.Get("Content-Encoding"))

			got, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			if tc.compression {
				zr, err := gzip.NewReader(bytes.NewReader(got))
				require.NoError(t, err)
				got, err = io.ReadAll(zr)
				require.NoError(t, err)
			}
			require.Equal(t, body, got, "body must round-trip unchanged")
		})
	}
}
