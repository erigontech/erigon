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
	"strconv"
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
	req := httptest.NewRequest(http.MethodPost, "/", nil)
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
	assert.Equal(t, strconv.Itoa(rec.Body.Len()), rec.Header().Get("Content-Length"))
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

	assert.Empty(t, rec.Header().Get("Content-Encoding"))
	assert.Equal(t, strconv.Itoa(len(body)), rec.Header().Get("Content-Length"))
	assert.Equal(t, body, rec.Body.Bytes())
}

// TestGzipHandlerNoAcceptEncoding verifies that the response is not compressed
// when the client does not advertise gzip support.
func TestGzipHandlerNoAcceptEncoding(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","result":"hello"}`)
	handler := newGzipHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))

	req := httptest.NewRequest(http.MethodPost, "/", nil)
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
		w.(http.Flusher).Flush() // activates streaming mode
		for _, p := range parts {
			_, _ = w.Write(p)
		}
	}))

	rec := gzipRequest(t, handler)

	assert.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
	want := []byte(`{"jsonrpc":"2.0","result":"hello"}`)
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
		w.(http.Flusher).Flush()
		_, _ = w.Write([]byte(`ok`))
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
	inBefore, outBefore := gzipBufferedInBytes.GetValueUint64(), gzipBufferedOutBytes.GetValueUint64()
	rec := gzipRequest(t, newGzipHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	})))
	require.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
	assert.Equal(t, uint64(len(body)), gzipBufferedInBytes.GetValueUint64()-inBefore, "buffered in must count the raw payload")
	assert.Equal(t, uint64(rec.Body.Len()), gzipBufferedOutBytes.GetValueUint64()-outBefore, "buffered out must count the compressed bytes")

	// Streaming path: bytes written both before the Flush (drained from the
	// buffer) and after it must be counted as raw input.
	inBefore, outBefore = gzipStreamingInBytes.GetValueUint64(), gzipStreamingOutBytes.GetValueUint64()
	rec = gzipRequest(t, newGzipHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body[:1000])
		w.(http.Flusher).Flush()
		_, _ = w.Write(body[1000:])
	})))
	require.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
	assert.Equal(t, uint64(len(body)), gzipStreamingInBytes.GetValueUint64()-inBefore, "streaming in must count raw bytes from both sides of the Flush")
	assert.Equal(t, uint64(rec.Body.Len()), gzipStreamingOutBytes.GetValueUint64()-outBefore, "streaming out must count the compressed bytes")
}

// TestGzipResponseWriterFlushActivatesStreaming is a unit test on
// gzipResponseWriter.Flush(): verifies it switches from buffered to streaming
// mode and that buffered bytes are drained into the gzip writer.
func TestGzipResponseWriterFlushActivatesStreaming(t *testing.T) {
	rec := httptest.NewRecorder()
	buf := &bytes.Buffer{}
	grw := &gzipResponseWriter{buf: buf, ResponseWriter: rec}

	// Write into the buffer before activating streaming.
	_, _ = grw.Write([]byte("pre-flush"))
	require.Nil(t, grw.gzw, "gzw must be nil before first Flush")

	grw.Flush()
	require.NotNil(t, grw.gzw, "Flush must activate the gzip writer")
	assert.Equal(t, 0, buf.Len(), "buf must be drained into gzw after Flush")

	// Write more data in streaming mode, then close.
	_, _ = grw.Write([]byte(" post-flush"))
	require.NoError(t, grw.gzw.Close())
	putStreamGzip(grw.gzw)

	got := decompressGzip(t, rec.Body)
	assert.Equal(t, []byte("pre-flush post-flush"), got)
}

// A response past maxBufferedGzipSize switches to the streaming path: it is
// served chunked without Content-Length, the way net/http, nginx and Caddy all
// frame a body whose size is not known before the headers go out. Smaller
// responses keep the exact Content-Length.
func TestGzipLargeResponseStreamsWithoutContentLength(t *testing.T) {
	for _, tc := range []struct {
		name          string
		size          int
		wantStreaming bool
	}{
		{"under threshold keeps Content-Length", 64 << 10, false},
		{"over threshold streams", maxBufferedGzipSize + (1 << 20), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := bytes.Repeat([]byte(`{"k":"0123456789abcdef"},`), tc.size/25)
			srv := httptest.NewServer(newGzipHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Write(payload) //nolint:errcheck
			})))
			defer srv.Close()

			req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
			req.Header.Set("Accept-Encoding", "gzip")
			resp, err := (&http.Client{Transport: &http.Transport{DisableCompression: true}}).Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
			if tc.wantStreaming {
				require.Empty(t, resp.Header.Get("Content-Length"), "streamed response must not declare a length")
			} else {
				require.NotEmpty(t, resp.Header.Get("Content-Length"), "buffered response must declare a length")
			}

			zr, err := gzip.NewReader(resp.Body)
			require.NoError(t, err)
			got, err := io.ReadAll(zr)
			require.NoError(t, err)
			require.Equal(t, payload, got, "decompressed body must be byte-identical")
		})
	}
}
