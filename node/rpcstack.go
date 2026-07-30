// Copyright 2020 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/gzip"
	"github.com/rs/cors"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/pool"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/rpc"
)

// NewHTTPHandlerStack returns wrapped http-related handlers.
// When tagAsRPC is true and rpcConcurrencyLimit > 0, enforces admission control
// (503 if inflight > limit) to prevent goroutine pile-up under load.
func NewHTTPHandlerStack(srv http.Handler, cors []string, vhosts []string, compression bool, rpcConcurrencyLimit int64, tagAsRPC bool) http.Handler {
	handler := newCorsHandler(srv, cors)
	handler = newVHostHandler(vhosts, handler)
	if compression {
		handler = newGzipHandler(handler)
	}
	if tagAsRPC {
		handler = newRPCAdmissionHandler(rpcConcurrencyLimit, handler)
	}
	return handler
}

// rpcAdmissionHandler limits the number of concurrent HTTP RPC requests.
// Requests that exceed the limit receive an immediate HTTP 503 without going
// through CORS, gzip, or JSON decoding.
type rpcAdmissionHandler struct {
	inflight atomic.Int64
	limit    int64
	next     http.Handler
}

var rpcAdmissionRejected = metrics.GetOrCreateCounter(`rpc_admission_rejected_total`)
var wsConnectionRejected = metrics.GetOrCreateCounter(`ws_connection_rejected_total`)

func newRPCAdmissionHandler(limit int64, next http.Handler) http.Handler {
	return &rpcAdmissionHandler{limit: limit, next: next}
}

func (h *rpcAdmissionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.limit > 0 {
		if h.inflight.Add(1) > h.limit {
			h.inflight.Add(-1)
			rpcAdmissionRejected.Inc()
			rpc.WriteOverloadedResponse(w)
			return
		}
		defer h.inflight.Add(-1)
	}
	ctx := r.Context()
	if h.limit > 0 {
		ctx = kv.WithNonBlockingAcquire(ctx)
	}
	h.next.ServeHTTP(w, r.WithContext(ctx))
}

// NewWSConnectionLimiter wraps next so that at most limit concurrent WebSocket
// connections are served. Connections beyond the limit receive HTTP 503. If
// limit is 0, next is returned unwrapped.
func NewWSConnectionLimiter(limit int64, next http.Handler) http.Handler {
	if limit <= 0 {
		return next
	}
	return &wsConnectionLimiter{limit: limit, next: next}
}

type wsConnectionLimiter struct {
	count atomic.Int64
	limit int64
	next  http.Handler
}

func (h *wsConnectionLimiter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.count.Add(1) > h.limit {
		h.count.Add(-1)
		wsConnectionRejected.Inc()
		rpc.WriteOverloadedResponse(w)
		return
	}
	defer h.count.Add(-1)
	h.next.ServeHTTP(w, r)
}

func newCorsHandler(srv http.Handler, allowedOrigins []string) http.Handler {
	// disable CORS support if user has not specified a custom CORS configuration
	if len(allowedOrigins) == 0 {
		return srv
	}
	c := cors.New(cors.Options{
		AllowedOrigins: allowedOrigins,
		AllowedMethods: []string{http.MethodPost, http.MethodGet},
		AllowedHeaders: []string{"*"},
		MaxAge:         600,
	})
	return c.Handler(srv)
}

// virtualHostHandler is a handler which validates the Host-header of incoming requests.
// Using virtual hosts can help prevent DNS rebinding attacks, where a 'random' domain name points to
// the service ip address (but without CORS headers). By verifying the targeted virtual host, we can
// ensure that it's a destination that the node operator has defined.
type virtualHostHandler struct {
	vhosts map[string]struct{}
	next   http.Handler
}

func newVHostHandler(vhosts []string, next http.Handler) http.Handler {
	vhostMap := make(map[string]struct{})
	for _, allowedHost := range vhosts {
		vhostMap[strings.ToLower(allowedHost)] = struct{}{}
	}
	return &virtualHostHandler{vhostMap, next}
}

// ServeHTTP serves JSON-RPC requests over HTTP, implements http.Handler
func (h *virtualHostHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// if r.Host is not set, we can continue serving since a browser would set the Host header
	if r.Host == "" {
		h.next.ServeHTTP(w, r)
		return
	}
	host, _, err := net.SplitHostPort(r.Host)
	if err != nil {
		// Either invalid (too many colons) or no port specified
		host = r.Host
	}
	host = strings.ToLower(host)
	if ipAddr := net.ParseIP(host); ipAddr != nil {
		// It's an IP address, we can serve that
		h.next.ServeHTTP(w, r)
		return

	}
	// Not an IP address, but a hostname. Need to validate
	if _, exist := h.vhosts["*"]; exist {
		h.next.ServeHTTP(w, r)
		return
	}
	if _, exist := h.vhosts["any"]; exist {
		h.next.ServeHTTP(w, r)
		return
	}
	if _, exist := h.vhosts[host]; exist {
		h.next.ServeHTTP(w, r)
		return
	}
	http.Error(w, "invalid host specified", http.StatusForbidden)
}

// minGzipBodySize is the minimum response body size to compress. Responses
// smaller than this are sent as-is: gzip framing overhead would exceed savings.
const minGzipBodySize = 1024

// Streaming responses compress at BestSpeed: bytes leave as they are produced,
// so per-flush latency matters more than ratio. Fully buffered responses can
// afford a higher level since nothing waits on incremental flushes.
const bufferedGzipLevel = 6

var gzStreamPool = sync.Pool{
	New: func() any { w, _ := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed); return w },
}

var gzBufferedPool = sync.Pool{
	New: func() any { w, _ := gzip.NewWriterLevel(io.Discard, bufferedGzipLevel); return w },
}

func putStreamGzip(gz *gzip.Writer) {
	gz.Reset(io.Discard)
	gzStreamPool.Put(gz)
}

func putBufferedGzip(gz *gzip.Writer) {
	gz.Reset(io.Discard)
	gzBufferedPool.Put(gz)
}

type gzipResponseWriter struct {
	buf    *bytes.Buffer
	gzw    *gzip.Writer
	status int
	http.ResponseWriter
}

func (w *gzipResponseWriter) WriteHeader(status int) {
	if w.gzw != nil {
		w.ResponseWriter.WriteHeader(status)
	} else {
		w.status = status
	}
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	if w.gzw != nil {
		return w.gzw.Write(b)
	}
	return w.buf.Write(b)
}

// Flush switches to streaming gzip on first call; subsequent calls flush incrementally.
func (w *gzipResponseWriter) Flush() {
	if w.gzw == nil {
		w.ResponseWriter.Header().Set("Content-Encoding", "gzip")
		w.ResponseWriter.Header().Del("Content-Length")
		if w.status != 0 {
			w.ResponseWriter.WriteHeader(w.status)
		}
		w.gzw = gzStreamPool.Get().(*gzip.Writer)
		w.gzw.Reset(w.ResponseWriter)
		if w.buf.Len() > 0 {
			_, _ = w.gzw.Write(w.buf.Bytes())
			w.buf.Reset()
		}
	}
	_ = w.gzw.Flush()
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// writeBufferedGzip compresses src fully before writing, so the response
// carries an exact Content-Length instead of chunked encoding.
func writeBufferedGzip(w http.ResponseWriter, src []byte, status int) {
	gz := gzBufferedPool.Get().(*gzip.Writer)
	defer putBufferedGzip(gz)

	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)
	gz.Reset(buf)
	_, _ = gz.Write(src)
	_ = gz.Close()

	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
	if status != 0 {
		w.WriteHeader(status)
	}
	w.Write(buf.Bytes()) //nolint:errcheck
}

func sendGzipResponse(w http.ResponseWriter, grw *gzipResponseWriter) {
	defer pool.PutBuffer(grw.buf)

	if grw.gzw != nil {
		defer putStreamGzip(grw.gzw)
		grw.gzw.Close() //nolint:errcheck
		return
	}

	src := grw.buf.Bytes()
	if len(src) < minGzipBodySize {
		w.Header().Set("Content-Length", strconv.Itoa(len(src)))
		if grw.status != 0 {
			w.WriteHeader(grw.status)
		}
		w.Write(src) //nolint:errcheck
		return
	}

	writeBufferedGzip(w, src, grw.status)
}

func newGzipHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}

		grw := &gzipResponseWriter{buf: pool.GetBuffer(), ResponseWriter: w}
		// The hook activates streaming mode before the first write; absent when gzip
		// is off so it cannot prematurely commit HTTP headers (e.g. 200 before 503).
		r = r.WithContext(rpc.WithGzipStreamingHook(r.Context(), grw.Flush))
		next.ServeHTTP(grw, r)
		sendGzipResponse(w, grw)
	})
}

// RegisterApisFromWhitelist checks the given modules' availability, generates a whitelist based on the allowed modules,
// and then registers all of the APIs exposed by the services.
func RegisterApisFromWhitelist(apis []rpc.API, modules []string, srv *rpc.Server, exposeAll bool, logger log.Logger) error {
	if bad, available := checkModuleAvailability(modules, apis); len(bad) > 0 {
		logger.Error("Non-existing modules in HTTP API list, please remove it", "non-existing", bad, "existing", available)
	}
	// Generate the whitelist based on the allowed modules
	whitelist := make(map[string]bool)
	for _, module := range modules {
		whitelist[module] = true
	}
	// Register all the APIs exposed by the services
	for _, api := range apis {
		if exposeAll || whitelist[api.Namespace] || (len(whitelist) == 0 && api.Public) {
			if err := srv.RegisterName(api.Namespace, api.Service); err != nil {
				return err
			}
		}
	}
	return nil
}
