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

// package httpreqresp encapsulates eth2 beacon chain resp-resp into http
package httpreqresp

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/sentinel/communication"
)

const (
	ResponseCodeHeader = "Reqresp-Response-Code"
	PeerIdHeader       = "Reqresp-Peer-Id"
	TopicHeader        = "Reqresp-Topic"
	// MaxResponseBytesHeader carries the caller's upper bound on the response body size,
	// used to size the response-body cap. Absent or 0 falls back to the tight single-object cap.
	MaxResponseBytesHeader = "Reqresp-Max-Response-Bytes"
)

const (
	// maxResponseChunks is MAX_REQUEST_BLOCKS; with MaxChunkSize it sets the absolute
	// multi-chunk ceiling (maxMultiChunkResponse).
	maxResponseChunks = 1024
	// maxSingleObjectResponse bounds single-chunk protocols (status, ping, metadata,
	// goodbye, light-client singles), whose responses are at most tens of KiB.
	maxSingleObjectResponse = 1024 * 1024
)

// maxMultiChunkResponse is the on-wire MAX_REQUEST_BLOCKS × MAX_CHUNK_SIZE ceiling: the backstop a
// caller-supplied multi-chunk budget is clamped to, so a miscomputed or overflowed budget can't make
// the handler stream more than a spec-maximal response.
var maxMultiChunkResponse = communication.MaxWireResponseBytes(int(clparams.MaxChunkSize), maxResponseChunks)

// ErrResponseTooLarge is surfaced by the response body when a peer's response exceeds the per-topic
// size cap. Callers treat it as a peer failure rather than a valid (truncated) response.
var ErrResponseTooLarge = errors.New("reqresp: response exceeds size cap")

// maxResponseBodySize is the byte ceiling the caller may read for a response on the given topic.
// Single-object protocols, and any request that arrives without a caller budget (an empty request,
// or a multi-chunk caller that failed to size one), get the tight single-object cap — never the
// multi-GiB ceiling, so a missing budget surfaces as ErrResponseTooLarge rather than an OOM. A
// multi-chunk caller that does supply a budget gets it, clamped to maxMultiChunkResponse.
func maxResponseBodySize(topic string, maxBytes uint64) uint64 {
	if maxBytes == 0 || !communication.IsMultiChunkProtocol(topic) {
		return maxSingleObjectResponse
	}
	if maxBytes < maxMultiChunkResponse {
		return maxBytes
	}
	return maxMultiChunkResponse
}

// maxBytesReader streams up to its limit and then returns ErrResponseTooLarge instead of truncating
// at EOF, so an over-cap response is rejected explicitly rather than silently accepted. It mirrors
// net/http.MaxBytesReader without the ResponseWriter coupling.
type maxBytesReader struct {
	r   io.Reader
	n   int64 // bytes still allowed
	err error
}

func newMaxBytesReader(r io.Reader, limit int64) *maxBytesReader {
	return &maxBytesReader{r: r, n: limit}
}

func (m *maxBytesReader) Read(p []byte) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	// Read at most one byte past the remaining allowance — enough to tell "fits" from "too large".
	if int64(len(p))-1 > m.n {
		p = p[:m.n+1]
	}
	n, err := m.r.Read(p)
	if int64(n) <= m.n {
		m.n -= int64(n)
		m.err = err
		return n, err
	}
	n = int(m.n)
	m.n = 0
	m.err = ErrResponseTooLarge
	return n, m.err
}

// streamBody is the response body Do hands back for a successful request: it reads through the
// size cap and closes the libp2p stream when the caller closes the body.
type streamBody struct {
	io.Reader // the capped reader over the stream
	io.Closer // the stream
}

// Do performs an http request against the http handler.
// NOTE: this is actually very similar to the http.RoundTripper interface... maybe we should investigate using that.
/*

the following headers have meaning when passed in to the request:

		REQRESP-PEER-ID - the peer id to target for the request
		REQRESP-TOPIC - the topic to request with
		REQRESP-EXPECTED-CHUNKS - this is an integer, which will be multiplied by 10 to calculate the amount of seconds the peer has to respond with all the data
		REQRESP-FALLBACK-BODY - hex-encoded request body to send if the peer negotiates a non-preferred protocol
		Reqresp-Max-Response-Bytes - caller's upper bound on the response body size; sizes the per-topic response cap
*/
func Do(handler http.Handler, r *http.Request) (*http.Response, error) {
	resultCh := make(chan *http.Response, 1)
	go func() {
		w := &captureWriter{}
		// Publish the response as the goroutine unwinds, so a handler panic becomes a 500 rather
		// than crashing the process. The handler's deferred stream.Close runs first, so no stream leaks.
		defer func() {
			if rec := recover(); rec != nil {
				w = &captureWriter{}
				http.Error(w, fmt.Sprintf("reqresp: handler panic: %v", rec), http.StatusInternalServerError)
			}
			resultCh <- w.result() //nolint:bodyclose
		}()
		handler.ServeHTTP(w, r)
	}()
	select {
	case resp := <-resultCh:
		return resp, nil
	case <-r.Context().Done():
		// The handler may still publish a response whose body owns a live stream; drain and close
		// it once it arrives so the stream isn't leaked.
		go func() {
			if resp := <-resultCh; resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
		}()
		return nil, r.Context().Err()
	}
}

// captureWriter records a handler's status, headers and either a small buffered body (error
// responses written via http.Error) or a streaming body handed off for a successful response.
type captureWriter struct {
	header        http.Header
	body          []byte
	streamingBody io.ReadCloser
	code          int
	wroteHeader   bool
}

func (w *captureWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *captureWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.code = code
		w.wroteHeader = true
	}
}

func (w *captureWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	w.body = append(w.body, b...)
	return len(b), nil
}

// setStreamingBody hands a live response body off to Do without buffering it; the body owns its
// underlying resources and closes them on Close.
func (w *captureWriter) setStreamingBody(body io.ReadCloser) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	w.streamingBody = body
}

func (w *captureWriter) result() *http.Response {
	code := w.code
	if code == 0 {
		code = http.StatusOK
	}
	header := w.header
	if header == nil {
		header = make(http.Header)
	}
	body := w.streamingBody
	contentLength := int64(-1)
	if body == nil {
		body = io.NopCloser(bytes.NewReader(w.body))
		contentLength = int64(len(w.body))
	}
	return &http.Response{
		StatusCode:    code,
		Status:        strconv.Itoa(code) + " " + http.StatusText(code),
		Header:        header,
		Body:          body,
		ContentLength: contentLength,
	}
}

// Handles a request
func NewRequestHandler(host host.Host) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// get the peer parameters
		peerIdBase58 := r.Header.Get("REQRESP-PEER-ID")
		topic := r.Header.Get("REQRESP-TOPIC")
		chunkCount := r.Header.Get("REQRESP-EXPECTED-CHUNKS")
		chunks, _ := strconv.Atoi(chunkCount)
		maxBytes, _ := strconv.ParseUint(r.Header.Get(MaxResponseBytesHeader), 10, 64)
		// some sanity checking on chunks
		if chunks < 1 {
			chunks = 1
		}
		// idk why this would happen, so lets make sure it doesn't. future-proofing from bad input
		if chunks > 512 {
			chunks = 512
		}
		// read the base58 encoded peer id to know which we are trying to dial
		peerId, err := peer.Decode(peerIdBase58)
		if err != nil {
			http.Error(w, "Invalid Peer Id", http.StatusBadRequest)
			return
		}
		// Parse comma-separated topics for multistream protocol negotiation.
		// The first topic is preferred; libp2p picks the first one the peer supports.
		var protocolIDs []protocol.ID
		for _, t := range strings.Split(topic, ",") {
			if t = strings.TrimSpace(t); t != "" {
				protocolIDs = append(protocolIDs, protocol.ID(t))
			}
		}
		//  we can't connect to the peer - so we should disconnect them. send a code 4xx
		stream, err := host.NewStream(r.Context(), peerId, protocolIDs...)
		if err != nil {
			http.Error(w, "can't Connect to Peer: "+err.Error(), http.StatusBadRequest)
			return
		}
		// The successful path hands the stream off to the response body, which closes it; on every
		// other path this deferred close releases it.
		streamTransferred := false
		defer func() {
			if !streamTransferred {
				stream.Close()
			}
		}()
		// Update topic to the actually negotiated protocol so callers know which version was used.
		topic = string(stream.Protocol())
		// this write deadline is not part of the eth p2p spec, but we are implying it.
		stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
		var bytesWritten int64
		// When multiple protocols are offered, the request body matches the
		// first (preferred) protocol. If the peer negotiated a different
		// protocol, use the fallback body instead (hex-encoded in a header).
		body := io.Reader(r.Body)
		if len(protocolIDs) > 1 && stream.Protocol() != protocolIDs[0] {
			if fbHex := r.Header.Get("REQRESP-FALLBACK-BODY"); fbHex != "" {
				if fbBytes, err := hex.DecodeString(fbHex); err == nil {
					body = bytes.NewReader(fbBytes)
				}
			}
		}
		if body != nil {
			bytesWritten, err = io.Copy(stream, body)
			if err != nil {
				http.Error(w, "processing Stream: "+err.Error(), http.StatusBadRequest)
				return
			}
		}
		err = stream.CloseWrite()
		if err != nil {
			http.Error(w, "Close Write Side: "+err.Error(), http.StatusBadRequest)
			return
		}
		code := make([]byte, 1)
		// we have 5 seconds to read the next byte. this is the 5 TTFB_TIMEOUT in the spec
		stream.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := io.ReadFull(stream, code)
		if err != nil {
			http.Error(w, "Read Code: "+err.Error()+", readBytes="+strconv.Itoa(n)+", bytesWritten="+strconv.FormatInt(bytesWritten, 10)+", contentLength="+strconv.FormatInt(r.ContentLength, 10)+", topic="+topic+", peer="+peerIdBase58, http.StatusBadRequest)
			return
		}
		// The streaming hand-off needs the *captureWriter that Do supplies; a different writer means
		// the handler was invoked outside Do and can't carry a streaming body.
		cw, ok := w.(*captureWriter)
		if !ok {
			http.Error(w, "reqresp: handler must be invoked via httpreqresp.Do", http.StatusInternalServerError)
			return
		}
		// Success path: these headers (incl. the snappy encoding) describe the body we stream back,
		// so they are set only here, not on the error responses above.
		w.Header().Set("CONTENT-TYPE", "application/octet-stream")
		w.Header().Set("CONTENT-ENCODING", "snappy/stream")
		w.Header().Set("REQRESP-PEER-ID", peerIdBase58)
		w.Header().Set("REQRESP-TOPIC", topic)
		w.Header().Set("REQRESP-RESPONSE-CODE", strconv.Itoa(int(code[0])))
		// the deadline is 10 * expected chunk count, which the user can send. otherwise we will only wait 10 seconds
		// this is technically incorrect, and more aggressive than the network might like.
		stream.SetReadDeadline(time.Now().Add(10 * time.Second * time.Duration(chunks)))
		// Stream the response through a per-topic size cap instead of buffering it: a compliant
		// response passes through untouched, a flood is bounded at the cap and the caller sees
		// ErrResponseTooLarge.
		limit := int64(maxResponseBodySize(topic, maxBytes))
		cw.setStreamingBody(&streamBody{Reader: newMaxBytesReader(stream, limit), Closer: stream})
		streamTransferred = true
	}
}
