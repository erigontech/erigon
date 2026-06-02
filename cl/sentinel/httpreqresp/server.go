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
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/erigontech/erigon/cl/clparams"
)

const (
	ResponseCodeHeader = "Reqresp-Response-Code"
	PeerIdHeader       = "Reqresp-Peer-Id"
	TopicHeader        = "Reqresp-Topic"
	// MaxResponseChunksHeader carries the caller's upper bound on the response's chunk
	// count, used to size the response-body cap. Absent or 0 falls back to maxResponseChunks.
	MaxResponseChunksHeader = "Reqresp-Max-Response-Chunks"
)

const (
	// maxResponseChunks is the absolute ceiling on a multi-chunk response's chunk count,
	// used when the caller's expected count is absent or implausibly large.
	maxResponseChunks = 1024
	// maxSingleObjectResponse bounds single-chunk protocols (status, ping, metadata,
	// goodbye, light-client singles), whose responses are at most tens of KiB.
	maxSingleObjectResponse = 1024 * 1024
)

// maxResponseBodySize is the byte ceiling the handler will buffer for a response on the
// given topic. Single-chunk protocols get a tight fixed cap; by_range / by_root / by_head
// responses are bounded by expectedChunks (the caller's item count) clamped to maxResponseChunks.
func maxResponseBodySize(topic string, expectedChunks int64) int64 {
	if !strings.Contains(topic, "_by_range") && !strings.Contains(topic, "_by_root") && !strings.Contains(topic, "_by_head") {
		return maxSingleObjectResponse
	}
	if expectedChunks <= 0 || expectedChunks > maxResponseChunks {
		expectedChunks = maxResponseChunks
	}
	return expectedChunks * int64(clparams.MaxChunkSize)
}

// Do performs an http request against the http handler.
// NOTE: this is actually very similar to the http.RoundTripper interface... maybe we should investigate using that.
/*

the following headers have meaning when passed in to the request:

		REQRESP-PEER-ID - the peer id to target for the request
		REQRESP-TOPIC - the topic to request with
		REQRESP-EXPECTED-CHUNKS - this is an integer, which will be multiplied by 10 to calculate the amount of seconds the peer has to respond with all the data
*/
func Do(handler http.Handler, r *http.Request) (resp *http.Response, err error) {
	// TODO: there potentially extra alloc here (responses are bufferd)
	// is that a big deal? not sure. maybe can reuse these buffers since they are read once (and known when close) if so
	ok := make(chan struct{})
	go func() {
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, r)
		// linter does not know we are passing the resposne through channel.
		// nolint: bodyclose
		resp = res.Result()
		close(ok)
	}()
	select {
	case <-ok:
		return resp, nil
	case <-r.Context().Done():
		return nil, r.Context().Err()
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
		expectedChunks, _ := strconv.Atoi(r.Header.Get(MaxResponseChunksHeader))
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
		defer stream.Close()
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
		// this is not necessary, but seems like the right thing to do
		w.Header().Set("CONTENT-TYPE", "application/octet-stream")
		w.Header().Set("CONTENT-ENCODING", "snappy/stream")
		// add the response code & headers
		w.Header().Set("REQRESP-RESPONSE-CODE", strconv.Itoa(int(code[0])))
		w.Header().Set("REQRESP-PEER-ID", peerIdBase58)
		w.Header().Set("REQRESP-TOPIC", topic)
		// the deadline is 10 * expected chunk count, which the user can send. otherwise we will only wait 10 seconds
		// this is technically incorrect, and more aggressive than the network might like.
		stream.SetReadDeadline(time.Now().Add(10 * time.Second * time.Duration(chunks)))
		// copy the data now to the stream
		// the first write to w will call code 200, so we do not need to
		_, err = io.Copy(w, io.LimitReader(stream, maxResponseBodySize(topic, int64(expectedChunks))))
		if err != nil {
			http.Error(w, "Reading Stream Response: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
}
