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
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	ResponseCodeHeader = "Reqresp-Response-Code"
	PeerIdHeader       = "Reqresp-Peer-Id"
	TopicHeader        = "Reqresp-Topic"
)

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
		//  we can't connect to the peer - so we should disconnect them. send a code 4xx
		stream, err := host.NewStream(r.Context(), peerId, protocol.ID(topic))
		if err != nil {
			http.Error(w, "can't Connect to Peer: "+err.Error(), http.StatusBadRequest)
			return
		}
		defer stream.Close()
		// this write deadline is not part of the eth p2p spec, but we are implying it.
		stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if r.Body != nil && r.ContentLength > 0 {
			_, err := io.Copy(stream, r.Body)
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
			http.Error(w, "Read Code: "+err.Error()+", readBytes="+strconv.Itoa(n), http.StatusBadRequest)
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
		_, err = io.Copy(w, stream)
		if err != nil {
			http.Error(w, "Reading Stream Response: "+err.Error(), http.StatusBadRequest)
			return
		}
		return
	}
}
