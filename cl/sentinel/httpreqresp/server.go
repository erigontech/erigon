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

func Do(handler http.Handler, r *http.Request) (*http.Response, error) {
	// TODO: there potentially extra alloc here (responses are bufferd)
	// is that a big deal? not sure. maybe can reuse these buffers since they are read once (and known when close) if so
	ans := make(chan *http.Response)
	go func() {
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, r)
		resp := res.Result()
		ans <- resp
	}()
	select {
	case res := <-ans:
		return res, nil
	case <-r.Context().Done():
		return nil, r.Context().Err()
	}
}

// Handles a request
func NewRequestHandler(host host.Host) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// get the peer parameter
		peerIdBase58 := r.Header.Get("REQRESP-PEER-ID")
		topic := r.Header.Get("REQRESP-TOPIC")
		chunkCount := r.Header.Get("REQRESP-EXPECTED-CHUNKS")
		chunks, _ := strconv.Atoi(chunkCount)
		if chunks < 1 {
			chunks = 1
		}

		peerId, err := peer.Decode(peerIdBase58)
		if err != nil {
			http.Error(w, "Invalid Peer Id", http.StatusBadRequest)
			return
		}
		//  we can't connect to the peer - so we should disconnect them. send a code 4xx
		stream, err := host.NewStream(r.Context(), peerId, protocol.ID(topic))
		if err != nil {
			http.Error(w, "Can't Connect to Peer: "+err.Error(), http.StatusBadRequest)
			return
		}
		defer stream.Close()
		stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if r.Body != nil && r.ContentLength > 0 {
			_, err := io.Copy(stream, r.Body)
			if err != nil {
				http.Error(w, "Processing Stream: "+err.Error(), http.StatusBadRequest)
				return
			}
		}
		err = stream.CloseWrite()
		if err != nil {
			http.Error(w, "Close Write Side: "+err.Error(), http.StatusBadRequest)
			return
		}
		code := make([]byte, 1)
		// we have 5 seconds to read the next byte. this is the 5 TTFB_TIMEOUT
		stream.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err = io.ReadFull(stream, code)
		if err != nil {
			http.Error(w, "Read Code: "+err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("CONTENT-TYPE", "application/octet-stream")
		w.Header().Set("CONTENT-ENCODING", "snappy/stream")
		// add the response code
		w.Header().Set("REQRESP-RESPONSE-CODE", strconv.Itoa(int(code[0])))
		w.Header().Set("REQRESP-PEER-ID", peerIdBase58)
		w.Header().Set("REQRESP-TOPIC", topic)
		// the deadline is 10 * expected chunk count, which the user can send.
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
