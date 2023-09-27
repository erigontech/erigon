// package httpreqresp encapsulates eth2 beacon chain resp-resp into http
package httpreqresp

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

func Do(handler http.Handler, r *http.Request) (*http.Response, error) {
	// TODO: there potentially extra alloc here (responses are bufferd)
	// is that a big deal? not sure. maybe can reuse these buffers since they are read once (and known when close) if so
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, r)
	resp := res.Result()
	if resp.StatusCode != http.StatusOK {
		resp, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%s", string(resp))
	}
	return resp, nil
}

// Handles a request
func NewRequestHandler(host host.Host) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// get the peer parameter
		peerIdBase58 := r.Header.Get("REQRESP-PEER-ID")
		topic := r.Header.Get("REQRESP-TOPIC")

		peerId, err := peer.Decode(peerIdBase58)
		if err != nil {
			http.Error(w, "Invalid Peer Id", http.StatusBadRequest)
			return
		}
		stream, err := host.NewStream(r.Context(), peerId, protocol.ID(topic))
		if err != nil {
			http.Error(w, "Can't Connect to Peer: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		defer stream.Close()

		if r.Body != nil && r.ContentLength > 0 {
			_, err := io.Copy(stream, r.Body)
			if err != nil {
				http.Error(w, "Processing Stream: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}
		err = stream.CloseWrite()
		if err != nil {
			http.Error(w, "Close Write Side: "+err.Error(), http.StatusInternalServerError)
			return
		}
		code := make([]byte, 1)
		_, err = io.ReadFull(stream, code)
		if err != nil {
			http.Error(w, "Read Code: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("CONTENT-TYPE", "application/octet-stream")
		w.Header().Set("CONTENT-ENCODING", "snappy/stream")
		// add the response code
		w.Header().Set("REQRESP-RESPONSE-CODE", strconv.Itoa(int(code[0])))
		w.Header().Set("REQRESP-PEER-ID", peerIdBase58)
		w.Header().Set("REQRESP-TOPIC", topic)
		// copy the data now to the stream
		// the first write to w will call code 200, so we do not need to
		_, err = io.Copy(w, stream)
		if err != nil {
			http.Error(w, "Reading Stream Response: "+err.Error(), http.StatusInternalServerError)
			return
		}
		return
	}
}
