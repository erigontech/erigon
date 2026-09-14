package rpc

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/erigontech/erigon/rpc/jsonstream"
)

// boomAt marshals fine until element boom, then fails. Enough elements marshal
// first that the encoder has flushed well past its internal 4KB buffer.
type boomAt struct{ i, boom int }

func (b boomAt) MarshalJSON() ([]byte, error) {
	if b.i == b.boom {
		return nil, errors.New("boom")
	}
	return []byte(`"` + strings.Repeat("x", 64) + `"`), nil
}

// json/v2's MarshalWrite streams, so by the time a value deep in a slice fails
// it has already written the earlier elements. Those bytes must never reach the
// client: writeResponse has to return a JSON-RPC error, not a truncated result.
func TestResponseDiscardsPartialMarshal(t *testing.T) {
	t.Parallel()

	vals := make([]boomAt, 10000)
	for i := range vals {
		vals[i] = boomAt{i: i, boom: 9000}
	}

	s := jsonstream.Get(nil)
	defer jsonstream.Put(s)
	resp := (&jsonrpcMessage{Version: vsn, ID: json.RawMessage("1")}).writeResponse(s, vals)

	if resp == nil || resp.Error == nil {
		t.Fatal("want an error response, got a result")
	}
	if n := len(s.Buffer()); n != 0 {
		t.Errorf("partial marshal leaked %d bytes into the stream", n)
	}
}
