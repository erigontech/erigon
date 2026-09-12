package rpc

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
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
// client: response() has to return a JSON-RPC error, not a truncated result.
func TestResponseDiscardsPartialMarshal(t *testing.T) {
	t.Parallel()

	vals := make([]boomAt, 10000)
	for i := range vals {
		vals[i] = boomAt{i: i, boom: 9000}
	}

	msg := &jsonrpcMessage{Version: vsn, ID: json.RawMessage("1")}
	resp := msg.response(vals)

	if resp.Error == nil {
		t.Fatal("want an error response, got a result")
	}
	if resp.Result != nil {
		t.Errorf("partial marshal leaked %d bytes into Result", len(resp.Result))
	}
	if resp.resultBuf != nil {
		t.Error("error path kept the pooled buffer instead of returning it")
	}
	if !json.Valid(mustMarshal(t, resp)) {
		t.Error("error response is not valid JSON")
	}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
