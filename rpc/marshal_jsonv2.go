//go:build go1.27

package rpc

import (
	"encoding/json"
	jsonv2 "encoding/json/v2"
)

// marshalAppend appends the v1-compatible JSON encoding of v to dst. Streaming
// into dst keeps the encoder's own buffer at its flush threshold instead of
// growing it to the response size, and skips the copy json.Marshal makes on the
// way out.
func marshalAppend(dst []byte, v any) ([]byte, error) {
	w := sliceWriter{dst}
	err := jsonv2.MarshalWrite(&w, v, json.DefaultOptionsV1())
	return w.b, err
}
