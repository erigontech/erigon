//go:build go1.27

package rpc

import (
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"io"
)

// marshalInto writes the v1-compatible JSON encoding of v to w. MarshalWrite
// streams, so a jsonstream destination flushes to the client once a response
// passes its threshold and never holds the whole thing.
func marshalInto(w io.Writer, v any) error {
	return jsonv2.MarshalWrite(w, v, json.DefaultOptionsV1())
}
