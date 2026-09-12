//go:build go1.27

package rpc

import (
	"bytes"
	"encoding/json"
	jsonv2 "encoding/json/v2"
)

// marshalInto appends the v1-compatible JSON encoding of v to buf. Streaming
// into buf keeps the encoder's own buffer at its flush threshold instead of
// growing it to the response size, and skips the copy json.Marshal makes on the
// way out.
func marshalInto(buf *bytes.Buffer, v any) error {
	return jsonv2.MarshalWrite(buf, v, json.DefaultOptionsV1())
}
