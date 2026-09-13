//go:build !go1.27

package rpc

import (
	"encoding/json"
	"io"
)

// marshalInto writes the JSON encoding of v to w. v1 has no streaming encoder:
// Marshal builds the whole value first either way, so there is nothing to gain
// from Encoder here and its trailing newline would corrupt the RPC framing.
func marshalInto(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}
