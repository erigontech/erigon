//go:build !go1.27

package rpc

import (
	"encoding/json"
	"io"
)

// marshalInto writes the JSON encoding of v to w. v1 has no streaming encoder,
// so Marshal is equivalent here.
func marshalInto(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}
