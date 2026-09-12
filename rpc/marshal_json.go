//go:build !go1.27

package rpc

import (
	"bytes"
	"encoding/json"
)

// marshalAppend appends the JSON encoding of v to dst. Encoding through a
// Writer skips the copy json.Marshal makes on the way out. Encode terminates
// the value with a newline that the RPC framing does not want.
func marshalAppend(dst []byte, v any) ([]byte, error) {
	w := sliceWriter{dst}
	if err := json.NewEncoder(&w).Encode(v); err != nil {
		return dst, err
	}
	return bytes.TrimSuffix(w.b, []byte("\n")), nil
}
