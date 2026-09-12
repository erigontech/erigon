//go:build !go1.27

package rpc

import (
	"bytes"
	"encoding/json"
)

// marshalInto appends the JSON encoding of v to buf. Encoding through a Writer
// skips the copy json.Marshal makes on the way out.
func marshalInto(buf *bytes.Buffer, v any) error {
	if err := json.NewEncoder(buf).Encode(v); err != nil {
		return err
	}
	buf.Truncate(buf.Len() - 1) // Encode terminates the value with a newline
	return nil
}
