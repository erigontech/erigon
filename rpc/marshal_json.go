//go:build !go1.27

package rpc

import "encoding/json"

func marshalAppend(dst []byte, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return dst, err
	}
	return append(dst, b...), nil
}
