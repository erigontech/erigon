//go:build !go1.27 || !goexperiment.jsonv2

package rpc

import "encoding/json"

func encodeResult(w *responseWriter, v any) error {
	return json.NewEncoder(w).Encode(v)
}
