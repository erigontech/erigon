//go:build go1.27

package rpc

import (
	"bytes"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"sync"

	"github.com/erigontech/erigon/rpc/jsonstream"
)

// marshalInto appends the v1-compatible JSON encoding of v to buf. Streaming
// into buf keeps the encoder's own buffer at its flush threshold instead of
// growing it to the response size, and skips the copy json.Marshal makes on the
// way out.
//
// The destination must stay a buffer the caller can discard. MarshalWrite emits
// as it goes, so a value that fails deep in a slice has already written the
// earlier elements; pointed at a connection those bytes would be unretractable
// and the client would see truncated JSON instead of an error response.
func marshalInto(buf *bytes.Buffer, v any) error {
	return jsonv2.MarshalWrite(buf, v, json.DefaultOptionsV1())
}

// maxPooledResult bounds what the pool retains. The bound must sit above the
// responses worth pooling: one that misses it is rebuilt from a small buffer
// every time, which costs more than not pooling at all.
const maxPooledResult = 128 * jsonstream.FlushThreshold

var resultBufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

func encodeResult(w *responseWriter, v any) error {
	buf := resultBufPool.Get().(*bytes.Buffer)
	defer func() {
		if buf.Cap() <= maxPooledResult {
			buf.Reset()
			resultBufPool.Put(buf)
		}
	}()
	if err := marshalInto(buf, v); err != nil {
		return err
	}
	// writeResult copies buf into the stream or writes it through before it returns.
	w.writeResult(buf.Bytes())
	return nil
}
