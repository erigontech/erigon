//go:build go1.27 && goexperiment.jsonv2

package rpc

import (
	"bytes"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"sync"

	"github.com/erigontech/erigon/rpc/jsonstream"
)

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
	// Into a discardable buffer, not the stream: MarshalWrite emits as it goes, so a value failing deep in a slice
	// has already written the earlier elements, and the client must get an error response, not truncated JSON.
	if err := jsonv2.MarshalWrite(buf, v, json.DefaultOptionsV1()); err != nil {
		return err
	}
	// writeResult copies buf into the stream or writes it through before it returns.
	w.writeResult(buf.Bytes())
	return nil
}
