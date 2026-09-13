package rpc

import (
	"bytes"

	"github.com/erigontech/erigon/rpc/jsonstream"
)

// spillWriter holds the first FlushThreshold bytes of a response so a marshal
// that fails inside them can still be replaced by an error object, and hands
// everything to the stream once the response proves larger than that. Past the
// spill point the bytes are gone and a failure can only be reported in place.
type spillWriter struct {
	s jsonstream.Stream
	// onSpill writes whatever must precede the value, and only runs if the
	// response gets large enough to stream. Until then nothing has been written
	// and the whole message can still be replaced by an error.
	onSpill func()
	buf     bytes.Buffer
	spilled bool
}

func (w *spillWriter) Write(p []byte) (int, error) {
	if w.spilled {
		w.s.WriteRawBytes(p)
		return len(p), nil
	}
	w.buf.Write(p)
	if w.buf.Len() >= jsonstream.FlushThreshold {
		w.spilled = true
		w.onSpill()
		w.s.WriteRawBytes(w.buf.Bytes())
		w.buf.Reset()
	}
	return len(p), nil
}

// commit emits a response that never spilled, prefix included.
func (w *spillWriter) commit() {
	if !w.spilled {
		w.onSpill()
	}
	w.s.WriteRawBytes(w.buf.Bytes())
}
