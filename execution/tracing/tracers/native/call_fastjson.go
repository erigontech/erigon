package native

import (
	"encoding/json"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/tracing/tracers"
)

// chunkSize only has to hold one frame: handover is free, so a bigger buffer
// buys no speed and costs its own size in peak memory.
const chunkSize = 8 * 1024

type frameWriter struct {
	buf []byte
	w   tracers.RawWriter
}

// flush hands the buffer over once it holds more than min bytes. Only ever
// called at a frame boundary, so a value is never split across two writes.
func (fw *frameWriter) flush(min int) {
	if len(fw.buf) > min {
		fw.w.WriteRawBytes(fw.buf)
		fw.buf = fw.buf[:0]
	}
}

// byteWriter collects everything into one slice, for callers that want the
// whole result rather than a stream.
type byteWriter struct{ b []byte }

func (b *byteWriter) WriteRawBytes(p []byte) { b.b = append(b.b, p...) }

// AppendJSON writes the same bytes as the gencodec MarshalJSON. Going through
// MarshalJSON makes encoding/json re-scan every child's output at each level
// (appendCompact), which is quadratic in call depth; recursing into one buffer
// is not.
func (c *callFrame) AppendJSON(w tracers.RawWriter) {
	fw := frameWriter{buf: make([]byte, 0, chunkSize), w: w}
	fw.frame(*c)
	fw.flush(0)
}

func (fw *frameWriter) frame(c callFrame) {
	dst := fw.buf
	dst = append(dst, `{"from":`...)
	dst = appendText(dst, c.From.AppendText)
	dst = append(dst, `,"gas":`...)
	dst = appendText(dst, hexutil.Uint64(c.Gas).AppendText)
	dst = append(dst, `,"gasUsed":`...)
	dst = appendText(dst, hexutil.Uint64(c.GasUsed).AppendText)
	if c.To != nil {
		dst = append(dst, `,"to":`...)
		dst = appendText(dst, c.To.AppendText)
	}
	dst = append(dst, `,"input":`...)
	dst = appendText(dst, hexutil.Bytes(c.Input).AppendText)
	if len(c.Output) > 0 {
		dst = append(dst, `,"output":`...)
		dst = appendText(dst, hexutil.Bytes(c.Output).AppendText)
	}
	if c.Error != "" {
		dst = append(dst, `,"error":`...)
		dst = appendJSONString(dst, c.Error)
	}
	if c.Revertal != "" {
		dst = append(dst, `,"revertReason":`...)
		dst = appendJSONString(dst, c.Revertal)
	}
	if len(c.Calls) > 0 {
		dst = append(dst, `,"calls":[`...)
		for i := range c.Calls {
			if i > 0 {
				dst = append(dst, ',')
			}
			fw.buf = dst
			fw.flush(chunkSize)
			fw.frame(c.Calls[i])
			dst = fw.buf
		}
		dst = append(dst, ']')
	}
	if len(c.Logs) > 0 {
		dst = append(dst, `,"logs":[`...)
		for i := range c.Logs {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = c.Logs[i].appendJSON(dst)
		}
		dst = append(dst, ']')
	}
	if c.Value != nil {
		dst = append(dst, `,"value":`...)
		dst = appendText(dst, (*hexutil.U256)(c.Value).AppendText)
	}
	dst = append(dst, `,"type":`...)
	dst = appendJSONString(dst, c.TypeString())
	fw.buf = append(dst, '}')
}

func (l callLog) appendJSON(dst []byte) []byte {
	dst = append(dst, `{"index":`...)
	dst = appendText(dst, l.Index.AppendText)
	dst = append(dst, `,"address":`...)
	dst = appendText(dst, l.Address.AppendText)
	dst = append(dst, `,"topics":`...)
	if l.Topics == nil {
		dst = append(dst, `null`...)
	} else {
		dst = append(dst, '[')
		for i := range l.Topics {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendText(dst, l.Topics[i].AppendText)
		}
		dst = append(dst, ']')
	}
	dst = append(dst, `,"data":`...)
	dst = appendText(dst, hexutil.Bytes(l.Data).AppendText)
	dst = append(dst, `,"position":`...)
	dst = appendText(dst, l.Position.AppendText)
	return append(dst, '}')
}

func appendText(dst []byte, appendTo func([]byte) ([]byte, error)) []byte {
	dst = append(dst, '"')
	dst, _ = appendTo(dst) // hexutil appenders never fail
	return append(dst, '"')
}

// appendJSONString matches encoding/json, HTML escaping included. The escape
// path is rare here: these are opcode names and EVM error strings.
func appendJSONString(dst []byte, s string) []byte {
	for i := range len(s) {
		if c := s[i]; c < 0x20 || c >= 0x7f || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			b, _ := json.Marshal(s)
			return append(dst, b...)
		}
	}
	dst = append(dst, '"')
	dst = append(dst, s...)
	return append(dst, '"')
}
