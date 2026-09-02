//go:build go1.27

package native

import (
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"

	"github.com/erigontech/erigon/common/hexutil"
)

// callFrame must keep satisfying MarshalerTo: json/v2 silently falls back to the
// generated MarshalJSON if the signature ever drifts, and the only symptom would
// be the allocation regression coming back.
var _ jsonv2.MarshalerTo = callFrame{}

// MarshalJSONTo streams the frame straight into the encoder. encoding/json/v2
// prefers it over MarshalJSON, which returns a []byte that v2 then has to
// re-parse and re-emit — a cost paid once per nesting level, since frames nest.
// Output is byte-identical to the generated MarshalJSON, which older toolchains
// keep using.
func (c callFrame) MarshalJSONTo(enc *jsontext.Encoder) error {
	if err := enc.WriteToken(jsontext.BeginObject); err != nil {
		return err
	}
	// buf keeps the hex scalars off the heap; every one of them fits.
	var buf [80]byte
	name := func(n string) error { return enc.WriteToken(jsontext.String(n)) }
	// hexField writes a 0x-quoted scalar without boxing it into an interface.
	hexField := func(n string, appendText func([]byte) ([]byte, error)) error {
		if err := name(n); err != nil {
			return err
		}
		v := append(buf[:0], '"')
		v, err := appendText(v)
		if err != nil {
			return err
		}
		return enc.WriteValue(append(v, '"'))
	}
	strField := func(n, v string) error {
		if err := name(n); err != nil {
			return err
		}
		return enc.WriteToken(jsontext.String(v))
	}
	field := func(n string, v any) error {
		if err := name(n); err != nil {
			return err
		}
		return jsonv2.MarshalEncode(enc, v)
	}
	if err := hexField("from", c.From.AppendText); err != nil {
		return err
	}
	if err := hexField("gas", hexutil.Uint64(c.Gas).AppendText); err != nil {
		return err
	}
	if err := hexField("gasUsed", hexutil.Uint64(c.GasUsed).AppendText); err != nil {
		return err
	}
	if err := hexField("to", c.To.AppendText); err != nil {
		return err
	}
	if err := field("input", hexutil.Bytes(c.Input)); err != nil {
		return err
	}
	if len(c.Output) > 0 {
		if err := field("output", hexutil.Bytes(c.Output)); err != nil {
			return err
		}
	}
	if c.Error != "" {
		if err := strField("error", c.Error); err != nil {
			return err
		}
	}
	if c.Revertal != "" {
		if err := strField("revertReason", c.Revertal); err != nil {
			return err
		}
	}
	if len(c.Calls) > 0 {
		if err := field("calls", c.Calls); err != nil {
			return err
		}
	}
	if len(c.Logs) > 0 {
		if err := field("logs", c.Logs); err != nil {
			return err
		}
	}
	if c.Value != nil {
		if err := hexField("value", (*hexutil.Big)(c.Value).AppendText); err != nil {
			return err
		}
	}
	if err := strField("type", c.TypeString()); err != nil {
		return err
	}
	return enc.WriteToken(jsontext.EndObject)
}
