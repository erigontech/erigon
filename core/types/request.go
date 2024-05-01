package types

import (
	"bytes"
	"fmt"
	"io"

	"github.com/ledgerwatch/erigon/rlp"
)

const (
	DepositRequestType byte = 0x00
)

type Request struct {
	inner RequestData
}

func (r *Request) Type() byte {
	return r.inner.requestType()
}

func (r *Request) EncodeRLP(w io.Writer) error {
	var buf bytes.Buffer    // TODO(racytech): find a solution to reuse the same buffer instead of recreating it
	buf.WriteByte(r.Type()) // first write type of request then encode inner data
	r.inner.encodeRLP(&buf)
	return rlp.Encode(w, buf.Bytes())
}

func (r *Request) DecodeRLP(s *rlp.Stream) error {
	kind, _, err := s.Kind()
	switch {
	case err != nil:
		return err
	case kind == rlp.List:
		return fmt.Errorf("error: untyped request (unexpected lit)")
	case kind == rlp.Byte:
		return fmt.Errorf("error: too short request")
	default:
		var buf []byte
		if buf, err = s.Bytes(); err != nil {
			return err
		}

		return r.decode(buf)
	}
}

func (r *Request) decode(data []byte) error {
	if len(data) <= 1 {
		return fmt.Errorf("error: too short type request")
	}
	var inner RequestData
	switch data[0] {
	case DepositRequestType:
		inner = new(Deposit)
	default:
		return fmt.Errorf("unknown request type - %d", data[0])
	}

	if err := inner.decodeRLP(data[1:]); err != nil {
		return err
	}
	r.inner = inner
	return nil
}

type Requests []*Request

func (r Requests) Len() int { return len(r) }

// EncodeIndex encodes the i'th request to w. Note that this does not check for errors
// because we assume that *request will only ever contain valid requests that were either
// constructed by decoding or via public API in this package.
func (r Requests) EncodeIndex(i int, w *bytes.Buffer) {
	rlp.Encode(w, r[i])
}

type RequestData interface {
	encodeRLP(*bytes.Buffer) error
	decodeRLP([]byte) error
	requestType() byte
	copy() RequestData
}
