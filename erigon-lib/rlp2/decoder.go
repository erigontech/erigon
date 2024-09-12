// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package rlp

import (
	"errors"
	"fmt"
	"io"
)

type Decoder struct {
	buf *buf
}

func NewDecoder(buf []byte) *Decoder {
	return &Decoder{
		buf: newBuf(buf, 0),
	}
}

func (d *Decoder) String() string {
	return fmt.Sprintf(`left=%x pos=%d`, d.buf.Bytes(), d.buf.off)
}

func (d *Decoder) Consumed() []byte {
	return d.buf.u[:d.buf.off]
}

func (d *Decoder) Underlying() []byte {
	return d.buf.Underlying()
}

func (d *Decoder) Empty() bool {
	return d.buf.empty()
}

func (d *Decoder) Offset() int {
	return d.buf.Offset()
}

func (d *Decoder) Bytes() []byte {
	return d.buf.Bytes()
}

func (d *Decoder) ReadByte() (n byte, err error) {
	return d.buf.ReadByte()
}

func (d *Decoder) PeekByte() (n byte, err error) {
	return d.buf.PeekByte()
}

func (d *Decoder) Rebase() {
	d.buf.u = d.Bytes()
	d.buf.off = 0
}
func (d *Decoder) Fork() *Decoder {
	return &Decoder{
		buf: newBuf(d.buf.u, d.buf.off),
	}
}

func (d *Decoder) PeekToken() (Token, error) {
	prefix, err := d.PeekByte()
	if err != nil {
		return TokenUnknown, err
	}
	return identifyToken(prefix), nil
}

func (d *Decoder) ElemDec() (*Decoder, Token, error) {
	a, t, err := d.Elem()
	return NewDecoder(a), t, err
}

func (d *Decoder) RawElemDec() (*Decoder, Token, error) {
	a, t, err := d.RawElem()
	return NewDecoder(a), t, err
}

func (d *Decoder) RawElem() ([]byte, Token, error) {
	w := d.buf
	start := w.Offset()
	// figure out what we are reading
	prefix, err := w.ReadByte()
	if err != nil {
		return nil, TokenUnknown, err
	}
	token := identifyToken(prefix)

	var (
		sz    int
		lenSz int
	)
	// switch on the token
	switch token {
	case TokenDecimal:
		// in this case, the value is just the byte itself
	case TokenShortList:
		sz = int(token.Diff(prefix))
		_, err = nextFull(w, sz)
	case TokenLongList:
		lenSz = int(token.Diff(prefix))
		sz, err = nextBeInt(w, lenSz)
		if err != nil {
			return nil, token, err
		}
		_, err = nextFull(w, sz)
	case TokenShortBlob:
		sz := int(token.Diff(prefix))
		_, err = nextFull(w, sz)
	case TokenLongBlob:
		lenSz := int(token.Diff(prefix))
		sz, err = nextBeInt(w, lenSz)
		if err != nil {
			return nil, token, err
		}
		_, err = nextFull(w, sz)
	default:
		return nil, token, fmt.Errorf("%w: unknown token", ErrDecode)
	}
	stop := w.Offset()
	//log.Printf("%x %s\n", buf, token)
	if err != nil {
		return nil, token, err
	}
	return w.Underlying()[start:stop], token, nil
}

func (d *Decoder) Elem() ([]byte, Token, error) {
	w := d.buf
	// figure out what we are reading
	prefix, err := w.ReadByte()
	if err != nil {
		return nil, TokenUnknown, err
	}
	token := identifyToken(prefix)

	var (
		buf   []byte
		sz    int
		lenSz int
	)
	// switch on the token
	switch token {
	case TokenDecimal:
		// in this case, the value is just the byte itself
		buf = []byte{prefix}
	case TokenShortList:
		sz = int(token.Diff(prefix))
		buf, err = nextFull(w, sz)
	case TokenLongList:
		lenSz = int(token.Diff(prefix))
		sz, err = nextBeInt(w, lenSz)
		if err != nil {
			return nil, token, err
		}
		buf, err = nextFull(w, sz)
	case TokenShortBlob:
		sz := int(token.Diff(prefix))
		buf, err = nextFull(w, sz)
	case TokenLongBlob:
		lenSz := int(token.Diff(prefix))
		sz, err = nextBeInt(w, lenSz)
		if err != nil {
			return nil, token, err
		}
		buf, err = nextFull(w, sz)
	default:
		return nil, token, fmt.Errorf("%w: unknown token", ErrDecode)
	}
	//log.Printf("%x %s\n", buf, token)
	if err != nil {
		return nil, token, fmt.Errorf("read data: %w", err)
	}
	return buf, token, nil
}

func ReadElem[T any](d *Decoder, fn func(*T, []byte) error, receiver *T) error {
	buf, token, err := d.Elem()
	if err != nil {
		return err
	}
	switch token {
	case TokenDecimal,
		TokenShortBlob,
		TokenLongBlob,
		TokenShortList,
		TokenLongList:
		return fn(receiver, buf)
	default:
		return fmt.Errorf("%w: ReadElem found unexpected token", ErrDecode)
	}
}

func (d *Decoder) ForList(fn func(*Decoder) error) error {
	// grab the list bytes
	buf, token, err := d.Elem()
	if err != nil {
		return err
	}
	switch token {
	case TokenShortList, TokenLongList:
		dec := NewDecoder(buf)
		for {
			if dec.buf.Len() == 0 {
				return nil
			}
			err := fn(dec)
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			// reset the byte
			dec = NewDecoder(dec.Bytes())
		}
	default:
		return fmt.Errorf("%w: ForList on non-list", ErrDecode)
	}
}

type buf struct {
	u   []byte
	off int
}

func newBuf(u []byte, off int) *buf {
	return &buf{u: u, off: off}
}

func (b *buf) empty() bool { return len(b.u) <= b.off }

func (b *buf) PeekByte() (n byte, err error) {
	if len(b.u) <= b.off {
		return 0, io.EOF
	}
	return b.u[b.off], nil
}
func (b *buf) ReadByte() (n byte, err error) {
	if len(b.u) <= b.off {
		return 0, io.EOF
	}
	b.off++
	return b.u[b.off-1], nil
}

func (b *buf) Next(n int) (xs []byte) {
	m := b.Len()
	if n > m {
		n = m
	}
	data := b.u[b.off : b.off+n]
	b.off += n
	return data
}

func (b *buf) Offset() int {
	return b.off
}

func (b *buf) Bytes() []byte {
	return b.u[b.off:]
}

func (b *buf) String() string {
	if b == nil {
		// Special case, useful in debugging.
		return "<nil>"
	}
	return string(b.u[b.off:])
}

func (b *buf) Len() int { return len(b.u) - b.off }

func (b *buf) Underlying() []byte {
	return b.u
}
