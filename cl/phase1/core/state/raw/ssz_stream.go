// Copyright 2026 The Erigon Authors
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

package raw

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"slices"

	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common/ssz"
)

// streamCodec is implemented by dynamic fields that read and write straight from the stream.
type streamCodec interface {
	EncodeSSZTo(w io.Writer) error
	DecodeSSZFrom(r io.Reader, n int) error
}

type countingWriter struct {
	w io.Writer
	n int
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += n
	return n, err
}

// encodeSSZ calls obj.EncodeSSZ and turns an encoder panic into an error, as ssz2.MarshalSSZ does.
func encodeSSZ(obj ssz2.SizedObjectSSZ, dst []byte) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic while encoding: %v", r)
		}
	}()
	return obj.EncodeSSZ(dst)
}

// fixedPartSize is the size of the fixed section: static fields in full, 4 bytes per dynamic field.
func fixedPartSize(schema []any) (size int, err error) {
	for i, element := range schema {
		switch obj := element.(type) {
		case *uint64:
			size += 8
		case []byte:
			size += len(obj)
		case ssz2.SizedObjectSSZ:
			if obj.Static() {
				size += obj.EncodingSizeSSZ()
			} else {
				size += 4
			}
		default:
			return 0, fmt.Errorf("bad schema component %d: %v", i, reflect.TypeOf(element))
		}
	}
	return size, nil
}

// marshalSSZTo writes the same bytes as ssz2.MarshalSSZ and returns how many it wrote.
// Each dynamic field must write exactly EncodingSizeSSZ bytes.
func marshalSSZTo(w io.Writer, schema ...any) (written int, err error) {
	cw := &countingWriter{w: w}
	fixedSize, err := fixedPartSize(schema)
	if err != nil {
		return 0, err
	}
	fixed := make([]byte, 0, fixedSize)
	var dynamic []ssz2.SizedObjectSSZ
	var static []byte
	nextOffset := fixedSize
	for _, element := range schema {
		switch obj := element.(type) {
		case *uint64:
			fixed = binary.LittleEndian.AppendUint64(fixed, *obj)
		case []byte:
			fixed = append(fixed, obj...)
		case ssz2.SizedObjectSSZ:
			if obj.Static() {
				// Some encoders write at dst[0:], so each one gets an empty slice, as in MarshalSSZ.
				if static, err = encodeSSZ(obj, static[:0]); err != nil {
					return 0, err
				}
				fixed = append(fixed, static...)
				continue
			}
			size := obj.EncodingSizeSSZ()
			if size < 0 || size > math.MaxUint32-nextOffset {
				return 0, fmt.Errorf("dynamic field size %d out of range", size)
			}
			fixed = binary.LittleEndian.AppendUint32(fixed, uint32(nextOffset))
			nextOffset += size
			dynamic = append(dynamic, obj)
		}
	}
	if len(fixed) != fixedSize {
		return 0, fmt.Errorf("fixed part is %d bytes, want %d", len(fixed), fixedSize)
	}
	if _, err = cw.Write(fixed); err != nil {
		return cw.n, err
	}

	var scratch []byte
	for i, obj := range dynamic {
		before := cw.n
		if codec, ok := obj.(streamCodec); ok {
			err = codec.EncodeSSZTo(cw)
		} else {
			if scratch, err = encodeSSZ(obj, scratch[:0]); err == nil {
				_, err = cw.Write(scratch)
			}
		}
		if err != nil {
			return cw.n, fmt.Errorf("dynamic element %d/%T: %w", i, obj, err)
		}
		if got, want := cw.n-before, obj.EncodingSizeSSZ(); got != want {
			return cw.n, fmt.Errorf("dynamic element %d/%T wrote %d bytes, want %d", i, obj, got, want)
		}
	}
	return cw.n, nil
}

// unmarshalSSZFrom decodes exactly length bytes of canonical SSZ from r.
// Offsets must be tight and every dynamic field must consume exactly its span.
func unmarshalSSZFrom(r io.Reader, length, version int, schema ...any) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("panic while decoding: %v", rec)
		}
	}()
	fixedSize, err := fixedPartSize(schema)
	if err != nil {
		return err
	}
	if length < fixedSize {
		return ssz.ErrLowBufferSize
	}
	total := &io.LimitedReader{R: r, N: int64(length)}
	fixed := make([]byte, fixedSize)
	if _, err = io.ReadFull(total, fixed); err != nil {
		return fmt.Errorf("fixed part: %w", err)
	}

	position := 0
	var offsets []int
	var dynamic []ssz2.SizedObjectSSZ
	for i, element := range schema {
		switch obj := element.(type) {
		case *uint64:
			*obj = binary.LittleEndian.Uint64(fixed[position:])
			position += 8
		case []byte:
			position += copy(obj, fixed[position:])
		case ssz2.SizedObjectSSZ:
			if obj.Static() {
				size := obj.EncodingSizeSSZ()
				if err = obj.DecodeSSZ(fixed[position:position+size], version); err != nil {
					return fmt.Errorf("static element %d: %w", i, err)
				}
				position += size
				continue
			}
			offsets = append(offsets, int(binary.LittleEndian.Uint32(fixed[position:])))
			dynamic = append(dynamic, obj)
			position += 4
		}
	}

	if len(dynamic) == 0 {
		if length != fixedSize {
			return ssz.ErrTrailingBytes
		}
		return nil
	}
	if offsets[0] != fixedSize {
		return ssz.ErrBadOffset
	}

	var scratch []byte
	for i, obj := range dynamic {
		end := length
		if i+1 < len(dynamic) {
			end = offsets[i+1]
		}
		if end < offsets[i] || end > length {
			return ssz.ErrBadOffset
		}
		span := end - offsets[i]
		field := &io.LimitedReader{R: total, N: int64(span)}
		if codec, ok := obj.(streamCodec); ok {
			err = codec.DecodeSSZFrom(field, span)
		} else {
			scratch = slices.Grow(scratch[:0], span)[:span]
			if _, err = io.ReadFull(field, scratch); err == nil {
				err = obj.DecodeSSZ(scratch, version)
			}
		}
		if err != nil {
			return fmt.Errorf("dynamic element (sz:%d) %d/%T: %w", span, i, obj, err)
		}
		if field.N != 0 {
			return fmt.Errorf("dynamic element %d/%T left %d of %d bytes unread", i, obj, field.N, span)
		}
	}
	return nil
}
