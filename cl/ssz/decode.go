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

package ssz2

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/erigontech/erigon/common/ssz"
)

func UnmarshalSSZ(buf []byte, version int, schema ...any) error {
	return unmarshalSSZ(buf, version, false, schema...)
}

// UnmarshalSSZStrict rejects non-canonical container offsets and propagates
// strict decoding to nested objects that support it.
func UnmarshalSSZStrict(buf []byte, version int, schema ...any) error {
	return unmarshalSSZ(buf, version, true, schema...)
}

func decodeObjectSSZStrict(obj SizedObjectSSZ, buf []byte, version int) error {
	if obj, ok := obj.(ssz.StrictUnmarshaler); ok {
		return obj.DecodeSSZStrict(buf, version)
	}
	return obj.DecodeSSZ(buf, version)
}

func unmarshalSSZ(buf []byte, version int, strict bool, schema ...any) (err error) {
	defer func() {
		if err2 := recover(); err2 != nil {
			err = fmt.Errorf("panic while decoding: %v", err2)
		}
	}()
	position := 0
	offsets := []int{}
	dynamicObjs := []SizedObjectSSZ{}

	for i, element := range schema {
		switch obj := element.(type) {
		case *uint64:
			if len(buf) < position+8 {
				return ssz.ErrLowBufferSize
			}
			*obj = binary.LittleEndian.Uint64(buf[position:])
			position += 8
		case []byte:
			if len(buf) < position+len(obj) {
				return ssz.ErrLowBufferSize
			}
			copy(obj, buf[position:])
			position += len(obj)
		case *bool:
			if len(buf) < position+1 {
				return ssz.ErrLowBufferSize
			}
			*obj = buf[position] != 0
			position += 1
		case SizedObjectSSZ:
			if obj.Static() {
				if len(buf) < position+obj.EncodingSizeSSZ() {
					return ssz.ErrLowBufferSize
				}
				if strict {
					err = decodeObjectSSZStrict(obj, buf[position:], version)
				} else {
					err = obj.DecodeSSZ(buf[position:], version)
				}
				if err != nil {
					return fmt.Errorf("static element %d: %w", i, err)
				}
				position += obj.EncodingSizeSSZ()
			} else {
				if len(buf) < position+4 {
					return ssz.ErrLowBufferSize
				}
				offsets = append(offsets, int(binary.LittleEndian.Uint32(buf[position:])))
				dynamicObjs = append(dynamicObjs, obj)
				position += 4
			}
		default:
			panic(fmt.Errorf("RTFM, bad schema component %d. Type %v", i, reflect.TypeOf(element).Name()))
		}
	}

	if strict && len(offsets) != 0 && offsets[0] != position {
		return ssz.ErrBadOffset
	}

	for i, obj := range dynamicObjs {
		endOffset := len(buf)
		if i != len(dynamicObjs)-1 {
			endOffset = offsets[i+1]
		}
		if offsets[i] > endOffset {
			return ssz.ErrBadOffset
		}
		if len(buf) < endOffset {
			return ssz.ErrLowBufferSize
		}
		if strict {
			err = decodeObjectSSZStrict(obj, buf[offsets[i]:endOffset], version)
		} else {
			err = obj.DecodeSSZ(buf[offsets[i]:endOffset], version)
		}
		if err != nil {
			return fmt.Errorf("dynamic element (sz:%d) %d/%s: %w", endOffset-offsets[i], i, reflect.TypeOf(obj), err)
		}
	}

	return
}
