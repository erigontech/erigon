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
	"runtime/debug"

	"github.com/erigontech/erigon-lib/types/ssz"
)

type Sized interface {
	Static() bool
}

type ObjectSSZ interface {
	ssz.EncodableSSZ
	ssz.Marshaler
}

type SizedObjectSSZ interface {
	ObjectSSZ
	Sized
}

/*
The function takes the initial byte slice buf and the schema as variadic arguments.

It initializes the dst slice with the contents of buf and sets the currentOffset to 0.

It creates two empty slices: dynamicComponents to store dynamic objects that require offsets, and offsetsStarts to store the start positions of the offsets.

It iterates over each element in the schema using a for loop.

For each element, it performs the following actions based on its type:

If the element is a uint64, it encodes the value using SSZ and appends it to the dst. It then increments the currentOffset by 8.
If the element is a pointer to uint64, it dereferences the pointer, encodes the value using SSZ, and appends it to he dst. It then increments the currentOffset by 8.
If the element is a byte slice ([]byte), it appends the slice to the dst. It then increments the currentOffset by the length of the slice.

If the element implements the SizedObjectSSZ interface, it checks if the object is static (fixed size) or dynamic (variable size). If it's static,
it calls obj.EncodeSSZ to encode the object and updates the dst accordingly. If it's dynamic, it stores the start offset in the offsetsStarts slice,
appends a placeholder 4-byte offset to the dst, and adds the object to the dynamicComponents slice.

After processing all elements in the schema, the function iterates over the dynamic components stored in the dynamicComponents slice.

For each dynamic component, it retrieves the corresponding start offset from the offsetsStarts slice and sets the
offset value in the dst using binary.LittleEndian.PutUint32. It then calls dynamicComponent.EncodeSSZ on
the sub-slice of the dst starting from the offset position to encode the dynamic component.
Finally, the function returns the updated dst slice and nil if the encoding process is successful,
or an error if an error occurs during encoding.
The Encode function is used to encode objects into an SSZ-encoded byte slice based on the provided schema.
It supports encoding of various types such as uint64, []byte, and objects that implement the SizedObjectSSZ interface.
It handles both static (fixed size) and dynamic (variable size) objects, including the calculation and placement of offsets for dynamic objects.
*/
func MarshalSSZ(buf []byte, schema ...any) (dst []byte, err error) {
	defer func() {
		if err2 := recover(); err2 != nil {
			debug.PrintStack()
			err = fmt.Errorf("panic while encoding: %v", err2)
		}
	}()

	dst = buf
	currentOffset := 0
	dynamicComponents := []SizedObjectSSZ{}
	offsetsStarts := []int{}

	// Iterate over each element in the schema
	for i, element := range schema {
		switch obj := element.(type) {
		case uint64:
			// If the element is a uint64, encode it using SSZ and append it to the dst
			dst = append(dst, ssz.Uint64SSZ(obj)...)
			currentOffset += 8
		case *uint64:
			// If the element is a pointer to uint64, dereference it, encode it using SSZ, and append it to the dst
			dst = append(dst, ssz.Uint64SSZ(*obj)...)
			currentOffset += 8
		case []byte:
			// If the element is a byte slice, append it to the dst
			dst = append(dst, obj...)
			currentOffset += len(obj)
		case SizedObjectSSZ:
			// If the element implements the SizedObjectSSZ interface
			startSize := len(dst)
			if obj.Static() {
				// If the object is static (fixed size), encode it using SSZ and update the dst
				encodedBytes, err := obj.EncodeSSZ(nil)
				if err != nil {
					return nil, err
				}
				dst = append(dst, encodedBytes...)
			} else {
				// If the object is dynamic (variable size), store the start offset and the object in separate slices
				offsetsStarts = append(offsetsStarts, startSize)
				dst = append(dst, make([]byte, 4)...)
				dynamicComponents = append(dynamicComponents, obj)
			}
			currentOffset += len(dst) - startSize
		default:
			// If the element does not match any supported types, panic with an error message
			panic(fmt.Sprintf("u must suffer from dementia, pls read the doc of this method (aka. comments), bad schema component %d", i))
		}
	}

	// Iterate over the dynamic components and encode them using SSZ
	for i, dynamicComponent := range dynamicComponents {
		startSize := len(dst)
		binary.LittleEndian.PutUint32(dst[offsetsStarts[i]:], uint32(currentOffset))

		if dst, err = dynamicComponent.EncodeSSZ(dst); err != nil {
			return nil, err
		}
		currentOffset += len(dst) - startSize
	}

	return dst, nil
}
