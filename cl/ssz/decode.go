package ssz2

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/ledgerwatch/erigon-lib/types/ssz"
)

/*
The function takes the input byte slice buf, the SSZ version, and the schema as variadic arguments.
It initializes a position pointer position to keep track of the current position in the buf.

It creates two empty slices: offsets to store the offsets for dynamic objects, and dynamicObjs to store the dynamic objects themselves.
It iterates over each element in the schema using a for loop.
For each element, it performs the following actions based on its type:

If the element is a pointer to uint64, it decodes the corresponding value from the buf using
little-endian encoding and assigns it to the pointer. It then increments the position by 8 bytes.

If the element is a byte slice ([]byte), it copies the corresponding data from the buf to the slice.
It then increments the position by the length of the slice.

If the element implements the SizedObjectSSZ interface, it checks if the object is static (fixed size) or dynamic (variable size).

  - If it's static, it decodes the object from the buf and updates the position accordingly by calling obj.DecodeSSZ and obj.EncodingSizeSSZ.
  - If it's dynamic, it stores the offset (as a 32-bit little-endian integer) in the offsets slice and stores the object itself in the dynamicObjs slice.
    It then increments the position by 4 bytes.

After processing all elements in the schema, the function iterates over the dynamic objects stored in the dynamicObjs slice.
For each dynamic object, it retrieves the corresponding offset and the end offset. If it's the last dynamic object, the end offset is set to the length of the buf.
It calls obj.DecodeSSZ on the sub-slice of the buf from the offset to the end offset and passes the SSZ version. This decodes the dynamic object.
Finally, the function returns nil if the decoding process is successful, or an error if an error occurs during decoding.
The Decode function is used to decode an SSZ-encoded byte slice into the specified schema. It supports decoding of various
types such as uint64, []byte, and objects that implement the SizedObjectSSZ interface.
It handles both static (fixed size) and dynamic (variable size) objects based on their respective decoding methods and offsets.
*/
func UnmarshalSSZ(buf []byte, version int, schema ...interface{}) (err error) {
	// defer func() {
	// 	if err2 := recover(); err2 != nil {
	// 		err = fmt.Errorf("panic while decoding: %v", err2)
	// 	}
	// }()

	position := 0
	offsets := []int{}
	dynamicObjs := []SizedObjectSSZ{}

	// Iterate over each element in the schema
	for i, element := range schema {
		switch obj := element.(type) {
		case *uint64:
			if len(buf) < position+8 {
				return ssz.ErrLowBufferSize
			}
			// If the element is a pointer to uint64, decode it from the buf using little-endian encoding
			*obj = binary.LittleEndian.Uint64(buf[position:])
			position += 8
		case []byte:
			if len(buf) < position+len(obj) {
				return ssz.ErrLowBufferSize
			}
			// If the element is a byte slice, copy the corresponding data from the buf to the slice
			copy(obj, buf[position:])
			position += len(obj)
		case SizedObjectSSZ:
			// If the element implements the SizedObjectSSZ interface
			if obj.Static() {
				if len(buf) < position+obj.EncodingSizeSSZ() {
					return ssz.ErrLowBufferSize
				}
				// If the object is static (fixed size), decode it from the buf and update the position
				if err = obj.DecodeSSZ(buf[position:], version); err != nil {
					return fmt.Errorf("static element %d: %w", i, err)
				}
				position += obj.EncodingSizeSSZ()
			} else {
				if len(buf) < position+4 {
					return ssz.ErrLowBufferSize
				}
				// If the object is dynamic (variable size), store the offset and the object in separate slices
				offsets = append(offsets, int(binary.LittleEndian.Uint32(buf[position:])))
				dynamicObjs = append(dynamicObjs, obj)
				position += 4
			}
		default:
			// If the element does not match any supported types, throw panic, will be caught by anti-panic condom
			// and we will have the trace.
			panic(fmt.Errorf("RTFM, bad schema component %d", i))
		}
	}

	// Iterate over the dynamic objects and decode them using the stored offsets
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
		if err = obj.DecodeSSZ(buf[offsets[i]:endOffset], version); err != nil {
			return fmt.Errorf("dynamic element (sz:%d) %d/%s: %w", endOffset-offsets[i], i, reflect.TypeOf(obj), err)
		}
	}

	return
}
