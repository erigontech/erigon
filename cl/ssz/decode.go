package ssz2

import (
	"encoding/binary"
	"fmt"
)

func Decode(buf []byte, version int, schema ...interface{}) (err error) {
	position := 0
	offsets := []int{}
	dynamicObjs := []SizedObjectSSZ{}
	for i, element := range schema {
		switch obj := element.(type) {
		case *uint64:
			*obj = binary.LittleEndian.Uint64(buf[position:])
			position += 8
		case []byte:
			copy(obj, buf[position:])
			position += len(obj)
		case SizedObjectSSZ:
			if obj.Static() {
				if err = obj.DecodeSSZ(buf[position:], version); err != nil {
					return
				}
				position += obj.EncodingSizeSSZ()
			} else {
				offsets = append(offsets, int(binary.LittleEndian.Uint32(buf[position:])))
				dynamicObjs = append(dynamicObjs, obj)
				position += 4
			}
		default:
			panic(
				fmt.Sprintf("RTFM, bad schema component %d", i))
		}
	}
	for i, obj := range dynamicObjs {
		endOffset := len(buf)
		if i != len(dynamicObjs)-1 {
			endOffset = offsets[i+1]
		}
		if err = obj.DecodeSSZ(buf[offsets[i]:endOffset], version); err != nil {
			return
		}

	}
	return
}
