package ssz2

import (
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/types/ssz"
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

func Encode(buf []byte, schema ...interface{}) (dst []byte, err error) {
	dst = buf
	currentOffset := 0
	dynamicComponents := []SizedObjectSSZ{}
	offsetsStarts := []int{}
	for i, element := range schema {
		switch obj := element.(type) {
		case uint64:
			dst = append(dst, ssz.Uint64SSZ(obj)...)
			currentOffset += 8
		case *uint64:
			dst = append(dst, ssz.Uint64SSZ(*obj)...)
			currentOffset += 8
		case []byte:
			dst = append(dst, obj...)
			currentOffset += len(obj)
		case SizedObjectSSZ:
			startSize := len(dst)
			if obj.Static() {
				if dst, err = obj.EncodeSSZ(dst); err != nil {
					return nil, err
				}
			} else {
				offsetsStarts = append(offsetsStarts, startSize)
				dst = append(dst, make([]byte, 4)...)
				dynamicComponents = append(dynamicComponents, obj)
			}
			currentOffset += len(dst) - startSize
		default:
			panic(
				fmt.Sprintf("u must suffer from dementia, pls read the doc of this method (aka. comments), bad schema compoent %d", i))
		}
	}
	for i, dynamicComponent := range dynamicComponents {
		startSize := len(dst)
		binary.LittleEndian.PutUint32(dst[offsetsStarts[i]:], uint32(currentOffset))

		if dst, err = dynamicComponent.EncodeSSZ(dst); err != nil {
			return nil, err
		}
		currentOffset += len(dst) - startSize
	}
	return
}
