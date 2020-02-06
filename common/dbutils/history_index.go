package dbutils

import (
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common/math"
)

const (
	LenBytes = 4
	ItemLen  = 8
)

func NewHistoryIndex() *HistoryIndexBytes {
	b := make(HistoryIndexBytes, LenBytes*2, 16)
	return &b
}

func WrapHistoryIndex(b []byte) *HistoryIndexBytes {
	index := HistoryIndexBytes(b)
	if len(index) == 0 {
		index = make(HistoryIndexBytes, LenBytes*2, 16)
	}
	return &index
}

type HistoryIndexBytes []byte

func (hi *HistoryIndexBytes) Decode() ([]uint64, error) {
	if hi == nil {
		return []uint64{}, nil
	}
	if len(*hi) <= LenBytes*2 {
		return []uint64{}, nil
	}

	numOfElements := binary.LittleEndian.Uint32((*hi)[0:LenBytes])
	numOfUint32Elements := binary.LittleEndian.Uint32((*hi)[LenBytes : 2*LenBytes])
	decoded := make([]uint64, numOfElements)

	for i := uint32(0); i < numOfElements; i++ {
		if i < numOfUint32Elements {
			decoded[i] = uint64(binary.LittleEndian.Uint32((*hi)[LenBytes*2+i*4 : LenBytes*2+i*4+4]))
		} else {
			decoded[i] = binary.LittleEndian.Uint64((*hi)[LenBytes*2+numOfUint32Elements*4+i*ItemLen : LenBytes*2+i*ItemLen+ItemLen])
		}
	}
	return decoded, nil
}

func (hi *HistoryIndexBytes) Append(v uint64) *HistoryIndexBytes {
	numOfElements := binary.LittleEndian.Uint32((*hi)[0:LenBytes])
	numOfUint32Elements := binary.LittleEndian.Uint32((*hi)[LenBytes : 2*LenBytes])
	var b []byte
	if v < math.MaxUint32 {
		b = make([]byte, 4)
		numOfUint32Elements++
		binary.LittleEndian.PutUint32(b, uint32(v))
	} else {
		b = make([]byte, ItemLen)
		binary.LittleEndian.PutUint64(b, v)
	}

	*hi = append(*hi, b...)
	binary.LittleEndian.PutUint32((*hi)[0:LenBytes], numOfElements+1)
	binary.LittleEndian.PutUint32((*hi)[LenBytes:2*LenBytes], numOfUint32Elements)
	return hi
}

func (hi *HistoryIndexBytes) Len() uint32 {
	return binary.LittleEndian.Uint32((*hi)[0:LenBytes])
}

//most common operation is remove one from the tail
func (hi *HistoryIndexBytes) Remove(v uint64) *HistoryIndexBytes {
	numOfElements := binary.LittleEndian.Uint32((*hi)[0:LenBytes])
	numOfUint32Elements := binary.LittleEndian.Uint32((*hi)[LenBytes : 2*LenBytes])

	var currentElement uint64
	var elemEnd uint32

	var itemLen = uint32(8)
Loop:
	for i := numOfElements; i > 0; i-- {
		if i > numOfUint32Elements {
			elemEnd = LenBytes*2 + numOfUint32Elements*4 + (i-numOfUint32Elements)*8
			currentElement = binary.LittleEndian.Uint64((*hi)[elemEnd-8 : elemEnd])
			itemLen = 8
		} else {
			elemEnd = LenBytes*2 + i*4
			currentElement = uint64(binary.LittleEndian.Uint32((*hi)[elemEnd-4 : elemEnd]))
			itemLen = 4
		}

		switch {
		case currentElement == v:
			*hi = append((*hi)[:elemEnd-itemLen], (*hi)[elemEnd:]...)
			numOfElements--
			if itemLen == 4 {
				numOfUint32Elements--
			}
		case currentElement < v:
			break Loop
		default:
			continue
		}
	}
	binary.LittleEndian.PutUint32((*hi)[0:LenBytes], numOfElements)
	binary.LittleEndian.PutUint32((*hi)[LenBytes:2*LenBytes], numOfUint32Elements)
	return hi
}

func (hi *HistoryIndexBytes) Search(v uint64) (uint64, bool) {
	if len(*hi) < 4 {
		fmt.Println(1)
		return 0, false
	}
	numOfElements := binary.LittleEndian.Uint32((*hi)[0:LenBytes])
	numOfUint32Elements := binary.LittleEndian.Uint32((*hi)[LenBytes : 2*LenBytes])
	var itemLen = uint32(8)

	if numOfElements == 0 {
		fmt.Println(2)
		return 0, false
	}

	//check last element
	var lastElement uint64
	if numOfUint32Elements < numOfElements {
		lastElement = binary.LittleEndian.Uint64((*hi)[LenBytes*2+numOfUint32Elements*4+ItemLen*(numOfElements-numOfUint32Elements)-ItemLen : LenBytes*2+numOfUint32Elements*4+ItemLen*(numOfElements-numOfUint32Elements)])
	} else {
		lastElement = uint64(binary.LittleEndian.Uint32((*hi)[LenBytes*2+numOfUint32Elements*4-4 : LenBytes*2+numOfUint32Elements*4]))
	}

	if lastElement < v {
		return 0, false
	}
	var currentElement uint64
	var elemEnd uint32

	for i := numOfElements - 1; i > 0; i-- {
		if i > numOfUint32Elements {
			elemEnd = LenBytes*2 + numOfUint32Elements*4 + (i-numOfUint32Elements)*8
			currentElement = binary.LittleEndian.Uint64((*hi)[elemEnd-8 : elemEnd])
			itemLen = 8
		} else {
			elemEnd = LenBytes*2 + i*4
			currentElement = uint64(binary.LittleEndian.Uint32((*hi)[elemEnd-4 : elemEnd]))
			itemLen = 4
		}

		switch {
		case currentElement == v:
			return v, true
		case currentElement < v:
			if itemLen == 4 {
				return uint64(binary.LittleEndian.Uint32((*hi)[elemEnd : elemEnd+itemLen])), true
			}
			return binary.LittleEndian.Uint64((*hi)[elemEnd : elemEnd+itemLen]), true
		default:
			continue
		}
	}
	if numOfUint32Elements == 0 {
		return binary.LittleEndian.Uint64((*hi)[LenBytes*2 : 2*LenBytes+ItemLen]), true
	}
	return uint64(binary.LittleEndian.Uint32((*hi)[2*LenBytes : 2*LenBytes+4])), true
}
