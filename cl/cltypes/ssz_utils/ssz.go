package ssz_utils

import (
	"encoding/binary"

	ssz "github.com/ferranbt/fastssz"
	"github.com/ledgerwatch/erigon/cl/cltypes/clonable"
)

var (
	BaseExtraDataSSZOffsetHeader = 536
	BaseExtraDataSSZOffsetBlock  = 508
)

type HashableSSZ interface {
	HashTreeRoot() ([32]byte, error)
}

type ObjectSSZ interface {
	ssz.Marshaler
	ssz.Unmarshaler
	HashableSSZ
}

type EncodableSSZ interface {
	Marshaler
	Unmarshaler
}

type Marshaler interface {
	MarshalSSZ() ([]byte, error)
	SizeSSZ() int
}

type Unmarshaler interface {
	UnmarshalSSZ(buf []byte) error
	UnmarshalSSZWithVersion(buf []byte, version int) error
	clonable.Clonable
}

func MarshalUint64SSZ(buf []byte, x uint64) {
	binary.LittleEndian.PutUint64(buf, x)
}

func Uint64SSZ(x uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, x)
	return b
}

func BoolSSZ(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func OffsetSSZ(x uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, x)
	return b
}

// EncodeOffset marshals a little endian uint32 to buf
func EncodeOffset(buf []byte, offset uint32) {
	binary.LittleEndian.PutUint32(buf, offset)
}

// ReadOffset unmarshals a little endian uint32 to dst
func DecodeOffset(x []byte) uint32 {
	return binary.LittleEndian.Uint32(x)
}

func UnmarshalUint64SSZ(x []byte) uint64 {
	return binary.LittleEndian.Uint64(x)
}

func DecodeDynamicList[T Unmarshaler](bytes []byte, start, end, max uint32) ([]T, error) {
	if start > end || len(bytes) < int(end) {
		return nil, ErrBadOffset
	}
	buf := bytes[start:end]
	var elementsNum, currentOffset uint32
	if len(buf) > 4 {
		currentOffset = DecodeOffset(buf)
		elementsNum = currentOffset / 4
	}
	inPos := 4
	if elementsNum > max {
		return nil, ErrTooBigList
	}
	objs := make([]T, elementsNum)
	for i := range objs {
		endOffset := uint32(len(buf))
		if i != len(objs)-1 {
			if len(buf[inPos:]) < 4 {
				return nil, ErrLowBufferSize
			}
			endOffset = DecodeOffset(buf[inPos:])
		}
		inPos += 4
		if endOffset < currentOffset || len(buf) < int(endOffset) {
			return nil, ErrBadOffset
		}
		objs[i] = objs[i].Clone().(T)
		objs[i].UnmarshalSSZ(buf[currentOffset:endOffset])
		currentOffset = endOffset
	}
	return objs, nil
}

func DecodeStaticList[T Unmarshaler](bytes []byte, start, end, bytesPerElement, max uint32) ([]T, error) {
	if start > end || len(bytes) < int(end) {
		return nil, ErrBadOffset
	}
	buf := bytes[start:end]
	elementsNum := uint32(len(buf)) / bytesPerElement
	// Check for errors
	if uint32(len(buf))%bytesPerElement != 0 {
		return nil, ErrBufferNotRounded
	}
	if elementsNum > max {
		return nil, ErrTooBigList
	}
	objs := make([]T, elementsNum)
	for i := range objs {
		objs[i] = objs[i].Clone().(T)
		objs[i].UnmarshalSSZ(buf[i*int(bytesPerElement):])
	}
	return objs, nil
}

func CalculateIndiciesLimit(maxCapacity, numItems, size uint64) uint64 {
	limit := (maxCapacity*size + 31) / 32
	if limit != 0 {
		return limit
	}
	if numItems == 0 {
		return 1
	}
	return numItems
}
