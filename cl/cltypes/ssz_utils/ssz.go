package ssz_utils

import (
	"encoding/binary"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"

	"github.com/ledgerwatch/erigon/cl/cltypes/clonable"
)

var (
	BaseExtraDataSSZOffsetHeader = 536
	BaseExtraDataSSZOffsetBlock  = 508
)

type HashableSSZ interface {
	HashSSZ() ([32]byte, error)
}

type EncodableSSZ interface {
	Marshaler
	Unmarshaler
}

type Marshaler interface {
	EncodeSSZ([]byte) ([]byte, error)
	EncodingSizeSSZ() int
}

type Unmarshaler interface {
	DecodeSSZ(buf []byte) error
	DecodeSSZWithVersion(buf []byte, version int) error
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

func DecodeDynamicList[T Unmarshaler](bytes []byte, start, end uint32, max uint64) ([]T, error) {
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
	if uint64(elementsNum) > max {
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
		objs[i].DecodeSSZ(buf[currentOffset:endOffset])
		currentOffset = endOffset
	}
	return objs, nil
}

func DecodeStaticList[T Unmarshaler](bytes []byte, start, end, bytesPerElement uint32, max uint64) ([]T, error) {
	if start > end || len(bytes) < int(end) {
		return nil, ErrBadOffset
	}
	buf := bytes[start:end]
	elementsNum := uint64(len(buf)) / uint64(bytesPerElement)
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
		objs[i].DecodeSSZ(buf[i*int(bytesPerElement):])
	}
	return objs, nil
}

func DecodeHashList(bytes []byte, start, end, max uint32) ([]libcommon.Hash, error) {
	if start > end || len(bytes) < int(end) {
		return nil, ErrBadOffset
	}
	buf := bytes[start:end]
	elementsNum := uint32(len(buf)) / length.Hash
	// Check for errors
	if uint32(len(buf))%length.Hash != 0 {
		return nil, ErrBufferNotRounded
	}
	if elementsNum > max {
		return nil, ErrTooBigList
	}
	objs := make([]libcommon.Hash, elementsNum)
	for i := range objs {
		copy(objs[i][:], buf[i*length.Hash:])
	}
	return objs, nil
}

func DecodeNumbersList(bytes []byte, start, end uint32, max uint64) ([]uint64, error) {
	if start > end || len(bytes) < int(end) {
		return nil, ErrBadOffset
	}
	buf := bytes[start:end]
	elementsNum := uint64(len(buf)) / length.BlockNum
	// Check for errors
	if uint64(len(buf))%length.BlockNum != 0 {
		return nil, ErrBufferNotRounded
	}
	if elementsNum > max {
		return nil, ErrTooBigList
	}
	objs := make([]uint64, elementsNum)
	for i := range objs {
		objs[i] = UnmarshalUint64SSZ(buf[i*length.BlockNum:])
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

func DecodeString(bytes []byte, start, end, max uint64) ([]byte, error) {
	if start > end || len(bytes) < int(end) {
		return nil, ErrBadOffset
	}
	buf := bytes[start:end]
	if uint64(len(buf)) > max {
		return nil, ErrTooBigList
	}
	return buf, nil
}

func EncodeDynamicList[T Marshaler](buf []byte, objs []T) (dst []byte, err error) {
	dst = buf
	// Attestation
	subOffset := len(objs) * 4
	for _, attestation := range objs {
		dst = append(dst, OffsetSSZ(uint32(subOffset))...)
		subOffset += attestation.EncodingSizeSSZ()
	}
	for _, obj := range objs {
		dst, err = obj.EncodeSSZ(dst)
		if err != nil {
			return
		}
	}
	return
}
