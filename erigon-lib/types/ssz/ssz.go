// Copyright 2021 The Erigon Authors
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

package ssz

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/types/clonable"
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
	DecodeSSZ(buf []byte, version int) error
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

func Uint64SSZDecode(buf []byte) uint64 {
	return binary.LittleEndian.Uint64(buf)
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

func DecodeDynamicList[T Unmarshaler](bytes []byte, start, end uint32, _max uint64, version int) ([]T, error) {
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
	if uint64(elementsNum) > _max {
		return nil, errors.Join(ErrTooBigList, fmt.Errorf("DecodeDynamicList: expected %d elements, got %d", _max, elementsNum))
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
		if err := objs[i].DecodeSSZ(buf[currentOffset:endOffset], version); err != nil {
			return nil, err
		}
		currentOffset = endOffset
	}
	return objs, nil
}

func DecodeStaticList[T Unmarshaler](bytes []byte, start, end, bytesPerElement uint32, _max uint64, version int) ([]T, error) {
	if start > end || len(bytes) < int(end) {
		return nil, ErrBadOffset
	}
	buf := bytes[start:end]
	elementsNum := uint64(len(buf)) / uint64(bytesPerElement)
	// Check for errors
	if uint32(len(buf))%bytesPerElement != 0 {
		return nil, ErrBufferNotRounded
	}
	if elementsNum > _max {
		return nil, errors.Join(ErrTooBigList, fmt.Errorf("DecodeStaticList: expected %d elements, got %d", _max, elementsNum))
	}
	objs := make([]T, elementsNum)
	for i := range objs {
		objs[i] = objs[i].Clone().(T)
		if err := objs[i].DecodeSSZ(buf[i*int(bytesPerElement):], version); err != nil {
			return nil, err
		}
	}
	return objs, nil
}

func DecodeHashList(bytes []byte, start, end, _max uint32) ([]common.Hash, error) {
	if start > end || len(bytes) < int(end) {
		return nil, ErrBadOffset
	}
	buf := bytes[start:end]
	elementsNum := uint32(len(buf)) / length.Hash
	// Check for errors
	if uint32(len(buf))%length.Hash != 0 {
		return nil, ErrBufferNotRounded
	}
	if elementsNum > _max {
		return nil, errors.Join(ErrTooBigList, fmt.Errorf("DecodeHashList: expected %d elements, got %d", _max, elementsNum))
	}
	objs := make([]common.Hash, elementsNum)
	for i := range objs {
		copy(objs[i][:], buf[i*length.Hash:])
	}
	return objs, nil
}

func DecodeNumbersList(bytes []byte, start, end uint32, _max uint64) ([]uint64, error) {
	if start > end || len(bytes) < int(end) {
		return nil, ErrBadOffset
	}
	buf := bytes[start:end]
	elementsNum := uint64(len(buf)) / length.BlockNum
	// Check for errors
	if uint64(len(buf))%length.BlockNum != 0 {
		return nil, ErrBufferNotRounded
	}
	if elementsNum > _max {
		return nil, errors.Join(ErrTooBigList, fmt.Errorf("DecodeNumbersList: expected %d elements, got %d", _max, elementsNum))
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

func DecodeString(bytes []byte, start, end, _max uint64) ([]byte, error) {
	if start > end || len(bytes) < int(end) {
		return nil, ErrBadOffset
	}
	buf := bytes[start:end]
	if uint64(len(buf)) > _max {
		return nil, errors.Join(ErrTooBigList, fmt.Errorf("DecodeString: expected %d bytes, got %d", _max, len(buf)))
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
