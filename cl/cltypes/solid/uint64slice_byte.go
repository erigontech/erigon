package solid

import (
	"encoding/binary"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cl/utils"
)

// byteBasedUint64Slice represents a dynamic Uint64Slice data type that is byte-backed.
// The underlying storage for the slice is a byte array. This approach allows for efficient
// memory usage, especially when dealing with large slices.
type byteBasedUint64Slice struct {
	// The bytes that back the slice
	u []byte

	// Length of the slice
	l int

	// Capacity of the slice
	c int

	hashBuf
}

// NewUint64Slice creates a new instance of byteBasedUint64Slice with a specified capacity limit.
func NewUint64Slice(limit int) *byteBasedUint64Slice {
	o := &byteBasedUint64Slice{
		c: limit,
	}
	o.u = make([]byte, 0)
	return o
}

// Clear clears the slice by setting its length to 0 and zeroing out its backing array.
func (arr *byteBasedUint64Slice) Clear() {
	arr.l = 0
	for i := range arr.u {
		arr.u[i] = 0
	}
}

// CopyTo copies the slice to a target slice.
func (arr *byteBasedUint64Slice) CopyTo(target *byteBasedUint64Slice) {
	target.Clear()

	target.c = arr.c
	target.l = arr.l
	if len(target.u) < len(arr.u) {
		target.u = make([]byte, len(arr.u))
	}
	target.u = target.u[:len(arr.u)]
	copy(target.u, arr.u)
}

// depth returns the depth of the Merkle tree representing the slice.
func (arr *byteBasedUint64Slice) depth() int {
	return int(GetDepth(uint64(arr.c) / 4))
}

// Range iterates over the slice and applies a provided function to each element.
func (arr *byteBasedUint64Slice) Range(fn func(index int, value uint64, length int) bool) {
	for i := 0; i < arr.l; i++ {
		cont := fn(i, arr.Get(i), arr.l)
		if !cont {
			break
		}
	}
}

// Pop removes and returns the last element of the slice.
func (arr *byteBasedUint64Slice) Pop() uint64 {
	offset := (arr.l - 1) * 8
	val := binary.LittleEndian.Uint64(arr.u[offset : offset+8])
	binary.LittleEndian.PutUint64(arr.u[offset:offset+8], 0)
	arr.l = arr.l - 1
	return val
}

// Append adds a new element to the end of the slice.
func (arr *byteBasedUint64Slice) Append(v uint64) {
	if len(arr.u) <= arr.l*8 {
		arr.u = append(arr.u, make([]byte, 32)...)
	}
	offset := arr.l * 8
	binary.LittleEndian.PutUint64(arr.u[offset:offset+8], v)
	arr.l = arr.l + 1
}

// Get returns the element at the given index.
func (arr *byteBasedUint64Slice) Get(index int) uint64 {
	if index >= arr.l {
		panic("index out of range")
	}
	offset := index * 8
	return binary.LittleEndian.Uint64(arr.u[offset : offset+8])
}

// Set replaces the element at the given index with a new value.
func (arr *byteBasedUint64Slice) Set(index int, v uint64) {
	if index >= arr.l {
		panic("index out of range")
	}
	offset := index * 8
	binary.LittleEndian.PutUint64(arr.u[offset:offset+8], v)
}

// Length returns the current length (number of elements) of the slice.
func (arr *byteBasedUint64Slice) Length() int {
	return arr.l
}

// Cap returns the capacity (maximum number of elements) of the slice.
func (arr *byteBasedUint64Slice) Cap() int {
	return arr.c
}

// HashListSSZ computes the SSZ hash of the slice as a list. It returns the hash and any error encountered.
func (arr *byteBasedUint64Slice) HashListSSZ() ([32]byte, error) {
	depth := GetDepth((uint64(arr.c)*8 + 31) / 32)
	baseRoot := [32]byte{}
	var err error
	if arr.l == 0 {
		copy(baseRoot[:], merkle_tree.ZeroHashes[depth][:])
	} else {
		baseRoot, err = arr.HashVectorSSZ()
		if err != nil {
			return [32]byte{}, err
		}
	}
	lengthRoot := merkle_tree.Uint64Root(uint64(arr.l))
	return utils.Keccak256(baseRoot[:], lengthRoot[:]), nil
}

// HashVectorSSZ computes the SSZ hash of the slice as a vector. It returns the hash and any error encountered.
func (arr *byteBasedUint64Slice) HashVectorSSZ() ([32]byte, error) {
	depth := GetDepth((uint64(arr.c)*8 + 31) / 32)
	offset := 32*((arr.l-1)/4) + 32
	elements := arr.u[:offset]
	for i := uint8(0); i < depth; i++ {
		layerLen := len(elements)
		if layerLen%64 == 32 {
			elements = append(elements, merkle_tree.ZeroHashes[i][:]...)
		}
		outputLen := len(elements) / 2
		arr.makeBuf(outputLen)
		if err := merkle_tree.HashByteSlice(arr.buf, elements); err != nil {
			return [32]byte{}, err
		}
		elements = arr.buf
	}

	return common.BytesToHash(elements[:32]), nil
}

// EncodeSSZ encodes the slice in SSZ format. It appends the encoded data to the provided buffer and returns the result.
func (arr *byteBasedUint64Slice) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, arr.u[:arr.l*8]...), nil
}

// DecodeSSZ decodes the slice from SSZ format. It takes a byte slice as input and updates the current slice.
func (arr *byteBasedUint64Slice) DecodeSSZ(buf []byte, _ int) error {
	if len(buf)%8 > 0 {
		return ssz.ErrBadDynamicLength
	}
	bufferLength := len(buf) + (length.Hash - (len(buf) % length.Hash))
	arr.u = make([]byte, bufferLength)
	copy(arr.u, buf)
	arr.l = len(buf) / 8
	return nil
}

// EncodingSizeSSZ returns the size in bytes that the slice would occupy when encoded in SSZ format.
func (arr *byteBasedUint64Slice) EncodingSizeSSZ() int {
	return arr.l * 8
}
