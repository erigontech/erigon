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

package solid

import (
	"encoding/binary"
	"encoding/json"
	"strconv"

	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/cl/utils"
)

func convertDepthToChunkSize(d int) int {
	return (1 << d) // just power of 2
}

func getTreeCacheSize(listLen int, cacheDepth int) int {
	treeChunks := convertDepthToChunkSize(cacheDepth)
	return (listLen + treeChunks - 1) / treeChunks

}

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

	*merkle_tree.MerkleTree
}

// NewUint64Slice creates a new instance of byteBasedUint64Slice with a specified capacity limit.
func NewUint64Slice(limit int) *byteBasedUint64Slice {
	o := &byteBasedUint64Slice{
		c: limit,
	}
	return o
}

// Clear clears the slice by setting its length to 0 and zeroing out its backing array.
func (arr *byteBasedUint64Slice) Clear() {
	arr.l = 0
	clear(arr.u)
	arr.MerkleTree = nil
}

// CopyTo copies the slice to a target slice.
func (arr *byteBasedUint64Slice) CopyTo(target *byteBasedUint64Slice) {
	target.c = arr.c
	target.l = arr.l
	if len(target.u) < len(arr.u) {
		target.u = make([]byte, len(arr.u))
	}
	if arr.MerkleTree != nil {
		if target.MerkleTree == nil {
			target.MerkleTree = &merkle_tree.MerkleTree{}
		}
		arr.MerkleTree.CopyInto(target.MerkleTree)
		target.SetComputeLeafFn(func(idx int, out []byte) {
			copy(out, target.u[idx*length.Hash:])
		})
	} else {
		target.MerkleTree = nil
	}

	target.u = target.u[:len(arr.u)]
	copy(target.u, arr.u)
}

func (arr *byteBasedUint64Slice) MarshalJSON() ([]byte, error) {
	list := make([]string, arr.l)
	for i := 0; i < arr.l; i++ {
		list[i] = strconv.FormatInt(int64(arr.Get(i)), 10)
	}
	return json.Marshal(list)
}

func (arr *byteBasedUint64Slice) UnmarshalJSON(buf []byte) error {
	var list []uint64

	if err := json.Unmarshal(buf, &list); err != nil {
		return err
	}
	arr.Clear()
	arr.l = len(list)
	for _, elem := range list {
		arr.Append(elem)
	}
	arr.MerkleTree = nil
	return nil
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
	arr.MerkleTree = nil
	return val
}

// Append adds a new element to the end of the slice.
func (arr *byteBasedUint64Slice) Append(v uint64) {
	if len(arr.u) <= arr.l*8 {
		arr.u = append(arr.u, merkle_tree.ZeroHashes[0][:]...)
		if arr.MerkleTree != nil {
			arr.MerkleTree.AppendLeaf()
		}
	}
	offset := arr.l * 8
	binary.LittleEndian.PutUint64(arr.u[offset:offset+8], v)
	if arr.MerkleTree != nil {
		arr.MerkleTree.MarkLeafAsDirty(arr.l / 4)
	}
	arr.l++
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
	if arr.MerkleTree != nil {
		arr.MerkleTree.MarkLeafAsDirty(index / 4)
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
	if arr.MerkleTree == nil {
		arr.MerkleTree = &merkle_tree.MerkleTree{}
		cap := uint64((arr.c*8 + length.Hash - 1) / length.Hash)

		arr.MerkleTree.Initialize((arr.l+3)/4, merkle_tree.OptimalMaxTreeCacheDepth, func(idx int, out []byte) {
			copy(out, arr.u[idx*length.Hash:])
		}, &cap)
	}

	coreRoot := arr.ComputeRoot()
	lengthRoot := merkle_tree.Uint64Root(uint64(arr.l))
	return utils.Sha256(coreRoot[:], lengthRoot[:]), nil
}

// HashVectorSSZ computes the SSZ hash of the slice as a vector. It returns the hash and any error encountered.
func (arr *byteBasedUint64Slice) HashVectorSSZ() ([32]byte, error) {
	if arr.MerkleTree == nil {
		arr.MerkleTree = &merkle_tree.MerkleTree{}
		arr.MerkleTree.Initialize((arr.l+3)/4, merkle_tree.OptimalMaxTreeCacheDepth, func(idx int, out []byte) {
			copy(out, arr.u[idx*length.Hash:])
		}, nil)
	}

	return arr.ComputeRoot(), nil
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
	arr.l = len(buf) / 8
	bufferLength := length.Hash*((arr.l-1)/4) + length.Hash
	arr.u = make([]byte, bufferLength)
	copy(arr.u, buf)
	arr.MerkleTree = nil
	return nil
}

// EncodingSizeSSZ returns the size in bytes that the slice would occupy when encoded in SSZ format.
func (arr *byteBasedUint64Slice) EncodingSizeSSZ() int {
	return arr.l * 8
}
