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

package merkle_tree

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/prysmaticlabs/gohashtree"

	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/common/ssz"
)

// maxStackLeaves covers every SSZ schema we hash, so the leaf buffer stays off the
// heap; anything larger falls back to make. The array is zeroed on every call, so
// it is sized to the schemas we actually see rather than generously.
const maxStackLeaves = 32

// HashTreeRoot returns the hash for a given schema of objects.
// IMPORTANT: DATA TYPE MUST IMPLEMENT HASHABLE
// SUPPORTED PRIMITIVES: uint64, *uint64 and []byte
func HashTreeRoot(schema ...any) ([32]byte, error) {
	if len(schema) == 0 {
		return [32]byte{}, errors.New("empty schema")
	}
	var stack [maxStackLeaves * length.Hash]byte // stack-allocation for most of cases
	size := math.NextPowerOfTwo(uint64(len(schema) * length.Hash))
	var leaves []byte
	if size <= uint64(len(stack)) {
		leaves = stack[:size]
	} else {
		leaves = make([]byte, size)
	}
	pos := 0

	// Iterate over each element in the schema
	for i, element := range schema {
		switch obj := element.(type) {
		case uint64:
			// If the element is a uint64, encode it as little-endian and store it in the leaves
			binary.LittleEndian.PutUint64(leaves[pos:], obj)
		case *uint64:
			// If the element is a pointer to uint64, dereference it, encode it as little-endian, and store it in the leaves
			binary.LittleEndian.PutUint64(leaves[pos:], *obj)
		case []byte:
			// If the element is a byte slice
			if len(obj) < length.Hash {
				// If the slice is shorter than the length of a hash, copy the slice into the leaves
				copy(leaves[pos:], obj)
			} else {
				// If the slice is longer or equal to the length of a hash, calculate the hash of the slice and store it in the leaves
				root, err := BytesRoot(obj)
				if err != nil {
					return [32]byte{}, err
				}
				copy(leaves[pos:], root[:])
			}
		case ssz.HashableSSZ:
			// If the element implements the HashableSSZ interface, calculate the SSZ hash and store it in the leaves
			root, err := obj.HashSSZ()
			if err != nil {
				return [32]byte{}, err
			}
			copy(leaves[pos:], root[:])
		default:
			// If the element does not match any supported types, panic with an error message
			panic(fmt.Sprintf("Can't create TreeRoot: unsupported type %T at index %d", obj, i))
		}

		// Move the position pointer to the next leaf
		pos += length.Hash
	}

	// Calculate the Merkle root from the flat leaves
	if err := MerkleRootFromFlatLeaves(leaves, leaves); err != nil {
		return [32]byte{}, err
	}

	// Convert the bytes of the resulting hash into a [32]byte and return it
	return common.BytesToHash(leaves[:length.Hash]), nil
}

func ProgressiveContainerRoot(activeFields []bool, schema ...any) ([32]byte, error) {
	if len(activeFields) == 0 || len(activeFields) > 256 || !activeFields[len(activeFields)-1] {
		return [32]byte{}, errors.New("invalid progressive container active fields")
	}
	roots, err := progressiveSchemaRoots(schema)
	if err != nil {
		return [32]byte{}, err
	}
	chunks := make([][32]byte, len(activeFields))
	rootIndex := 0
	var activeRoot [32]byte
	for i, active := range activeFields {
		if !active {
			continue
		}
		if rootIndex >= len(roots) {
			return [32]byte{}, errors.New("progressive container has fewer fields than active bits")
		}
		chunks[i] = roots[rootIndex]
		activeRoot[i/8] |= 1 << uint(i%8)
		rootIndex++
	}
	if rootIndex != len(roots) {
		return [32]byte{}, errors.New("progressive container has more fields than active bits")
	}
	root, err := MerkleizeProgressive(chunks)
	if err != nil {
		return [32]byte{}, err
	}
	return hashPair(root, activeRoot), nil
}

func ProgressiveContainerRootAll(schema ...any) ([32]byte, error) {
	activeFields := make([]bool, len(schema))
	for i := range activeFields {
		activeFields[i] = true
	}
	return ProgressiveContainerRoot(activeFields, schema...)
}

func ProgressiveListRoot(roots [][32]byte, listLength uint64) ([32]byte, error) {
	root, err := MerkleizeProgressive(roots)
	if err != nil {
		return [32]byte{}, err
	}
	return hashPair(root, Uint64Root(listLength)), nil
}

func ProgressiveBitlistRoot(packed []byte, bitLength uint64) ([32]byte, error) {
	chunks := make([][32]byte, (len(packed)+31)/32)
	for i := range packed {
		chunks[i/32][i%32] = packed[i]
	}
	return ProgressiveListRoot(chunks, bitLength)
}

func ProgressiveBasicListRoot(packed []byte, listLength uint64) ([32]byte, error) {
	chunks := make([][32]byte, (len(packed)+31)/32)
	for i := range packed {
		chunks[i/32][i%32] = packed[i]
	}
	return ProgressiveListRoot(chunks, listLength)
}

func MerkleizeProgressive(chunks [][32]byte) ([32]byte, error) {
	return merkleizeProgressive(chunks, 1)
}

func merkleizeProgressive(chunks [][32]byte, numLeaves uint64) ([32]byte, error) {
	if len(chunks) == 0 {
		return [32]byte{}, nil
	}
	count := min(uint64(len(chunks)), numLeaves)
	left, err := MerkleizeVector(chunks[:count], numLeaves)
	if err != nil {
		return [32]byte{}, err
	}
	if numLeaves > ^uint64(0)/4 {
		return [32]byte{}, errors.New("progressive merkle tree is too large")
	}
	right, err := merkleizeProgressive(chunks[count:], numLeaves*4)
	if err != nil {
		return [32]byte{}, err
	}
	return hashPair(left, right), nil
}

func progressiveSchemaRoots(schema []any) ([][32]byte, error) {
	roots := make([][32]byte, len(schema))
	for i, element := range schema {
		switch obj := element.(type) {
		case uint64:
			binary.LittleEndian.PutUint64(roots[i][:], obj)
		case *uint64:
			binary.LittleEndian.PutUint64(roots[i][:], *obj)
		case []byte:
			if len(obj) < length.Hash {
				copy(roots[i][:], obj)
				continue
			}
			root, err := BytesRoot(obj)
			if err != nil {
				return nil, err
			}
			roots[i] = root
		case ssz.HashableSSZ:
			root, err := obj.HashSSZ()
			if err != nil {
				return nil, err
			}
			roots[i] = root
		default:
			panic(fmt.Sprintf("Can't create TreeRoot: unsported type %T at index %d", i, obj))
		}
	}
	return roots, nil
}

func hashPair(left, right [32]byte) [32]byte {
	var pair [64]byte
	copy(pair[:32], left[:])
	copy(pair[32:], right[:])
	return sha256.Sum256(pair[:])
}

// HashByteSlice is gohashtree HashBytSlice but using our hopefully safer header conversion
func HashByteSlice(out, in []byte) error {
	if len(in) == 0 {
		return errors.New("zero leaves provided")
	}

	if len(out)%32 != 0 {
		return errors.New("output must be multple of 32")
	}
	if len(in)%64 != 0 {
		return errors.New("input must be multple of 64")
	}
	c_in := convertHeader(in)
	c_out := convertHeader(out)
	err := gohashtree.Hash(c_out, c_in)
	if err != nil {
		return err
	}
	return nil
}

func convertHeader(xs []byte) [][32]byte {
	// i won't pretend to understand, but my solution for the problem is as so

	// first i grab the slice header of the input
	header := (*reflect.SliceHeader)(unsafe.Pointer(&xs))
	// then i allocate a new result slice of no size - this should make the escape analyzer happy i think?
	dat := make([][32]byte, 0)
	// we then get the header of our output  to modify
	chunkedHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dat))
	// then we move over the values
	chunkedHeader.Len = header.Len / 32
	chunkedHeader.Cap = header.Cap / 32
	chunkedHeader.Data = header.Data
	return dat
}

func MerkleRootFromFlatLeaves(leaves []byte, out []byte) (err error) {
	if len(leaves) <= 32 {
		copy(out, leaves)
		return
	}
	return globalHasher.merkleizeTrieLeavesFlat(leaves, out, math.NextPowerOfTwo(uint64((len(leaves)+31)/32)))
}

func MerkleRootFromFlatFromIntermediateLevel(nodes []byte, out []byte, leavesLen, intermediateLevel int) (err error) {
	if len(nodes) <= 32 {
		copy(out, nodes)
		return
	}
	return globalHasher.merkleizeTrieLeavesFlatWithStart(nodes, out, math.NextPowerOfTwo(uint64((leavesLen+31)/32)), uint64(intermediateLevel))
}

func MerkleRootFromFlatFromIntermediateLevelWithLimit(nodes []byte, out []byte, limit, intermediateLevel int) (err error) {
	return globalHasher.merkleizeTrieLeavesFlatWithStart(nodes, out, uint64(limit), uint64(intermediateLevel))
}

func MerkleRootFromFlatLeavesWithLimit(leaves []byte, out []byte, limit uint64) (err error) {
	return globalHasher.merkleizeTrieLeavesFlat(leaves, out, limit)
}

// Merkle Proof computes the merkle proof for a given schema of objects.
func MerkleProof(depth, proofIndex int, schema ...any) ([][32]byte, error) {
	// Calculate the total number of leaves needed based on the schema length
	maxDepth := GetDepth(uint64(len(schema)))
	if utils.PowerOf2(uint64(maxDepth)) != uint64(len(schema)) {
		maxDepth++
	}

	if depth != int(maxDepth) { // TODO: Add support for lower depths
		return nil, fmt.Errorf("depth is different than maximum depth, have %d, want %d", depth, maxDepth)
	}
	var err error
	proof := make([][32]byte, maxDepth)
	currentSizeDepth := utils.PowerOf2(uint64(maxDepth))
	for len(schema) != int(currentSizeDepth) { // Augment the schema to be a power of 2
		schema = append(schema, make([]byte, 32))
	}

	for i := range depth {
		// Hash the left branch
		if proofIndex >= int(currentSizeDepth)/2 {
			proof[depth-i-1], err = HashTreeRoot(schema[0 : currentSizeDepth/2]...)
			if err != nil {
				return nil, err
			}
			schema = schema[currentSizeDepth/2:] // explore the right branch
			proofIndex -= int(currentSizeDepth) / 2
			currentSizeDepth /= 2
			continue
		}
		// Hash the right branch
		proof[depth-i-1], err = HashTreeRoot(schema[currentSizeDepth/2:]...)
		if err != nil {
			return nil, err
		}
		schema = schema[0 : currentSizeDepth/2] // explore the left branch
		currentSizeDepth /= 2
	}
	return proof, nil
}
