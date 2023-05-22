package merkle_tree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/prysmaticlabs/gohashtree"
)

// HashTreeRoot returns the hash for a given schema of objects.
// IMPORTANT: DATA TYPE MUST IMPLEMENT HASHABLE
// SUPPORTED PRIMITIVES: uint64, *uint64 and []byte
func HashTreeRoot(schema ...interface{}) ([32]byte, error) {
	// Calculate the total number of leaves needed based on the schema length
	leaves := make([]byte, NextPowerOfTwo(uint64(len(schema)*length.Hash)))
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
			panic(fmt.Sprintf("get it out of my face, u put a bad component in the schema. index %d", i))
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

// HashByteSlice is gohashtree HashBytSlice but using our hopefully safer header converstion
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
	// i wont pretend to understand, but my solution for the problem is as so

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
	return globalHasher.merkleizeTrieLeavesFlat(leaves, out, NextPowerOfTwo(uint64((len(leaves)+31)/32)))
}

func MerkleRootFromFlatLeavesWithLimit(leaves []byte, out []byte, limit uint64) (err error) {
	return globalHasher.merkleizeTrieLeavesFlat(leaves, out, limit)
}
