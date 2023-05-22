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

func HashTreeRoot(schema ...interface{}) ([32]byte, error) {
	leaves := make([]byte, NextPowerOfTwo(uint64(len(schema)*length.Hash)))
	pos := 0
	for i, element := range schema {
		switch obj := element.(type) {
		case uint64:
			binary.LittleEndian.PutUint64(leaves[pos:], obj)
		case *uint64:
			binary.LittleEndian.PutUint64(leaves[pos:], *obj)
		case []byte:
			if len(obj) < length.Hash {
				copy(leaves[pos:], obj)
			} else {
				root, err := BytesRoot(obj)
				if err != nil {
					return [32]byte{}, err
				}
				copy(leaves[pos:], root[:])
			}
		case ssz.HashableSSZ:
			root, err := obj.HashSSZ()
			if err != nil {
				return [32]byte{}, err
			}
			copy(leaves[pos:], root[:])
		default:
			panic(fmt.Sprintf("get it out of my face, u put a bad component in the schema. index %d", i))
		}
		pos += length.Hash
	}
	if err := MerkleRootFromFlatLeaves(leaves, leaves); err != nil {
		return [32]byte{}, err
	}
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
	// this commented naive method of conversion supposedly leads to corruption https://github.com/golang/go/issues/40701
	//header := *(*reflect.SliceHeader)(unsafe.Pointer(&xs))
	//header.Len /= 32
	//header.Cap /= 32
	//chunkedChunks := *(*[][32]byte)(unsafe.Pointer(&header))
	//supposedly, this is because escape analysis does not correctly analyze this, and so you have this ghost header?

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
