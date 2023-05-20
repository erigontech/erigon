package merkle_tree

import (
	"encoding/binary"
	"errors"
	"reflect"
	"unsafe"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/prysmaticlabs/gohashtree"
)

func HashTreeRoot(schema ...interface{}) ([32]byte, error) {
	leaves := make([]byte, nextPowerOf2(len(schema))*length.Hash)
	pos := 0
	for _, element := range schema {
		switch obj := element.(type) {
		case uint64:
			binary.LittleEndian.PutUint64(leaves[pos:], obj)
		case [32]byte:
			copy(leaves[pos:], obj[:])
		case common.Hash:
			copy(leaves[pos:], obj[:])
		case ssz.HashableSSZ:
			root, err := obj.HashSSZ()
			if err != nil {
				return [32]byte{}, err
			}
			copy(leaves[pos:], root[:])
		case [48]byte:
			root, err := PublicKeyRoot(obj)
			if err != nil {
				return [32]byte{}, err
			}
			copy(leaves[pos:], root[:])
		case [96]byte:
			root, err := SignatureRoot(obj)
			if err != nil {
				return [32]byte{}, err
			}
			copy(leaves[pos:], root[:])
		default:
			panic("get it out of my face")
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

func MerkleRootFromLeaves(leaves [][32]byte) ([32]byte, error) {
	if len(leaves) == 0 {
		return [32]byte{}, errors.New("zero leaves provided")
	}
	if len(leaves) == 1 {
		return leaves[0], nil
	}
	hashLayer := leaves
	return globalHasher.merkleizeTrieLeaves(hashLayer)
}

func MerkleRootFromFlatLeaves(leaves []byte, out []byte) (err error) {
	if len(leaves) <= 32 {
		copy(out, leaves)
		return
	}
	return globalHasher.merkleizeTrieLeavesFlat(leaves, out)
}
