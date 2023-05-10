package merkle_tree

import (
	"errors"
	"reflect"
	"unsafe"

	"github.com/prysmaticlabs/gohashtree"
)

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

// getDepth returns the depth of a merkle tree with a given number of nodes.
// The depth is defined as the number of levels in the tree, with the root
// node at level 0 and each child node at a level one greater than its parent.
// If the number of nodes is less than or equal to 1, the depth is 0.
func getDepth(v uint64) uint8 {
	// If there are 0 or 1 nodes, the depth is 0.
	if v <= 1 {
		return 0
	}

	// Initialize the depth to 0.
	depth := uint8(0)

	// Divide the number of nodes by 2 until it is less than or equal to 1.
	// The number of iterations is the depth of the tree.
	for v > 1 {
		v >>= 1
		depth++
	}

	return depth
}
