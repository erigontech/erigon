package merkle_tree

import (
	"encoding/binary"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
)

// Uint64Root retrieves the root hash of a uint64 value by converting it to a byte array and returning it as a hash.
func Uint64Root(val uint64) libcommon.Hash {
	var root libcommon.Hash
	binary.LittleEndian.PutUint64(root[:], val)
	return root
}

func BoolRoot(b bool) (root libcommon.Hash) {
	if b {
		root[0] = 1
	}
	return
}

func BytesRoot(b []byte) (out [32]byte, err error) {
	leafCount := NextPowerOfTwo(uint64((len(b) + 31) / length.Hash))
	leaves := make([]byte, leafCount*length.Hash)
	copy(leaves, b)
	if err = MerkleRootFromFlatLeaves(leaves, leaves); err != nil {
		return [32]byte{}, err
	}
	copy(out[:], leaves)
	return
}

func InPlaceRoot(key []byte) error {
	err := MerkleRootFromFlatLeaves(key, key)
	if err != nil {
		return err
	}
	return nil
}
