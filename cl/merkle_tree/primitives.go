package merkle_tree

import (
	"encoding/binary"

	"github.com/ledgerwatch/erigon/common"
)

// Uint64Root retrieves the root hash of a uint64 value by converting it to a byte array and returning it as a hash.
func Uint64Root(val uint64) common.Hash {
	var root common.Hash
	binary.LittleEndian.PutUint64(root[:], val)
	return root
}

func SignatureRoot(signature [96]byte) (common.Hash, error) {
	return ArraysRoot([][32]byte{
		common.BytesToHash(signature[0:32]),
		common.BytesToHash(signature[32:64]),
		common.BytesToHash(signature[64:]),
	}, 4)
}

func PublicKeyRoot(key [48]byte) (common.Hash, error) {
	var lastByte [32]byte
	copy(lastByte[:], key[32:])
	return ArraysRoot([][32]byte{
		common.BytesToHash(key[:32]),
		lastByte,
	}, 2)
}
