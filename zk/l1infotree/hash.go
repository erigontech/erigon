package l1infotree

import (
	"encoding/binary"

	"github.com/iden3/go-iden3-crypto/keccak256"
	"github.com/ledgerwatch/erigon-lib/common"
	"golang.org/x/crypto/sha3"
)

// Hash calculates the keccak hash of elements.
func Hash(data ...[32]byte) [32]byte {
	var res [32]byte
	hash := sha3.NewLegacyKeccak256()
	for _, d := range data {
		hash.Write(d[:]) //nolint:errcheck,gosec
	}
	copy(res[:], hash.Sum(nil))
	return res
}

func generateZeroHashes(height uint8) [][32]byte {
	var zeroHashes = [][32]byte{
		common.Hash{},
	}
	// This generates a leaf = HashZero in position 0. In the rest of the positions that are equivalent to the ascending levels,
	// we set the hashes of the nodes. So all nodes from level i=5 will have the same value and same children nodes.
	for i := 1; i <= int(height); i++ {
		zeroHashes = append(zeroHashes, Hash(zeroHashes[i-1], zeroHashes[i-1]))
	}
	return zeroHashes
}

// HashLeafData calculates the keccak hash of the leaf values.
func HashLeafData(ger, prevBlockHash common.Hash, minTimestamp uint64) [32]byte {
	var res [32]byte
	t := make([]byte, 8) //nolint:gomnd
	binary.BigEndian.PutUint64(t, minTimestamp)
	copy(res[:], keccak256.Hash(ger.Bytes(), prevBlockHash.Bytes(), t))
	return res
}
