package merkle_tree

import (
	"math/bits"

	"github.com/prysmaticlabs/gohashtree"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/ssz"

	"github.com/ledgerwatch/erigon/cl/utils"
)

// MerkleizeVector uses our optimized routine to hash a list of 32-byte
// elements.
func MerkleizeVector(elements [][32]byte, length uint64) ([32]byte, error) {
	depth := GetDepth(length)
	// Return zerohash at depth
	if len(elements) == 0 {
		return ZeroHashes[depth], nil
	}
	for i := uint8(0); i < depth; i++ {
		// Sequential
		layerLen := len(elements)
		if layerLen%2 == 1 {
			elements = append(elements, ZeroHashes[i])
		}
		outputLen := len(elements) / 2
		if err := gohashtree.Hash(elements, elements); err != nil {
			return [32]byte{}, err
		}
		elements = elements[:outputLen]
	}
	return elements[0], nil
}

// MerkleizeVector uses our optimized routine to hash a list of 32-byte
// elements.
func MerkleizeVectorFlat(in []byte, limit uint64) ([32]byte, error) {
	elements := make([]byte, len(in))
	copy(elements, in)
	for i := uint8(0); i < GetDepth(limit); i++ {
		// Sequential
		layerLen := len(elements)
		if layerLen%64 == 32 {
			elements = append(elements, ZeroHashes[i][:]...)
		}
		outputLen := len(elements) / 2
		if err := HashByteSlice(elements, elements); err != nil {
			return [32]byte{}, err
		}
		elements = elements[:outputLen]
	}
	return common.BytesToHash(elements[:length.Hash]), nil
}

// BitlistRootWithLimit computes the HashSSZ merkleization of
// participation roots.
func BitlistRootWithLimit(bits []byte, limit uint64) ([32]byte, error) {
	var (
		unpackedRoots []byte
		size          uint64
	)
	unpackedRoots, size = parseBitlist(unpackedRoots, bits)

	roots := packBits(unpackedRoots)
	base, err := MerkleizeVector(roots, (limit+255)/256)
	if err != nil {
		return [32]byte{}, err
	}

	lengthRoot := Uint64Root(size)
	return utils.Keccak256(base[:], lengthRoot[:]), nil
}

func packBits(bytes []byte) [][32]byte {
	var chunks [][32]byte
	for i := 0; i < len(bytes); i += 32 {
		var chunk [32]byte
		copy(chunk[:], bytes[i:])
		chunks = append(chunks, chunk)
	}
	return chunks
}

func parseBitlist(dst, buf []byte) ([]byte, uint64) {
	msb := uint8(bits.Len8(buf[len(buf)-1])) - 1
	size := uint64(8*(len(buf)-1) + int(msb))

	dst = append(dst, buf...)
	dst[len(dst)-1] &^= uint8(1 << msb)

	newLen := len(dst)
	for i := len(dst) - 1; i >= 0; i-- {
		if dst[i] != 0x00 {
			break
		}
		newLen = i
	}
	res := dst[:newLen]
	return res, size
}

func TransactionsListRoot(transactions [][]byte) ([32]byte, error) {
	return globalHasher.transactionsListRoot(transactions)
}

func ListObjectSSZRoot[T ssz.HashableSSZ](list []T, limit uint64) ([32]byte, error) {
	globalHasher.mu2.Lock()
	defer globalHasher.mu2.Unlock()
	// due to go generics we cannot make a method for global hasher.
	subLeaves := globalHasher.getBufferForSSZList(len(list))
	for i, element := range list {
		subLeaf, err := element.HashSSZ()
		if err != nil {
			return [32]byte{}, err
		}
		subLeaves[i] = subLeaf
	}
	vectorLeaf, err := MerkleizeVector(subLeaves, limit)
	if err != nil {
		return [32]byte{}, err
	}
	lenLeaf := Uint64Root(uint64(len(list)))
	return utils.Keccak256(vectorLeaf[:], lenLeaf[:]), nil
}
