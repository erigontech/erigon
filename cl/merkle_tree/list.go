package merkle_tree

import (
	"math/bits"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/prysmaticlabs/gohashtree"

	"github.com/ledgerwatch/erigon/cl/cltypes/ssz"
	"github.com/ledgerwatch/erigon/cl/utils"
)

// MerkleizeVector uses our optimized routine to hash a list of 32-byte
// elements.
func MerkleizeVector(elements [][32]byte, length uint64) ([32]byte, error) {
	depth := getDepth(length)
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

// ArraysRootWithLimit calculates the root hash of an array of hashes by first vectorizing the input array using the MerkleizeVector function, then calculating the root hash of the vectorized array using the Keccak256 function and the root hash of the length of the input array.
func ArraysRootWithLimit(input [][32]byte, limit uint64) ([32]byte, error) {
	base, err := MerkleizeVector(input, limit)
	if err != nil {
		return [32]byte{}, err
	}

	lengthRoot := Uint64Root(uint64(len(input)))
	return utils.Keccak256(base[:], lengthRoot[:]), nil
}

// ArraysRoot calculates the root hash of an array of hashes by first making a copy of the input array, then calculating the Merkle root of the copy using the MerkleRootFromLeaves function.
func ArraysRoot(input [][32]byte, length uint64) ([32]byte, error) {
	for uint64(len(input)) != length {
		input = append(input, [32]byte{})
	}

	res, err := MerkleRootFromLeaves(input)
	if err != nil {
		return [32]byte{}, err
	}

	return res, nil
}

// Uint64ListRootWithLimit calculates the root hash of an array of uint64 values by first packing the input array into chunks using the PackUint64IntoChunks function,
// then vectorizing the chunks using the MerkleizeVector function, then calculating the
// root hash of the vectorized array using the Keccak256 function and
// the root hash of the length of the input array.
func Uint64ListRootWithLimit(list []uint64, limit uint64) ([32]byte, error) {
	var err error
	roots := PackUint64IntoChunks(list)

	base, err := MerkleizeVector(roots, limit)
	if err != nil {
		return [32]byte{}, err
	}

	lengthRoot := Uint64Root(uint64(len(list)))
	return utils.Keccak256(base[:], lengthRoot[:]), nil
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

// BitlistRootWithLimitForState computes the HashSSZ merkleization of
// participation roots.
func BitlistRootWithLimitForState(bits []byte, limit uint64) ([32]byte, error) {
	roots := packBits(bits)

	base, err := MerkleizeVector(roots, (limit+31)/32)
	if err != nil {
		return [32]byte{}, err
	}

	lengthRoot := Uint64Root(uint64(len(bits)))
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

func TransactionsListRoot(transactions [][]byte) (libcommon.Hash, error) {
	txCount := uint64(len(transactions))

	leaves := [][32]byte{}
	for _, transaction := range transactions {
		transactionLength := uint64(len(transaction))
		packedTransactions := packBits(transaction) // Pack transactions
		transactionsBaseRoot, err := MerkleizeVector(packedTransactions, 33554432)
		if err != nil {
			return libcommon.Hash{}, err
		}

		lengthRoot := Uint64Root(transactionLength)
		leaves = append(leaves, utils.Keccak256(transactionsBaseRoot[:], lengthRoot[:]))
	}
	transactionsBaseRoot, err := MerkleizeVector(leaves, 1048576)
	if err != nil {
		return libcommon.Hash{}, err
	}

	countRoot := Uint64Root(txCount)

	return utils.Keccak256(transactionsBaseRoot[:], countRoot[:]), nil
}

func ListObjectSSZRoot[T ssz.HashableSSZ](list []T, limit uint64) ([32]byte, error) {
	subLeaves := make([][32]byte, 0, len(list))
	for _, element := range list {
		subLeaf, err := element.HashSSZ()
		if err != nil {
			return [32]byte{}, err
		}
		subLeaves = append(subLeaves, subLeaf)
	}
	vectorLeaf, err := MerkleizeVector(subLeaves, limit)
	if err != nil {
		return [32]byte{}, err
	}
	lenLeaf := Uint64Root(uint64(len(list)))
	return utils.Keccak256(vectorLeaf[:], lenLeaf[:]), nil
}
