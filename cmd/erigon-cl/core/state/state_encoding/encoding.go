package state_encoding

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/prysmaticlabs/gohashtree"
)

const (
	BlockRootsLength        = 8192
	StateRootsLength        = 8192
	HistoricalRootsLength   = 16777216
	Eth1DataVotesRootsLimit = 2048
	ValidatorRegistryLimit  = 1099511627776
	RandaoMixesLength       = 65536
	SlashingsLength         = 8192
)

// This code is a collection of functions related to encoding and
// hashing state data in the Ethereum 2.0 beacon chain.

// Uint64Root retrieves the root hash of a uint64 value by converting it to a byte array and returning it as a hash.
func Uint64Root(val uint64) common.Hash {
	var root common.Hash
	binary.LittleEndian.PutUint64(root[:], val)
	return root
}

// ArraysRoot calculates the root hash of an array of hashes by first making a copy of the input array, then calculating the Merkle root of the copy using the MerkleRootFromLeaves function.
func ArraysRoot(input [][32]byte, length uint64) ([32]byte, error) {
	leaves := make([][32]byte, length)
	copy(leaves, input)

	res, err := MerkleRootFromLeaves(leaves)
	if err != nil {
		return [32]byte{}, err
	}

	return res, nil
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

// Eth1DataVectorRoot calculates the root hash of an array of Eth1Data values by first vectorizing the input array using
// the HashTreeRoot method on each Eth1Data value, then calculating the root hash of the vectorized array using
// the ArraysRootWithLimit function and the Eth1DataVotesRootsLimit constant.
func Eth1DataVectorRoot(votes []*cltypes.Eth1Data) ([32]byte, error) {
	var err error

	vectorizedVotesRoot := make([][32]byte, len(votes))
	// Vectorize ETH1 Data first of all
	for i, vote := range votes {
		vectorizedVotesRoot[i], err = vote.HashTreeRoot()
		if err != nil {
			return [32]byte{}, err
		}
	}

	return ArraysRootWithLimit(vectorizedVotesRoot, Eth1DataVotesRootsLimit)
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

func ValidatorsVectorRoot(validators []*cltypes.Validator) ([32]byte, error) {
	var err error

	vectorizedValidatorsRoot := make([][32]byte, len(validators))
	// Vectorize ETH1 Data first of all
	for i, validator := range validators {
		vectorizedValidatorsRoot[i], err = validator.HashTreeRoot()
		if err != nil {
			return [32]byte{}, err
		}
	}

	return ArraysRootWithLimit(vectorizedValidatorsRoot, ValidatorRegistryLimit)
}

func MerkleRootFromLeaves(leaves [][32]byte) ([32]byte, error) {
	if len(leaves) == 0 {
		return [32]byte{}, errors.New("zero leaves provided")
	}
	if len(leaves) == 1 {
		return leaves[0], nil
	}
	hashLayer := leaves
	return merkleizeTrieLeaves(hashLayer)
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

// merkleizeTrieLeaves returns intermediate roots of given leaves.
func merkleizeTrieLeaves(leaves [][32]byte) ([32]byte, error) {
	for len(leaves) > 1 {
		if !utils.IsPowerOf2(uint64(len(leaves))) {
			return [32]byte{}, fmt.Errorf("hash layer is a non power of 2: %d", len(leaves))
		}
		layer := make([][32]byte, len(leaves)/2)
		if err := gohashtree.Hash(layer, leaves); err != nil {
			return [32]byte{}, err
		}
		leaves = layer
	}
	return leaves[0], nil
}

func ValidatorLimitForBalancesChunks() uint64 {
	maxValidatorLimit := uint64(ValidatorRegistryLimit)
	bytesInUint64 := uint64(8)
	return (maxValidatorLimit*bytesInUint64 + 31) / 32 // round to nearest chunk
}

func SlashingsRoot(slashings []uint64) ([32]byte, error) {
	slashingMarshaling := make([][]byte, SlashingsLength)
	for i := 0; i < len(slashings) && i < len(slashingMarshaling); i++ {
		slashBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(slashBuf, slashings[i])
		slashingMarshaling[i] = slashBuf
	}
	slashingChunks, err := PackSlashings(slashingMarshaling)
	if err != nil {
		return [32]byte{}, err
	}
	return ArraysRoot(slashingChunks, uint64(len(slashingChunks)))
}

// MerkleizeVector uses our optimized routine to hash a list of 32-byte
// elements.
func MerkleizeVector(elements [][32]byte, length uint64) ([32]byte, error) {
	depth := getDepth(length)
	// Return zerohash at depth
	if len(elements) == 0 {
		return ZeroHashes[depth], nil
	}
	for i := uint8(0); i < depth; i++ {
		layerLen := len(elements)
		oddNodeLength := layerLen%2 == 1
		if oddNodeLength {
			zerohash := ZeroHashes[i]
			elements = append(elements, zerohash)
		}
		outputLen := len(elements) / 2
		if err := gohashtree.Hash(elements, elements); err != nil {
			return [32]byte{}, err
		}
		elements = elements[:outputLen]
	}
	return elements[0], nil
}
