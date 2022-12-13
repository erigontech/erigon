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

// Bits and masks are used for depth calculation.
const (
	mask0 = ^uint64((1 << (1 << iota)) - 1)
	mask1
	mask2
	mask3
	mask4
	mask5
)

const (
	bit0 = uint8(1 << iota)
	bit1
	bit2
	bit3
	bit4
	bit5
)

// Uint64Root retrieve the root of uint64 fields
func Uint64Root(val uint64) common.Hash {
	var root common.Hash
	binary.LittleEndian.PutUint64(root[:], val)
	return root
}

func ArraysRoot(input [][32]byte, length uint64) ([32]byte, error) {
	leaves := make([][32]byte, length)
	copy(leaves, input)

	res, err := MerkleRootFromLeaves(leaves)
	if err != nil {
		return [32]byte{}, err
	}

	return res, nil
}

func ArraysRootWithLimit(input [][32]byte, limit uint64) ([32]byte, error) {
	base, err := MerkleizeVector(input, limit)
	if err != nil {
		return [32]byte{}, err
	}

	lengthRoot := Uint64Root(uint64(len(input)))
	return utils.Keccak256(base[:], lengthRoot[:]), nil
}

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

func Uint64ListRootWithLimit(list []uint64, limit uint64) ([32]byte, error) {
	var err error
	roots, err := PackUint64IntoChunks(list)
	if err != nil {
		return [32]byte{}, err
	}

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

// depth retrieves the appropriate depth for the provided trie size.
func getDepth(v uint64) (out uint8) {
	if v <= 1 {
		return 0
	}
	v--
	if v&mask5 != 0 {
		v >>= bit5
		out |= bit5
	}
	if v&mask4 != 0 {
		v >>= bit4
		out |= bit4
	}
	if v&mask3 != 0 {
		v >>= bit3
		out |= bit3
	}
	if v&mask2 != 0 {
		v >>= bit2
		out |= bit2
	}
	if v&mask1 != 0 {
		v >>= bit1
		out |= bit1
	}
	if v&mask0 != 0 {
		out |= bit0
	}
	out++
	return
}

// merkleizeTrieLeaves returns intermediate roots of given leaves.
func merkleizeTrieLeaves(leaves [][32]byte) ([32]byte, error) {
	for len(leaves) > 1 {
		if !utils.IsPowerOf2(uint64(len(leaves))) {
			return [32]byte{}, fmt.Errorf("hash layer is a non power of 2: %d", len(leaves))
		}
		layer := make([][32]byte, len(leaves)/2)
		for j := 0; j < len(leaves); j += 2 {
			layer[j/2] = utils.Keccak256(leaves[j][:], leaves[j+1][:])
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
