package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/prysmaticlabs/gohashtree"
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
	res, err := merkleRootFromLeaves(input, length)
	if err != nil {
		return [32]byte{}, err
	}

	return res, nil
}

func ArraysRootWithLength(input [][32]byte, length uint64) ([32]byte, error) {
	base, err := merkleRootFromLeaves(input, length)
	if err != nil {
		return [32]byte{}, err
	}

	lengthRoot := Uint64Root(uint64(len(input)))
	return utils.Keccak256(base[:], lengthRoot[:]), nil
}

func Eth1DataVectorRoot(votes []*cltypes.Eth1Data, length uint64) ([32]byte, error) {
	var err error

	vectorizedVotesRoot := make([][32]byte, len(votes))
	// Vectorize ETH1 Data first of all
	for i, vote := range votes {
		vectorizedVotesRoot[i], err = vote.HashTreeRoot()
		if err != nil {
			return [32]byte{}, err
		}
	}

	return ArraysRootWithLength(vectorizedVotesRoot, length)
}

func ValidatorsVectorRoot(validators []*cltypes.Validator, length uint64) ([32]byte, error) {
	var err error

	vectorizedValidatorsRoot := make([][32]byte, len(validators))
	// Vectorize ETH1 Data first of all
	for i, validator := range validators {
		vectorizedValidatorsRoot[i], err = validator.HashTreeRoot()
		if err != nil {
			return [32]byte{}, err
		}
	}

	return ArraysRootWithLength(vectorizedValidatorsRoot, length)
}

func merkleRootFromLeaves(leaves [][32]byte, length uint64) ([32]byte, error) {
	if len(leaves) == 0 {
		return [32]byte{}, errors.New("zero leaves provided")
	}
	if len(leaves) == 1 {
		return leaves[0], nil
	}
	hashLayer := leaves
	layers := make([][][32]byte, depth(length)+1)
	layers[0] = hashLayer
	var err error
	hashLayer, err = merkleizeTrieLeaves(layers, hashLayer)
	if err != nil {
		return [32]byte{}, err
	}
	root := hashLayer[0]
	return root, nil
}

// depth retrieves the appropriate depth for the provided trie size.
func depth(v uint64) (out uint8) {
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
func merkleizeTrieLeaves(layers [][][32]byte, hashLayer [][32]byte) ([][32]byte, error) {
	i := 1
	chunkBuffer := bytes.NewBuffer([]byte{})
	chunkBuffer.Grow(64)
	for len(hashLayer) > 1 && i < len(layers) {
		if !utils.IsPowerOf2(uint64(len(hashLayer))) {
			return nil, fmt.Errorf("hash layer is a non power of 2: %d", len(hashLayer))
		}
		newLayer := make([][32]byte, len(hashLayer)/2)
		err := gohashtree.Hash(hashLayer, newLayer)
		if err != nil {
			return nil, err
		}
		hashLayer = newLayer
		layers[i] = hashLayer
		i++
	}
	return hashLayer, nil
}
