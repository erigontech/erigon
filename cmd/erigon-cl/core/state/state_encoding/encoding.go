package state_encoding

import (
	"encoding/binary"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
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

	return merkle_tree.ArraysRootWithLimit(vectorizedVotesRoot, Eth1DataVotesRootsLimit)
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

	return merkle_tree.ArraysRootWithLimit(vectorizedValidatorsRoot, ValidatorRegistryLimit)
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
	slashingChunks, err := merkle_tree.PackSlashings(slashingMarshaling)
	if err != nil {
		return [32]byte{}, err
	}
	return merkle_tree.ArraysRoot(slashingChunks, uint64(len(slashingChunks)))
}
