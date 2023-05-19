package state_encoding

import "github.com/ledgerwatch/erigon/cl/merkle_tree"

const (
	BlockRootsLength        = 8192
	StateRootsLength        = 8192
	HistoricalRootsLength   = 16777216
	Eth1DataVotesRootsLimit = 2048
	ValidatorRegistryLimit  = 1099511627776
	RandaoMixesLength       = 65536
	SlashingsLength         = 8192
)

func ValidatorLimitForBalancesChunks() uint64 {
	maxValidatorLimit := uint64(ValidatorRegistryLimit)
	bytesInUint64 := uint64(8)
	return (maxValidatorLimit*bytesInUint64 + 31) / 32 // round to nearest chunk
}

func SlashingsRoot(slashings []uint64) ([32]byte, error) {
	roots := merkle_tree.PackUint64IntoChunks(slashings)
	return merkle_tree.ArraysRoot(roots, uint64(len(roots)))
}
