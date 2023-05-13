package state_encoding

import (
	"encoding/binary"

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
