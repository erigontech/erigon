package shuffling

import (
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state/raw"
)

func ComputeProposerIndex(b *raw.BeaconState, indices []uint64, seed [32]byte) (uint64, error) {
	if len(indices) == 0 {
		return 0, nil
	}
	maxRandomByte := uint64(1<<8 - 1)
	i := uint64(0)
	total := uint64(len(indices))
	input := make([]byte, 40)
	preInputs := ComputeShuffledIndexPreInputs(b.BeaconConfig(), seed)
	for {
		shuffled, err := ComputeShuffledIndex(b.BeaconConfig(), i%total, total, seed, preInputs, utils.Keccak256)
		if err != nil {
			return 0, err
		}
		candidateIndex := indices[shuffled]
		if candidateIndex >= uint64(b.ValidatorLength()) {
			return 0, fmt.Errorf("candidate index out of range: %d for validator set of length: %d", candidateIndex, b.ValidatorLength())
		}
		copy(input, seed[:])
		binary.LittleEndian.PutUint64(input[32:], i/32)
		randomByte := uint64(utils.Keccak256(input)[i%32])
		validator, err := b.ValidatorForValidatorIndex(int(candidateIndex))
		if err != nil {
			return 0, err
		}
		if validator.EffectiveBalance()*maxRandomByte >= b.BeaconConfig().MaxEffectiveBalance*randomByte {
			return candidateIndex, nil
		}
		i += 1
	}
}
