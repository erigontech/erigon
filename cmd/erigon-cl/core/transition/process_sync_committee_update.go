package transition

import (
	"encoding/binary"
	"math"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

// ProcessSyncCommitteeUpdate implements processing for the sync committee update. unfortunately there is no easy way to test it.
func ProcessSyncCommitteeUpdate(state *state.BeaconState) error {
	if (state.Epoch()+1)%state.BeaconConfig().EpochsPerSyncCommitteePeriod != 0 {
		return nil
	}
	// Set new current sync committee.
	state.SetCurrentSyncCommittee(state.NextSyncCommittee())
	// Compute next new sync committee
	committee, err := computeNextSyncCommittee(state)
	if err != nil {
		return err
	}
	state.SetNextSyncCommittee(committee)
	return nil
}

func computeNextSyncCommittee(state *state.BeaconState) (*cltypes.SyncCommittee, error) {
	beaconConfig := state.BeaconConfig()
	optimizedHashFunc := utils.OptimizedKeccak256()
	epoch := state.Epoch() + 1
	//math.MaxUint8
	activeValidatorIndicies := state.GetActiveValidatorsIndices(epoch)
	activeValidatorCount := uint64(len(activeValidatorIndicies))
	seed := state.GetSeed(epoch, beaconConfig.DomainSyncCommittee)
	i := uint64(0)
	syncCommitteePubKeys := make([][48]byte, 0, cltypes.SyncCommitteeSize)
	preInputs := state.ComputeShuffledIndexPreInputs(seed)
	for len(syncCommitteePubKeys) < cltypes.SyncCommitteeSize {
		shuffledIndex, err := state.ComputeShuffledIndex(i%activeValidatorCount, activeValidatorCount, seed, preInputs, optimizedHashFunc)
		if err != nil {
			return nil, err
		}
		candidateIndex := activeValidatorIndicies[shuffledIndex]
		// Compute random byte.
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, i/32)
		input := append(seed[:], buf...)
		randomByte := uint64(utils.Keccak256(input)[i%32])
		// retrieve validator.
		validator, err := state.ValidatorAt(int(candidateIndex))
		if err != nil {
			return nil, err
		}
		if validator.EffectiveBalance*math.MaxUint8 >= beaconConfig.MaxEffectiveBalance*randomByte {
			syncCommitteePubKeys = append(syncCommitteePubKeys, validator.PublicKey)
		}
		i++
	}
	// Format public keys.
	formattedKeys := make([][]byte, cltypes.SyncCommitteeSize)
	for i := range formattedKeys {
		formattedKeys[i] = make([]byte, 48)
		copy(formattedKeys[i], syncCommitteePubKeys[i][:])
	}
	aggregatePublicKeyBytes, err := bls.AggregatePublickKeys(formattedKeys)
	if err != nil {
		return nil, err
	}
	var aggregate [48]byte
	copy(aggregate[:], aggregatePublicKeyBytes)
	return &cltypes.SyncCommittee{
		PubKeys:            syncCommitteePubKeys,
		AggregatePublicKey: aggregate,
	}, nil
}
