package transition

import (
	"encoding/binary"
	"math"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
)

// ProcessSyncCommitteeUpdate implements processing for the sync committee update. unfortunately there is no easy way to test it.
func (s *StateTransistor) ProcessSyncCommitteeUpdate() error {
	if (s.state.Epoch()+1)%s.beaconConfig.EpochsPerSyncCommitteePeriod != 0 {
		return nil
	}
	// Set new current sync committee.
	s.state.SetCurrentSyncCommittee(s.state.NextSyncCommittee())
	// Compute next new sync committee
	committee, err := s.computeNextSyncCommittee()
	if err != nil {
		return err
	}
	s.state.SetNextSyncCommittee(committee)
	return nil
}

func (s *StateTransistor) computeNextSyncCommittee() (*cltypes.SyncCommittee, error) {
	optimizedHashFunc := utils.OptimizedKeccak256()
	epoch := s.state.Epoch() + 1
	//math.MaxUint8
	activeValidatorIndicies := s.state.GetActiveValidatorsIndices(epoch)
	activeValidatorCount := uint64(len(activeValidatorIndicies))
	seed := s.state.GetSeed(epoch, s.beaconConfig.DomainSyncCommittee)
	i := uint64(0)
	syncCommitteePubKeys := make([][48]byte, 0, cltypes.SyncCommitteeSize)
	preInputs := s.state.ComputeShuffledIndexPreInputs(seed)
	for len(syncCommitteePubKeys) < cltypes.SyncCommitteeSize {
		shuffledIndex, err := s.state.ComputeShuffledIndex(i%activeValidatorCount, activeValidatorCount, seed, preInputs, optimizedHashFunc)
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
		validator, err := s.state.ValidatorAt(int(candidateIndex))
		if err != nil {
			return nil, err
		}
		if validator.EffectiveBalance*math.MaxUint8 >= s.beaconConfig.MaxEffectiveBalance*randomByte {
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
