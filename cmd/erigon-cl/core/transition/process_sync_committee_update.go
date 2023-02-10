package transition

import (
	"encoding/binary"
	"math"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
)

/*
def process_sync_committee_updates(state: BeaconState) -> None:
    next_epoch = get_current_epoch(state) + Epoch(1)
    if next_epoch % EPOCHS_PER_SYNC_COMMITTEE_PERIOD == 0:
        state.current_sync_committee = state.next_sync_committee
        state.next_sync_committee = get_next_sync_committee(state)
*/
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
	// TODO(Giulio2002): Aggregate public keys.
	return &cltypes.SyncCommittee{
		PubKeys: syncCommitteePubKeys,
	}, nil
}

/*
def get_next_sync_committee_indices(state: BeaconState) -> Sequence[ValidatorIndex]:
    """
    Return the sync committee indices, with possible duplicates, for the next sync committee.
    """
    epoch = Epoch(get_current_epoch(state) + 1)

    MAX_RANDOM_BYTE = 2**8 - 1
    active_validator_indices = get_active_validator_indices(state, epoch)
    active_validator_count = uint64(len(active_validator_indices))
    seed = get_seed(state, epoch, DOMAIN_SYNC_COMMITTEE)
    i = 0
    sync_committee_indices: List[ValidatorIndex] = []
    while len(sync_committee_indices) < SYNC_COMMITTEE_SIZE:
        shuffled_index = compute_shuffled_index(uint64(i % active_validator_count), active_validator_count, seed)
        candidate_index = active_validator_indices[shuffled_index]
        random_byte = hash(seed + uint_to_bytes(uint64(i // 32)))[i % 32]
        effective_balance = state.validators[candidate_index].effective_balance
        if effective_balance * MAX_RANDOM_BYTE >= MAX_EFFECTIVE_BALANCE * random_byte:
            sync_committee_indices.append(candidate_index)
        i += 1
    return sync_committee_indices
*/
