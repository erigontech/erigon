package transition

import (
	"errors"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/utils"
)

// processSyncAggregate applies all the logic in the spec function `process_sync_aggregate` except
// verifying the BLS signatures. It returns the modified beacons state and the list of validators'
// public keys that voted, for future signature verification.
func (s *StateTransistor) processSyncAggregate(sync *cltypes.SyncAggregate) ([][]byte, error) {
	currentSyncCommittee := s.state.CurrentSyncCommittee()

	if currentSyncCommittee == nil {
		return nil, errors.New("nil current sync committee in state")
	}
	committeeKeys := currentSyncCommittee.PubKeys
	if len(sync.SyncCommiteeBits)*8 > len(committeeKeys) {
		return nil, errors.New("bits length exceeds committee length")
	}
	votedKeys := make([][]byte, 0, len(committeeKeys))

	proposerReward, participantReward, err := s.state.SyncRewards()
	if err != nil {
		return nil, err
	}

	proposerIndex, err := s.state.GetBeaconProposerIndex()
	if err != nil {
		return nil, err
	}

	earnedProposerReward := uint64(0)
	for i := 0; i < len(sync.SyncCommiteeBits)*8; i++ {
		vIdx, exists := s.state.ValidatorIndexByPubkey(committeeKeys[i])
		// Impossible scenario.
		if !exists {
			return nil, errors.New("validator public key does not exist in state")
		}
		bit := i % 8
		currByte := sync.SyncCommiteeBits[i/8]
		if (currByte & (1 << bit)) > 0 {
			votedKeys = append(votedKeys, committeeKeys[i][:])
			s.state.IncreaseBalance(int(vIdx), participantReward)
			earnedProposerReward += proposerReward
		} else {
			s.state.DecreaseBalance(vIdx, participantReward)
		}
	}
	s.state.IncreaseBalance(int(proposerIndex), earnedProposerReward)
	return votedKeys, err
}

func (s *StateTransistor) ProcessSyncAggregate(sync *cltypes.SyncAggregate) error {
	votedKeys, err := s.processSyncAggregate(sync)
	if err != nil {
		return err
	}
	if !s.noValidate {
		previousSlot := s.state.PreviousSlot()

		domain, err := fork.Domain(s.state.Fork(), previousSlot/s.beaconConfig.SlotsPerEpoch, s.beaconConfig.DomainSyncCommittee, s.state.GenesisValidatorsRoot())
		if err != nil {
			return nil
		}
		blockRoot, err := s.state.GetBlockRootAtSlot(previousSlot)
		if err != nil {
			return err
		}
		msg := utils.Keccak256(blockRoot[:], domain)
		isValid, err := bls.VerifyAggregate(sync.SyncCommiteeSignature[:], msg[:], votedKeys)
		if err != nil {
			return err
		}
		if !isValid {
			return errors.New("ProcessSyncAggregate: cannot validate sync committee signature")
		}
	}
	return nil
}
