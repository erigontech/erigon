package transition

import (
	"errors"
	state2 "github.com/ledgerwatch/erigon/cl/phase1/core/state"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/utils"
)

// processSyncAggregate applies all the logic in the spec function `process_sync_aggregate` except
// verifying the BLS signatures. It returns the modified beacons state and the list of validators'
// public keys that voted, for future signature verification.
func processSyncAggregate(s *state2.BeaconState, sync *cltypes.SyncAggregate) ([][]byte, error) {
	currentSyncCommittee := s.CurrentSyncCommittee()

	if currentSyncCommittee == nil {
		return nil, errors.New("nil current sync committee in s")
	}
	committeeKeys := currentSyncCommittee.PubKeys
	if len(sync.SyncCommiteeBits)*8 > len(committeeKeys) {
		return nil, errors.New("bits length exceeds committee length")
	}
	var votedKeys [][]byte

	proposerReward, participantReward, err := s.SyncRewards()
	if err != nil {
		return nil, err
	}

	proposerIndex, err := s.GetBeaconProposerIndex()
	if err != nil {
		return nil, err
	}

	syncAggregateBits := sync.SyncCommiteeBits
	earnedProposerReward := uint64(0)
	currPubKeyIndex := 0
	for i := range syncAggregateBits {
		for bit := 1; bit <= 128; bit *= 2 {
			vIdx, exists := s.ValidatorIndexByPubkey(committeeKeys[currPubKeyIndex])
			// Impossible scenario.
			if !exists {
				return nil, errors.New("validator public key does not exist in state")
			}
			if syncAggregateBits[i]&byte(bit) > 0 {
				votedKeys = append(votedKeys, currentSyncCommittee.PubKeys[currPubKeyIndex][:])
				if err := state2.IncreaseBalance(s.BeaconState, vIdx, participantReward); err != nil {
					return nil, err
				}
				earnedProposerReward += proposerReward
			} else {
				if err := state2.DecreaseBalance(s.BeaconState, vIdx, participantReward); err != nil {
					return nil, err
				}
			}
			currPubKeyIndex++
		}
	}

	return votedKeys, state2.IncreaseBalance(s.BeaconState, proposerIndex, earnedProposerReward)
}

func ProcessSyncAggregate(s *state2.BeaconState, sync *cltypes.SyncAggregate, fullValidation bool) error {
	votedKeys, err := processSyncAggregate(s, sync)
	if err != nil {
		return err
	}
	if fullValidation {
		previousSlot := s.PreviousSlot()

		domain, err := fork.Domain(s.Fork(), state2.GetEpochAtSlot(s.BeaconConfig(), previousSlot), s.BeaconConfig().DomainSyncCommittee, s.GenesisValidatorsRoot())
		if err != nil {
			return nil
		}
		blockRoot, err := s.GetBlockRootAtSlot(previousSlot)
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
