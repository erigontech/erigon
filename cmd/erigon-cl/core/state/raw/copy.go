package raw

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

func (b *BeaconState) CopyInto(dst *BeaconState) error {
	dst.genesisTime = b.genesisTime
	dst.genesisValidatorsRoot = b.genesisValidatorsRoot
	dst.slot = b.slot
	dst.fork = b.fork.Copy()
	dst.latestBlockHeader = b.latestBlockHeader.Copy()
	copy(dst.blockRoots[:], b.blockRoots[:])
	copy(dst.stateRoots[:], b.stateRoots[:])
	dst.historicalRoots = make([]libcommon.Hash, len(b.historicalRoots))
	copy(dst.historicalRoots, b.historicalRoots)
	dst.eth1Data = b.eth1Data.Copy()
	dst.eth1DataVotes = make([]*cltypes.Eth1Data, len(b.eth1DataVotes))
	for i := range b.eth1DataVotes {
		dst.eth1DataVotes[i] = b.eth1DataVotes[i].Copy()
	}
	dst.eth1DepositIndex = b.eth1DepositIndex
	for i, validator := range b.validators {
		if i >= len(dst.validators) {
			dst.validators = append(dst.validators, validator.Copy())
			continue
		}
		copy(dst.validators[i].WithdrawalCredentials[:], validator.WithdrawalCredentials[:])
		copy(dst.validators[i].PublicKey[:], validator.PublicKey[:])
		dst.validators[i].ActivationEligibilityEpoch = validator.ActivationEligibilityEpoch
		dst.validators[i].ActivationEpoch = validator.ActivationEpoch
		dst.validators[i].ExitEpoch = validator.ExitEpoch
		dst.validators[i].EffectiveBalance = validator.EffectiveBalance
		dst.validators[i].Slashed = validator.Slashed
		dst.validators[i].WithdrawableEpoch = validator.WithdrawableEpoch
	}
	dst.validators = dst.validators[:len(b.validators)]
	dst.balances = make([]uint64, len(b.balances))
	copy(dst.balances, b.balances)
	copy(dst.randaoMixes[:], b.randaoMixes[:])
	copy(dst.slashings[:], b.slashings[:])
	dst.previousEpochParticipation = b.previousEpochParticipation.Copy()
	dst.currentEpochParticipation = b.currentEpochParticipation.Copy()
	dst.finalizedCheckpoint = b.finalizedCheckpoint.Copy()
	dst.currentJustifiedCheckpoint = b.currentJustifiedCheckpoint.Copy()
	dst.previousJustifiedCheckpoint = b.previousJustifiedCheckpoint.Copy()
	if b.version == clparams.Phase0Version {
		return dst.init()
	}
	dst.currentSyncCommittee = b.currentSyncCommittee.Copy()
	dst.nextSyncCommittee = b.nextSyncCommittee.Copy()
	dst.inactivityScores = make([]uint64, len(b.inactivityScores))
	copy(dst.inactivityScores, b.inactivityScores)
	dst.justificationBits = b.justificationBits.Copy()

	if b.version >= clparams.BellatrixVersion {
		dst.latestExecutionPayloadHeader = b.latestExecutionPayloadHeader.Copy()
	}
	dst.nextWithdrawalIndex = b.nextWithdrawalIndex
	dst.nextWithdrawalValidatorIndex = b.nextWithdrawalValidatorIndex
	dst.historicalSummaries = make([]*cltypes.HistoricalSummary, len(b.historicalSummaries))
	for i := range b.historicalSummaries {
		dst.historicalSummaries[i] = &cltypes.HistoricalSummary{
			BlockSummaryRoot: b.historicalSummaries[i].BlockSummaryRoot,
			StateSummaryRoot: b.historicalSummaries[i].StateSummaryRoot,
		}
	}
	dst.version = b.version
	// Now sync internals
	copy(dst.leaves[:], b.leaves[:])
	dst.touchedLeaves = make(map[StateLeafIndex]bool)
	for leafIndex, touchedVal := range b.touchedLeaves {
		dst.touchedLeaves[leafIndex] = touchedVal
	}
	return nil
}

func (b *BeaconState) Copy() (*BeaconState, error) {
	copied := New(b.BeaconConfig())
	return copied, b.CopyInto(copied)
}
