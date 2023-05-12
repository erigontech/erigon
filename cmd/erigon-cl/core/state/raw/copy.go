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
			nv := &cltypes.Validator{}
			validator.CopyTo(nv)
			dst.validators = append(dst.validators, nv)
			continue
		}
		validator.CopyTo(dst.validators[i])
	}
	dst.validators = dst.validators[:len(b.validators)]
	b.balances.CopyTo(dst.balances)
	copy(dst.randaoMixes[:], b.randaoMixes[:])
	copy(dst.slashings[:], b.slashings[:])
	b.previousEpochParticipation.CopyTo(dst.previousEpochParticipation)
	b.currentEpochParticipation.CopyTo(dst.currentEpochParticipation)
	dst.finalizedCheckpoint = b.finalizedCheckpoint.Copy()
	dst.currentJustifiedCheckpoint = b.currentJustifiedCheckpoint.Copy()
	dst.previousJustifiedCheckpoint = b.previousJustifiedCheckpoint.Copy()
	if b.version == clparams.Phase0Version {
		dst.init()
		return nil
	}
	dst.currentSyncCommittee = b.currentSyncCommittee.Copy()
	dst.nextSyncCommittee = b.nextSyncCommittee.Copy()
	b.inactivityScores.CopyTo(dst.inactivityScores)
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
