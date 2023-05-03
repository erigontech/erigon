package raw

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

func (b *BeaconState) Copy() (*BeaconState, error) {
	copied := New(b.BeaconConfig())
	copied.genesisTime = b.genesisTime
	copied.genesisValidatorsRoot = b.genesisValidatorsRoot
	copied.slot = b.slot
	copied.fork = b.fork.Copy()
	copied.latestBlockHeader = b.latestBlockHeader.Copy()
	copy(copied.blockRoots[:], b.blockRoots[:])
	copy(copied.stateRoots[:], b.stateRoots[:])
	copied.historicalRoots = make([]libcommon.Hash, len(b.historicalRoots))
	copy(copied.historicalRoots, b.historicalRoots)
	copied.eth1Data = b.eth1Data.Copy()
	copied.eth1DataVotes = make([]*cltypes.Eth1Data, len(b.eth1DataVotes))
	for i := range b.eth1DataVotes {
		copied.eth1DataVotes[i] = b.eth1DataVotes[i].Copy()
	}
	copied.eth1DepositIndex = b.eth1DepositIndex
	copied.validators = make([]*cltypes.Validator, len(b.validators))
	for i := range b.validators {
		copied.validators[i] = b.validators[i].Copy()
	}
	copied.balances = make([]uint64, len(b.balances))
	copy(copied.balances, b.balances)
	copy(copied.randaoMixes[:], b.randaoMixes[:])
	copy(copied.slashings[:], b.slashings[:])
	copied.previousEpochParticipation = b.previousEpochParticipation.Copy()
	copied.currentEpochParticipation = b.currentEpochParticipation.Copy()
	copied.finalizedCheckpoint = b.finalizedCheckpoint.Copy()
	copied.currentJustifiedCheckpoint = b.currentJustifiedCheckpoint.Copy()
	copied.previousJustifiedCheckpoint = b.previousJustifiedCheckpoint.Copy()
	if b.version == clparams.Phase0Version {
		return copied, copied.init()
	}
	copied.currentSyncCommittee = b.currentSyncCommittee.Copy()
	copied.nextSyncCommittee = b.nextSyncCommittee.Copy()
	copied.inactivityScores = make([]uint64, len(b.inactivityScores))
	copy(copied.inactivityScores, b.inactivityScores)
	copied.justificationBits = b.justificationBits.Copy()

	if b.version >= clparams.BellatrixVersion {
		copied.latestExecutionPayloadHeader = b.latestExecutionPayloadHeader.Copy()
	}
	copied.nextWithdrawalIndex = b.nextWithdrawalIndex
	copied.nextWithdrawalValidatorIndex = b.nextWithdrawalValidatorIndex
	copied.historicalSummaries = make([]*cltypes.HistoricalSummary, len(b.historicalSummaries))
	for i := range b.historicalSummaries {
		copied.historicalSummaries[i] = &cltypes.HistoricalSummary{
			BlockSummaryRoot: b.historicalSummaries[i].BlockSummaryRoot,
			StateSummaryRoot: b.historicalSummaries[i].StateSummaryRoot,
		}
	}
	copied.version = b.version
	// Now sync internals
	copy(copied.leaves[:], b.leaves[:])
	copied.touchedLeaves = make(map[StateLeafIndex]bool)
	for leafIndex, touchedVal := range b.touchedLeaves {
		copied.touchedLeaves[leafIndex] = touchedVal
	}
	return copied, nil
}
