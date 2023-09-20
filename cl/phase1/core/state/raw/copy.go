package raw

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

func (b *BeaconState) CopyInto(dst *BeaconState) error {
	dst.genesisTime = b.genesisTime
	dst.genesisValidatorsRoot = b.genesisValidatorsRoot
	dst.slot = b.slot
	dst.fork = b.fork.Copy()
	dst.latestBlockHeader = b.latestBlockHeader.Copy()
	b.blockRoots.CopyTo(dst.blockRoots)
	b.stateRoots.CopyTo(dst.stateRoots)
	b.historicalRoots.CopyTo(dst.historicalRoots)
	dst.eth1Data = b.eth1Data.Copy()
	dst.eth1DataVotes = solid.NewDynamicListSSZ[*cltypes.Eth1Data](int(b.beaconConfig.Eth1DataVotesLength()))
	b.eth1DataVotes.Range(func(index int, value *cltypes.Eth1Data, length int) bool {
		dst.eth1DataVotes.Append(value.Copy())
		return true
	})

	dst.eth1DepositIndex = b.eth1DepositIndex
	b.validators.CopyTo(dst.validators)
	b.balances.CopyTo(dst.balances)
	b.randaoMixes.CopyTo(dst.randaoMixes)
	b.slashings.CopyTo(dst.slashings)
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
	dst.historicalSummaries = solid.NewStaticListSSZ[*cltypes.HistoricalSummary](int(b.beaconConfig.HistoricalRootsLimit), 64)
	b.historicalSummaries.Range(func(_ int, value *cltypes.HistoricalSummary, _ int) bool {
		dst.historicalSummaries.Append(value)
		return true
	})
	dst.version = b.version
	// Now sync internals
	copy(dst.leaves, b.leaves)
	dst.touchedLeaves = make(map[StateLeafIndex]bool)
	for leafIndex, touchedVal := range b.touchedLeaves {
		dst.touchedLeaves[leafIndex] = touchedVal
	}
	return nil
}

func (b *BeaconState) Copy() (*BeaconState, error) {
	copied := New(b.BeaconConfig())
	fmt.Println(copied.slashings)
	return copied, b.CopyInto(copied)
}
