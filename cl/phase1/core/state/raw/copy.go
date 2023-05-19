package raw

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/generic"
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
	dst.eth1DataVotes = generic.NewDynamicListSSZ[*cltypes.Eth1Data](int(b.beaconConfig.Eth1DataVotesLength()))
	b.eth1DataVotes.Range(func(index int, value *cltypes.Eth1Data, length int) bool {
		dst.eth1DataVotes.Append(value.Copy())
		return true
	})

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
	dst.historicalSummaries = generic.NewStaticListSSZ[*cltypes.HistoricalSummary](int(b.beaconConfig.HistoricalRootsLimit), 64)
	b.historicalSummaries.Range(func(_ int, value *cltypes.HistoricalSummary, _ int) bool {
		dst.historicalSummaries.Append(value)
		return true
	})
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
