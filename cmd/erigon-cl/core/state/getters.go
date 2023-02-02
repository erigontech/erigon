package state

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/core/types"
)

// Just a bunch of simple getters.

func (b *BeaconState) GenesisTime() uint64 {
	return b.genesisTime
}

func (b *BeaconState) GenesisValidatorsRoot() libcommon.Hash {
	return b.genesisValidatorsRoot
}

func (b *BeaconState) Slot() uint64 {
	return b.slot
}

func (b *BeaconState) PreviousSlot() uint64 {
	if b.slot == 0 {
		return 0
	}
	return b.slot - 1
}

func (b *BeaconState) Fork() *cltypes.Fork {
	return b.fork
}

func (b *BeaconState) LatestBlockHeader() *cltypes.BeaconBlockHeader {
	return b.latestBlockHeader
}

func (b *BeaconState) BlockRoots() [blockRootsLength]libcommon.Hash {
	return b.blockRoots
}

func (b *BeaconState) StateRoots() [stateRootsLength]libcommon.Hash {
	return b.stateRoots
}

func (b *BeaconState) HistoricalRoots() []libcommon.Hash {
	return b.historicalRoots
}

func (b *BeaconState) Eth1Data() *cltypes.Eth1Data {
	return b.eth1Data
}

func (b *BeaconState) Eth1DataVotes() []*cltypes.Eth1Data {
	return b.eth1DataVotes
}

func (b *BeaconState) Eth1DepositIndex() uint64 {
	return b.eth1DepositIndex
}

func (b *BeaconState) Validators() []*cltypes.Validator {
	return b.validators
}

func (b *BeaconState) ValidatorAt(index int) *cltypes.Validator {
	return b.validators[index]
}

func (b *BeaconState) Balances() []uint64 {
	return b.balances
}

func (b *BeaconState) ValidatorBalance(index int) uint64 {
	return b.balances[index]
}

func (b *BeaconState) RandaoMixes() [randoMixesLength]libcommon.Hash {
	return b.randaoMixes
}

func (b *BeaconState) Slashings() [slashingsLength]uint64 {
	return b.slashings
}

func (b *BeaconState) SlashingSegmentAt(pos int) uint64 {
	return b.slashings[pos]
}

func (b *BeaconState) PreviousEpochParticipation() cltypes.ParticipationFlagsList {
	return b.previousEpochParticipation
}

func (b *BeaconState) CurrentEpochParticipation() cltypes.ParticipationFlagsList {
	return b.currentEpochParticipation
}

func (b *BeaconState) JustificationBits() cltypes.JustificationBits {
	return b.justificationBits
}

func (b *BeaconState) PreviousJustifiedCheckpoint() *cltypes.Checkpoint {
	return b.previousJustifiedCheckpoint
}

func (b *BeaconState) CurrentJustifiedCheckpoint() *cltypes.Checkpoint {
	return b.currentJustifiedCheckpoint
}

func (b *BeaconState) FinalizedCheckpoint() *cltypes.Checkpoint {
	return b.finalizedCheckpoint
}

func (b *BeaconState) CurrentSyncCommittee() *cltypes.SyncCommittee {
	return b.currentSyncCommittee
}

func (b *BeaconState) NextSyncCommittee() *cltypes.SyncCommittee {
	return b.nextSyncCommittee
}

func (b *BeaconState) LatestExecutionPayloadHeader() *types.Header {
	return b.latestExecutionPayloadHeader
}

func (b *BeaconState) NextWithdrawalIndex() uint64 {
	return b.nextWithdrawalIndex
}

func (b *BeaconState) NextWithdrawalValidatorIndex() uint64 {
	return b.nextWithdrawalValidatorIndex
}

func (b *BeaconState) HistoricalSummaries() []*cltypes.HistoricalSummary {
	return b.historicalSummaries
}

func (b *BeaconState) Version() clparams.StateVersion {
	return b.version
}

func (b *BeaconState) ValidatorIndexByPubkey(key [48]byte) (uint64, bool) {
	val, ok := b.publicKeyIndicies[key]
	return val, ok
}
