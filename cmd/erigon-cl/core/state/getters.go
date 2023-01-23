package state

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
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

func (b *BeaconState) RandaoMixes() [randoMixesLength]libcommon.Hash {
	return b.randaoMixes
}

func (b *BeaconState) Slashings() [slashingsLength]uint64 {
	return b.slashings
}

func (b *BeaconState) SlashingSegmentAt(pos int) uint64 {
	return b.slashings[pos]
}

func (b *BeaconState) PreviousEpochParticipation() []byte {
	return b.previousEpochParticipation
}

func (b *BeaconState) CurrentEpochParticipation() []byte {
	return b.currentEpochParticipation
}

func (b *BeaconState) JustificationBits() byte {
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

// GetStateSSZObject allows us to use ssz methods.
func (b *BeaconState) GetStateSSZObject() ssz_utils.ObjectSSZ {
	switch b.version {
	case clparams.BellatrixVersion:
		return &cltypes.BeaconStateBellatrix{
			GenesisTime:                  b.genesisTime,
			GenesisValidatorsRoot:        b.genesisValidatorsRoot,
			Slot:                         b.slot,
			Fork:                         b.fork,
			LatestBlockHeader:            b.latestBlockHeader,
			BlockRoots:                   preparateRootsForHashing(b.blockRoots[:]),
			StateRoots:                   preparateRootsForHashing(b.stateRoots[:]),
			HistoricalRoots:              preparateRootsForHashing(b.historicalRoots),
			Eth1Data:                     b.eth1Data,
			Eth1DataVotes:                b.eth1DataVotes,
			Eth1DepositIndex:             b.eth1DepositIndex,
			Validators:                   b.validators,
			Balances:                     b.balances,
			RandaoMixes:                  preparateRootsForHashing(b.randaoMixes[:]),
			Slashings:                    b.slashings[:],
			PreviousEpochParticipation:   b.previousEpochParticipation,
			CurrentEpochParticipation:    b.currentEpochParticipation,
			JustificationBits:            []byte{b.justificationBits},
			FinalizedCheckpoint:          b.finalizedCheckpoint,
			CurrentJustifiedCheckpoint:   b.currentJustifiedCheckpoint,
			PreviousJustifiedCheckpoint:  b.previousJustifiedCheckpoint,
			InactivityScores:             b.inactivityScores,
			CurrentSyncCommittee:         b.currentSyncCommittee,
			NextSyncCommittee:            b.nextSyncCommittee,
			LatestExecutionPayloadHeader: b.latestExecutionPayloadHeader,
		}
	case clparams.CapellaVersion:
		panic("not implemented")
	default:
		panic("not a valid version")
	}
}
