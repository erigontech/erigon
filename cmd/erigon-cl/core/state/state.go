package state

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
)

type HashFunc func([]byte) ([32]byte, error)

type BeaconState struct {
	// State fields
	genesisTime                  uint64
	genesisValidatorsRoot        common.Hash
	slot                         uint64
	fork                         *cltypes.Fork
	latestBlockHeader            *cltypes.BeaconBlockHeader
	blockRoots                   [][32]byte
	stateRoots                   [][32]byte
	historicalRoots              [][32]byte
	eth1Data                     *cltypes.Eth1Data
	eth1DataVotes                []*cltypes.Eth1Data
	eth1DepositIndex             uint64
	validators                   []*cltypes.Validator
	balances                     []uint64
	randaoMixes                  [][32]byte
	slashings                    []uint64
	previousEpochParticipation   []byte
	currentEpochParticipation    []byte
	justificationBits            []byte
	previousJustifiedCheckpoint  *cltypes.Checkpoint
	currentJustifiedCheckpoint   *cltypes.Checkpoint
	finalizedCheckpoint          *cltypes.Checkpoint
	inactivityScores             []uint64
	currentSyncCommittee         *cltypes.SyncCommittee
	nextSyncCommittee            *cltypes.SyncCommittee
	latestExecutionPayloadHeader *types.Header
	// Internals
	version       clparams.StateVersion   // State version
	leaves        [][32]byte              // Pre-computed leaves.
	touchedLeaves map[StateLeafIndex]bool // Maps each leaf to whether they were touched or not.
}

// FromBellatrixState initialize the beacon state as a bellatrix state.
func FromBellatrixState(state *cltypes.BeaconStateBellatrix) *BeaconState {
	return &BeaconState{
		genesisTime:                 state.GenesisTime,
		genesisValidatorsRoot:       state.GenesisValidatorsRoot,
		slot:                        state.Slot,
		fork:                        state.Fork,
		latestBlockHeader:           state.LatestBlockHeader,
		blockRoots:                  state.BlockRoots,
		stateRoots:                  state.StateRoots,
		historicalRoots:             state.HistoricalRoots,
		eth1Data:                    state.Eth1Data,
		eth1DataVotes:               state.Eth1DataVotes,
		eth1DepositIndex:            state.Eth1DepositIndex,
		validators:                  state.Validators,
		balances:                    state.Balances,
		randaoMixes:                 state.RandaoMixes,
		slashings:                   state.Slashings,
		previousEpochParticipation:  state.PreviousEpochParticipation,
		currentEpochParticipation:   state.CurrentEpochParticipation,
		justificationBits:           state.JustificationBits,
		previousJustifiedCheckpoint: state.PreviousJustifiedCheckpoint,
		currentJustifiedCheckpoint:  state.CurrentJustifiedCheckpoint,
		finalizedCheckpoint:         state.FinalizedCheckpoint,
		inactivityScores:            state.InactivityScores,
		currentSyncCommittee:        state.CurrentSyncCommittee,
		nextSyncCommittee:           state.NextSyncCommittee,
		// Bellatrix only
		latestExecutionPayloadHeader: state.LatestExecutionPayloadHeader,
		// Internals
		version:       clparams.BellatrixVersion,
		leaves:        make([][32]byte, BellatrixLeavesSize),
		touchedLeaves: map[StateLeafIndex]bool{},
		// TODO: Make proper hasher
	}
}

// MarshallSSZTo encodes the state into the given buffer.
func (b *BeaconState) MarshalSSZTo(dst []byte) ([]byte, error) {
	return b.GetStateSSZObject().MarshalSSZTo(dst)
}

// MarshallSSZTo encodes the state.
func (b *BeaconState) MarshalSSZ() ([]byte, error) {
	return b.GetStateSSZObject().MarshalSSZ()
}

// MarshallSSZTo retrieve the SSZ encoded length of the state.
func (b *BeaconState) SizeSSZ() int {
	return b.GetStateSSZObject().SizeSSZ()
}

// MarshallSSZTo retrieve the SSZ encoded length of the state.
func (b *BeaconState) UnmarshalSSZ(buf []byte) error {
	panic("beacon state should be derived, use FromBellatrixState instead.")
}

// BlockRoot computes the block root for the state.
func (b *BeaconState) BlockRoot() ([32]byte, error) {
	stateRoot, err := b.HashTreeRoot()
	if err != nil {
		return [32]byte{}, err
	}
	return (&cltypes.BeaconBlockHeader{
		Slot:          b.latestBlockHeader.Slot,
		ProposerIndex: b.latestBlockHeader.ProposerIndex,
		BodyRoot:      b.latestBlockHeader.BodyRoot,
		ParentRoot:    b.latestBlockHeader.ParentRoot,
		Root:          stateRoot,
	}).HashTreeRoot()
}
