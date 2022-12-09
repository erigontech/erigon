package state

import (
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/common"
)

type HashFunc func([]byte) ([32]byte, error)

type StateVersion int

// We do not care about Phase0 and Altair because they are both pre-merge.
const (
	BellatrixVersion StateVersion = 0
	CapellaVersion   StateVersion = 1 // Unimplemented!
)

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
	latestExecutionPayloadHeader *cltypes.ExecutionHeader
	// Internals
	version       StateVersion  // State version
	leaves        []common.Hash // Pre-computed leaves.
	touchedLeaves map[int]bool  // Maps each leaf to whether they were touched or not.
	/*root          common.Hash   // Cached state root.
	hasher        HashFunc      // Merkle root hasher.*/
}

// FromBellatrixState initialize the beacon state as a bellatrix state.
func FromBellatrixState(state *cltypes.BeaconStateBellatrix) *BeaconState {
	return &BeaconState{
		genesisTime:                  state.GenesisTime,
		genesisValidatorsRoot:        state.GenesisValidatorsRoot,
		slot:                         state.Slot,
		fork:                         state.Fork,
		latestBlockHeader:            state.LatestBlockHeader,
		blockRoots:                   state.BlockRoots,
		stateRoots:                   state.StateRoots,
		historicalRoots:              state.HistoricalRoots,
		eth1Data:                     state.Eth1Data,
		eth1DataVotes:                state.Eth1DataVotes,
		eth1DepositIndex:             state.Eth1DepositIndex,
		validators:                   state.Validators,
		balances:                     state.Balances,
		randaoMixes:                  state.RandaoMixes,
		slashings:                    state.Slashings,
		previousEpochParticipation:   state.PreviousEpochParticipation,
		currentEpochParticipation:    state.CurrentEpochParticipation,
		justificationBits:            state.JustificationBits,
		previousJustifiedCheckpoint:  state.PreviousJustifiedCheckpoint,
		currentJustifiedCheckpoint:   state.CurrentJustifiedCheckpoint,
		finalizedCheckpoint:          state.FinalizedCheckpoint,
		inactivityScores:             state.InactivityScores,
		currentSyncCommittee:         state.CurrentSyncCommittee,
		nextSyncCommittee:            state.NextSyncCommittee,
		latestExecutionPayloadHeader: state.LatestExecutionPayloadHeader,
		// Internals
		version:       BellatrixVersion,
		leaves:        make([]common.Hash, BellatrixLeavesSize),
		touchedLeaves: map[int]bool{},
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
