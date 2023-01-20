package state

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/core/types"
)

type HashFunc func([]byte) ([32]byte, error)

const (
	blockRootsLength = 8192
	stateRootsLength = 8192
	randoMixesLength = 65536
	slashingsLength  = 8192
)

type BeaconState struct {
	// State fields
	genesisTime                  uint64
	genesisValidatorsRoot        libcommon.Hash
	slot                         uint64
	fork                         *cltypes.Fork
	latestBlockHeader            *cltypes.BeaconBlockHeader
	blockRoots                   [blockRootsLength]libcommon.Hash
	stateRoots                   [stateRootsLength]libcommon.Hash
	historicalRoots              []libcommon.Hash
	eth1Data                     *cltypes.Eth1Data
	eth1DataVotes                []*cltypes.Eth1Data
	eth1DepositIndex             uint64
	validators                   []*cltypes.Validator
	balances                     []uint64
	randaoMixes                  [randoMixesLength]libcommon.Hash
	slashings                    [slashingsLength]uint64
	previousEpochParticipation   []byte
	currentEpochParticipation    []byte
	justificationBits            byte
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

func preparateRootsForHashing(roots []libcommon.Hash) [][32]byte {
	ret := make([][32]byte, len(roots))
	for i := range roots {
		copy(ret[i][:], roots[i][:])
	}
	return ret
}

// FromBellatrixState initialize the beacon state as a bellatrix state.
func FromBellatrixState(state *cltypes.BeaconStateBellatrix) *BeaconState {
	var blockRoots [stateRootsLength]libcommon.Hash
	var stateRoots [stateRootsLength]libcommon.Hash
	var randaoMixes [randoMixesLength]libcommon.Hash
	historicalRoots := make([]libcommon.Hash, len(state.HistoricalRoots))
	var slashings [slashingsLength]uint64
	copy(slashings[:], state.Slashings)
	for i := range state.BlockRoots {
		copy(blockRoots[i][:], state.BlockRoots[i][:])
	}
	for i := range state.StateRoots {
		copy(stateRoots[i][:], state.StateRoots[i][:])
	}
	for i := range state.RandaoMixes {
		copy(randaoMixes[i][:], state.RandaoMixes[i][:])
	}
	for i := range state.HistoricalRoots {
		copy(historicalRoots[i][:], state.HistoricalRoots[i][:])
	}

	return &BeaconState{
		genesisTime:                 state.GenesisTime,
		genesisValidatorsRoot:       state.GenesisValidatorsRoot,
		slot:                        state.Slot,
		fork:                        state.Fork,
		latestBlockHeader:           state.LatestBlockHeader,
		blockRoots:                  blockRoots,
		stateRoots:                  stateRoots,
		historicalRoots:             historicalRoots,
		eth1Data:                    state.Eth1Data,
		eth1DataVotes:               state.Eth1DataVotes,
		eth1DepositIndex:            state.Eth1DepositIndex,
		validators:                  state.Validators,
		balances:                    state.Balances,
		randaoMixes:                 randaoMixes,
		slashings:                   slashings,
		previousEpochParticipation:  state.PreviousEpochParticipation,
		currentEpochParticipation:   state.CurrentEpochParticipation,
		justificationBits:           state.JustificationBits[0],
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
