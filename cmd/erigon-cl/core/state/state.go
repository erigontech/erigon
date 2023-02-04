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
	genesisTime                uint64
	genesisValidatorsRoot      libcommon.Hash
	slot                       uint64
	fork                       *cltypes.Fork
	latestBlockHeader          *cltypes.BeaconBlockHeader
	blockRoots                 [blockRootsLength]libcommon.Hash
	stateRoots                 [stateRootsLength]libcommon.Hash
	historicalRoots            []libcommon.Hash
	eth1Data                   *cltypes.Eth1Data
	eth1DataVotes              []*cltypes.Eth1Data
	eth1DepositIndex           uint64
	validators                 []*cltypes.Validator
	balances                   []uint64
	randaoMixes                [randoMixesLength]libcommon.Hash
	slashings                  [slashingsLength]uint64
	previousEpochParticipation cltypes.ParticipationFlagsList
	currentEpochParticipation  cltypes.ParticipationFlagsList
	justificationBits          cltypes.JustificationBits
	// Altair
	previousJustifiedCheckpoint *cltypes.Checkpoint
	currentJustifiedCheckpoint  *cltypes.Checkpoint
	finalizedCheckpoint         *cltypes.Checkpoint
	inactivityScores            []uint64
	currentSyncCommittee        *cltypes.SyncCommittee
	nextSyncCommittee           *cltypes.SyncCommittee
	// Bellatrix
	latestExecutionPayloadHeader *types.Header
	// Capella
	nextWithdrawalIndex          uint64
	nextWithdrawalValidatorIndex uint64
	historicalSummaries          []*cltypes.HistoricalSummary
	// Internals
	version           clparams.StateVersion   // State version
	leaves            [32][32]byte            // Pre-computed leaves.
	touchedLeaves     map[StateLeafIndex]bool // Maps each leaf to whether they were touched or not.
	publicKeyIndicies map[[48]byte]uint64
	// Configs
	beaconConfig *clparams.BeaconChainConfig
}

func New(cfg *clparams.BeaconChainConfig) *BeaconState {
	state := &BeaconState{
		beaconConfig: cfg,
	}
	state.initBeaconState()
	return state
}

func preparateRootsForHashing(roots []libcommon.Hash) [][32]byte {
	ret := make([][32]byte, len(roots))
	for i := range roots {
		copy(ret[i][:], roots[i][:])
	}
	return ret
}

// MarshallSSZTo retrieve the SSZ encoded length of the state.
func (b *BeaconState) DecodeSSZ(buf []byte) error {
	panic("not implemented")
}

// BlockRoot computes the block root for the state.
func (b *BeaconState) BlockRoot() ([32]byte, error) {
	stateRoot, err := b.HashSSZ()
	if err != nil {
		return [32]byte{}, err
	}
	return (&cltypes.BeaconBlockHeader{
		Slot:          b.latestBlockHeader.Slot,
		ProposerIndex: b.latestBlockHeader.ProposerIndex,
		BodyRoot:      b.latestBlockHeader.BodyRoot,
		ParentRoot:    b.latestBlockHeader.ParentRoot,
		Root:          stateRoot,
	}).HashSSZ()
}

func (b *BeaconState) initBeaconState() {
	b.touchedLeaves = make(map[StateLeafIndex]bool)
	b.publicKeyIndicies = make(map[[48]byte]uint64)
	for i, validator := range b.validators {
		b.publicKeyIndicies[validator.PublicKey] = uint64(i)
	}
}
