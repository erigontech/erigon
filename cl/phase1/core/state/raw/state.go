package raw

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

const (
	blockRootsLength = 8192
	stateRootsLength = 8192
	randoMixesLength = 65536
	slashingsLength  = 8192
)

type BeaconState struct {
	// State fields
	genesisTime                uint64
	genesisValidatorsRoot      common.Hash
	slot                       uint64
	fork                       *cltypes.Fork
	latestBlockHeader          *cltypes.BeaconBlockHeader
	blockRoots                 solid.HashVectorSSZ
	stateRoots                 solid.HashVectorSSZ
	historicalRoots            solid.HashListSSZ
	eth1Data                   *cltypes.Eth1Data
	eth1DataVotes              *solid.ListSSZ[*cltypes.Eth1Data]
	eth1DepositIndex           uint64
	validators                 *cltypes.ValidatorSet
	balances                   solid.Uint64ListSSZ
	randaoMixes                solid.HashVectorSSZ
	slashings                  solid.Uint64VectorSSZ
	previousEpochParticipation *solid.BitList
	currentEpochParticipation  *solid.BitList
	justificationBits          cltypes.JustificationBits
	// Altair
	previousJustifiedCheckpoint solid.Checkpoint
	currentJustifiedCheckpoint  solid.Checkpoint
	finalizedCheckpoint         solid.Checkpoint
	inactivityScores            solid.Uint64ListSSZ
	currentSyncCommittee        *solid.SyncCommittee
	nextSyncCommittee           *solid.SyncCommittee
	// Bellatrix
	latestExecutionPayloadHeader *cltypes.Eth1Header
	// Capella
	nextWithdrawalIndex          uint64
	nextWithdrawalValidatorIndex uint64
	historicalSummaries          *solid.ListSSZ[*cltypes.HistoricalSummary]
	// Phase0: genesis fork. these 2 fields replace participation bits.
	previousEpochAttestations *solid.ListSSZ[*solid.PendingAttestation]
	currentEpochAttestations  *solid.ListSSZ[*solid.PendingAttestation]

	//  leaves for computing hashes
	leaves        []byte                  // Pre-computed leaves.
	touchedLeaves map[StateLeafIndex]bool // Maps each leaf to whether they were touched or not.

	// cl version
	version      clparams.StateVersion // State version
	beaconConfig *clparams.BeaconChainConfig
}

func New(cfg *clparams.BeaconChainConfig) *BeaconState {
	state := &BeaconState{
		beaconConfig:                 cfg,
		fork:                         &cltypes.Fork{},
		latestBlockHeader:            &cltypes.BeaconBlockHeader{},
		eth1Data:                     &cltypes.Eth1Data{},
		currentSyncCommittee:         &solid.SyncCommittee{},
		nextSyncCommittee:            &solid.SyncCommittee{},
		latestExecutionPayloadHeader: &cltypes.Eth1Header{},
		//inactivityScores: solid.NewSimpleUint64Slice(int(cfg.ValidatorRegistryLimit)),
		inactivityScores:            solid.NewUint64ListSSZ(int(cfg.ValidatorRegistryLimit)),
		balances:                    solid.NewUint64ListSSZ(int(cfg.ValidatorRegistryLimit)),
		previousEpochParticipation:  solid.NewBitList(0, int(cfg.ValidatorRegistryLimit)),
		currentEpochParticipation:   solid.NewBitList(0, int(cfg.ValidatorRegistryLimit)),
		slashings:                   solid.NewUint64VectorSSZ(slashingsLength),
		currentEpochAttestations:    solid.NewDynamicListSSZ[*solid.PendingAttestation](int(cfg.CurrentEpochAttestationsLength())),
		previousEpochAttestations:   solid.NewDynamicListSSZ[*solid.PendingAttestation](int(cfg.PreviousEpochAttestationsLength())),
		historicalRoots:             solid.NewHashList(int(cfg.HistoricalRootsLimit)),
		blockRoots:                  solid.NewHashVector(blockRootsLength),
		stateRoots:                  solid.NewHashVector(stateRootsLength),
		randaoMixes:                 solid.NewHashVector(randoMixesLength),
		validators:                  cltypes.NewValidatorSet(int(cfg.ValidatorRegistryLimit)),
		previousJustifiedCheckpoint: solid.NewCheckpoint(),
		currentJustifiedCheckpoint:  solid.NewCheckpoint(),
		finalizedCheckpoint:         solid.NewCheckpoint(),
		leaves:                      make([]byte, 32*32),
	}
	state.init()
	return state
}

func (b *BeaconState) init() error {
	if b.touchedLeaves == nil {
		b.touchedLeaves = make(map[StateLeafIndex]bool)
	}
	return nil
}
