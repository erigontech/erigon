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
	blockRoots                 [blockRootsLength]common.Hash
	stateRoots                 [stateRootsLength]common.Hash
	historicalRoots            []common.Hash
	eth1Data                   *cltypes.Eth1Data
	eth1DataVotes              *solid.ListSSZ[*cltypes.Eth1Data]
	eth1DepositIndex           uint64
	validators                 []*cltypes.Validator
	balances                   solid.Uint64ListSSZ
	randaoMixes                [randoMixesLength]common.Hash
	slashings                  solid.Uint64VectorSSZ
	previousEpochParticipation solid.BitList
	currentEpochParticipation  solid.BitList
	justificationBits          cltypes.JustificationBits
	// Altair
	previousJustifiedCheckpoint solid.Checkpoint
	currentJustifiedCheckpoint  solid.Checkpoint
	finalizedCheckpoint         solid.Checkpoint
	inactivityScores            solid.Uint64ListSSZ
	currentSyncCommittee        *cltypes.SyncCommittee
	nextSyncCommittee           *cltypes.SyncCommittee
	// Bellatrix
	latestExecutionPayloadHeader *cltypes.Eth1Header
	// Capella
	nextWithdrawalIndex          uint64
	nextWithdrawalValidatorIndex uint64
	historicalSummaries          *solid.ListSSZ[*cltypes.HistoricalSummary]
	// Phase0: genesis fork. these 2 fields replace participation bits.
	previousEpochAttestations *solid.ListSSZ[*cltypes.PendingAttestation]
	currentEpochAttestations  *solid.ListSSZ[*cltypes.PendingAttestation]

	//  leaves for computing hashes
	leaves        [32][32]byte            // Pre-computed leaves.
	touchedLeaves map[StateLeafIndex]bool // Maps each leaf to whether they were touched or not.

	// cl version
	version      clparams.StateVersion // State version
	beaconConfig *clparams.BeaconChainConfig
}

func New(cfg *clparams.BeaconChainConfig) *BeaconState {
	state := &BeaconState{
		beaconConfig: cfg,
		//inactivityScores: solid.NewSimpleUint64Slice(int(cfg.ValidatorRegistryLimit)),
		inactivityScores:           solid.NewUint64ListSSZ(int(cfg.ValidatorRegistryLimit)),
		balances:                   solid.NewUint64ListSSZ(int(cfg.ValidatorRegistryLimit)),
		previousEpochParticipation: solid.NewBitList(0, int(cfg.ValidatorRegistryLimit)),
		currentEpochParticipation:  solid.NewBitList(0, int(cfg.ValidatorRegistryLimit)),
		slashings:                  solid.NewUint64VectorSSZ(slashingsLength),
		currentEpochAttestations:   solid.NewDynamicListSSZ[*cltypes.PendingAttestation](int(cfg.CurrentEpochAttestationsLength())),
		previousEpochAttestations:  solid.NewDynamicListSSZ[*cltypes.PendingAttestation](int(cfg.PreviousEpochAttestationsLength())),
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
