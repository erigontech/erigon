package raw

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

func GetEmptyBeaconState() *BeaconState {
	cfg := &clparams.MainnetBeaconConfig
	b := &BeaconState{
		fork:                         &cltypes.Fork{},
		latestBlockHeader:            &cltypes.BeaconBlockHeader{},
		eth1Data:                     &cltypes.Eth1Data{},
		currentSyncCommittee:         &solid.SyncCommittee{},
		nextSyncCommittee:            &solid.SyncCommittee{},
		blockRoots:                   solid.NewHashVector(blockRootsLength),
		stateRoots:                   solid.NewHashVector(stateRootsLength),
		randaoMixes:                  solid.NewHashVector(randoMixesLength),
		previousJustifiedCheckpoint:  solid.NewCheckpoint(),
		currentJustifiedCheckpoint:   solid.NewCheckpoint(),
		finalizedCheckpoint:          solid.NewCheckpoint(),
		latestExecutionPayloadHeader: cltypes.NewEth1Header(clparams.BellatrixVersion),
		version:                      clparams.BellatrixVersion,
		beaconConfig:                 cfg,
		validators:                   cltypes.NewValidatorSet(int(cfg.ValidatorRegistryLimit)),
		inactivityScores:             solid.NewUint64ListSSZ(int(cfg.ValidatorRegistryLimit)),
		balances:                     solid.NewUint64ListSSZ(int(cfg.ValidatorRegistryLimit)),
		previousEpochParticipation:   solid.NewBitList(0, int(cfg.ValidatorRegistryLimit)),
		currentEpochParticipation:    solid.NewBitList(0, int(cfg.ValidatorRegistryLimit)),
	}
	b.init()
	return b
}

func GetEmptyBeaconStateWithVersion(v clparams.StateVersion) *BeaconState {
	b := GetEmptyBeaconState()
	b.version = v
	return b
}
