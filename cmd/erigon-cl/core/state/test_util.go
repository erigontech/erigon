package state

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

func GetEmptyBeaconState() *BeaconState {
	b := &BeaconState{
		fork:              &cltypes.Fork{},
		latestBlockHeader: &cltypes.BeaconBlockHeader{},
		eth1Data:          &cltypes.Eth1Data{},
		currentSyncCommittee: &cltypes.SyncCommittee{
			PubKeys: make([][48]byte, 512),
		},
		nextSyncCommittee: &cltypes.SyncCommittee{
			PubKeys: make([][48]byte, 512),
		},
		previousJustifiedCheckpoint:  &cltypes.Checkpoint{},
		currentJustifiedCheckpoint:   &cltypes.Checkpoint{},
		finalizedCheckpoint:          &cltypes.Checkpoint{},
		latestExecutionPayloadHeader: cltypes.NewEth1Header(clparams.BellatrixVersion),
		version:                      clparams.BellatrixVersion,
		beaconConfig:                 &clparams.MainnetBeaconConfig,
	}
	b.initBeaconState()
	return b
}

func GetEmptyBeaconStateWithVersion(v clparams.StateVersion) *BeaconState {
	b := GetEmptyBeaconState()
	b.version = v
	return b
}
