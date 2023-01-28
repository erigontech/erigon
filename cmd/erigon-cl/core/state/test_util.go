package state

import (
	"math/big"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/core/types"
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
		previousJustifiedCheckpoint: &cltypes.Checkpoint{},
		currentJustifiedCheckpoint:  &cltypes.Checkpoint{},
		finalizedCheckpoint:         &cltypes.Checkpoint{},
		latestExecutionPayloadHeader: &types.Header{
			BaseFee: big.NewInt(0),
			Number:  big.NewInt(0),
		},
		version: clparams.BellatrixVersion,
	}
	b.initBeaconState()
	return b
}
