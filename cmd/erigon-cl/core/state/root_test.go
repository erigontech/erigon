package state_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

func getTestBeaconState() *cltypes.BeaconStateBellatrix {
	return &cltypes.BeaconStateBellatrix{
		BlockRoots:        make([][32]byte, 8192),
		StateRoots:        make([][32]byte, 8192),
		RandaoMixes:       make([][32]byte, 65536),
		Slashings:         make([]uint64, 8192),
		JustificationBits: make([]byte, 1),
		CurrentSyncCommittee: &cltypes.SyncCommittee{
			PubKeys: make([][48]byte, 512),
		},
		NextSyncCommittee: &cltypes.SyncCommittee{
			PubKeys: make([][48]byte, 512),
		},
		LatestExecutionPayloadHeader: &cltypes.ExecutionHeader{
			LogsBloom:     make([]byte, 256),
			BaseFeePerGas: make([]byte, 32),
		},
		LatestBlockHeader: &cltypes.BeaconBlockHeader{
			Root: [32]byte{},
		},
		Fork:                        &cltypes.Fork{},
		Eth1Data:                    &cltypes.Eth1Data{},
		PreviousJustifiedCheckpoint: &cltypes.Checkpoint{},
		CurrentJustifiedCheckpoint:  &cltypes.Checkpoint{},
		FinalizedCheckpoint:         &cltypes.Checkpoint{},
	}
}

func BenchmarkStateRootNonCached(b *testing.B) {
	base := getTestBeaconState()
	for i := 0; i < b.N; i++ {
		state := state.FromBellatrixState(base)
		state.HashTreeRoot()
	}
}

func BenchmarkStateRootCached(b *testing.B) {
	// Re-use same fields
	state := state.FromBellatrixState(getTestBeaconState())
	for i := 0; i < b.N; i++ {
		state.HashTreeRoot()
	}
}
