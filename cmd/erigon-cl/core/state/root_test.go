package state_test

import (
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/core/types"
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
		LatestExecutionPayloadHeader: &types.Header{
			Bloom:   types.Bloom{},
			BaseFee: big.NewInt(0),
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

// Prev: 151849172
// Curr: 5463452
func BenchmarkStateRootNonCached(b *testing.B) {
	base := getTestBeaconState()
	for i := 0; i < b.N; i++ {
		state := state.FromBellatrixState(base)
		state.HashTreeRoot()
	}
}

// Prev: 13953
// Curr: 2093
func BenchmarkStateRootCached(b *testing.B) {
	// Re-use same fields
	state := state.FromBellatrixState(getTestBeaconState())
	for i := 0; i < b.N; i++ {
		state.HashTreeRoot()
	}
}
