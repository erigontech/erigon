package shuffling_test

import (
	_ "embed"
	"github.com/ledgerwatch/erigon-lib/common/eth2shuffle"
	"testing"

	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/raw"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/shuffling"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func BenchmarkLambdaShuffledIndex(b *testing.B) {
	keccakOptimized := utils.OptimizedKeccak256()
	eth2ShuffleHash := func(data []byte) []byte {
		hashed := keccakOptimized(data)
		return hashed[:]
	}
	seed := [32]byte{2, 35, 6}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eth2shuffle.PermuteIndex(eth2ShuffleHash, uint8(clparams.MainnetBeaconConfig.ShuffleRoundCount), 10, 1000, seed)
	}
}

// Faster by ~40%, the effects of it will be felt mostly on computation of the proposer index.
func BenchmarkErigonShuffledIndex(b *testing.B) {
	s := state.New(&clparams.MainnetBeaconConfig)
	keccakOptimized := utils.OptimizedKeccak256NotThreadSafe()

	seed := [32]byte{2, 35, 6}
	preInputs := shuffling.ComputeShuffledIndexPreInputs(s.BeaconConfig(), seed)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		shuffling.ComputeShuffledIndex(s.BeaconConfig(), 10, 1000, seed, preInputs, keccakOptimized)
	}
}

func TestShuffling(t *testing.T) {
	s := raw.GetTestState()
	idx, err := shuffling.ComputeProposerIndex(s, []uint64{1, 2, 3, 4, 5, 6, 7, 8}, [32]byte{1})
	require.NoError(t, err)
	require.Equal(t, idx, uint64(2))
}
