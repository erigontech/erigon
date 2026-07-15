package raw

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/stretchr/testify/require"
)

func TestGloasBeaconStateUsesProgressiveHashing(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	state := New(&cfg)
	state.version = clparams.GloasVersion

	require.NoError(t, state.computeDirtyLeaves())
	emptyListRoot, err := merkle_tree.ProgressiveListRoot(nil, 0)
	require.NoError(t, err)
	for _, idx := range []StateLeafIndex{
		ValidatorsLeafIndex,
		BalancesLeafIndex,
		PreviousEpochParticipationLeafIndex,
		CurrentEpochParticipationLeafIndex,
		InactivityScoresLeafIndex,
		PendingDepositsLeafIndex,
		PendingPartialWithdrawalsLeafIndex,
		PendingConsolidationsLeafIndex,
		BuildersLeafIndex,
		BuilderPendingWithdrawalsLeafIndex,
		PayloadExpectedWithdrawalsLeafIndex,
	} {
		copy(state.leaves[idx*32:], emptyListRoot[:])
	}
	schema := make([]any, StateLeafSizeGloas)
	for i := range schema {
		schema[i] = state.leaves[i*32 : (i+1)*32]
	}
	expected, err := merkle_tree.ProgressiveContainerRootAll(schema...)
	require.NoError(t, err)

	state.markLeaf(
		ValidatorsLeafIndex,
		BalancesLeafIndex,
		PreviousEpochParticipationLeafIndex,
		CurrentEpochParticipationLeafIndex,
		InactivityScoresLeafIndex,
		PendingDepositsLeafIndex,
		PendingPartialWithdrawalsLeafIndex,
		PendingConsolidationsLeafIndex,
		BuildersLeafIndex,
		BuilderPendingWithdrawalsLeafIndex,
		PayloadExpectedWithdrawalsLeafIndex,
	)
	actual, err := state.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestSetVersionAcrossGloasInvalidatesRoots(t *testing.T) {
	for _, tc := range []struct {
		name string
		from clparams.StateVersion
		to   clparams.StateVersion
	}{
		{name: "upgrade", from: clparams.FuluVersion, to: clparams.GloasVersion},
		{name: "downgrade", from: clparams.GloasVersion, to: clparams.FuluVersion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			state := New(&cfg)
			state.SetVersion(tc.from)
			_, err := state.HashSSZ()
			require.NoError(t, err)

			state.SetVersion(tc.to)
			actual, err := state.HashSSZ()
			require.NoError(t, err)

			fresh := New(&cfg)
			fresh.SetVersion(tc.to)
			expected, err := fresh.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		})
	}
}
