package raw

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
)

type trackingProgressiveUint64List struct {
	solid.Uint64ListSSZ
	modeChanges []bool
	hashes      int
}

func (l *trackingProgressiveUint64List) HashSSZProgressive() ([32]byte, error) {
	l.hashes++
	return [32]byte{}, nil
}

func (l *trackingProgressiveUint64List) SetProgressiveHashing(enabled bool) {
	l.modeChanges = append(l.modeChanges, enabled)
}

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
			state.balances.Append(1)
			state.inactivityScores.Append(2)
			state.SetVersion(tc.from)
			_, err := state.HashSSZ()
			require.NoError(t, err)

			state.SetVersion(tc.to)
			actual, err := state.HashSSZ()
			require.NoError(t, err)

			fresh := New(&cfg)
			fresh.balances.Append(1)
			fresh.inactivityScores.Append(2)
			fresh.SetVersion(tc.to)
			expected, err := fresh.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		})
	}
}

func TestGloasSwitchesUint64ListsToProgressiveHashing(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	state := New(&cfg)
	balances := &trackingProgressiveUint64List{Uint64ListSSZ: solid.NewUint64ListSSZ(int(cfg.ValidatorRegistryLimit))}
	inactivityScores := &trackingProgressiveUint64List{Uint64ListSSZ: solid.NewUint64ListSSZ(int(cfg.ValidatorRegistryLimit))}
	state.balances = balances
	state.inactivityScores = inactivityScores
	state.SetVersion(clparams.FuluVersion)

	state.SetVersion(clparams.GloasVersion)
	require.Equal(t, []bool{true}, balances.modeChanges)
	require.Equal(t, []bool{true}, inactivityScores.modeChanges)
	require.NoError(t, state.computeDirtyLeaves())
	require.Equal(t, 1, balances.hashes)
	require.Equal(t, 1, inactivityScores.hashes)

	state.SetVersion(clparams.FuluVersion)
	require.Equal(t, []bool{true, false}, balances.modeChanges)
	require.Equal(t, []bool{true, false}, inactivityScores.modeChanges)
}

func TestGloasUint64ListFallback(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	withVectors := New(&cfg)
	withLists := New(&cfg)
	withVectors.SetVersion(clparams.GloasVersion)
	withLists.SetVersion(clparams.GloasVersion)

	balancesVector := solid.NewUint64VectorSSZ(1)
	balancesVector.Set(0, 7)
	inactivityVector := solid.NewUint64VectorSSZ(1)
	inactivityVector.Set(0, 8)
	withVectors.SetBalances(balancesVector)
	withVectors.SetInactivityScoresRaw(inactivityVector)

	withLists.SetBalances(solid.NewUint64ListSSZFromSlice(1, []uint64{7}))
	withLists.SetInactivityScoresRaw(solid.NewUint64ListSSZFromSlice(1, []uint64{8}))

	vectorRoot, err := withVectors.HashSSZ()
	require.NoError(t, err)
	listRoot, err := withLists.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, listRoot, vectorRoot)
}
