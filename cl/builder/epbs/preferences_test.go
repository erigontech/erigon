package epbs

import (
	"context"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestPooledPreferencesReturnsExactStoredPreference(t *testing.T) {
	epbsPool := pool.NewEpbsPool()
	root := common.HexToHash("0x01")
	want := signedPreferences(12, root)
	epbsPool.ProposerPreferences.Add(pool.ProposerPreferencesKey{Slot: 12, DependentRoot: root}, want)

	source := newPooledPreferences(epbsPool, time.Millisecond)
	got, err := source.WaitForPreferences(t.Context(), 12, root, time.Second)
	require.NoError(t, err)
	require.Same(t, want, got)
}

func TestPooledPreferencesDoesNotCrossDependentRoots(t *testing.T) {
	epbsPool := pool.NewEpbsPool()
	wantedRoot := common.HexToHash("0x01")
	otherRoot := common.HexToHash("0x02")
	epbsPool.ProposerPreferences.Add(
		pool.ProposerPreferencesKey{Slot: 12, DependentRoot: otherRoot},
		signedPreferences(12, otherRoot),
	)

	source := newPooledPreferences(epbsPool, time.Millisecond)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := source.WaitForPreferences(ctx, 12, wantedRoot, time.Second)
	require.ErrorIs(t, err, context.Canceled)
}

func TestPooledPreferencesWaitsForArrival(t *testing.T) {
	epbsPool := pool.NewEpbsPool()
	root := common.HexToHash("0x01")
	want := signedPreferences(12, root)
	source := newPooledPreferences(epbsPool, time.Millisecond)

	go func() {
		time.Sleep(5 * time.Millisecond)
		epbsPool.ProposerPreferences.Add(pool.ProposerPreferencesKey{Slot: 12, DependentRoot: root}, want)
	}()

	got, err := source.WaitForPreferences(t.Context(), 12, root, time.Second)
	require.NoError(t, err)
	require.Same(t, want, got)
}

func TestPooledPreferencesRejectsMismatchedStoredValue(t *testing.T) {
	epbsPool := pool.NewEpbsPool()
	root := common.HexToHash("0x01")
	epbsPool.ProposerPreferences.Add(
		pool.ProposerPreferencesKey{Slot: 12, DependentRoot: root},
		signedPreferences(13, root),
	)

	source := newPooledPreferences(epbsPool, time.Millisecond)
	_, err := source.WaitForPreferences(t.Context(), 12, root, time.Second)
	require.ErrorContains(t, err, "does not match lookup key")
}

func TestPooledPreferencesRejectsMissingDependencies(t *testing.T) {
	root := common.HexToHash("0x01")
	_, err := NewPooledPreferences(nil).WaitForPreferences(t.Context(), 12, root, time.Second)
	require.ErrorContains(t, err, "nil pool")

	epbsPool := pool.NewEpbsPool()
	epbsPool.ProposerPreferences.Add(
		pool.ProposerPreferencesKey{Slot: 12, DependentRoot: root},
		&cltypes.SignedProposerPreferences{},
	)
	_, err = newPooledPreferences(epbsPool, time.Millisecond).WaitForPreferences(t.Context(), 12, root, time.Second)
	require.ErrorContains(t, err, "nil message")
}

func signedPreferences(slot uint64, root common.Hash) *cltypes.SignedProposerPreferences {
	return &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{ProposalSlot: slot, DependentRoot: root},
	}
}
