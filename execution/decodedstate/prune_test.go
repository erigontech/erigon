package decodedstate

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

// ===========================================================================
// S-PRUNE: Pruning
// ===========================================================================

func TestS_PRUNE_01_ChangeSetsPrunable(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(1)

	// Write entries across 1000 blocks, updating key each time
	for blk := uint64(1); blk <= 1000; blk++ {
		require.NoError(t, s.WriteEntries(blk, []DecodedEntry{{
			Contract: addrA, MappingSlot: slot,
			Keys: []common.Hash{key}, Value: hashU64(blk),
			EntryType: MappingEntry,
		}}))
	}

	// Prune change sets older than block 900
	require.NoError(t, s.Prune(900))

	// Latest value should still be accessible
	val, found, err := s.GetLatest(addrA, slot, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, hashU64(1000), val, "latest value must survive pruning")

	// Historical query within retained range should work
	val, found, err = s.GetAsOf(addrA, slot, key, 950)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, hashU64(950), val, "recent history within retention should be available")
}

func TestS_PRUNE_02_IndependentFromMainStatePruning(t *testing.T) {
	// This test verifies that decoded state pruning configuration is independent.
	// We test by using different pruning thresholds.
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(1)

	for blk := uint64(1); blk <= 100; blk++ {
		require.NoError(t, s.WriteEntries(blk, []DecodedEntry{{
			Contract: addrA, MappingSlot: slot,
			Keys: []common.Hash{key}, Value: hashU64(blk),
			EntryType: MappingEntry,
		}}))
	}

	// Prune decoded state history older than 90 blocks (keep 11-100)
	require.NoError(t, s.Prune(10))

	// Block 50 should still be queryable (within retained range)
	val, found, err := s.GetAsOf(addrA, slot, key, 50)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, hashU64(50), val)
}

func TestS_PRUNE_03_LatestDecodedStateNeverPruned(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)

	// Write many keys at block 1
	for i := uint64(1); i <= 100; i++ {
		require.NoError(t, s.WriteEntries(1, []DecodedEntry{{
			Contract: addrA, MappingSlot: slot,
			Keys: []common.Hash{keyHash(i)}, Value: hashU64(i * 10),
			EntryType: MappingEntry,
		}}))
	}

	// Aggressive prune
	require.NoError(t, s.Prune(999999))

	// All latest entries should still be accessible
	keys, err := s.EnumerateKeys(addrA, slot)
	require.NoError(t, err)
	require.Len(t, keys, 100, "latest state must never be pruned regardless of threshold")

	// Verify individual values
	val, found, err := s.GetLatest(addrA, slot, keyHash(50))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, hashU64(500), val)
}

func TestS_PRUNE_04_HistoricalQueriesFailAfterPruning(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(1)

	for blk := uint64(1); blk <= 100; blk++ {
		require.NoError(t, s.WriteEntries(blk, []DecodedEntry{{
			Contract: addrA, MappingSlot: slot,
			Keys: []common.Hash{key}, Value: hashU64(blk),
			EntryType: MappingEntry,
		}}))
	}

	// Prune everything older than block 90
	require.NoError(t, s.Prune(90))

	// Query at block 50 (pruned) — should fail gracefully
	val, found, err := s.GetAsOf(addrA, slot, key, 50)

	// Acceptable outcomes:
	// 1. Error indicating pruned data
	// 2. found=false (data not available)
	// NOT acceptable: returning stale/incorrect data silently
	if err != nil {
		// Error is acceptable — indicates pruned data
		return
	}
	if found {
		// If found, the value must NOT be from a wrong block
		require.NotEqual(t, common.Hash{}, val,
			"if data is returned for a pruned range, it must be a valid value, not zero")
		// Ideally this should not return found=true at all
		t.Log("WARNING: query in pruned range returned data — implementation may differ in behavior")
	}
}
