package decodedstate

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

// ===========================================================================
// S-SNAP: Snapshots (.seg Files)
// ===========================================================================

func TestS_SNAP_01_OldDecodedStateFreezes(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)

	// Simulate 100 blocks of decoded state writes
	for blk := uint64(1); blk <= 100; blk++ {
		require.NoError(t, s.WriteEntries(blk, []DecodedEntry{{
			Contract: addrA, MappingSlot: slot,
			Keys: []common.Hash{keyHash(blk)}, Value: hashU64(blk * 10),
			EntryType: MappingEntry,
		}}))
	}

	// After freezing, older entries should still be queryable
	// (they've been moved to .seg files, but the API is transparent)
	val, found, err := s.GetAsOf(addrA, slot, keyHash(1), 1)
	require.NoError(t, err)
	require.True(t, found, "frozen data should still be readable")
	require.Equal(t, hashU64(10), val)

	val, found, err = s.GetLatest(addrA, slot, keyHash(100))
	require.NoError(t, err)
	require.True(t, found, "latest data should be readable")
	require.Equal(t, hashU64(1000), val)
}

func TestS_SNAP_02_SnapshotFollowsMainStateLifecycle(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)

	// Write entries across a range that would span snapshot boundaries
	for blk := uint64(1); blk <= 50; blk++ {
		require.NoError(t, s.WriteEntries(blk, []DecodedEntry{{
			Contract: addrA, MappingSlot: slot,
			Keys: []common.Hash{keyHash(blk)}, Value: hashU64(blk),
			EntryType: MappingEntry,
		}}))
	}

	// The store should report the correct latest block
	blk, err := s.LatestBlock()
	require.NoError(t, err)
	require.Equal(t, uint64(50), blk)

	// Enumerate all keys — should include all 50
	keys, err := s.EnumerateKeys(addrA, slot)
	require.NoError(t, err)
	require.Len(t, keys, 50, "all 50 keys should be enumerable across hot+cold data")
}

func TestS_SNAP_03_QueriesSpanningHotAndColdData(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(1)

	// Simulate a key written early (cold) and overwritten recently (hot)
	require.NoError(t, s.WriteEntries(1, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(10),
		EntryType: MappingEntry,
	}}))
	require.NoError(t, s.WriteEntries(1000, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(1000),
		EntryType: MappingEntry,
	}}))

	// Historical query into cold range
	val, found, err := s.GetAsOf(addrA, slot, key, 500)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, hashU64(10), val, "historical query in cold range should work")

	// Latest query (hot)
	val, found, err = s.GetLatest(addrA, slot, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, hashU64(1000), val)
}

func TestS_SNAP_04_SnapshotDataIsImmutable(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)

	// Write blocks 1-10 (assume frozen)
	for blk := uint64(1); blk <= 10; blk++ {
		require.NoError(t, s.WriteEntries(blk, []DecodedEntry{{
			Contract: addrA, MappingSlot: slot,
			Keys: []common.Hash{keyHash(blk)}, Value: hashU64(blk * 100),
			EntryType: MappingEntry,
		}}))
	}

	// Write blocks 11-15 (hot)
	for blk := uint64(11); blk <= 15; blk++ {
		require.NoError(t, s.WriteEntries(blk, []DecodedEntry{{
			Contract: addrA, MappingSlot: slot,
			Keys: []common.Hash{keyHash(blk)}, Value: hashU64(blk * 100),
			EntryType: MappingEntry,
		}}))
	}

	// Unwind to block 10 — should only affect blocks 11-15
	require.NoError(t, s.Unwind(10))

	// Frozen data (blocks 1-10) must be intact
	for blk := uint64(1); blk <= 10; blk++ {
		val, found, err := s.GetLatest(addrA, slot, keyHash(blk))
		require.NoError(t, err)
		require.True(t, found, "frozen block %d data must survive unwind", blk)
		require.Equal(t, hashU64(blk*100), val)
	}

	// Hot data (blocks 11-15) must be gone
	for blk := uint64(11); blk <= 15; blk++ {
		_, found, err := s.GetLatest(addrA, slot, keyHash(blk))
		require.NoError(t, err)
		require.False(t, found, "hot block %d data should be unwound", blk)
	}
}
