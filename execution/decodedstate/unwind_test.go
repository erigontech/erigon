package decodedstate

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

// ===========================================================================
// S-UNW: Unwind (Reorg)
// ===========================================================================

func TestS_UNW_01_UnwindRemovesEntriesFromUnwoundBlocks(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(42)

	// Block 10: first appearance of key 42
	require.NoError(t, s.WriteEntries(10, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(100),
		EntryType: MappingEntry,
	}}))

	// Unwind to block 9 (before the key was written)
	require.NoError(t, s.Unwind(9))

	_, found, err := s.GetLatest(addrA, slot, key)
	require.NoError(t, err)
	require.False(t, found, "entry added in unwound block must be removed")
}

func TestS_UNW_02_UnwindRestoresPreviousValues(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(42)

	// Block 9: map[42] = 50
	require.NoError(t, s.WriteEntries(9, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(50),
		EntryType: MappingEntry,
	}}))

	// Block 10: map[42] = 100 (overwrite)
	require.NoError(t, s.WriteEntries(10, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(100),
		EntryType: MappingEntry,
	}}))

	// Unwind to block 9
	require.NoError(t, s.Unwind(9))

	val, found, err := s.GetLatest(addrA, slot, key)
	require.NoError(t, err)
	require.True(t, found, "entry from block 9 should be restored")
	require.Equal(t, hashU64(50), val, "value should revert to block 9 state")
}

func TestS_UNW_03_UnwindOfDeleteRestoresEntry(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(42)

	// Block 9: map[42] = 100
	require.NoError(t, s.WriteEntries(9, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(100),
		EntryType: MappingEntry,
	}}))

	// Block 10: map[42] = 0 (delete)
	require.NoError(t, s.WriteEntries(10, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: common.Hash{},
		EntryType: MappingEntry,
	}}))

	// Verify deletion
	_, found, err := s.GetLatest(addrA, slot, key)
	require.NoError(t, err)
	require.False(t, found, "key should be deleted after block 10")

	// Unwind to block 9 → entry should be restored
	require.NoError(t, s.Unwind(9))

	val, found, err := s.GetLatest(addrA, slot, key)
	require.NoError(t, err)
	require.True(t, found, "unwind should restore deleted entry")
	require.Equal(t, hashU64(100), val)
}

func TestS_UNW_04_MultiBlockUnwind(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)

	// Block 8: map[1] = 10
	require.NoError(t, s.WriteEntries(8, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{keyHash(1)}, Value: hashU64(10),
		EntryType: MappingEntry,
	}}))

	// Block 9: map[1] = 11, map[2] = 20
	require.NoError(t, s.WriteEntries(9, []DecodedEntry{
		{Contract: addrA, MappingSlot: slot, Keys: []common.Hash{keyHash(1)}, Value: hashU64(11), EntryType: MappingEntry},
		{Contract: addrA, MappingSlot: slot, Keys: []common.Hash{keyHash(2)}, Value: hashU64(20), EntryType: MappingEntry},
	}))

	// Block 10: map[1] = 12, map[3] = 30
	require.NoError(t, s.WriteEntries(10, []DecodedEntry{
		{Contract: addrA, MappingSlot: slot, Keys: []common.Hash{keyHash(1)}, Value: hashU64(12), EntryType: MappingEntry},
		{Contract: addrA, MappingSlot: slot, Keys: []common.Hash{keyHash(3)}, Value: hashU64(30), EntryType: MappingEntry},
	}))

	// Block 11: map[2] = 21
	require.NoError(t, s.WriteEntries(11, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{keyHash(2)}, Value: hashU64(21),
		EntryType: MappingEntry,
	}}))

	// Unwind to block 8 — revert blocks 9, 10, 11
	require.NoError(t, s.Unwind(8))

	// map[1] should be 10 (from block 8)
	val, found, err := s.GetLatest(addrA, slot, keyHash(1))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, hashU64(10), val, "map[1] should revert to block 8 value")

	// map[2] should not exist (first written in block 9)
	_, found, err = s.GetLatest(addrA, slot, keyHash(2))
	require.NoError(t, err)
	require.False(t, found, "map[2] was first written in block 9, should be gone")

	// map[3] should not exist (first written in block 10)
	_, found, err = s.GetLatest(addrA, slot, keyHash(3))
	require.NoError(t, err)
	require.False(t, found, "map[3] was first written in block 10, should be gone")
}

func TestS_UNW_05_UnwindDoesNotTouchSnapshots(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)

	// Write a range of blocks simulating frozen + hot data
	// Blocks 1-5: "frozen" range
	for blk := uint64(1); blk <= 5; blk++ {
		require.NoError(t, s.WriteEntries(blk, []DecodedEntry{{
			Contract: addrA, MappingSlot: slot,
			Keys: []common.Hash{keyHash(blk)}, Value: hashU64(blk * 10),
			EntryType: MappingEntry,
		}}))
	}

	// Blocks 6-10: "hot" range
	for blk := uint64(6); blk <= 10; blk++ {
		require.NoError(t, s.WriteEntries(blk, []DecodedEntry{{
			Contract: addrA, MappingSlot: slot,
			Keys: []common.Hash{keyHash(blk)}, Value: hashU64(blk * 10),
			EntryType: MappingEntry,
		}}))
	}

	// Unwind to block 5 — should only affect blocks 6-10
	require.NoError(t, s.Unwind(5))

	// Blocks 1-5 data should still be intact
	for blk := uint64(1); blk <= 5; blk++ {
		val, found, err := s.GetLatest(addrA, slot, keyHash(blk))
		require.NoError(t, err)
		require.True(t, found, "block %d entry should survive unwind", blk)
		require.Equal(t, hashU64(blk*10), val)
	}

	// Blocks 6-10 data should be gone
	for blk := uint64(6); blk <= 10; blk++ {
		_, found, err := s.GetLatest(addrA, slot, keyHash(blk))
		require.NoError(t, err)
		require.False(t, found, "block %d entry should be unwound", blk)
	}
}
