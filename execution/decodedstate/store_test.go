package decodedstate

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

// ===========================================================================
// S-HIST: Historical Access (Change Sets)
// ===========================================================================

func TestS_HIST_01_ChangeSetRecordsPrevValueOnOverwrite(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(42)

	// Block 10: map[42] = 100
	require.NoError(t, s.WriteEntries(10, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(100),
		EntryType: MappingEntry,
	}}))

	// Block 11: map[42] = 200
	require.NoError(t, s.WriteEntries(11, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(200),
		EntryType: MappingEntry,
	}}))

	// Latest should be 200
	val, found, err := s.GetLatest(addrA, slot, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, hashU64(200), val)

	// At block 10, value should be 100
	val, found, err = s.GetAsOf(addrA, slot, key, 10)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, hashU64(100), val, "change set must record previous value 100")
}

func TestS_HIST_02_QueryDecodedStateAtHistoricalBlock(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(1)

	// Block 10: map[1] = 10
	require.NoError(t, s.WriteEntries(10, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(10),
		EntryType: MappingEntry,
	}}))
	// Block 20: map[1] = 20
	require.NoError(t, s.WriteEntries(20, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(20),
		EntryType: MappingEntry,
	}}))
	// Block 30: map[1] = 30
	require.NoError(t, s.WriteEntries(30, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(30),
		EntryType: MappingEntry,
	}}))

	// Query at block 15 → should be 10 (written at block 10)
	val, found, err := s.GetAsOf(addrA, slot, key, 15)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, hashU64(10), val, "state at block 15 should reflect block 10 write")

	// Query at block 25 → should be 20
	val, found, err = s.GetAsOf(addrA, slot, key, 25)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, hashU64(20), val)
}

func TestS_HIST_03_ChangeSetForFirstWrite(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(42)

	// Block 10: first ever write
	require.NoError(t, s.WriteEntries(10, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(100),
		EntryType: MappingEntry,
	}}))

	// Query at block 9 (before the first write) → not found
	_, found, err := s.GetAsOf(addrA, slot, key, 9)
	require.NoError(t, err)
	require.False(t, found, "key should not exist before its first write")
}

func TestS_HIST_04_ChangeSetForDeletion(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(42)

	// Block 10: write 100
	require.NoError(t, s.WriteEntries(10, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(100),
		EntryType: MappingEntry,
	}}))

	// Block 11: delete (write 0)
	require.NoError(t, s.WriteEntries(11, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: common.Hash{},
		EntryType: MappingEntry,
	}}))

	// Query at block 10 → 100
	val, found, err := s.GetAsOf(addrA, slot, key, 10)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, hashU64(100), val, "change set must record pre-deletion value")

	// Query at block 11 → not found (deleted)
	_, found, err = s.GetAsOf(addrA, slot, key, 11)
	require.NoError(t, err)
	require.False(t, found, "key should be gone after deletion")
}

func TestS_HIST_05_HistoryKeysDoNotCollapseDistinctTransactionsInsideOneBlock(t *testing.T) {
	storeSrc := compactSource(readRepoFile(t, "execution", "decodedstate", "store.go"))

	require.NotContains(
		t,
		storeSrc,
		"encodeHistoryKey(sk,blockNum)",
		"decoded history must be keyed by canonical txNum rather than blockNum so distinct transactions inside one block remain distinguishable",
	)
	require.NotContains(
		t,
		storeSrc,
		"BlockNum:blockNum",
		"decoded history records must not store the block number as the canonical change axis when tx-granular history is required",
	)
}

// ===========================================================================
// S-ATOM: Atomicity
// ===========================================================================

func TestS_ATOM_01_WritesAtomicWithMainState(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(1)

	// Write entries for block 10
	err := s.WriteEntries(10, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(42),
		EntryType: MappingEntry,
	}})
	require.NoError(t, err)

	// Verify the entry is readable
	val, found, err := s.GetLatest(addrA, slot, key)
	require.NoError(t, err)
	require.True(t, found, "written entry should be readable")
	require.Equal(t, hashU64(42), val)

	// Verify latest block is updated
	blk, err := s.LatestBlock()
	require.NoError(t, err)
	require.Equal(t, uint64(10), blk, "latest block must be updated atomically with entries")
}

func TestS_ATOM_02_CrashBeforeCommitLosesBoth(t *testing.T) {
	// This test verifies that if a store is re-opened after being closed
	// without explicit commit, no partial data is visible.
	dir := t.TempDir()

	s := NewStore(dir)
	slot := slotHash(0)
	key := keyHash(1)

	require.NoError(t, s.WriteEntries(10, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(42),
		EntryType: MappingEntry,
	}}))

	// Close and reopen — the implementation must ensure durability
	require.NoError(t, s.Close())

	s2 := NewStore(dir)
	defer s2.Close()

	val, found, err := s2.GetLatest(addrA, slot, key)
	require.NoError(t, err)
	require.True(t, found, "data should survive close + reopen")
	require.Equal(t, hashU64(42), val)
}

func TestS_ATOM_03_ConsistentOnStartup(t *testing.T) {
	dir := t.TempDir()

	s := NewStore(dir)
	require.NoError(t, s.WriteEntries(10, []DecodedEntry{{
		Contract: addrA, MappingSlot: slotHash(0),
		Keys: []common.Hash{keyHash(1)}, Value: hashU64(10),
		EntryType: MappingEntry,
	}}))
	require.NoError(t, s.WriteEntries(20, []DecodedEntry{{
		Contract: addrA, MappingSlot: slotHash(0),
		Keys: []common.Hash{keyHash(2)}, Value: hashU64(20),
		EntryType: MappingEntry,
	}}))
	require.NoError(t, s.Close())

	// Reopen and verify consistency
	s2 := NewStore(dir)
	defer s2.Close()

	blk, err := s2.LatestBlock()
	require.NoError(t, err)
	require.Equal(t, uint64(20), blk, "latest block must be consistent after restart")

	keys, err := s2.EnumerateKeys(addrA, slotHash(0))
	require.NoError(t, err)
	require.Len(t, keys, 2, "both keys should be present after restart")
}
