package decodedstate

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

// ===========================================================================
// S-EDGE: Edge Cases
// ===========================================================================

func TestS_EDGE_01_NoMappingsProducesNoDecodedState(t *testing.T) {
	c := NewCollector(enabledFullConfig())

	// Direct SSTORE to slot 0 (not through SHA3 — just a plain storage write)
	c.OnSStore(addrA, slotHash(0), u256(42))

	// No SHA3 preceded this SSTORE, so it's not a mapping/array access
	entries := c.Entries()
	require.Empty(t, entries, "plain SSTORE without SHA3 should not produce decoded entries")
}

func TestS_EDGE_02_LargeKeyCount(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	const numKeys = 10000

	entries := make([]DecodedEntry, numKeys)
	for i := 0; i < numKeys; i++ {
		entries[i] = DecodedEntry{
			Contract:    addrA,
			MappingSlot: slot,
			Keys:        []common.Hash{keyHash(uint64(i))},
			// Keep this a pure cardinality test: zero values are deletes in decoded state.
			Value:     hashU64(uint64(i*10 + 1)),
			EntryType: MappingEntry,
		}
	}
	require.NoError(t, s.WriteEntries(1, entries))

	keys, err := s.EnumerateKeys(addrA, slot)
	require.NoError(t, err)
	require.Len(t, keys, numKeys, "all 10,000 keys must be enumerable")
}

func TestS_EDGE_03_SameKeyDifferentContracts(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(42)

	// Same key, same slot, different contracts
	require.NoError(t, s.WriteEntries(1, []DecodedEntry{
		{Contract: addrA, MappingSlot: slot, Keys: []common.Hash{key}, Value: hashU64(100), EntryType: MappingEntry},
		{Contract: addrB, MappingSlot: slot, Keys: []common.Hash{key}, Value: hashU64(200), EntryType: MappingEntry},
	}))

	valA, foundA, err := s.GetLatest(addrA, slot, key)
	require.NoError(t, err)
	require.True(t, foundA)
	require.Equal(t, hashU64(100), valA)

	valB, foundB, err := s.GetLatest(addrB, slot, key)
	require.NoError(t, err)
	require.True(t, foundB)
	require.Equal(t, hashU64(200), valB, "entries must be separate per contract")
}

func TestS_EDGE_04_SelfDestructedContract(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(1)

	// Write entry
	require.NoError(t, s.WriteEntries(10, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(42),
		EntryType: MappingEntry,
	}}))

	// Self-destruct: delete all decoded state for the contract
	require.NoError(t, s.DeleteContract(addrA))

	_, found, err := s.GetLatest(addrA, slot, key)
	require.NoError(t, err)
	require.False(t, found, "self-destructed contract's entries should be removed")

	keys, err := s.EnumerateKeys(addrA, slot)
	require.NoError(t, err)
	require.Empty(t, keys)
}

func TestS_EDGE_05_CREATE2AddressReuse(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(1)

	// First contract at addrA writes decoded state
	require.NoError(t, s.WriteEntries(10, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(100),
		EntryType: MappingEntry,
	}}))

	// Self-destruct
	require.NoError(t, s.DeleteContract(addrA))

	// New contract deployed at same address via CREATE2
	require.NoError(t, s.WriteEntries(20, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(999),
		EntryType: MappingEntry,
	}}))

	// Should see the new contract's value, not the old one
	val, found, err := s.GetLatest(addrA, slot, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, hashU64(999), val, "new contract at reused address should have fresh state")
}

func TestS_EDGE_06_ZeroAddressHandling(t *testing.T) {
	c := NewCollector(enabledFullConfig())

	zeroAddr := common.Address{}
	slot := slotHash(0)
	key := keyHash(1)
	loc := mappingLocation(key, slot)

	// Should not panic — zero address is handled gracefully
	c.OnKeccak256(zeroAddr, mappingInput(key, slot), loc)
	c.OnSStore(zeroAddr, loc, u256(1))

	// Whether entries are produced or not depends on policy,
	// but the system must not panic.
	// If entries are produced, they should have the zero address.
	entries := c.Entries()
	for _, e := range entries {
		require.Equal(t, zeroAddr, e.Contract)
	}
}
