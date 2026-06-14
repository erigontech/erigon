package decodedstate

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

// ===========================================================================
// S-WL: Whitelist Mode
// ===========================================================================

func TestS_WL_01_OnlyWhitelistedContractsTracked(t *testing.T) {
	cfg := whitelistConfig(addrA)
	c := NewCollector(cfg)

	slot := slotHash(0)
	key := keyHash(1)

	// Write to whitelisted contract A
	locA := mappingLocation(key, slot)
	c.OnKeccak256(addrA, mappingInput(key, slot), locA)
	c.OnSStore(addrA, locA, u256(10))

	// Write to non-whitelisted contract B
	locB := mappingLocation(key, slot)
	c.OnKeccak256(addrB, mappingInput(key, slot), locB)
	c.OnSStore(addrB, locB, u256(20))

	entries := c.Entries()
	require.Len(t, entries, 1, "only whitelisted contract should be tracked")
	require.Equal(t, addrA, entries[0].Contract)
}

func TestS_WL_02_AddContractToWhitelist(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(1)

	// Phase 1: only contract A whitelisted, write entries for A
	require.NoError(t, s.WriteEntries(10, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(100),
		EntryType: MappingEntry,
	}}))

	// Phase 2: contract C is added to whitelist and re-executed from its deployment block
	require.NoError(t, s.WriteEntries(5, []DecodedEntry{{
		Contract: addrC, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(50),
		EntryType: MappingEntry,
	}}))

	// Both should be queryable
	valA, foundA, err := s.GetLatest(addrA, slot, key)
	require.NoError(t, err)
	require.True(t, foundA)
	require.Equal(t, hashU64(100), valA)

	valC, foundC, err := s.GetLatest(addrC, slot, key)
	require.NoError(t, err)
	require.True(t, foundC, "newly added contract should have decoded state after re-execution")
	require.Equal(t, hashU64(50), valC)
}

func TestS_WL_03_RemoveContractFromWhitelist(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(1)

	// Write decoded state for contract A
	require.NoError(t, s.WriteEntries(10, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(100),
		EntryType: MappingEntry,
	}}))

	// Remove contract A → drops its decoded state
	require.NoError(t, s.DeleteContract(addrA))

	_, found, err := s.GetLatest(addrA, slot, key)
	require.NoError(t, err)
	require.False(t, found, "removed contract's decoded state should be dropped")

	keys, err := s.EnumerateKeys(addrA, slot)
	require.NoError(t, err)
	require.Empty(t, keys, "no keys should remain after contract removal")
}

func TestS_WL_04_EmptyWhitelistNoTracking(t *testing.T) {
	cfg := whitelistConfig() // empty whitelist
	c := NewCollector(cfg)

	slot := slotHash(0)
	key := keyHash(1)
	loc := mappingLocation(key, slot)

	c.OnKeccak256(addrA, mappingInput(key, slot), loc)
	c.OnSStore(addrA, loc, u256(42))

	entries := c.Entries()
	require.Empty(t, entries, "empty whitelist should track nothing")
}

// ===========================================================================
// S-FULL: Full Mode
// ===========================================================================

func TestS_FULL_01_AllContractsTracked(t *testing.T) {
	c := NewCollector(enabledFullConfig())

	slot := slotHash(0)
	key := keyHash(1)

	// Multiple contracts
	for _, addr := range []common.Address{addrA, addrB, addrC} {
		loc := mappingLocation(key, slot)
		c.OnKeccak256(addr, mappingInput(key, slot), loc)
		c.OnSStore(addr, loc, u256(42))
	}

	entries := c.Entries()
	require.Len(t, entries, 3, "full mode should track all contracts")

	contracts := make(map[common.Address]bool)
	for _, e := range entries {
		contracts[e.Contract] = true
	}
	require.True(t, contracts[addrA])
	require.True(t, contracts[addrB])
	require.True(t, contracts[addrC])
}

func TestS_FULL_02_SwitchFromWhitelistToFullRequiresReExecution(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(1)

	// Whitelist mode: only A is tracked
	require.NoError(t, s.WriteEntries(10, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(100),
		EntryType: MappingEntry,
	}}))

	// Contract B has no decoded state (wasn't tracked)
	_, found, err := s.GetLatest(addrB, slot, key)
	require.NoError(t, err)
	require.False(t, found, "B was not tracked in whitelist mode")

	// Switching to full mode requires re-execution from genesis.
	// After re-execution, B should have entries.
	// For this test, we verify that the store supports overwriting with re-executed data.
	require.NoError(t, s.WriteEntries(5, []DecodedEntry{{
		Contract: addrB, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(50),
		EntryType: MappingEntry,
	}}))

	val, found, err := s.GetLatest(addrB, slot, key)
	require.NoError(t, err)
	require.True(t, found, "after re-execution, B should have decoded state")
	require.Equal(t, hashU64(50), val)
}
