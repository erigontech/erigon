package decodedstate

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

// ===========================================================================
// S-SHA3: SHA3 / Keccak Interception
// ===========================================================================

func TestS_SHA3_01_CapturePreimageForSimpleMapping(t *testing.T) {
	c := NewCollector(enabledFullConfig())

	key := keyHash(42)
	slot := slotHash(0)
	input := mappingInput(key, slot)
	hash := keccak256(input)

	c.OnKeccak256(addrA, input, hash)
	c.OnSStore(addrA, hash, u256(100))

	entries := c.Entries()
	require.Len(t, entries, 1, "expected one decoded entry for simple mapping write")
	require.Equal(t, addrA, entries[0].Contract)
	require.Equal(t, slot, entries[0].MappingSlot)
	require.Equal(t, []common.Hash{key}, entries[0].Keys)
	require.Equal(t, hashU64(100), entries[0].Value)
	require.Equal(t, MappingEntry, entries[0].EntryType)
}

func TestS_SHA3_02_SHA3InputIs64Bytes(t *testing.T) {
	c := NewCollector(enabledFullConfig())

	key := keyHash(7)
	slot := slotHash(0)
	input := mappingInput(key, slot)
	require.Len(t, input, 64, "mapping SHA3 input must be 64 bytes")

	hash := keccak256(input)
	c.OnKeccak256(addrA, input, hash)
	c.OnSStore(addrA, hash, u256(1))

	entries := c.Entries()
	require.Len(t, entries, 1, "64-byte SHA3 input should be recognized as mapping access")
}

func TestS_SHA3_03_SHA3InputIs32BytesForArray(t *testing.T) {
	c := NewCollector(enabledFullConfig())

	slot := slotHash(3)
	input := arrayBaseInput(slot)
	require.Len(t, input, 32, "array base SHA3 input must be 32 bytes")

	base := keccak256(input)
	c.OnKeccak256(addrA, input, base)
	// Array element 0: write at base + 0
	c.OnSStore(addrA, base, u256(42))

	entries := c.Entries()
	require.Len(t, entries, 1, "32-byte SHA3 input followed by SSTORE at base should be an array entry")
	require.Equal(t, ArrayEntry, entries[0].EntryType)
	require.Equal(t, slot, entries[0].MappingSlot)
}

func TestS_SHA3_04_SHA3NotUsedInSSTORE(t *testing.T) {
	c := NewCollector(enabledFullConfig())

	// SHA3 called on arbitrary data — result never used in SSTORE
	input := []byte("some message for ecrecover")
	hash := keccak256(input)
	c.OnKeccak256(addrA, input, hash)

	// No SSTORE follows with this hash
	entries := c.Entries()
	require.Empty(t, entries, "SHA3 not correlated with SSTORE must not produce decoded entries")
}

// ===========================================================================
// S-MAP: Simple Mappings
// ===========================================================================

func TestS_MAP_01_SingleKeyWrite(t *testing.T) {
	c := NewCollector(enabledFullConfig())

	key := keyHash(42)
	slot := slotHash(0)
	loc := mappingLocation(key, slot)

	c.OnKeccak256(addrA, mappingInput(key, slot), loc)
	c.OnSStore(addrA, loc, u256(100))

	entries := c.Entries()
	require.Len(t, entries, 1)
	require.Equal(t, addrA, entries[0].Contract)
	require.Equal(t, slot, entries[0].MappingSlot)
	require.Equal(t, []common.Hash{key}, entries[0].Keys)
	require.Equal(t, hashU64(100), entries[0].Value)
}

func TestS_MAP_02_MultipleKeysInSameMapping(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(0)

	for _, kv := range []struct {
		k, v uint64
	}{{1, 10}, {2, 20}, {3, 30}} {
		key := keyHash(kv.k)
		loc := mappingLocation(key, slot)
		c.OnKeccak256(addrA, mappingInput(key, slot), loc)
		c.OnSStore(addrA, loc, u256(kv.v))
	}

	entries := c.Entries()
	require.Len(t, entries, 3, "three writes should produce three decoded entries")

	// Verify all three keys are present
	keys := make(map[common.Hash]common.Hash)
	for _, e := range entries {
		require.Len(t, e.Keys, 1)
		keys[e.Keys[0]] = e.Value
	}
	require.Equal(t, hashU64(10), keys[keyHash(1)])
	require.Equal(t, hashU64(20), keys[keyHash(2)])
	require.Equal(t, hashU64(30), keys[keyHash(3)])
}

func TestS_MAP_03_OverwriteExistingKey(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(0)
	key := keyHash(42)
	loc := mappingLocation(key, slot)

	// First write
	c.OnKeccak256(addrA, mappingInput(key, slot), loc)
	c.OnSStore(addrA, loc, u256(100))

	// Overwrite
	c.OnKeccak256(addrA, mappingInput(key, slot), loc)
	c.OnSStore(addrA, loc, u256(200))

	entries := c.Entries()
	// The collector should produce one entry per SSTORE, or coalesce to latest.
	// At minimum, the final value must be 200.
	require.NotEmpty(t, entries, "overwrite must still produce entries")

	var found bool
	for _, e := range entries {
		if len(e.Keys) == 1 && e.Keys[0] == key {
			require.Equal(t, hashU64(200), e.Value, "latest value should be 200")
			found = true
		}
	}
	require.True(t, found, "entry for key 42 must exist")
}

func TestS_MAP_04_AddressKeyedMapping(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(1)
	key := addrHash(addrB) // address left-padded to 32 bytes
	loc := mappingLocation(key, slot)

	c.OnKeccak256(addrA, mappingInput(key, slot), loc)
	c.OnSStore(addrA, loc, u256(500))

	entries := c.Entries()
	require.Len(t, entries, 1)
	require.Equal(t, key, entries[0].Keys[0], "key should be left-padded address")
}

func TestS_MAP_05_Bytes32KeyedMapping(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(2)
	key := common.HexToHash("0xDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF")
	loc := mappingLocation(key, slot)

	c.OnKeccak256(addrA, mappingInput(key, slot), loc)
	c.OnSStore(addrA, loc, u256(999))

	entries := c.Entries()
	require.Len(t, entries, 1)
	require.Equal(t, key, entries[0].Keys[0], "full 32-byte key must be preserved")
}

func TestS_MAP_06_MultipleMappingsInSameContract(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot0 := slotHash(0)
	slot1 := slotHash(1)
	key := keyHash(1)

	loc0 := mappingLocation(key, slot0)
	loc1 := mappingLocation(key, slot1)

	c.OnKeccak256(addrA, mappingInput(key, slot0), loc0)
	c.OnSStore(addrA, loc0, u256(10))

	c.OnKeccak256(addrA, mappingInput(key, slot1), loc1)
	c.OnSStore(addrA, loc1, u256(20))

	entries := c.Entries()
	require.Len(t, entries, 2, "writes to two different mapping slots should produce two entries")

	slotValues := make(map[common.Hash]common.Hash)
	for _, e := range entries {
		slotValues[e.MappingSlot] = e.Value
	}
	require.Equal(t, hashU64(10), slotValues[slot0])
	require.Equal(t, hashU64(20), slotValues[slot1])
}

// ===========================================================================
// S-NEST: Nested Mappings
// ===========================================================================

func TestS_NEST_01_TwoLevelNesting(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(0)
	owner := addrHash(addrA)
	spender := addrHash(addrB)

	// First hash: keccak256(owner ++ slot) → intermediate
	innerInput := mappingInput(owner, slot)
	intermediate := keccak256(innerInput)
	c.OnKeccak256(addrC, innerInput, intermediate)

	// Second hash: keccak256(spender ++ intermediate) → final location
	outerInput := mappingInput(spender, intermediate)
	finalLoc := keccak256(outerInput)
	c.OnKeccak256(addrC, outerInput, finalLoc)

	c.OnSStore(addrC, finalLoc, u256(1000))

	entries := c.Entries()
	require.Len(t, entries, 1, "nested mapping write should produce exactly one entry")
	require.Equal(t, NestedMappingEntry, entries[0].EntryType)
	require.Equal(t, slot, entries[0].MappingSlot, "should resolve to the base mapping slot")
	require.Equal(t, []common.Hash{owner, spender}, entries[0].Keys, "composite keys in order")
	require.Equal(t, hashU64(1000), entries[0].Value)
}

func TestS_NEST_02_ThreeLevelNesting(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(0)
	k1 := keyHash(1)
	k2 := keyHash(2)
	k3 := keyHash(3)

	// Level 1: keccak256(k1 ++ slot) → h1
	h1 := keccak256(mappingInput(k1, slot))
	c.OnKeccak256(addrA, mappingInput(k1, slot), h1)

	// Level 2: keccak256(k2 ++ h1) → h2
	h2 := keccak256(mappingInput(k2, h1))
	c.OnKeccak256(addrA, mappingInput(k2, h1), h2)

	// Level 3: keccak256(k3 ++ h2) → h3
	h3 := keccak256(mappingInput(k3, h2))
	c.OnKeccak256(addrA, mappingInput(k3, h2), h3)

	c.OnSStore(addrA, h3, u256(777))

	entries := c.Entries()
	require.Len(t, entries, 1)
	require.Equal(t, NestedMappingEntry, entries[0].EntryType)
	require.Equal(t, slot, entries[0].MappingSlot)
	require.Equal(t, []common.Hash{k1, k2, k3}, entries[0].Keys)
}

func TestS_NEST_03_IntermediateHashIsNotStandalone(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(0)
	k1 := keyHash(1)
	k2 := keyHash(2)

	intermediate := keccak256(mappingInput(k1, slot))
	c.OnKeccak256(addrA, mappingInput(k1, slot), intermediate)

	finalLoc := keccak256(mappingInput(k2, intermediate))
	c.OnKeccak256(addrA, mappingInput(k2, intermediate), finalLoc)

	// Only SSTORE at the final location — intermediate is not stored
	c.OnSStore(addrA, finalLoc, u256(42))

	entries := c.Entries()
	require.Len(t, entries, 1, "only the final SSTORE should produce an entry, not intermediates")
}

// ===========================================================================
// S-ARR: Dynamic Arrays
// ===========================================================================

func TestS_ARR_01_ArrayElementWrite(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(3)

	// SHA3(slot) = base
	base := arrayBase(slot)
	c.OnKeccak256(addrA, arrayBaseInput(slot), base)

	// SSTORE at base + 0
	c.OnSStore(addrA, base, u256(42))

	entries := c.Entries()
	require.Len(t, entries, 1)
	require.Equal(t, ArrayEntry, entries[0].EntryType)
	require.Equal(t, slot, entries[0].MappingSlot)
	require.Equal(t, uint64(0), entries[0].ArrayIndex)
	require.Equal(t, hashU64(42), entries[0].Value)
}

func TestS_ARR_02_MultipleArrayIndices(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(3)
	base := arrayBase(slot)
	c.OnKeccak256(addrA, arrayBaseInput(slot), base)

	for _, iv := range []struct {
		idx, val uint64
	}{{0, 10}, {2, 20}, {4, 30}} {
		loc := arrayElementLocation(slot, iv.idx)
		c.OnSStore(addrA, loc, u256(iv.val))
	}

	entries := c.Entries()
	require.Len(t, entries, 3, "three array indices should produce three entries")

	indices := make(map[uint64]common.Hash)
	for _, e := range entries {
		require.Equal(t, ArrayEntry, e.EntryType)
		indices[e.ArrayIndex] = e.Value
	}
	require.Equal(t, hashU64(10), indices[0])
	require.Equal(t, hashU64(20), indices[2])
	require.Equal(t, hashU64(30), indices[4])
}

func TestS_ARR_03_BytesStringStorage(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(5)
	base := arrayBase(slot)
	c.OnKeccak256(addrA, arrayBaseInput(slot), base)

	// Long string: multiple 32-byte chunks at base+0, base+1, ...
	c.OnSStore(addrA, base, u256(0xDEAD))
	loc1 := arrayElementLocation(slot, 1)
	c.OnSStore(addrA, loc1, u256(0xBEEF))

	entries := c.Entries()
	require.GreaterOrEqual(t, len(entries), 2, "long bytes/string should produce multiple entries")
}

// ===========================================================================
// S-DEL: Delete on Zero
// ===========================================================================

func TestS_DEL_01_SetValueToZeroRemovesEntry(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(0)
	key := keyHash(42)
	loc := mappingLocation(key, slot)

	// Write value
	c.OnKeccak256(addrA, mappingInput(key, slot), loc)
	c.OnSStore(addrA, loc, u256(100))

	// Delete: set to 0
	c.OnKeccak256(addrA, mappingInput(key, slot), loc)
	c.OnSStore(addrA, loc, u256(0))

	entries := c.Entries()
	// After delete, either no entries or an entry with zero-value marker
	for _, e := range entries {
		if len(e.Keys) == 1 && e.Keys[0] == key {
			require.Equal(t, common.Hash{}, e.Value,
				"after deletion, value must be zero (or entry removed)")
		}
	}
	// Check via store that the entry is gone
	s := NewStore(t.TempDir())
	defer s.Close()

	s.WriteEntries(1, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(100),
		EntryType: MappingEntry,
	}})
	s.WriteEntries(2, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: common.Hash{}, // zero → delete
		EntryType: MappingEntry,
	}})

	_, found, err := s.GetLatest(addrA, slot, key)
	require.NoError(t, err)
	require.False(t, found, "entry should be deleted after writing zero value")
}

func TestS_DEL_02_SetToZeroProducesChangeSetEntry(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(42)

	// Block 10: write 100
	s.WriteEntries(10, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: hashU64(100),
		EntryType: MappingEntry,
	}})

	// Block 11: delete (write 0)
	s.WriteEntries(11, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: common.Hash{},
		EntryType: MappingEntry,
	}})

	// Query at block 10 should still show 100
	val, found, err := s.GetAsOf(addrA, slot, key, 10)
	require.NoError(t, err)
	require.True(t, found, "change set must preserve the pre-deletion value")
	require.Equal(t, hashU64(100), val)
}

func TestS_DEL_03_WritingZeroToNonExistentKeyIsNoop(t *testing.T) {
	s := NewStore(t.TempDir())
	defer s.Close()

	slot := slotHash(0)
	key := keyHash(99)

	// Write zero to a key that never existed
	s.WriteEntries(1, []DecodedEntry{{
		Contract: addrA, MappingSlot: slot,
		Keys: []common.Hash{key}, Value: common.Hash{},
		EntryType: MappingEntry,
	}})

	_, found, err := s.GetLatest(addrA, slot, key)
	require.NoError(t, err)
	require.False(t, found, "writing zero to non-existent key should not create an entry")

	// No change set should exist either
	keys, err := s.EnumerateKeys(addrA, slot)
	require.NoError(t, err)
	require.Empty(t, keys, "no keys should be enumerable")
}

// ===========================================================================
// S-REV: Revert Handling
// ===========================================================================

func TestS_REV_01_RevertedSSTORENotInDecodedState(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(0)
	key := keyHash(42)
	loc := mappingLocation(key, slot)

	c.OnEnterCall(addrA, addrA, false) // outer call

	c.OnEnterCall(addrA, addrA, false) // inner call
	c.OnKeccak256(addrA, mappingInput(key, slot), loc)
	c.OnSStore(addrA, loc, u256(100))
	c.OnExitCall(true) // inner call REVERTS

	c.OnExitCall(false) // outer call succeeds

	entries := c.Entries()
	require.Empty(t, entries, "reverted SSTORE must not produce decoded entries")
}

func TestS_REV_02_PartialRevertInNestedCalls(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(0)

	c.OnEnterCall(addrA, addrA, false) // TX-level call

	// Call A: map[1] = 10 (succeeds)
	c.OnEnterCall(addrA, addrA, false)
	k1 := keyHash(1)
	loc1 := mappingLocation(k1, slot)
	c.OnKeccak256(addrA, mappingInput(k1, slot), loc1)
	c.OnSStore(addrA, loc1, u256(10))
	c.OnExitCall(false)

	// Call B: map[2] = 20 (reverts)
	c.OnEnterCall(addrA, addrA, false)
	k2 := keyHash(2)
	loc2 := mappingLocation(k2, slot)
	c.OnKeccak256(addrA, mappingInput(k2, slot), loc2)
	c.OnSStore(addrA, loc2, u256(20))
	c.OnExitCall(true) // REVERT

	// Call C: map[3] = 30 (succeeds)
	c.OnEnterCall(addrA, addrA, false)
	k3 := keyHash(3)
	loc3 := mappingLocation(k3, slot)
	c.OnKeccak256(addrA, mappingInput(k3, slot), loc3)
	c.OnSStore(addrA, loc3, u256(30))
	c.OnExitCall(false)

	c.OnExitCall(false) // TX-level

	entries := c.Entries()
	require.Len(t, entries, 2, "only non-reverted calls should produce entries")

	keySet := make(map[common.Hash]bool)
	for _, e := range entries {
		keySet[e.Keys[0]] = true
	}
	require.True(t, keySet[k1], "key 1 should be present (call A succeeded)")
	require.False(t, keySet[k2], "key 2 should NOT be present (call B reverted)")
	require.True(t, keySet[k3], "key 3 should be present (call C succeeded)")
}

func TestS_REV_03_EntireTransactionRevert(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(0)

	c.OnEnterCall(addrA, addrA, false)

	for i := uint64(1); i <= 5; i++ {
		key := keyHash(i)
		loc := mappingLocation(key, slot)
		c.OnKeccak256(addrA, mappingInput(key, slot), loc)
		c.OnSStore(addrA, loc, u256(i*10))
	}

	c.OnExitCall(true) // entire TX reverts

	entries := c.Entries()
	require.Empty(t, entries, "fully reverted transaction must produce zero decoded entries")
}

func TestS_REV_04_WriteThenOverwriteOuterRevert(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(0)
	key := keyHash(5)
	loc := mappingLocation(key, slot)

	c.OnEnterCall(addrA, addrA, false) // outer

	// Write map[5] = 50
	c.OnKeccak256(addrA, mappingInput(key, slot), loc)
	c.OnSStore(addrA, loc, u256(50))

	// Inner call overwrites map[5] = 60
	c.OnEnterCall(addrA, addrA, false)
	c.OnKeccak256(addrA, mappingInput(key, slot), loc)
	c.OnSStore(addrA, loc, u256(60))
	c.OnExitCall(false) // inner succeeds

	c.OnExitCall(true) // outer REVERTS

	entries := c.Entries()
	require.Empty(t, entries, "outer revert should undo all writes including inner call's")
}

// ===========================================================================
// S-FP: SHA3 False Positive Filtering
// ===========================================================================

func TestS_FP_01_SHA3ForAbiEncodePackedNotRecorded(t *testing.T) {
	c := NewCollector(enabledFullConfig())

	// abi.encodePacked produces variable-length input, not 64 bytes
	input := make([]byte, 48) // neither 32 nor 64
	input[0] = 0xAB
	hash := keccak256(input)

	c.OnKeccak256(addrA, input, hash)
	// No SSTORE with this hash

	entries := c.Entries()
	require.Empty(t, entries)
}

func TestS_FP_02_SHA3ForSignatureVerification(t *testing.T) {
	c := NewCollector(enabledFullConfig())

	// SHA3 on a 32-byte message hash (for ecrecover), but result not used in SSTORE
	msg := make([]byte, 32)
	msg[0] = 0xFF
	hash := keccak256(msg)

	c.OnKeccak256(addrA, msg, hash)
	// Result used in STATICCALL to precompile, not SSTORE

	entries := c.Entries()
	require.Empty(t, entries, "SHA3 for signature verification must not produce entries")
}

func TestS_FP_03_OnlySHA3ToSSTORECorrelation(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(0)
	key := keyHash(10)

	// SHA3 #1: non-mapping use (variable-length data)
	noise := make([]byte, 100)
	c.OnKeccak256(addrA, noise, keccak256(noise))

	// SHA3 #2: actual mapping access
	loc := mappingLocation(key, slot)
	c.OnKeccak256(addrA, mappingInput(key, slot), loc)
	c.OnSStore(addrA, loc, u256(42))

	// SHA3 #3: another non-mapping use
	noise2 := make([]byte, 20)
	c.OnKeccak256(addrA, noise2, keccak256(noise2))

	entries := c.Entries()
	require.Len(t, entries, 1, "only the SHA3 that correlates with SSTORE should produce an entry")
	require.Equal(t, hashU64(42), entries[0].Value)
}

// ===========================================================================
// S-DC: DELEGATECALL Attribution
// ===========================================================================

func TestS_DC_01_StorageWritesAttributedToProxy(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(0)
	key := keyHash(1)

	// Proxy P delegates to implementation I
	c.OnEnterCall(addrProxy, addrImpl, true) // isDelegateCall=true

	loc := mappingLocation(key, slot)
	c.OnKeccak256(addrProxy, mappingInput(key, slot), loc) // addr is proxy (storage owner)
	c.OnSStore(addrProxy, loc, u256(42))

	c.OnExitCall(false)

	entries := c.Entries()
	require.Len(t, entries, 1)
	require.Equal(t, addrProxy, entries[0].Contract,
		"decoded entry must use proxy address (storage owner), not implementation")
}

func TestS_DC_02_SameImplementationMultipleProxies(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(0)
	key := keyHash(1)

	// Proxy P1 delegates to I
	c.OnEnterCall(addrProxy, addrImpl, true)
	loc1 := mappingLocation(key, slot)
	c.OnKeccak256(addrProxy, mappingInput(key, slot), loc1)
	c.OnSStore(addrProxy, loc1, u256(100))
	c.OnExitCall(false)

	// Proxy P2 delegates to same I
	c.OnEnterCall(addrProxy2, addrImpl, true)
	loc2 := mappingLocation(key, slot)
	c.OnKeccak256(addrProxy2, mappingInput(key, slot), loc2)
	c.OnSStore(addrProxy2, loc2, u256(200))
	c.OnExitCall(false)

	entries := c.Entries()
	require.Len(t, entries, 2)

	contracts := make(map[common.Address]common.Hash)
	for _, e := range entries {
		contracts[e.Contract] = e.Value
	}
	require.Equal(t, hashU64(100), contracts[addrProxy], "P1 entry")
	require.Equal(t, hashU64(200), contracts[addrProxy2], "P2 entry")
}

func TestS_DC_03_NestedDelegateCall(t *testing.T) {
	c := NewCollector(enabledFullConfig())
	slot := slotHash(0)
	key := keyHash(7)

	// P → delegatecall → I1 → delegatecall → I2
	c.OnEnterCall(addrProxy, addrImpl, true)  // P delegates to I1
	c.OnEnterCall(addrProxy, addrImpl2, true) // I1 delegates to I2 (storage still P's)

	loc := mappingLocation(key, slot)
	c.OnKeccak256(addrProxy, mappingInput(key, slot), loc)
	c.OnSStore(addrProxy, loc, u256(999))

	c.OnExitCall(false) // I2 returns
	c.OnExitCall(false) // I1 returns

	entries := c.Entries()
	require.Len(t, entries, 1)
	require.Equal(t, addrProxy, entries[0].Contract,
		"nested DELEGATECALL must still attribute storage to the original proxy")
}
