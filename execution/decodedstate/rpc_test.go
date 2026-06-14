package decodedstate

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

// rpcSetup creates a Store pre-loaded with decoded state and an RPCHandler.
func rpcSetup(t *testing.T, cfg Config) (Store, RPCHandler) {
	t.Helper()
	s := NewStore(t.TempDir())
	t.Cleanup(func() { s.Close() })

	slot := slotHash(0)

	// Block 10: map[1]=10, map[2]=20
	require.NoError(t, s.WriteEntries(10, []DecodedEntry{
		{Contract: addrA, MappingSlot: slot, Keys: []common.Hash{keyHash(1)}, Value: hashU64(10), EntryType: MappingEntry},
		{Contract: addrA, MappingSlot: slot, Keys: []common.Hash{keyHash(2)}, Value: hashU64(20), EntryType: MappingEntry},
	}))

	// Block 20: map[3]=30, map[1]=15 (overwrite)
	require.NoError(t, s.WriteEntries(20, []DecodedEntry{
		{Contract: addrA, MappingSlot: slot, Keys: []common.Hash{keyHash(3)}, Value: hashU64(30), EntryType: MappingEntry},
		{Contract: addrA, MappingSlot: slot, Keys: []common.Hash{keyHash(1)}, Value: hashU64(15), EntryType: MappingEntry},
	}))

	// Also add an array entry at slot 2
	arrSlot := slotHash(2)
	require.NoError(t, s.WriteEntries(10, []DecodedEntry{
		{Contract: addrA, MappingSlot: arrSlot, Value: hashU64(99), EntryType: ArrayEntry, ArrayIndex: 0},
	}))

	// Also add entries for a second mapping at slot 1
	slot1 := slotHash(1)
	require.NoError(t, s.WriteEntries(10, []DecodedEntry{
		{Contract: addrA, MappingSlot: slot1, Keys: []common.Hash{keyHash(100)}, Value: hashU64(999), EntryType: MappingEntry},
	}))

	rpc := NewRPCHandler(s, cfg)
	return s, rpc
}

// ===========================================================================
// S-RPC: RPC Methods
// ===========================================================================

func TestS_RPC_01_EnumerateMappingKeysReturnsAllKeys(t *testing.T) {
	_, rpc := rpcSetup(t, enabledFullConfig())

	slot := slotHash(0)
	keys, err := rpc.EnumerateMappingKeys(addrA, slot, nil) // nil = latest
	require.NoError(t, err)
	require.Len(t, keys, 3, "should return keys 1, 2, 3")

	keySet := make(map[common.Hash]bool)
	for _, k := range keys {
		keySet[k] = true
	}
	require.True(t, keySet[keyHash(1)])
	require.True(t, keySet[keyHash(2)])
	require.True(t, keySet[keyHash(3)])
}

func TestS_RPC_02_EnumerateMappingKeysAtHistoricalBlock(t *testing.T) {
	_, rpc := rpcSetup(t, enabledFullConfig())

	slot := slotHash(0)
	blk := uint64(15)
	keys, err := rpc.EnumerateMappingKeys(addrA, slot, &blk)
	require.NoError(t, err)
	require.Len(t, keys, 2, "at block 15, only keys 1 and 2 should exist (key 3 added at block 20)")
}

func TestS_RPC_03_EnumerateMappingKeysForNonTrackedContract(t *testing.T) {
	cfg := whitelistConfig(addrA) // only A is tracked
	_, rpc := rpcSetup(t, cfg)

	slot := slotHash(0)
	keys, err := rpc.EnumerateMappingKeys(addrB, slot, nil)
	// Either returns empty or an error — both are acceptable
	if err == nil {
		require.Empty(t, keys, "non-tracked contract should return empty keys")
	}
	// If err != nil, that's also acceptable (indicates contract not tracked)
}

func TestS_RPC_04_GetDecodedStorageReturnsFull(t *testing.T) {
	_, rpc := rpcSetup(t, enabledFullConfig())

	storage, err := rpc.GetDecodedStorage(addrA, nil)
	require.NoError(t, err)
	require.NotNil(t, storage, "full decoded storage must not be nil")

	// Should contain entries for slot 0 (mapping), slot 1 (mapping), slot 2 (array)
	require.GreaterOrEqual(t, len(storage), 2,
		"decoded storage should be grouped by mapping/array slot")

	slot0Entries := storage[slotHash(0)]
	require.NotEmpty(t, slot0Entries, "slot 0 should have mapping entries")
}

func TestS_RPC_05_GetDecodedStorageAtHistoricalBlock(t *testing.T) {
	_, rpc := rpcSetup(t, enabledFullConfig())

	blk := uint64(15)
	storage, err := rpc.GetDecodedStorage(addrA, &blk)
	require.NoError(t, err)
	require.NotNil(t, storage)

	// At block 15, slot 0 should have keys 1 and 2 (not 3)
	slot0Entries := storage[slotHash(0)]
	require.Len(t, slot0Entries, 2, "at block 15, only 2 entries in slot 0")
}

func TestS_RPC_06_GetMappingValueReturnsSpecificValue(t *testing.T) {
	_, rpc := rpcSetup(t, enabledFullConfig())

	slot := slotHash(0)
	val, err := rpc.GetMappingValue(addrA, slot, keyHash(1), nil) // latest
	require.NoError(t, err)
	require.Equal(t, hashU64(15), val, "map[1] was overwritten to 15 at block 20")
}

func TestS_RPC_07_GetMappingValueForNonExistentKey(t *testing.T) {
	_, rpc := rpcSetup(t, enabledFullConfig())

	slot := slotHash(0)
	val, err := rpc.GetMappingValue(addrA, slot, keyHash(99), nil)
	require.NoError(t, err)
	require.Equal(t, common.Hash{}, val, "non-existent key should return zero value")
}

func TestS_RPC_08_GetMappingValueAtHistoricalBlock(t *testing.T) {
	_, rpc := rpcSetup(t, enabledFullConfig())

	slot := slotHash(0)
	blk := uint64(15)
	val, err := rpc.GetMappingValue(addrA, slot, keyHash(1), &blk)
	require.NoError(t, err)
	require.Equal(t, hashU64(10), val,
		"at block 15, map[1] should still be 10 (overwritten at block 20)")
}

func TestS_RPC_09_DefaultsToLatestBlock(t *testing.T) {
	_, rpc := rpcSetup(t, enabledFullConfig())

	slot := slotHash(0)

	// Calling with nil blockNum should return latest state
	val, err := rpc.GetMappingValue(addrA, slot, keyHash(1), nil)
	require.NoError(t, err)
	require.Equal(t, hashU64(15), val, "nil blockNum should default to latest")

	keys, err := rpc.EnumerateMappingKeys(addrA, slot, nil)
	require.NoError(t, err)
	require.Len(t, keys, 3, "nil blockNum should enumerate latest keys")
}
