package jsonrpc

import (
	"encoding/json"
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/decodedstate"
	"github.com/erigontech/erigon/rpc"
)

func hashU64(v uint64) common.Hash {
	return common.BigToHash(new(big.Int).SetUint64(v))
}

func newDecodedStateRPCClient(t *testing.T, api *ErigonImpl) *rpc.Client {
	t.Helper()

	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	t.Cleanup(server.Stop)
	require.NoError(t, server.RegisterName("erigon", api))

	client := rpc.DialInProc(server, log.New())
	t.Cleanup(client.Close)
	return client
}

func seedDecodedStateFixture(t *testing.T) (*ErigonImpl, common.Address, common.Hash, []common.Hash, common.Hash) {
	t.Helper()

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	store := decodedstate.NewStoreFromDB(db)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	contract := common.HexToAddress("0x00000000000000000000000000000000000000AA")
	slot := hashU64(7)
	keys := []common.Hash{hashU64(1), hashU64(2), hashU64(3)}
	value := hashU64(111)

	require.NoError(t, store.WriteEntries(11, []decodedstate.DecodedEntry{
		{
			Contract:    contract,
			MappingSlot: slot,
			Keys:        []common.Hash{keys[0]},
			Value:       value,
			EntryType:   decodedstate.MappingEntry,
		},
		{
			Contract:    contract,
			MappingSlot: slot,
			Keys:        []common.Hash{keys[1]},
			Value:       hashU64(222),
			EntryType:   decodedstate.MappingEntry,
		},
		{
			Contract:    contract,
			MappingSlot: slot,
			Keys:        []common.Hash{keys[2]},
			Value:       hashU64(333),
			EntryType:   decodedstate.MappingEntry,
		},
	}))

	return NewErigonAPI(&BaseAPI{}, db, nil), contract, slot, keys, value
}

func TestS_RPC_10_LiveErigonNamespaceDispatchesDecodedStateMethods(t *testing.T) {
	client := newDecodedStateRPCClient(t, NewErigonAPI(&BaseAPI{}, nil, nil))
	block := rpc.BlockNumberOrHashWithNumber(11)
	contract := common.HexToAddress("0x00000000000000000000000000000000000000AA")
	slot := hashU64(7)
	key := hashU64(1)

	tests := []struct {
		name   string
		method string
		call   func() error
	}{
		{
			name:   "enumerateMappingKeys",
			method: "erigon_enumerateMappingKeys",
			call: func() error {
				var keys []common.Hash
				return client.Call(&keys, "erigon_enumerateMappingKeys", contract, slot, block)
			},
		},
		{
			name:   "getDecodedStorage",
			method: "erigon_getDecodedStorage",
			call: func() error {
				var storage map[string]any
				return client.Call(&storage, "erigon_getDecodedStorage", contract, block)
			},
		},
		{
			name:   "getMappingValue",
			method: "erigon_getMappingValue",
			call: func() error {
				var value common.Hash
				return client.Call(&value, "erigon_getMappingValue", contract, slot, key, block)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			require.Error(t, err)
			require.ErrorContains(t, err, "decoded state is not yet implemented")
			require.Falsef(t, strings.Contains(strings.ToLower(err.Error()), "method not found"), "%s must dispatch through the live erigon namespace", tc.method)
		})
	}
}

func TestS_RPC_01_LiveErigonEnumerateMappingKeysReturnsAllKeys(t *testing.T) {
	api, contract, slot, expectedKeys, _ := seedDecodedStateFixture(t)
	client := newDecodedStateRPCClient(t, api)

	var keys []common.Hash
	err := client.Call(&keys, "erigon_enumerateMappingKeys", contract, slot, rpc.BlockNumberOrHashWithNumber(11))
	require.NoError(t, err, "live erigon namespace must enumerate decoded mapping keys from shared chain state")
	require.ElementsMatch(t, expectedKeys, keys)
}

func TestS_RPC_04_LiveErigonGetDecodedStorageReturnsStructuredState(t *testing.T) {
	api, contract, slot, expectedKeys, expectedValue := seedDecodedStateFixture(t)
	client := newDecodedStateRPCClient(t, api)

	var storage map[string]any
	err := client.Call(&storage, "erigon_getDecodedStorage", contract, rpc.BlockNumberOrHashWithNumber(11))
	require.NoError(t, err, "live erigon namespace must return structured decoded storage from shared chain state")
	require.Contains(t, storage, slot.String(), "decoded storage must be grouped by original mapping slot")

	slotEntries, err := json.Marshal(storage[slot.String()])
	require.NoError(t, err)
	require.NotEmpty(t, slotEntries)
	require.Contains(t, string(slotEntries), expectedKeys[0].String())
	require.Contains(t, string(slotEntries), expectedValue.String())
}

func TestS_RPC_06_LiveErigonGetMappingValueReturnsSpecificValue(t *testing.T) {
	api, contract, slot, keys, expectedValue := seedDecodedStateFixture(t)
	client := newDecodedStateRPCClient(t, api)

	var value common.Hash
	err := client.Call(&value, "erigon_getMappingValue", contract, slot, keys[0], rpc.BlockNumberOrHashWithNumber(11))
	require.NoError(t, err, "live erigon namespace must read decoded mapping values from shared chain state")
	require.Equal(t, expectedValue, value)
}
