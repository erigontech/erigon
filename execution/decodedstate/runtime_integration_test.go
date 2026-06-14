package decodedstate

import (
	"context"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
	"github.com/erigontech/erigon/execution/vm/runtime"
	"github.com/erigontech/erigon/rpc"
)

var runtimeContractAddr = common.BytesToAddress([]byte("contract"))

type collectorTracer struct {
	collector Collector
}

func newCollectorTracer(c Collector) *tracing.Hooks {
	tr := &collectorTracer{collector: c}
	return &tracing.Hooks{
		OnEnter:  tr.onEnter,
		OnExit:   tr.onExit,
		OnOpcode: tr.onOpcode,
	}
}

func (tr *collectorTracer) onEnter(_ int, typ byte, _ accounts.Address, to accounts.Address, _ bool, _ []byte, _ uint64, _ uint256.Int, _ []byte) {
	isDelegate := vm.OpCode(typ) == vm.DELEGATECALL || vm.OpCode(typ) == vm.CALLCODE
	addr := to.Value()
	tr.collector.OnEnterCall(addr, addr, isDelegate)
}

func (tr *collectorTracer) onExit(_ int, _ []byte, _ uint64, _ error, reverted bool) {
	tr.collector.OnExitCall(reverted)
}

func (tr *collectorTracer) onOpcode(_ uint64, op byte, _ uint64, _ uint64, scope tracing.OpContext, _ []byte, _ int, _ error) {
	addr := scope.Address().Value()
	stack := scope.StackData()
	switch vm.OpCode(op) {
	case vm.KECCAK256:
		if len(stack) < 2 {
			return
		}
		offset := stack[len(stack)-1]
		size := stack[len(stack)-2]
		if !offset.IsUint64() || !size.IsUint64() {
			return
		}
		mem := scope.MemoryData()
		start := offset.Uint64()
		length := size.Uint64()
		end := start + length
		if end < start || end > uint64(len(mem)) {
			return
		}
		input := append([]byte(nil), mem[start:end]...)
		tr.collector.OnKeccak256(addr, input, keccak256(input))
	case vm.SSTORE:
		if len(stack) < 2 {
			return
		}
		tr.collector.OnSStore(addr, stack[len(stack)-1].Bytes32(), stack[len(stack)-2])
	}
}

type decodedStateRPCBridge struct {
	handler RPCHandler
}

func (b *decodedStateRPCBridge) EnumerateMappingKeys(contract common.Address, mappingSlot common.Hash, blockNum uint64) ([]common.Hash, error) {
	return b.handler.EnumerateMappingKeys(contract, mappingSlot, &blockNum)
}

func (b *decodedStateRPCBridge) GetDecodedStorage(contract common.Address, blockNum uint64) (map[common.Hash][]DecodedEntry, error) {
	return b.handler.GetDecodedStorage(contract, &blockNum)
}

func (b *decodedStateRPCBridge) GetMappingValue(contract common.Address, mappingSlot, key common.Hash, blockNum uint64) (common.Hash, error) {
	return b.handler.GetMappingValue(contract, mappingSlot, key, &blockNum)
}

func mappingWriteProgram(slot, key, value common.Hash) []byte {
	return program.New().
		Mstore(key[:], 0).
		Mstore(slot[:], 32).
		Push(64).
		Push(0).
		Op(vm.KECCAK256).
		Push(value).
		Op(vm.SWAP1, vm.SSTORE, vm.STOP).
		Bytes()
}

func executeMappingWriteWithCollector(t *testing.T, key, slot, value common.Hash) []DecodedEntry {
	t.Helper()

	collector := NewCollector(enabledFullConfig())
	_, _, err := runtime.Execute(
		mappingWriteProgram(slot, key, value),
		nil,
		&runtime.Config{
			ChainConfig: chain.AllProtocolChanges,
			Difficulty:  uint256.NewInt(0),
			Origin:      accounts.InternAddress(addrA),
			GasLimit:    1_000_000,
			BlockNumber: 1,
			EVMConfig:   vm.Config{Tracer: newCollectorTracer(collector)},
		},
		t.TempDir(),
	)
	require.NoError(t, err)

	return collector.Entries()
}

func newChainDBStore(t *testing.T) (kv.TemporalRwDB, Store) {
	t.Helper()

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	return db, NewStoreFromDB(db)
}

func TestS_NODE_01_DecodedStateDisabledByDefault(t *testing.T) {
	c := NewCollector(Config{})

	slot := slotHash(0)
	key := keyHash(42)
	loc := mappingLocation(key, slot)

	c.OnKeccak256(addrA, mappingInput(key, slot), loc)
	c.OnSStore(addrA, loc, u256(100))

	require.Empty(t, c.Entries())
}

func TestS_NODE_02_EnabledWithoutModeDefaultsToEmptyWhitelist(t *testing.T) {
	c := NewCollector(Config{Enabled: true})

	slot := slotHash(0)
	key := keyHash(42)
	loc := mappingLocation(key, slot)

	c.OnKeccak256(addrA, mappingInput(key, slot), loc)
	c.OnSStore(addrA, loc, u256(100))

	require.Empty(t, c.Entries())
}

func TestS_NODE_03_DecodedTablesRegisteredInSharedChainDB(t *testing.T) {
	db, store := newChainDBStore(t)
	slot := slotHash(0)
	key := keyHash(7)
	entry := DecodedEntry{
		Contract:    addrA,
		MappingSlot: slot,
		Keys:        []common.Hash{key},
		Value:       hashU64(11),
		EntryType:   MappingEntry,
	}

	require.NoError(t, store.WriteEntries(3, []DecodedEntry{entry}))

	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	latestCursor, err := tx.Cursor(kv.DecodedLatest)
	require.NoError(t, err)
	defer latestCursor.Close()

	historyCursor, err := tx.Cursor(kv.DecodedHistory)
	require.NoError(t, err)
	defer historyCursor.Close()

	latestRaw, err := tx.GetOne(kv.DecodedLatest, encodeStoreKey(makeStoreKey(&entry)))
	require.NoError(t, err)
	require.NotNil(t, latestRaw)
}

func TestS_NODE_06_RealEVMExecutionFeedsCollectorAndStore(t *testing.T) {
	slot := slotHash(0)
	key := keyHash(42)
	value := hashU64(100)

	entries := executeMappingWriteWithCollector(t, key, slot, value)
	require.Len(t, entries, 1)
	require.Equal(t, runtimeContractAddr, entries[0].Contract)
	require.Equal(t, slot, entries[0].MappingSlot)
	require.Equal(t, []common.Hash{key}, entries[0].Keys)
	require.Equal(t, value, entries[0].Value)

	_, store := newChainDBStore(t)
	require.NoError(t, store.WriteEntries(1, entries))

	got, found, err := store.GetLatest(runtimeContractAddr, slot, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, value, got)
}

func TestS_NODE_07_RealEVMExecutionPersistsIntoSharedChainDB(t *testing.T) {
	slot := slotHash(1)
	key := keyHash(9)
	value := hashU64(77)

	entries := executeMappingWriteWithCollector(t, key, slot, value)
	db, store := newChainDBStore(t)
	require.NoError(t, store.WriteEntries(12, entries))

	latestBlock, err := store.LatestBlock()
	require.NoError(t, err)
	require.Equal(t, uint64(12), latestBlock)

	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	raw, err := tx.GetOne(kv.DecodedLatest, encodeStoreKey(makeStoreKey(&entries[0])))
	require.NoError(t, err)
	require.NotNil(t, raw)
}

func TestS_RPC_10_JSONRPCTransportExposesDecodedStateMethods(t *testing.T) {
	_, handler := rpcSetup(t, enabledFullConfig())

	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	defer server.Stop()
	require.NoError(t, server.RegisterName("erigon", &decodedStateRPCBridge{handler: handler}))

	client := rpc.DialInProc(server, log.New())
	defer client.Close()

	slot := slotHash(0)

	var keys []common.Hash
	require.NoError(t, client.Call(&keys, "erigon_enumerateMappingKeys", addrA, slot, uint64(20)))
	require.Len(t, keys, 3)

	var value common.Hash
	require.NoError(t, client.Call(&value, "erigon_getMappingValue", addrA, slot, keyHash(1), uint64(20)))
	require.Equal(t, hashU64(15), value)
}

func TestS_RPC_11_DisabledFeatureReturnsClearRPCError(t *testing.T) {
	_, handler := rpcSetup(t, Config{})

	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	defer server.Stop()
	require.NoError(t, server.RegisterName("erigon", &decodedStateRPCBridge{handler: handler}))

	client := rpc.DialInProc(server, log.New())
	defer client.Close()

	var value common.Hash
	err := client.Call(&value, "erigon_getMappingValue", addrA, slotHash(0), keyHash(1), uint64(20))
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), ErrDisabled.Error()))
}
