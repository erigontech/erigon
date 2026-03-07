package exec

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	arbtypes "github.com/erigontech/erigon/arb/chain/types"
	"github.com/erigontech/erigon/arb/ethdb/wasmdb"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
)

func TestSetArbitrumWasmDB_Arbitrum(t *testing.T) {
	arbConfig := &chain.Config{
		ArbitrumChainParams: arbtypes.ArbitrumChainParams{EnableArbOS: true},
	}

	ibs := state.New(nil)
	state.NewArbitrum(ibs)

	w := &Worker{
		chainConfig: arbConfig,
		ibs:         ibs,
	}

	db := memdb.NewTestDB(t, dbcfg.ArbWasmDB)
	wdb := wasmdb.WrapDatabaseWithWasm(db, []wasmdb.WasmTarget{wasmdb.TargetAmd64})

	w.SetArbitrumWasmDB(wdb)

	moduleHash := common.HexToHash("0xdeadbeef")
	assert.NotPanics(t, func() {
		asm, err := ibs.ActivatedAsm(wasmdb.TargetAmd64, moduleHash)
		assert.Error(t, err, "should return 'not found' for missing module")
		assert.Nil(t, asm)
	}, "ActivatedAsm should not panic after SetArbitrumWasmDB")
}

func TestSetArbitrumWasmDB_RoundTrip(t *testing.T) {
	arbConfig := &chain.Config{
		ArbitrumChainParams: arbtypes.ArbitrumChainParams{EnableArbOS: true},
	}

	ibs := state.New(nil)
	state.NewArbitrum(ibs)

	w := &Worker{
		chainConfig: arbConfig,
		ibs:         ibs,
	}

	db := memdb.NewTestDB(t, dbcfg.ArbWasmDB)
	wdb := wasmdb.WrapDatabaseWithWasm(db, []wasmdb.WasmTarget{wasmdb.TargetAmd64})

	moduleHash := common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	asmData := []byte("compiled-wasm-code")
	err := wdb.(*wasmdb.WasmDB).WriteActivatedAsm(moduleHash, map[wasmdb.WasmTarget][]byte{
		wasmdb.TargetAmd64: asmData,
	})
	require.NoError(t, err)

	w.SetArbitrumWasmDB(wdb)

	asm, err := ibs.ActivatedAsm(wasmdb.TargetAmd64, moduleHash)
	require.NoError(t, err)
	assert.Equal(t, asmData, asm, "should retrieve the written asm data")
}

func TestSetArbitrumWasmDB_NonArbitrum(t *testing.T) {
	ethConfig := &chain.Config{}

	ibs := state.New(nil)

	w := &Worker{
		chainConfig: ethConfig,
		ibs:         ibs,
	}

	db := memdb.NewTestDB(t, dbcfg.ArbWasmDB)
	wdb := wasmdb.WrapDatabaseWithWasm(db, []wasmdb.WasmTarget{wasmdb.TargetAmd64})

	w.SetArbitrumWasmDB(wdb)

	assert.Panics(t, func() {
		_, _ = ibs.ActivatedAsm(wasmdb.TargetAmd64, common.Hash{})
	}, "ActivatedAsm should panic when wasmDB not set (non-Arbitrum config)")
}

func TestSetArbitrumWasmDB_SurvivesSetReader(t *testing.T) {
	arbConfig := &chain.Config{
		ArbitrumChainParams: arbtypes.ArbitrumChainParams{EnableArbOS: true},
	}

	ibs := state.New(nil)
	state.NewArbitrum(ibs)

	w := &Worker{
		chainConfig: arbConfig,
		ibs:         ibs,
	}

	db := memdb.NewTestDB(t, dbcfg.ArbWasmDB)
	wdb := wasmdb.WrapDatabaseWithWasm(db, []wasmdb.WasmTarget{wasmdb.TargetAmd64})

	moduleHash := common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	asmData := []byte("compiled-wasm-code")
	err := wdb.(*wasmdb.WasmDB).WriteActivatedAsm(moduleHash, map[wasmdb.WasmTarget][]byte{
		wasmdb.TargetAmd64: asmData,
	})
	require.NoError(t, err)

	w.SetArbitrumWasmDB(wdb)

	// SetReader creates a new IBS — wasmDB must survive
	w.SetReader(nil)

	assert.NotPanics(t, func() {
		asm, err := w.ibs.ActivatedAsm(wasmdb.TargetAmd64, moduleHash)
		require.NoError(t, err)
		assert.Equal(t, asmData, asm, "wasmDB should survive SetReader")
	})
}
