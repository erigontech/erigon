package exec

import (
	"testing"

	"github.com/stretchr/testify/assert"

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
		_, _ = ibs.ActivatedAsm(wasmdb.TargetAmd64, moduleHash)
	}, "ActivatedAsm should not panic after SetArbitrumWasmDB")
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
