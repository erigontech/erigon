package wasmdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func TestOpenArbitrumWasmDBSingleton(t *testing.T) {
	openedArbitrumWasmDB = nil
	t.Cleanup(func() { openedArbitrumWasmDB = nil })

	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db1 := OpenArbitrumWasmDB(ctx, dir)
	require.NotNil(t, db1)

	db2 := OpenArbitrumWasmDB(ctx, dir)
	assert.Same(t, db1.(*WasmDB), db2.(*WasmDB), "OpenArbitrumWasmDB should return same instance")

	cancel()
}

func TestWrapDatabaseWithWasmRoundTrip(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ArbWasmDB)
	wdb := WrapDatabaseWithWasm(db, []WasmTarget{TargetAmd64})

	moduleHash := common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	asmData := []byte("test-asm-data-for-module")

	wdbTyped := wdb.(*WasmDB)
	err := wdbTyped.WriteActivatedAsm(moduleHash, map[WasmTarget][]byte{
		TargetAmd64: asmData,
	})
	require.NoError(t, err)

	asm, err := wdb.ActivatedAsm(TargetAmd64, moduleHash)
	require.NoError(t, err)
	assert.Equal(t, asmData, asm)
}

func TestActivatedAsmMissingModuleHash(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ArbWasmDB)
	wdb := WrapDatabaseWithWasm(db, []WasmTarget{TargetAmd64})

	missingHash := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	asm, err := wdb.ActivatedAsm(TargetAmd64, missingHash)
	assert.Error(t, err)
	assert.Nil(t, asm)
}

func TestWasmDBAccessors(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ArbWasmDB)
	targets := []WasmTarget{TargetAmd64, TargetArm64}
	wdb := WrapDatabaseWithWasm(db, targets)

	assert.NotNil(t, wdb.WasmStore())
	assert.Equal(t, uint32(constantCacheTag), wdb.WasmCacheTag())
	assert.Equal(t, targets, wdb.WasmTargets())
}
