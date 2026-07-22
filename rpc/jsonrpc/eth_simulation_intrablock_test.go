package jsonrpc

import (
	"context"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestSimulationIntraBlockHasStorageRAMBatch demonstrates the HasStorage bug in
// simulationIntraBlockStateReader: storage written to the RAM batch by a prior
// simulated block is not visible to HasStorage in subsequent blocks because
// HasStorage only queries RangeAsOf(firstMinTxNum) and never inspects the
// in-memory batch.
//
// With the bug:  HasStorage returns false for a brand-new contract even though
//
//	its storage slot lives in sd.GetMemBatch().
//
// With the fix:  HasStorage checks the RAM batch first (via HasPrefixInRAM or
//
//	IteratePrefix) and returns true.
func TestSimulationIntraBlockHasStorageRAMBatch(t *testing.T) {
	ctx := context.Background()
	logger := log.New()

	// Build an in-memory temporal DB (no files on disk).
	dirs := datadir.New(t.TempDir())
	db := mdbx.New(dbcfg.ChainDB, logger).
		InMem(t, dirs.Chaindata).
		GrowthStep(32 * datasize.MB).
		MapSize(2 * datasize.GB).
		MustOpen()
	t.Cleanup(db.Close)

	agg := state.NewTest(dirs).Logger(logger).MustOpen(t.Context(), db)
	t.Cleanup(agg.Close)
	require.NoError(t, agg.OpenFolder())

	tdb, err := temporal.New(db, agg, nil)
	require.NoError(t, err)
	t.Cleanup(tdb.Close)

	rwTx, err := tdb.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, rwTx, logger)
	require.NoError(t, err)
	defer sd.Close()

	// Contract address whose storage we'll write during "block 1" simulation.
	contractAddr := accounts.InternAddress(common.Address{0xcc, 0x01})
	addrVal := contractAddr.Value() // [20]byte

	// Storage slot 0: key = address (20 bytes) || slot (32 bytes).
	slotKey := accounts.InternKey(common.Hash{}) // slot 0x00...00
	slotVal := slotKey.Value()                   // [32]byte

	storageKey := append(addrVal[:], slotVal[:]...)

	// firstMinTxNum = 0: this is the canonical base state (before any simulated blocks).
	// The contract does NOT exist in the canonical chain (empty DB), so RangeAsOf(0)
	// will find no storage for it.
	const firstMinTxNum = uint64(0)

	// Simulate block 1: write storage[0] = 42 for the contract at txNum=1.
	// After this, the value lives only in sd.GetMemBatch() (the RAM batch).
	require.NoError(t, sd.DomainPut(kv.StorageDomain, rwTx, storageKey, []byte{42}, 1, nil))

	// Verify the value is indeed in the RAM batch (sanity check).
	got, _, ok := sd.GetMemBatch().GetLatest(kv.StorageDomain, storageKey)
	require.True(t, ok, "storage must be in the RAM batch after DomainPut")
	require.Equal(t, []byte{42}, got)

	// Create the reader used for block 2 (reads RAM batch + canonical base at firstMinTxNum).
	reader := newSimulationIntraBlockStateReader(rwTx, sd, firstMinTxNum)

	// HasStorage must return true: the contract has storage in the RAM batch from block 1.
	// BUG: returns false — only RangeAsOf(firstMinTxNum=0) is checked, which finds nothing
	//      because the contract had no storage in the canonical chain at txNum 0.
	// FIX: returns true — RAM batch is checked first and slot 0 = 42 is found.
	hasStorage, err := reader.HasStorage(contractAddr)
	require.NoError(t, err)
	require.True(t, hasStorage,
		"HasStorage must see storage written to the RAM batch by a prior simulated block")
}
