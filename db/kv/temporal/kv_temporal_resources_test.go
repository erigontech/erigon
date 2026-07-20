package temporal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/state"
)

// Iterators are no longer registered for tx-lifetime cleanup, so the layers
// underneath have to do it: abandoning many of them must still leave the tx
// able to roll back cleanly, without mdbx objecting to open cursors at abort.
func TestTemporalTx_AbandonedIteratorsRollbackCleanly(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	mdbxDb := memdb.NewTestDB(t, dbcfg.ChainDB)
	dirs := datadir.New(t.TempDir())
	agg := state.NewTest(dirs).StepSize(1).MustOpen(ctx, mdbxDb)
	defer agg.Close()

	temporalDb, err := New(mdbxDb, agg, nil)
	require.NoError(t, err)
	defer temporalDb.Close()

	ttx, err := temporalDb.BeginTemporalRo(ctx)
	require.NoError(t, err)
	t.Cleanup(ttx.Rollback)

	addr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	to, _ := kv.NextSubtree(addr[:])
	for range 100 {
		_, err := ttx.RangeAsOf(kv.StorageDomain, addr[:], to, 0, order.Asc, kv.Unlim)
		require.NoError(t, err)
		_, err = ttx.HistoryRange(kv.StorageDomain, 0, -1, order.Asc, kv.Unlim)
		require.NoError(t, err)
		_, err = ttx.IndexRange(kv.StorageHistoryIdx, addr[:], 0, -1, order.Asc, kv.Unlim)
		require.NoError(t, err)
	}

	require.NotPanics(t, ttx.Rollback)
	// Re-entrant by design, and the second pass must not touch a released view.
	require.NotPanics(t, ttx.Rollback)

	// The db is still usable, so nothing was left pinned by the abandoned iterators.
	ttx2, err := temporalDb.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer ttx2.Rollback()
}
