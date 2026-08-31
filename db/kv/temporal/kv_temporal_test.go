package temporal

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/execfinality"
	"github.com/erigontech/erigon/node/ethconfig"
)

var unboundedFinalityCtx = execfinality.NewContext(^uint64(0), ^uint64(0), 0, false)

// TestTemporalTx_PinsBlockFilesView: with block snapshots wired at construction,
// every temporal tx pins its own block-files view (the peer of aggtx); with none
// wired, block reads keep their own view.
func TestTemporalTx_PinsBlockFilesView(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	newDB := func(withBlocks bool) *DB {
		mdbxDb := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
		dirs := datadir.New(t.TempDir())
		agg := state.NewTest(dirs).StepSize(1).MustOpen(ctx, mdbxDb)
		t.Cleanup(agg.Close)

		var blockSnaps *blocksnapshots.RoSnapshots
		if withBlocks {
			cfg := ethconfig.Defaults.Snapshot
			cfg.ChainName = networkname.Mainnet
			blockSnaps = blocksnapshots.NewRoSnapshots(cfg, dirs.Snap, log.New())
			t.Cleanup(blockSnaps.Close)
		}

		db, err := New(mdbxDb, agg, blockSnaps)
		require.NoError(t, err)
		t.Cleanup(db.Close)
		return db
	}

	roTx, err := newDB(false).BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	require.Nil(t, roTx.(*Tx).blocktx)

	roTx2, err := newDB(true).BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx2.Rollback()
	require.NotNil(t, roTx2.(*Tx).blocktx)
}

// DomainVisibleEnd's memo serves repeat readers lock-free while first loads
// run under the memo mutex. Fresh txs each round make the two paths
// interleave across goroutines; results must stay stable (run with -race).
func TestTemporalTx_DomainVisibleEndConcurrent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	mdbxDb := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	dirs := datadir.New(t.TempDir())
	agg := state.NewTest(dirs).StepSize(1).MustOpen(ctx, mdbxDb)
	defer agg.Close()
	temporalDb, err := New(mdbxDb, agg, nil)
	require.NoError(t, err)
	defer temporalDb.Close()

	acc := common.HexToAddress("0x1234567890123456789012345678901234567890")
	slot := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	storageK := append(append([]byte{}, acc[:]...), slot[:]...)

	rwTtx, err := temporalDb.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTtx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, rwTtx, log.Root())
	require.NoError(t, err)
	defer sd.Close()
	require.NoError(t, sd.DomainPut(kv.StorageDomain, rwTtx, storageK, []byte{1}, 1, nil))
	require.NoError(t, sd.Flush(ctx, rwTtx))
	require.NoError(t, rwTtx.Commit())

	var expectedEnd [kv.DomainLen]uint64
	var expectedOk [kv.DomainLen]bool
	baseTtx, err := temporalDb.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer baseTtx.Rollback()
	for d := range kv.DomainLen {
		expectedEnd[d], expectedOk[d] = baseTtx.Debug().DomainVisibleEnd(d)
	}
	baseTtx.Rollback()
	require.Equal(t, uint64(2), expectedEnd[kv.StorageDomain])
	require.True(t, expectedOk[kv.StorageDomain])

	for range 25 {
		require.NoError(t, temporalDb.ViewTemporal(ctx, func(roTtx kv.TemporalTx) error {
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(func() {
					for range 4 {
						for d := range kv.DomainLen {
							end, ok := roTtx.Debug().DomainVisibleEnd(d)
							if end != expectedEnd[d] || ok != expectedOk[d] {
								t.Errorf("domain %v: got (%d, %t), want (%d, %t)", d, end, ok, expectedEnd[d], expectedOk[d])
							}
						}
					}
				})
			}
			wg.Wait()
			return nil
		}))
	}
}

// A read-only temporal tx memoizes DomainVisibleEnd, while
// ForceReopenUnderlyingFilesTx swaps in a fresh files view that can extend the
// frontier — the memo must be re-derived after the swap.
func TestTemporalTx_ForceReopenRefreshesDomainVisibleEnd(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	mdbxDb := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	dirs := datadir.New(t.TempDir())
	agg := state.NewTest(dirs).StepSize(1).MustOpen(ctx, mdbxDb)
	defer agg.Close()
	temporalDb, err := New(mdbxDb, agg, nil)
	require.NoError(t, err)
	defer temporalDb.Close()

	acc := common.HexToAddress("0x1234567890123456789012345678901234567890")
	slot := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	storageK := append(append([]byte{}, acc[:]...), slot[:]...)

	rwTtx1, err := temporalDb.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTtx1.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, rwTtx1, log.Root())
	require.NoError(t, err)
	defer sd.Close()
	require.NoError(t, sd.DomainPut(kv.StorageDomain, rwTtx1, storageK, []byte{1}, 1, nil))
	require.NoError(t, sd.Flush(ctx, rwTtx1))
	require.NoError(t, rwTtx1.Commit())

	roTtx, err := temporalDb.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTtx.Rollback()
	end, ok := roTtx.Debug().DomainVisibleEnd(kv.StorageDomain)
	require.True(t, ok)
	require.Equal(t, uint64(2), end)

	// Write past the RO tx's MVCC view and move the data into files, which are
	// visible regardless of the DB read view.
	for txNum := uint64(2); txNum <= 3; txNum++ {
		func() {
			rwTtx, err := temporalDb.BeginTemporalRw(ctx)
			require.NoError(t, err)
			defer rwTtx.Rollback()
			require.NoError(t, sd.DomainPut(kv.StorageDomain, rwTtx, storageK, []byte{byte(txNum)}, txNum, nil))
			require.NoError(t, sd.Flush(ctx, rwTtx))
			require.NoError(t, rwTtx.Commit())
		}()
	}
	require.NoError(t, agg.BuildFiles(3, unboundedFinalityCtx))

	freshRoTtx, err := temporalDb.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer freshRoTtx.Rollback()
	filesEnd := freshRoTtx.Debug().TxNumsInFiles(kv.StorageDomain)
	require.Greater(t, filesEnd, uint64(2), "the new files must extend past the memoized frontier")

	end, ok = roTtx.Debug().DomainVisibleEnd(kv.StorageDomain)
	require.True(t, ok)
	require.Equal(t, uint64(2), end, "the pinned files view cannot see the new files before reopen")

	roTtx.(*Tx).ForceReopenUnderlyingFilesTx()
	end, ok = roTtx.Debug().DomainVisibleEnd(kv.StorageDomain)
	require.True(t, ok)
	require.Equal(t, filesEnd, end, "the frontier must reflect the fresh files view after reopen")
}

func TestTemporalTx_RangeAsOf_StorageDomain(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	mdbxDb := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	dirs := datadir.New(t.TempDir())
	stepSize := uint64(1)
	agg := state.NewTest(dirs).StepSize(stepSize).MustOpen(ctx, mdbxDb)
	defer agg.Close()
	temporalDb, err := New(mdbxDb, agg, nil)
	require.NoError(t, err)
	defer temporalDb.Close()

	// empty range when nothing has been written yet
	acc1 := common.HexToAddress("0x1234567890123456789012345678901234567890")
	acc1slot1 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	storageK1 := append(append([]byte{}, acc1[:]...), acc1slot1[:]...)
	nextSubTree, ok := kv.NextSubtree(acc1[:])
	require.True(t, ok)

	// write storage at txn num 1, update it at txn num 2, then delete it at txn num 3, then write to it again
	// txn num 1
	rwTtx1, err := temporalDb.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTtx1.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, rwTtx1, log.Root())
	require.NoError(t, err)
	defer sd.Close()

	err = sd.DomainPut(kv.StorageDomain, rwTtx1, storageK1, []byte{1}, 1, nil)
	require.NoError(t, err)
	err = sd.Flush(ctx, rwTtx1)
	require.NoError(t, err)
	err = rwTtx1.Commit()
	require.NoError(t, err)
	// txn num 2
	rwTtx2, err := temporalDb.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTtx2.Rollback()
	err = sd.DomainPut(kv.StorageDomain, rwTtx2, storageK1, []byte{2}, 2, nil)
	require.NoError(t, err)
	err = sd.Flush(ctx, rwTtx2)
	require.NoError(t, err)
	err = rwTtx2.Commit()
	require.NoError(t, err)
	// txn num 3
	rwTtx3, err := temporalDb.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTtx3.Rollback()
	err = sd.DomainDelPrefix(kv.StorageDomain, rwTtx3, acc1[:], 3)
	require.NoError(t, err)
	err = sd.Flush(ctx, rwTtx3)
	require.NoError(t, err)
	err = rwTtx3.Commit()
	require.NoError(t, err)
	// txn num 4
	rwTtx4, err := temporalDb.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTtx4.Rollback()

	err = sd.DomainPut(kv.StorageDomain, rwTtx4, storageK1, []byte{3}, 4, nil)
	require.NoError(t, err)
	err = sd.Flush(ctx, rwTtx4)
	require.NoError(t, err)
	err = rwTtx4.Commit()
	require.NoError(t, err)

	// empty value at txn 0
	roTtx1, err := temporalDb.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTtx1.Rollback()
	it1, err := roTtx1.RangeAsOf(kv.StorageDomain, acc1[:], nextSubTree, 1, order.Asc, kv.Unlim)
	require.NoError(t, err)
	defer it1.Close()

	require.True(t, it1.HasNext())
	k, v, err := it1.Next()
	require.NoError(t, err)
	require.Equal(t, append(append([]byte{}, acc1[:]...), acc1slot1[:]...), k)
	require.Len(t, v, 0)
	require.False(t, it1.HasNext())

	// value 1 at txn num 1
	it2, err := roTtx1.RangeAsOf(kv.StorageDomain, acc1[:], nextSubTree, 2, order.Asc, kv.Unlim)
	require.NoError(t, err)
	defer it2.Close()
	require.True(t, it2.HasNext())
	k, v, err = it2.Next()
	require.NoError(t, err)
	require.Equal(t, append(append([]byte{}, acc1[:]...), acc1slot1[:]...), k)
	require.Equal(t, []byte{1}, v)
	require.False(t, it2.HasNext())

	// value 2 at txn num 2
	it3, err := roTtx1.RangeAsOf(kv.StorageDomain, acc1[:], nextSubTree, 3, order.Asc, kv.Unlim)
	require.NoError(t, err)
	defer it3.Close()
	require.True(t, it3.HasNext())
	k, v, err = it3.Next()
	require.NoError(t, err)
	require.Equal(t, append(append([]byte{}, acc1[:]...), acc1slot1[:]...), k)
	require.Equal(t, []byte{2}, v)
	require.False(t, it3.HasNext())

	// empty value at txn num 3
	it4, err := roTtx1.RangeAsOf(kv.StorageDomain, acc1[:], nextSubTree, 4, order.Asc, kv.Unlim)
	require.NoError(t, err)
	defer it4.Close()
	require.True(t, it4.HasNext())
	k, v, err = it4.Next()
	require.NoError(t, err)
	require.Equal(t, append(append([]byte{}, acc1[:]...), acc1slot1[:]...), k)
	require.Len(t, v, 0)
	require.False(t, it4.HasNext())

	// value 3 at txn num 4 - note under the hood this will use latest vals instead of historical
	it5, err := roTtx1.RangeAsOf(kv.StorageDomain, acc1[:], nextSubTree, 5, order.Asc, kv.Unlim)
	require.NoError(t, err)
	defer it5.Close()
	require.True(t, it5.HasNext())
	k, v, err = it5.Next()
	require.NoError(t, err)
	require.Equal(t, append(append([]byte{}, acc1[:]...), acc1slot1[:]...), k)
	require.Equal(t, []byte{3}, v)
	require.False(t, it5.HasNext())
}
