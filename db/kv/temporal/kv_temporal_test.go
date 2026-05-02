package temporal

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
)

const temporalCommitGateTimeout = 2 * time.Second

type beginRoFailDB struct {
	kv.RwDB
	err error
}

func (db beginRoFailDB) BeginRo(context.Context) (kv.Tx, error) {
	return nil, db.err
}

func newTemporalCommitGateTestDB(t *testing.T, rwDB kv.RwDB) *DB {
	t.Helper()

	dirs := datadir.New(t.TempDir())
	agg := state.NewTest(dirs).DisableHistory().MustOpen(t.Context(), rwDB)
	t.Cleanup(agg.Close)

	temporalDb, err := New(rwDB, agg)
	require.NoError(t, err)
	t.Cleanup(temporalDb.Close)
	return temporalDb
}

func startTemporalWriter(t *testing.T, ctx context.Context, db *DB, drained bool) (<-chan struct{}, <-chan error, chan struct{}, <-chan struct{}) {
	t.Helper()

	attempting := make(chan struct{})
	opened := make(chan error, 1)
	release := make(chan struct{})
	done := make(chan struct{})

	go func() {
		close(attempting)

		var tx kv.TemporalRwTx
		var err error
		if drained {
			tx, err = db.BeginTemporalRwDrained(ctx)
		} else {
			tx, err = db.BeginTemporalRw(ctx)
		}
		if err != nil {
			opened <- err
			close(done)
			return
		}

		opened <- nil
		<-release
		tx.Rollback()
		close(done)
	}()

	return attempting, opened, release, done
}

func requireTemporalWriterNotOpened(t *testing.T, opened <-chan error) {
	t.Helper()

	select {
	case err := <-opened:
		require.NoError(t, err)
		t.Fatal("temporal writer opened before the reader released the commit gate")
	case <-time.After(100 * time.Millisecond):
	}
}

func requireTemporalWriterOpened(t *testing.T, opened <-chan error) {
	t.Helper()

	select {
	case err := <-opened:
		require.NoError(t, err)
	case <-time.After(temporalCommitGateTimeout):
		t.Fatal("temporal writer did not open")
	}
}

func releaseTemporalWriter(t *testing.T, release chan struct{}, done <-chan struct{}) {
	t.Helper()

	close(release)
	select {
	case <-done:
	case <-time.After(temporalCommitGateTimeout):
		t.Fatal("temporal writer did not close")
	}
}

func waitForTemporalGateUnavailable(t *testing.T, ctx context.Context, db *DB) {
	t.Helper()

	deadline := time.After(temporalCommitGateTimeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		tx, ok, err := db.TryBeginTemporalRo(ctx)
		require.NoError(t, err)
		if !ok {
			require.Nil(t, tx)
			return
		}
		require.NotNil(t, tx)
		tx.Rollback()

		select {
		case <-deadline:
			t.Fatal("temporal commit gate did not become unavailable")
		case <-ticker.C:
		}
	}
}

func TestTemporalCommitGateDrainedWriterWaitsForExistingTemporalReader(t *testing.T) {
	ctx := t.Context()
	temporalDb := newTemporalCommitGateTestDB(t, memdb.NewTestDB(t, dbcfg.ChainDB))

	roTx, err := temporalDb.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	attempting, opened, release, done := startTemporalWriter(t, ctx, temporalDb, true)
	<-attempting
	waitForTemporalGateUnavailable(t, ctx, temporalDb)
	requireTemporalWriterNotOpened(t, opened)

	roTx.Rollback()
	requireTemporalWriterOpened(t, opened)
	releaseTemporalWriter(t, release, done)
}

func TestTemporalCommitGateDrainedWriterReleasesGateAfterOpeningRw(t *testing.T) {
	ctx := t.Context()
	temporalDb := newTemporalCommitGateTestDB(t, memdb.NewTestDB(t, dbcfg.ChainDB))

	rwTx, err := temporalDb.BeginTemporalRwDrained(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	roOpened := make(chan error, 1)
	go func() {
		roTx, err := temporalDb.BeginTemporalRo(ctx)
		if err != nil {
			roOpened <- err
			return
		}
		defer roTx.Rollback()
		roOpened <- nil
	}()

	var roErr error
	select {
	case roErr = <-roOpened:
	case <-time.After(temporalCommitGateTimeout):
		rwTx.Rollback()
		t.Fatal("temporal reader did not open while drained writer transaction was still open")
	}

	rwTx.Rollback()
	require.NoError(t, roErr)
}

func TestTemporalCommitGateTryBeginTemporalRoUnavailableWhileDrainedWriterWaits(t *testing.T) {
	ctx := t.Context()
	temporalDb := newTemporalCommitGateTestDB(t, memdb.NewTestDB(t, dbcfg.ChainDB))

	roTx, err := temporalDb.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	attempting, opened, release, done := startTemporalWriter(t, ctx, temporalDb, true)
	<-attempting
	waitForTemporalGateUnavailable(t, ctx, temporalDb)

	tx, ok, err := temporalDb.TryBeginTemporalRo(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, tx)

	roTx.Rollback()
	requireTemporalWriterOpened(t, opened)
	releaseTemporalWriter(t, release, done)
}

func TestTemporalCommitGateTryViewsUnavailableWhileDrainedWriterWaits(t *testing.T) {
	ctx := t.Context()
	temporalDb := newTemporalCommitGateTestDB(t, memdb.NewTestDB(t, dbcfg.ChainDB))

	roTx, err := temporalDb.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	attempting, opened, release, done := startTemporalWriter(t, ctx, temporalDb, true)
	<-attempting
	waitForTemporalGateUnavailable(t, ctx, temporalDb)

	called := false
	ok, err := temporalDb.TryView(ctx, func(tx kv.Tx) error {
		called = true
		return nil
	})
	require.NoError(t, err)
	require.False(t, ok)
	require.False(t, called)

	called = false
	ok, err = temporalDb.TryViewTemporal(ctx, func(tx kv.TemporalTx) error {
		called = true
		return nil
	})
	require.NoError(t, err)
	require.False(t, ok)
	require.False(t, called)

	roTx.Rollback()
	requireTemporalWriterOpened(t, opened)
	releaseTemporalWriter(t, release, done)
}

func TestTemporalCommitGateBeginTemporalRwDoesNotDrain(t *testing.T) {
	ctx := t.Context()
	temporalDb := newTemporalCommitGateTestDB(t, memdb.NewTestDB(t, dbcfg.ChainDB))

	roTx, err := temporalDb.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	attempting, opened, release, done := startTemporalWriter(t, ctx, temporalDb, false)
	<-attempting
	requireTemporalWriterOpened(t, opened)
	releaseTemporalWriter(t, release, done)
}

func TestTemporalCommitGateReadRollbackIdempotent(t *testing.T) {
	ctx := t.Context()
	temporalDb := newTemporalCommitGateTestDB(t, memdb.NewTestDB(t, dbcfg.ChainDB))

	roTx, err := temporalDb.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	roTx.Rollback()
	roTx.Rollback()

	attempting, opened, release, done := startTemporalWriter(t, ctx, temporalDb, true)
	<-attempting
	requireTemporalWriterOpened(t, opened)
	releaseTemporalWriter(t, release, done)
}

func TestTemporalCommitGateBeginRoErrorReleasesGate(t *testing.T) {
	ctx := t.Context()
	beginRoErr := errors.New("begin ro failed")
	underlying := memdb.NewTestDB(t, dbcfg.ChainDB)
	temporalDb := newTemporalCommitGateTestDB(t, beginRoFailDB{RwDB: underlying, err: beginRoErr})

	beginTx, err := temporalDb.BeginTemporalRo(ctx) //nolint:gocritic
	defer func() {
		if beginTx != nil {
			beginTx.Rollback()
		}
	}()
	require.ErrorIs(t, err, beginRoErr)
	require.Nil(t, beginTx)

	attempting, opened, release, done := startTemporalWriter(t, ctx, temporalDb, true)
	<-attempting
	requireTemporalWriterOpened(t, opened)
	releaseTemporalWriter(t, release, done)

	tryTx, ok, err := temporalDb.TryBeginTemporalRo(ctx)
	require.ErrorIs(t, err, beginRoErr)
	require.False(t, ok)
	require.Nil(t, tryTx)

	attempting, opened, release, done = startTemporalWriter(t, ctx, temporalDb, true)
	<-attempting
	requireTemporalWriterOpened(t, opened)
	releaseTemporalWriter(t, release, done)
}

func TestTemporalTx_HasPrefix_StorageDomain(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	mdbxDb := memdb.NewTestDB(t, dbcfg.ChainDB)
	dirs := datadir.New(t.TempDir())
	stepSize := uint64(1)
	agg := state.NewTest(dirs).StepSize(stepSize).MustOpen(ctx, mdbxDb)
	defer agg.Close()

	temporalDb, err := New(mdbxDb, agg)
	require.NoError(t, err)
	defer temporalDb.Close()

	rwTtx1, err := temporalDb.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTtx1.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, rwTtx1, log.Root())
	require.NoError(t, err)
	defer sd.Close()

	acc1 := common.HexToAddress("0x1234567890123456789012345678901234567890")
	acc1slot1 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	storageK1 := append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...)
	acc2 := common.HexToAddress("0x1234567890123456789012345678901234567891")
	acc2slot2 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002")
	storageK2 := append(append([]byte{}, acc2.Bytes()...), acc2slot2.Bytes()...)

	// --- check 1: non-existing storage ---
	{
		firstKey, firstVal, ok, err := rwTtx1.HasPrefix(kv.StorageDomain, acc1.Bytes())
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, firstKey)
		require.Nil(t, firstVal)
	}

	// --- check 2: storage exists in DB - TemporalTx.HasPrefix should catch this ---
	{
		// write to storage
		err = sd.DomainPut(kv.StorageDomain, rwTtx1, storageK1, []byte{1}, 1, nil)
		require.NoError(t, err)
		err = sd.Flush(ctx, rwTtx1)
		require.NoError(t, err)
		err = rwTtx1.Commit()
		require.NoError(t, err)

		// make sure it is indeed in db using a db tx
		dbRoTx1, err := mdbxDb.BeginRo(ctx)
		require.NoError(t, err)
		defer dbRoTx1.Rollback()
		c1, err := dbRoTx1.CursorDupSort(kv.TblStorageVals)
		require.NoError(t, err)
		defer c1.Close()
		k, v, err := c1.Next()
		require.NoError(t, err)
		require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), k)
		wantValueBytes := make([]byte, 8)                      // 8 bytes for uint64 step num
		binary.BigEndian.PutUint64(wantValueBytes, ^uint64(1)) // step num
		wantValueBytes = append(wantValueBytes, byte(1))       // value we wrote to the storage slot
		require.Equal(t, wantValueBytes, v)
		k, v, err = c1.Next()
		require.NoError(t, err)
		require.Nil(t, k)
		require.Nil(t, v)

		// all good
		// now move on to temporal tx
		roTtx1, err := temporalDb.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTtx1.Rollback()

		// make sure there are no files yet and we are only hitting the DB
		require.Equal(t, uint64(0), roTtx1.Debug().TxNumsInFiles(kv.StorageDomain))

		// finally, verify TemporalTx.HasPrefix returns true
		firstKey, firstVal, ok, err := roTtx1.HasPrefix(kv.StorageDomain, acc1.Bytes())
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), firstKey)
		require.Equal(t, []byte{1}, firstVal)

		// check some other non-existing storages for non-existence after write operation
		firstKey, firstVal, ok, err = roTtx1.HasPrefix(kv.StorageDomain, acc2.Bytes())
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, firstKey)
		require.Nil(t, firstVal)
	}

	// --- check 3: storage exists in files only - TemporalTx.HasPrefix should catch this
	{
		// move data to files and trigger prune (need one more step for prune so write to some other storage)
		rwTtx2, err := temporalDb.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTtx2.Rollback()
		err = sd.DomainPut(kv.StorageDomain, rwTtx2, storageK2, []byte{2}, 2, nil)
		require.NoError(t, err)
		err = sd.Flush(ctx, rwTtx2)
		require.NoError(t, err)
		err = rwTtx2.Commit()
		require.NoError(t, err)

		// build files
		err = agg.BuildFiles(2)
		require.NoError(t, err)
		rwTtx3, err := temporalDb.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTtx3.Rollback()

		// prune
		haveMore, err := rwTtx3.PruneSmallBatches(ctx, time.Minute)
		require.NoError(t, err)
		require.False(t, haveMore)
		err = rwTtx3.Commit()
		require.NoError(t, err)

		// double check acc1 storage data not in the mdbx DB
		dbRoTx2, err := mdbxDb.BeginRo(ctx)
		require.NoError(t, err)
		defer dbRoTx2.Rollback()
		c2, err := dbRoTx2.CursorDupSort(kv.TblStorageVals)
		require.NoError(t, err)
		defer c2.Close()
		k, v, err := c2.Next() // acc2 storage from step 2 will be there
		require.NoError(t, err)
		require.Equal(t, append(append([]byte{}, acc2.Bytes()...), acc2slot2.Bytes()...), k)
		wantValueBytes := make([]byte, 8)                      // 8 bytes for uint64 step num
		binary.BigEndian.PutUint64(wantValueBytes, ^uint64(2)) // step num
		wantValueBytes = append(wantValueBytes, byte(2))       // value we wrote to the storage slot
		require.Equal(t, wantValueBytes, v)
		k, v, err = c2.Next() // acc1 storage from step 1 must not be there
		require.NoError(t, err)
		require.Nil(t, k)
		require.Nil(t, v)

		// double check files for 2 steps have been created
		roTtx2, err := temporalDb.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTtx2.Rollback()
		require.Equal(t, uint64(2), roTtx2.Debug().TxNumsInFiles(kv.StorageDomain))

		// finally, verify TemporalTx.HasPrefix returns true
		firstKey, firstVal, ok, err := roTtx2.HasPrefix(kv.StorageDomain, acc1.Bytes())
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), firstKey)
		require.Equal(t, []byte{1}, firstVal)
	}

	// --- check 4: delete storage - TemporalTx.HasPrefix should catch this and say it does not exist
	{
		rwTtx4, err := temporalDb.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTtx4.Rollback()
		err = sd.DomainDelPrefix(kv.StorageDomain, rwTtx4, acc1.Bytes(), 3)
		require.NoError(t, err)
		err = sd.Flush(ctx, rwTtx4)
		require.NoError(t, err)
		err = rwTtx4.Commit()
		require.NoError(t, err)

		roTtx3, err := temporalDb.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTtx3.Rollback()

		firstKey, firstVal, ok, err := roTtx3.HasPrefix(kv.StorageDomain, acc1.Bytes())
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, firstKey)
		require.Nil(t, firstVal)
	}

	// --- check 5: write to it again after deletion - TemporalTx.HasPrefix should catch
	{
		rwTtx5, err := temporalDb.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTtx5.Rollback()
		err = sd.DomainPut(kv.StorageDomain, rwTtx5, storageK1, []byte{3}, 4, nil)
		require.NoError(t, err)
		err = sd.Flush(ctx, rwTtx5)
		require.NoError(t, err)
		err = rwTtx5.Commit()
		require.NoError(t, err)

		roTtx4, err := temporalDb.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTtx4.Rollback()

		firstKey, firstVal, ok, err := roTtx4.HasPrefix(kv.StorageDomain, acc1.Bytes())
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), firstKey)
		require.Equal(t, []byte{3}, firstVal)
	}
}

func TestTemporalTx_RangeAsOf_StorageDomain(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	mdbxDb := memdb.NewTestDB(t, dbcfg.ChainDB)
	dirs := datadir.New(t.TempDir())
	stepSize := uint64(1)
	agg := state.NewTest(dirs).StepSize(stepSize).MustOpen(ctx, mdbxDb)
	defer agg.Close()
	temporalDb, err := New(mdbxDb, agg)
	require.NoError(t, err)
	defer temporalDb.Close()

	// empty range when nothing has been written yet
	acc1 := common.HexToAddress("0x1234567890123456789012345678901234567890")
	acc1slot1 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	storageK1 := append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...)
	nextSubTree, ok := kv.NextSubtree(acc1.Bytes())
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
	err = sd.DomainDelPrefix(kv.StorageDomain, rwTtx3, acc1.Bytes(), 3)
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
	it1, err := roTtx1.RangeAsOf(kv.StorageDomain, acc1.Bytes(), nextSubTree, 1, order.Asc, kv.Unlim)
	require.NoError(t, err)
	defer it1.Close()

	require.True(t, it1.HasNext())
	k, v, err := it1.Next()
	require.NoError(t, err)
	require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), k)
	require.Len(t, v, 0)
	require.False(t, it1.HasNext())

	// value 1 at txn num 1
	it2, err := roTtx1.RangeAsOf(kv.StorageDomain, acc1.Bytes(), nextSubTree, 2, order.Asc, kv.Unlim)
	require.NoError(t, err)
	defer it2.Close()
	require.True(t, it2.HasNext())
	k, v, err = it2.Next()
	require.NoError(t, err)
	require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), k)
	require.Equal(t, []byte{1}, v)
	require.False(t, it2.HasNext())

	// value 2 at txn num 2
	it3, err := roTtx1.RangeAsOf(kv.StorageDomain, acc1.Bytes(), nextSubTree, 3, order.Asc, kv.Unlim)
	require.NoError(t, err)
	defer it3.Close()
	require.True(t, it3.HasNext())
	k, v, err = it3.Next()
	require.NoError(t, err)
	require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), k)
	require.Equal(t, []byte{2}, v)
	require.False(t, it3.HasNext())

	// empty value at txn num 3
	it4, err := roTtx1.RangeAsOf(kv.StorageDomain, acc1.Bytes(), nextSubTree, 4, order.Asc, kv.Unlim)
	require.NoError(t, err)
	defer it4.Close()
	require.True(t, it4.HasNext())
	k, v, err = it4.Next()
	require.NoError(t, err)
	require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), k)
	require.Len(t, v, 0)
	require.False(t, it4.HasNext())

	// value 3 at txn num 4 - note under the hood this will use latest vals instead of historical
	it5, err := roTtx1.RangeAsOf(kv.StorageDomain, acc1.Bytes(), nextSubTree, 5, order.Asc, kv.Unlim)
	require.NoError(t, err)
	defer it5.Close()
	require.True(t, it5.HasNext())
	k, v, err = it5.Next()
	require.NoError(t, err)
	require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), k)
	require.Equal(t, []byte{3}, v)
	require.False(t, it5.HasNext())
}
