package temporal

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
)

func TestHasPrefix(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlTrace, log.StderrHandler))

	mdbxDb := memdb.NewTestDB(t, kv.ChainDB)
	dirs := datadir.New(t.TempDir())
	aggStep := uint64(1)
	agg, err := state.NewAggregator(ctx, dirs, aggStep, mdbxDb, logger)
	require.NoError(t, err)
	t.Cleanup(agg.Close)
	temporalDb, err := New(mdbxDb, agg)
	require.NoError(t, err)
	t.Cleanup(temporalDb.Close)

	rwTtx1, err := temporalDb.BeginTemporalRw(ctx)
	require.NoError(t, err)
	t.Cleanup(rwTtx1.Rollback)
	sd, err := state.NewSharedDomains(rwTtx1, logger)
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	acc1 := common.HexToAddress("0x1234567890123456789012345678901234567890")
	acc1slot1 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")

	// --- check 1: non-existing storage ---
	{
		firstKey, ok, err := rwTtx1.HasPrefix(kv.StorageDomain, acc1.Bytes())
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, firstKey)
	}

	// --- check 2: storage exists in DB - TemporalTx.HasPrefix should catch this ---
	{
		// write to storage
		sd.SetTxNum(1)
		err = sd.DomainPut(kv.StorageDomain, acc1.Bytes(), acc1slot1.Bytes(), []byte{1}, nil, 0)
		require.NoError(t, err)
		err = sd.Flush(ctx, rwTtx1)
		require.NoError(t, err)
		err = rwTtx1.Commit()
		require.NoError(t, err)

		// make sure it is indeed in db using a db tx
		dbRoTx1, err := mdbxDb.BeginRo(ctx)
		require.NoError(t, err)
		t.Cleanup(dbRoTx1.Rollback)
		c1, err := dbRoTx1.CursorDupSort(kv.TblStorageVals)
		require.NoError(t, err)
		t.Cleanup(c1.Close)
		k, v, err := c1.Next()
		require.NoError(t, err)
		require.Equal(t, append(acc1.Bytes(), acc1slot1.Bytes()...), k)
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
		t.Cleanup(roTtx1.Rollback)

		// make sure there are no files yet and we are only hitting the DB
		require.Equal(t, uint64(0), roTtx1.Debug().TxNumsInFiles(kv.StorageDomain))

		// finally, verify TemporalTx.HasPrefix returns true
		firstKey, ok, err := roTtx1.HasPrefix(kv.StorageDomain, acc1.Bytes())
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, append(acc1.Bytes(), acc1slot1.Bytes()...), firstKey)
	}

	// --- check 3: storage exists in files only - TemporalTx.HasPrefix should catch this
	{
		// move data to files and trigger prune (need one more step for prune so write to some other storage)
		acc2 := common.HexToAddress("0x1234567890123456789012345678901234567891")
		acc2slot2 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002")
		rwTtx2, err := temporalDb.BeginTemporalRw(ctx)
		require.NoError(t, err)
		t.Cleanup(rwTtx2.Rollback)
		sd.SetTxNum(2)
		sd.SetTx(rwTtx2)
		err = sd.DomainPut(kv.StorageDomain, acc2.Bytes(), acc2slot2.Bytes(), []byte{2}, nil, 0)
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
		t.Cleanup(rwTtx3.Rollback)

		// prune
		haveMore, err := rwTtx3.Debug().PruneSmallBatches(ctx, time.Minute)
		require.NoError(t, err)
		require.False(t, haveMore)
		err = rwTtx3.Commit()
		require.NoError(t, err)

		// double check acc1 storage data not in the mdbx DB
		dbRoTx2, err := mdbxDb.BeginRo(ctx)
		require.NoError(t, err)
		t.Cleanup(dbRoTx2.Rollback)
		c2, err := dbRoTx2.CursorDupSort(kv.TblStorageVals)
		require.NoError(t, err)
		t.Cleanup(c2.Close)
		k, v, err := c2.Next() // acc2 storage from step 2 will be there
		require.NoError(t, err)
		require.Equal(t, append(acc2.Bytes(), acc2slot2.Bytes()...), k)
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
		t.Cleanup(roTtx2.Rollback)
		require.Equal(t, uint64(2), roTtx2.Debug().TxNumsInFiles(kv.StorageDomain))

		// finally, verify TemporalTx.HasPrefix returns true
		firstKey, ok, err := roTtx2.HasPrefix(kv.StorageDomain, acc1.Bytes())
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, append(acc1.Bytes(), acc1slot1.Bytes()...), firstKey)
	}
}

func TestRangeAsOf(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlTrace, log.StderrHandler))

	mdbxDb := memdb.NewTestDB(t, kv.ChainDB)
	dirs := datadir.New(t.TempDir())
	aggStep := uint64(1)
	agg, err := state.NewAggregator(ctx, dirs, aggStep, mdbxDb, logger)
	require.NoError(t, err)
	t.Cleanup(agg.Close)
	temporalDb, err := New(mdbxDb, agg)
	require.NoError(t, err)
	t.Cleanup(temporalDb.Close)

	roTtx1, err := temporalDb.BeginTemporalRo(ctx)
	require.NoError(t, err)
	t.Cleanup(roTtx1.Rollback)

	// empty range when nothing has been written yet
	acc1 := common.HexToAddress("0x1234567890123456789012345678901234567890")
	acc1slot1 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	it1, err := roTtx1.RangeAsOf(kv.StorageDomain, acc1.Bytes(), nil, 0, order.Asc, -1)
	require.NoError(t, err)
	t.Cleanup(it1.Close)
	require.False(t, it1.HasNext())

	// write storage at txn num 1 and then delete storage at txn num 2
	rwTtx1, err := temporalDb.BeginTemporalRw(ctx)
	require.NoError(t, err)
	t.Cleanup(rwTtx1.Rollback)
	sd, err := state.NewSharedDomains(rwTtx1, logger)
	require.NoError(t, err)
	t.Cleanup(sd.Close)
	sd.SetTxNum(1)
	err = sd.DomainPut(kv.StorageDomain, acc1.Bytes(), acc1slot1.Bytes(), []byte{1}, nil, 0)
	require.NoError(t, err)
	err = sd.Flush(ctx, rwTtx1)
	require.NoError(t, err)
	err = rwTtx1.Commit()
	require.NoError(t, err)
	// tnx num 2
	//rwTtx2, err := temporalDb.BeginTemporalRw(ctx)
	//require.NoError(t, err)
	//t.Cleanup(rwTtx2.Rollback)
	//sd.SetTx(rwTtx2)
	//sd.SetTxNum(2)
	//err = sd.DomainDel(kv.StorageDomain, acc1.Bytes(), acc1slot1.Bytes(), nil, 0)
	//require.NoError(t, err)
	//err = sd.Flush(ctx, rwTtx2)
	//require.NoError(t, err)
	//err = rwTtx2.Commit()
	//require.NoError(t, err)
	//err = agg.BuildFiles(2)
	//require.NoError(t, err)

	// non-empty range at txn num 1
	roTtx2, err := temporalDb.BeginTemporalRo(ctx)
	require.NoError(t, err)
	t.Cleanup(roTtx2.Rollback)
	it2, err := roTtx2.RangeAsOf(kv.StorageDomain, acc1.Bytes(), nil, 2, order.Asc, -1)
	require.NoError(t, err)
	t.Cleanup(it2.Close)
	require.True(t, it2.HasNext())
	k, v, err := it2.Next()
	require.NoError(t, err)
	require.Equal(t, append(acc1.Bytes(), acc1slot1.Bytes()...), k)
	require.Equal(t, []byte{1}, v)

	// empty range at txn num 2
	//it3, err := roTtx2.RangeAsOf(kv.StorageDomain, acc1.Bytes(), nil, 2, order.Asc, -1)
	//require.NoError(t, err)
	//t.Cleanup(it3.Close)
	//require.False(t, it3.HasNext())
}
