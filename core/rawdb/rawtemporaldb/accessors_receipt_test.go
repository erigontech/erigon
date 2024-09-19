package rawtemporaldb

import (
	"context"
	"testing"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/temporal/temporaltest"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/stretchr/testify/require"
)

func TestAppendReceipt(t *testing.T) {
	dirs, require := datadir.New(t.TempDir()), require.New(t)
	db, _ := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginRw(context.Background())
	require.NoError(err)
	defer tx.Rollback()

	doms, err := state.NewSharedDomains(tx, log.New())
	require.NoError(err)
	defer doms.Close()
	doms.SetTx(tx)

	doms.SetTxNum(0) // block1
	err = AppendReceipt(doms, &types.Receipt{CumulativeGasUsed: 10, FirstLogIndexWithinBlock: 0}, 0)
	require.NoError(err)

	doms.SetTxNum(1) // block1
	err = AppendReceipt(doms, &types.Receipt{CumulativeGasUsed: 11, FirstLogIndexWithinBlock: 1}, 0)
	require.NoError(err)

	doms.SetTxNum(2) // block1

	doms.SetTxNum(3) // block2
	err = AppendReceipt(doms, &types.Receipt{CumulativeGasUsed: 12, FirstLogIndexWithinBlock: 0}, 0)
	require.NoError(err)

	doms.SetTxNum(4) // block2
	err = AppendReceipt(doms, &types.Receipt{CumulativeGasUsed: 14, FirstLogIndexWithinBlock: 4}, 0)
	require.NoError(err)

	doms.SetTxNum(5) // block2

	err = doms.Flush(context.Background(), tx)
	require.NoError(err)

	ttx := tx.(kv.TemporalTx)
	v, ok, err := ttx.HistorySeek(kv.ReceiptHistory, FirstLogIndexKey, 0)
	require.NoError(err)
	require.True(ok)
	require.Empty(v)

	v, ok, err = ttx.HistorySeek(kv.ReceiptHistory, FirstLogIndexKey, 1)
	require.NoError(err)
	require.True(ok)
	require.Equal(uint64(0), uvarint(v))

	v, ok, err = ttx.HistorySeek(kv.ReceiptHistory, FirstLogIndexKey, 2)
	require.NoError(err)
	require.True(ok)
	require.Equal(uint64(1), uvarint(v))

	v, ok, err = ttx.HistorySeek(kv.ReceiptHistory, FirstLogIndexKey, 3)
	require.NoError(err)
	require.True(ok)
	require.Equal(uint64(1), uvarint(v))

	v, ok, err = ttx.HistorySeek(kv.ReceiptHistory, FirstLogIndexKey, 4)
	require.NoError(err)
	require.True(ok)
	require.Equal(uint64(0), uvarint(v))

	//block1
	cumGasUsed, cumBlobGasUsed, firstLogIndex, err := ReceiptAsOf(ttx, 0)
	require.NoError(err)
	require.Equal(uint64(0), firstLogIndex)
	require.Equal(uint64(0), cumGasUsed)

	cumGasUsed, cumBlobGasUsed, firstLogIndex, err = ReceiptAsOf(ttx, 1)
	require.NoError(err)
	require.Equal(uint64(0), firstLogIndex)
	require.Equal(uint64(10), cumGasUsed)
	_, _ = cumBlobGasUsed, firstLogIndex

	cumGasUsed, cumBlobGasUsed, firstLogIndex, err = ReceiptAsOf(ttx, 2)
	require.NoError(err)
	require.Equal(uint64(1), firstLogIndex)
	require.Equal(uint64(11), cumGasUsed)

	//block2
	cumGasUsed, cumBlobGasUsed, firstLogIndex, err = ReceiptAsOf(ttx, 3)
	require.NoError(err)
	require.Equal(uint64(1), firstLogIndex)
	require.Equal(uint64(11), cumGasUsed)

	cumGasUsed, cumBlobGasUsed, firstLogIndex, err = ReceiptAsOf(ttx, 4)
	require.NoError(err)
	require.Equal(uint64(0), firstLogIndex)
	require.Equal(uint64(12), cumGasUsed)

	cumGasUsed, cumBlobGasUsed, firstLogIndex, err = ReceiptAsOf(ttx, 5)
	require.NoError(err)
	require.Equal(uint64(4), firstLogIndex)
	require.Equal(uint64(14), cumGasUsed)

	// reader

}
