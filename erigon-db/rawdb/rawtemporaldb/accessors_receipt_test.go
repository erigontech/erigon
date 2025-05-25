package rawtemporaldb

import (
	"context"
	"testing"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/temporal/temporaltest"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/types"
	"github.com/stretchr/testify/require"
)

func TestAppendReceipt(t *testing.T) {
	dirs, require := datadir.New(t.TempDir()), require.New(t)
	db := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(err)
	defer tx.Rollback()

	ttx := tx
	doms, err := state.NewSharedDomains(ttx, log.New())
	require.NoError(err)
	defer doms.Close()
	doms.SetTxNum(0) // block1
	err = AppendReceipt(doms.AsPutDel(ttx), &types.Receipt{CumulativeGasUsed: 10, FirstLogIndexWithinBlock: 0}, 0, 0)
	require.NoError(err)

	doms.SetTxNum(1) // block1
	err = AppendReceipt(doms.AsPutDel(ttx), &types.Receipt{CumulativeGasUsed: 11, FirstLogIndexWithinBlock: 1}, 0, 1)
	require.NoError(err)

	doms.SetTxNum(2) // block1

	doms.SetTxNum(3) // block2
	err = AppendReceipt(doms.AsPutDel(ttx), &types.Receipt{CumulativeGasUsed: 12, FirstLogIndexWithinBlock: 0}, 0, 3)
	require.NoError(err)

	doms.SetTxNum(4) // block2
	err = AppendReceipt(doms.AsPutDel(ttx), &types.Receipt{CumulativeGasUsed: 14, FirstLogIndexWithinBlock: 4}, 0, 4)
	require.NoError(err)

	doms.SetTxNum(5) // block2

	err = doms.Flush(context.Background(), tx)
	require.NoError(err)

	v, ok, err := ttx.HistorySeek(kv.ReceiptDomain, FirstLogIndexKey, 0)
	require.NoError(err)
	require.True(ok)
	require.Empty(v)

	v, ok, err = ttx.HistorySeek(kv.ReceiptDomain, FirstLogIndexKey, 1)
	require.NoError(err)
	require.True(ok)
	require.Equal(uint64(0), uvarint(v))

	v, ok, err = ttx.HistorySeek(kv.ReceiptDomain, FirstLogIndexKey, 2)
	require.NoError(err)
	require.True(ok)
	require.Equal(uint64(1), uvarint(v))

	v, ok, err = ttx.HistorySeek(kv.ReceiptDomain, FirstLogIndexKey, 3)
	require.NoError(err)
	require.True(ok)
	require.Equal(uint64(1), uvarint(v))

	v, ok, err = ttx.HistorySeek(kv.ReceiptDomain, FirstLogIndexKey, 4)
	require.NoError(err)
	require.True(ok)
	require.Equal(uint64(0), uvarint(v))

	//block1
	cumGasUsed, _, firstLogIndex, err := ReceiptAsOf(ttx, 0)
	require.NoError(err)
	require.Equal(uint32(0), firstLogIndex)
	require.Equal(uint64(0), cumGasUsed)

	cumGasUsed, _, firstLogIndex, err = ReceiptAsOf(ttx, 1)
	require.NoError(err)
	require.Equal(uint32(0), firstLogIndex)
	require.Equal(uint64(10), cumGasUsed)

	cumGasUsed, _, firstLogIndex, err = ReceiptAsOf(ttx, 2)
	require.NoError(err)
	require.Equal(uint32(1), firstLogIndex)
	require.Equal(uint64(11), cumGasUsed)

	//block2
	cumGasUsed, _, firstLogIndex, err = ReceiptAsOf(ttx, 3)
	require.NoError(err)
	require.Equal(uint32(1), firstLogIndex)
	require.Equal(uint64(11), cumGasUsed)

	cumGasUsed, _, firstLogIndex, err = ReceiptAsOf(ttx, 4)
	require.NoError(err)
	require.Equal(uint32(0), firstLogIndex)
	require.Equal(uint64(12), cumGasUsed)

	cumGasUsed, _, firstLogIndex, err = ReceiptAsOf(ttx, 5)
	require.NoError(err)
	require.Equal(uint32(4), firstLogIndex)
	require.Equal(uint64(14), cumGasUsed)

	// reader

}
