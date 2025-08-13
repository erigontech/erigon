package rawtemporaldb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state"
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

	doms.SetTxNum(0)                                     // block1
	err = AppendReceipt(doms.AsPutDel(ttx), 1, 10, 0, 0) // 1 log
	require.NoError(err)

	doms.SetTxNum(1)                                     // block1
	err = AppendReceipt(doms.AsPutDel(ttx), 1, 11, 0, 1) // 0 log
	require.NoError(err)

	doms.SetTxNum(2) // block1

	doms.SetTxNum(3)                                     // block2
	err = AppendReceipt(doms.AsPutDel(ttx), 4, 12, 0, 3) // 3 logs
	require.NoError(err)

	doms.SetTxNum(4)                                     // block2
	err = AppendReceipt(doms.AsPutDel(ttx), 4, 14, 0, 4) // 0 log
	require.NoError(err)

	doms.SetTxNum(5) // block2

	err = doms.Flush(context.Background(), tx)
	require.NoError(err)

	v, ok, err := ttx.HistorySeek(kv.ReceiptDomain, LogIndexAfterTxKey, 0)
	require.NoError(err)
	require.True(ok)
	require.Empty(v)

	v, ok, err = ttx.HistorySeek(kv.ReceiptDomain, LogIndexAfterTxKey, 1)
	require.NoError(err)
	require.True(ok)
	require.Equal(uint64(1), uvarint(v))

	v, ok, err = ttx.HistorySeek(kv.ReceiptDomain, LogIndexAfterTxKey, 2)
	require.NoError(err)
	require.True(ok)
	require.Equal(uint64(1), uvarint(v))

	v, ok, err = ttx.HistorySeek(kv.ReceiptDomain, LogIndexAfterTxKey, 3)
	require.NoError(err)
	require.True(ok)
	require.Equal(uint64(1), uvarint(v))

	_, ok, err = ttx.HistorySeek(kv.ReceiptDomain, LogIndexAfterTxKey, 4)
	require.NoError(err)
	require.False(ok)

	_, ok, err = ttx.HistorySeek(kv.ReceiptDomain, LogIndexAfterTxKey, 5)
	require.NoError(err)
	require.False(ok)

	//block1
	cumGasUsed, _, logIdxAfterTx, err := ReceiptAsOf(ttx, 0)
	require.NoError(err)
	require.Equal(uint32(0), logIdxAfterTx)
	require.Equal(uint64(0), cumGasUsed)

	cumGasUsed, _, logIdxAfterTx, err = ReceiptAsOf(ttx, 1)
	require.NoError(err)
	require.Equal(uint32(1), logIdxAfterTx)
	require.Equal(uint64(10), cumGasUsed)

	cumGasUsed, _, logIdxAfterTx, err = ReceiptAsOf(ttx, 2)
	require.NoError(err)
	require.Equal(uint32(1), logIdxAfterTx)
	require.Equal(uint64(11), cumGasUsed)

	//block2
	cumGasUsed, _, logIdxAfterTx, err = ReceiptAsOf(ttx, 3)
	require.NoError(err)
	require.Equal(uint32(1), logIdxAfterTx)
	require.Equal(uint64(11), cumGasUsed)

	cumGasUsed, _, logIdxAfterTx, err = ReceiptAsOf(ttx, 4)
	require.NoError(err)
	require.Equal(uint32(4), logIdxAfterTx)
	require.Equal(uint64(12), cumGasUsed)

	cumGasUsed, _, logIdxAfterTx, err = ReceiptAsOf(ttx, 5)
	require.NoError(err)
	require.Equal(uint32(4), logIdxAfterTx)
	require.Equal(uint64(14), cumGasUsed)

	// reader

}
