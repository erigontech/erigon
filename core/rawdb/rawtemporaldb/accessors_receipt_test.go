package rawtemporaldb

import (
	"context"
	"encoding/binary"
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
	dirs := datadir.New(t.TempDir())
	db, _ := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	doms, err := state.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer doms.Close()
	doms.SetTx(tx)

	doms.SetTxNum(0) // block1
	err = AppendReceipt(doms, &types.Receipt{CumulativeGasUsed: 0, FirstLogIndexWithinBlock: 0}, 0)
	require.NoError(t, err)

	doms.SetTxNum(1) // block1
	err = AppendReceipt(doms, &types.Receipt{CumulativeGasUsed: 1, FirstLogIndexWithinBlock: 1}, 1)
	require.NoError(t, err)

	doms.SetTxNum(2) // block1

	doms.SetTxNum(3) // block2
	err = AppendReceipt(doms, &types.Receipt{CumulativeGasUsed: 0, FirstLogIndexWithinBlock: 0}, 0)
	require.NoError(t, err)

	doms.SetTxNum(4) // block2
	err = AppendReceipt(doms, &types.Receipt{CumulativeGasUsed: 4, FirstLogIndexWithinBlock: 4}, 4)
	require.NoError(t, err)

	doms.SetTxNum(5) // block2

	err = doms.Flush(context.Background(), tx)
	require.NoError(t, err)

	ttx := tx.(kv.TemporalTx)
	v, ok, err := ttx.HistorySeek(kv.ReceiptHistory, CumulativeGasUsedInBlockKey, 0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Empty(t, v)

	v, ok, err = ttx.HistorySeek(kv.ReceiptHistory, CumulativeGasUsedInBlockKey, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(0), binary.BigEndian.Uint64(v))

	v, ok, err = ttx.HistorySeek(kv.ReceiptHistory, CumulativeGasUsedInBlockKey, 2)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(v))

	v, ok, err = ttx.HistorySeek(kv.ReceiptHistory, CumulativeGasUsedInBlockKey, 3)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(v))

	v, ok, err = ttx.HistorySeek(kv.ReceiptHistory, CumulativeGasUsedInBlockKey, 4)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(0), binary.BigEndian.Uint64(v))

	//block1
	v, ok, err = ttx.DomainGetAsOf(kv.ReceiptDomain, CumulativeGasUsedInBlockKey, nil, 0)
	require.NoError(t, err)
	require.False(t, ok)

	v, ok, err = ttx.DomainGetAsOf(kv.ReceiptDomain, CumulativeGasUsedInBlockKey, nil, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(0), binary.BigEndian.Uint64(v))

	v, ok, err = ttx.DomainGetAsOf(kv.ReceiptDomain, CumulativeGasUsedInBlockKey, nil, 2)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(v))

	//block2
	v, ok, err = ttx.DomainGetAsOf(kv.ReceiptDomain, CumulativeGasUsedInBlockKey, nil, 3)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(v))

	v, ok, err = ttx.DomainGetAsOf(kv.ReceiptDomain, CumulativeGasUsedInBlockKey, nil, 4)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(0), binary.BigEndian.Uint64(v))

	v, ok, err = ttx.DomainGetAsOf(kv.ReceiptDomain, CumulativeGasUsedInBlockKey, nil, 5)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(4), binary.BigEndian.Uint64(v))
}
