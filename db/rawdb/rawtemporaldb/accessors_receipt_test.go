package rawtemporaldb_test

import (
	"encoding/binary"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

func TestAppendReceiptMetadata(t *testing.T) {
	dirs, require := datadir.New(t.TempDir()), require.New(t)
	db := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(err)
	defer tx.Rollback()

	ttx := tx
	doms, err := execctx.NewSharedDomains(t.Context(), ttx, log.New())
	require.NoError(err)
	defer doms.Close()

	err = rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(ttx), 1, 10, 0, 0) // 1 log
	require.NoError(err)

	err = rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(ttx), 1, 11, 0, 1) // 0 log
	require.NoError(err)

	err = rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(ttx), 4, 12, 0, 3) // 3 logs
	require.NoError(err)

	err = rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(ttx), 4, 14, 0, 4) // 0 log
	require.NoError(err)

	err = doms.Flush(t.Context(), tx)
	require.NoError(err)

	v, ok, err := ttx.HistorySeek(kv.ReceiptDomain, rawtemporaldb.LogIndexAfterTxKey, 0)
	require.NoError(err)
	require.True(ok)
	require.Empty(v)

	v, ok, err = ttx.HistorySeek(kv.ReceiptDomain, rawtemporaldb.LogIndexAfterTxKey, 1)
	require.NoError(err)
	require.True(ok)
	require.Equal(uint64(1), uvarint(v))

	v, ok, err = ttx.HistorySeek(kv.ReceiptDomain, rawtemporaldb.LogIndexAfterTxKey, 2)
	require.NoError(err)
	require.True(ok)
	require.Equal(uint64(1), uvarint(v))

	v, ok, err = ttx.HistorySeek(kv.ReceiptDomain, rawtemporaldb.LogIndexAfterTxKey, 3)
	require.NoError(err)
	require.True(ok)
	require.Equal(uint64(1), uvarint(v))

	_, ok, err = ttx.HistorySeek(kv.ReceiptDomain, rawtemporaldb.LogIndexAfterTxKey, 4)
	require.NoError(err)
	require.False(ok)

	_, ok, err = ttx.HistorySeek(kv.ReceiptDomain, rawtemporaldb.LogIndexAfterTxKey, 5)
	require.NoError(err)
	require.False(ok)

	//block1
	cumGasUsed, _, logIdxAfterTx, err := rawtemporaldb.ReceiptAsOf(ttx, 0)
	require.NoError(err)
	require.Equal(uint32(0), logIdxAfterTx)
	require.Equal(uint64(0), cumGasUsed)

	cumGasUsed, _, logIdxAfterTx, err = rawtemporaldb.ReceiptAsOf(ttx, 1)
	require.NoError(err)
	require.Equal(uint32(1), logIdxAfterTx)
	require.Equal(uint64(10), cumGasUsed)

	cumGasUsed, _, logIdxAfterTx, err = rawtemporaldb.ReceiptAsOf(ttx, 2)
	require.NoError(err)
	require.Equal(uint32(1), logIdxAfterTx)
	require.Equal(uint64(11), cumGasUsed)

	//block2
	cumGasUsed, _, logIdxAfterTx, err = rawtemporaldb.ReceiptAsOf(ttx, 3)
	require.NoError(err)
	require.Equal(uint32(1), logIdxAfterTx)
	require.Equal(uint64(11), cumGasUsed)

	cumGasUsed, _, logIdxAfterTx, err = rawtemporaldb.ReceiptAsOf(ttx, 4)
	require.NoError(err)
	require.Equal(uint32(4), logIdxAfterTx)
	require.Equal(uint64(12), cumGasUsed)

	cumGasUsed, _, logIdxAfterTx, err = rawtemporaldb.ReceiptAsOf(ttx, 5)
	require.NoError(err)
	require.Equal(uint32(4), logIdxAfterTx)
	require.Equal(uint64(14), cumGasUsed)

	// reader

}

// One ReceiptWriter reused across transactions hands the same scratch to
// SharedDomains every time; each value must still land distinct.
func TestReceiptWriterReuseAgainstDomains(t *testing.T) {
	dirs, require := datadir.New(t.TempDir()), require.New(t)

	// RCacheDomain ignores writes unless the node opts in.
	savedRCache := statecfg.Schema.RCacheDomain
	statecfg.EnableHistoricalRCache()
	t.Cleanup(func() { statecfg.Schema.RCacheDomain = savedRCache })

	db := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(err)
	defer tx.Rollback()

	doms, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(err)
	defer doms.Close()

	var w rawtemporaldb.ReceiptWriter
	putter := doms.AsPutDel(tx)

	const txs = 4
	for i := uint64(0); i < txs; i++ {
		receipt := &types.Receipt{
			Type:                     types.DynamicFeeTxType,
			Status:                   types.ReceiptStatusSuccessful,
			CumulativeGasUsed:        10 + i,
			GasUsed:                  21000 + i,
			ContractAddress:          common.BigToAddress(new(big.Int).SetUint64(i + 1)),
			TransactionIndex:         uint(i),
			BlockNumber:              uint256.NewInt(1),
			FirstLogIndexWithinBlock: uint32(i),
		}
		require.NoError(w.Append(putter, receipt, i))
		require.NoError(w.AppendMetadata(putter, uint32(i), 10+i, 100+i, i))
	}
	require.NoError(doms.Flush(t.Context(), tx))

	for i := uint64(0); i < txs; i++ {
		cumGasUsed, cumBlobGasUsed, logIdxAfterTx, err := rawtemporaldb.ReceiptAsOf(tx, i+1)
		require.NoError(err)
		require.Equal(10+i, cumGasUsed)
		require.Equal(100+i, cumBlobGasUsed)
		require.Equal(uint32(i), logIdxAfterTx)

		// i < txs-1 resolves out of history (the ETL copy), the last out of
		// latest state (the bytes.Clone copy).
		v, ok, err := tx.GetAsOf(kv.RCacheDomain, rawtemporaldb.ReceiptCacheKey, i+1)
		require.NoError(err)
		require.True(ok)
		var got types.ReceiptForStorage
		require.NoError(rlp.DecodeBytes(v, &got))
		require.Equal(10+i, got.CumulativeGasUsed)
		require.Equal(21000+i, got.GasUsed)
		require.Equal(common.BigToAddress(new(big.Int).SetUint64(i+1)), got.ContractAddress)
		require.Equal(uint(i), got.TransactionIndex)
	}
}

func uvarint(in []byte) (res uint64) {
	res, _ = binary.Uvarint(in)
	return res
}
