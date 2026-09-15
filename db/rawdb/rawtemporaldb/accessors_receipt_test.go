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
	"github.com/erigontech/erigon/db/version"
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

	const txs uint64 = 4
	for i := range txs {
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

	for i := range txs {
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

// TestFirstLogIndex pins the two things the callers must not re-derive: the
// first txn of a block gets 0 rather than the previous block's tail count, and
// a txNum the receipt domain has no record for is an error, not a silent 0.
func TestFirstLogIndex(t *testing.T) {
	dirs, require := datadir.New(t.TempDir()), require.New(t)
	db := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(err)
	defer tx.Rollback()

	doms, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(err)
	defer doms.Close()

	require.False(rawtemporaldb.ReceiptStoresFirstLogIdx(tx))

	// block1: txn0 at txNum 1 emits 1 log, txn1 at txNum 2 emits 2.
	require.NoError(rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(tx), 1, 10, 0, 1))
	require.NoError(rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(tx), 3, 20, 0, 2))
	// block2: txn0 at txNum 5 emits 2 logs, txn1 at txNum 6 emits none.
	require.NoError(rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(tx), 2, 30, 0, 5))
	require.NoError(rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(tx), 2, 40, 0, 6))
	require.NoError(doms.Flush(t.Context(), tx))

	first, err := rawtemporaldb.FirstLogIndex(tx, 1, 0)
	require.NoError(err)
	require.Equal(uint32(0), first)

	first, err = rawtemporaldb.FirstLogIndex(tx, 2, 1)
	require.NoError(err)
	require.Equal(uint32(1), first)

	first, err = rawtemporaldb.FirstLogIndex(tx, 5, 0)
	require.NoError(err)
	require.Equal(uint32(0), first)

	first, err = rawtemporaldb.FirstLogIndex(tx, 6, 1)
	require.NoError(err)
	require.Equal(uint32(2), first)

	_, err = rawtemporaldb.FirstLogIndex(tx, 1, 1)
	require.Error(err)
}

// TestFirstLogIndexPreV1_1 covers the other shape: a receipt domain older than
// V1_1 keeps each txn's own first log index, so the record read is the txn's own
// and the first txn of a block has one like every other.
func TestFirstLogIndexPreV1_1(t *testing.T) {
	dirs, require := datadir.New(t.TempDir()), require.New(t)

	saved := statecfg.Schema.ReceiptDomain.FileVersion.DataKV
	statecfg.Schema.ReceiptDomain.FileVersion.DataKV = version.V1_0_standart
	t.Cleanup(func() { statecfg.Schema.ReceiptDomain.FileVersion.DataKV = saved })

	db := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(err)
	defer tx.Rollback()

	doms, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(err)
	defer doms.Close()

	require.True(rawtemporaldb.ReceiptStoresFirstLogIdx(tx))

	// txn0 at txNum 1 starts at 0 and emits 2 logs, txn1 at txNum 2 starts at 2.
	require.NoError(rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(tx), 0, 10, 0, 1))
	require.NoError(rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(tx), 2, 20, 0, 2))
	require.NoError(doms.Flush(t.Context(), tx))

	first, err := rawtemporaldb.FirstLogIndex(tx, 1, 0)
	require.NoError(err)
	require.Equal(uint32(0), first)

	first, err = rawtemporaldb.FirstLogIndex(tx, 2, 1)
	require.NoError(err)
	require.Equal(uint32(2), first)

	_, err = rawtemporaldb.FirstLogIndex(tx, 0, 0)
	require.Error(err)
}
