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

	// block1
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

	// block2
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

// TestReceiptGasAsOf checks that the gas before a txn is read from the receipt
// of the txn before it, and reported missing where the receipt domain ends
// before that txn instead of read from an earlier block.
func TestReceiptGasAsOf(t *testing.T) {
	dirs, require := datadir.New(t.TempDir()), require.New(t)
	db := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(err)
	defer tx.Rollback()

	doms, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(err)
	defer doms.Close()

	// block1: txn0 at txNum 1, txn1 at txNum 2 with a blob; block2 spans txNums
	// 4 (start) to 7 (end), and the domain holds none of its receipts.
	require.NoError(rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(tx), 0, 10, 0, 1))
	require.NoError(rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(tx), 0, 30, 5, 2))
	require.NoError(doms.Flush(t.Context(), tx))

	for _, tc := range []struct {
		txNum, gas, blobGas uint64
		ok                  bool
	}{
		{0, 0, 0, false},
		{1, 0, 0, false},
		{2, 10, 0, true},
		{3, 30, 5, true},
		{6, 0, 0, false},
	} {
		gas, blobGas, ok, err := rawtemporaldb.ReceiptGasAsOf(tx, tc.txNum)
		require.NoError(err)
		require.Equal(tc.ok, ok, "txNum %d", tc.txNum)
		require.Equal(tc.gas, gas, "txNum %d", tc.txNum)
		require.Equal(tc.blobGas, blobGas, "txNum %d", tc.txNum)
	}
}

// TestReceiptGasAsOfOverlay checks that the gas before a txn is read through
// the block overlay, which holds a block executed but not yet flushed, and
// reported missing past the end of what it holds.
func TestReceiptGasAsOfOverlay(t *testing.T) {
	dirs, require := datadir.New(t.TempDir()), require.New(t)
	db := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(err)
	defer tx.Rollback()

	// block1: txn0 at txNum 1, flushed by its own batch, which would otherwise
	// still answer for it; block2: txn0 at txNum 4 and txn1 at txNum 5, in
	// memory only.
	flushed, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(err)
	require.NoError(rawtemporaldb.AppendReceiptMetadata(flushed.AsPutDel(tx), 0, 10, 0, 1))
	require.NoError(flushed.Flush(t.Context(), tx))
	flushed.Close()

	doms, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(err)
	defer doms.Close()
	require.NoError(rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(tx), 0, 20, 0, 4))
	require.NoError(rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(tx), 0, 30, 0, 5))
	require.NoError(doms.InitBlockOverlay(tx))
	overlay := doms.BlockOverlayTemporalTx(tx)

	// block3: txn0 at txNum 8, not executed, and txn1 at txNum 9, whose gas the
	// batch would answer with txn1 of block2's.
	for _, tc := range []struct {
		txNum, gas uint64
		ok         bool
	}{{5, 20, true}, {6, 30, true}, {9, 0, false}} {
		gas, _, ok, err := rawtemporaldb.ReceiptGasAsOf(overlay, tc.txNum)
		require.NoError(err)
		require.Equal(tc.ok, ok, "txNum %d", tc.txNum)
		require.Equal(tc.gas, gas, "txNum %d", tc.txNum)

		_, _, ok, err = rawtemporaldb.ReceiptGasAsOf(tx, tc.txNum)
		require.NoError(err)
		require.False(ok, "txNum %d without the overlay", tc.txNum)
	}
}

// TestReceiptGasAsOfLostRecord checks that the gas is reported missing where the
// record of txNum-1 is lost and the one before it is not, or a later one is not.
func TestReceiptGasAsOfLostRecord(t *testing.T) {
	for _, tc := range []struct {
		name           string
		flushed, batch [][2]uint64 // txNum, cumulative gas
		txNum          uint64
	}{
		// block2 spans txNums 3 to 8; the overlay holds its txn0 at 4, not its
		// txn1 at 5, before txn2 at 6.
		{"overlay one txn behind", [][2]uint64{{1, 10}}, [][2]uint64{{4, 20}}, 6},
		// block2: txn0 at 4 and txn1 at 5 lost, txn2 at 6 and txn3 at 7 flushed.
		{"flushed past lost receipts", [][2]uint64{{1, 10}, {6, 90}, {7, 120}}, nil, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dirs, require := datadir.New(t.TempDir()), require.New(t)
			db := temporaltest.NewTestDB(t, dirs)
			tx, err := db.BeginTemporalRw(t.Context())
			require.NoError(err)
			defer tx.Rollback()

			flushed, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
			require.NoError(err)
			for _, r := range tc.flushed {
				require.NoError(rawtemporaldb.AppendReceiptMetadata(flushed.AsPutDel(tx), 0, r[1], 0, r[0]))
			}
			require.NoError(flushed.Flush(t.Context(), tx))
			flushed.Close()

			var reader kv.TemporalTx = tx
			if len(tc.batch) > 0 {
				var doms *execctx.SharedDomains
				doms, err = execctx.NewSharedDomains(t.Context(), tx, log.New())
				require.NoError(err)
				defer doms.Close()
				for _, r := range tc.batch {
					require.NoError(rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(tx), 0, r[1], 0, r[0]))
				}
				require.NoError(doms.InitBlockOverlay(tx))
				reader = doms.BlockOverlayTemporalTx(tx)
			}

			_, _, ok, err := rawtemporaldb.ReceiptGasAsOf(reader, tc.txNum)
			require.NoError(err)
			require.False(ok)
		})
	}
}

// TestReceiptGasAsOfOverlayResumedBlock checks that a block overlay resumed
// within a block whose earlier receipts are lost does not prove them by its own:
// the gas before its first txn is reported missing, not read from an earlier
// block.
func TestReceiptGasAsOfOverlayResumedBlock(t *testing.T) {
	dirs, require := datadir.New(t.TempDir()), require.New(t)
	db := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(err)
	defer tx.Rollback()

	// block1: txn0 at txNum 1, flushed; block2: txn0 at txNum 4, executed by an
	// earlier run whose receipt is lost, then txn1 at txNum 5 and txn2 at txNum
	// 6 in memory only.
	flushed, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(err)
	require.NoError(rawtemporaldb.AppendReceiptMetadata(flushed.AsPutDel(tx), 0, 10, 0, 1))
	require.NoError(flushed.Flush(t.Context(), tx))
	flushed.Close()

	doms, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(err)
	defer doms.Close()
	require.NoError(rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(tx), 0, 60, 0, 5))
	require.NoError(rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(tx), 0, 90, 0, 6))
	require.NoError(doms.InitBlockOverlay(tx))
	overlay := doms.BlockOverlayTemporalTx(tx)

	_, _, ok, err := rawtemporaldb.ReceiptGasAsOf(overlay, 5)
	require.NoError(err)
	require.False(ok)

	gas, _, ok, err := rawtemporaldb.ReceiptGasAsOf(overlay, 6)
	require.NoError(err)
	require.True(ok)
	require.Equal(uint64(60), gas)
}

// TestReceiptGasAsOfUnchangedFirstTxn checks the gas before the second txn of a
// block whose first left the cumulative gas unchanged, so unwritten: the second
// txn's own record shows the domain covers the first, and without it the gas is
// reported missing, flushed or in a block overlay batch holding the block before.
func TestReceiptGasAsOfUnchangedFirstTxn(t *testing.T) {
	for _, tc := range []struct {
		name            string
		overlay, second bool
	}{
		{"flushed", false, true},
		{"flushed without txn1", false, false},
		{"overlay", true, true},
		{"overlay without txn1", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dirs, require := datadir.New(t.TempDir()), require.New(t)
			db := temporaltest.NewTestDB(t, dirs)
			tx, err := db.BeginTemporalRw(t.Context())
			require.NoError(err)
			defer tx.Rollback()

			doms, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
			require.NoError(err)
			defer doms.Close()
			// block1: txn0 at txNum 1; block2: txn0 at txNum 4 with the same
			// cumulative gas, and txn1 at txNum 5.
			require.NoError(rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(tx), 0, 21_000, 0, 1))
			require.NoError(rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(tx), 0, 21_000, 0, 4))
			if tc.second {
				require.NoError(rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(tx), 0, 42_000, 0, 5))
			}
			var reader kv.TemporalTx = tx
			if tc.overlay {
				require.NoError(doms.InitBlockOverlay(tx))
				reader = doms.BlockOverlayTemporalTx(tx)
			} else {
				require.NoError(doms.Flush(t.Context(), tx))
			}

			gas, _, ok, err := rawtemporaldb.ReceiptGasAsOf(reader, 5)
			require.NoError(err)
			require.Equal(tc.second, ok)
			if tc.second {
				require.Equal(uint64(21_000), gas)
			}
		})
	}
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
