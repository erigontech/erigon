package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/stretchr/testify/require"
)

func TestHistoryRangeWithPrune(t *testing.T) {
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()

	db, h, _ := filledHistory(t, true, logger)
	collateAndMergeHistory(t, db, h, 32, true)

	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	hc := h.BeginFilesRo()
	defer hc.Close()

	var keys, vals []string
	it, err := hc.HistoryRange(14, 31, order.Asc, -1, roTx)
	require.NoError(t, err)

	for it.HasNext() {
		k, v, err := it.Next()
		require.NoError(t, err)
		keys = append(keys, fmt.Sprintf("%x", k))
		vals = append(vals, fmt.Sprintf("%x", v))
	}

	db2, h2, _ := filledHistory(t, true, logger)
	collateAndMergeHistory(t, db2, h2, 32, false)

	roTx2, err := db2.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx2.Rollback()
	hc2 := h2.BeginFilesRo()
	defer hc2.Close()

	var keys2, vals2 []string
	it2, err := hc2.HistoryRange(14, 31, order.Asc, -1, roTx2)
	require.NoError(t, err)

	for it2.HasNext() {
		k, v, err := it2.Next()
		require.NoError(t, err)
		keys2 = append(keys2, fmt.Sprintf("%x", k))
		vals2 = append(vals2, fmt.Sprintf("%x", v))
	}

	require.Equal(t, keys, keys2)
	require.Equal(t, vals, vals2)
}

func TestHistoryAsOfWithPrune(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()

	db, h, _ := filledHistory(t, true, logger)
	collateAndMergeHistory(t, db, h, 200, false)

	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	hc := h.BeginFilesRo()
	defer hc.Close()

	var keys, vals []string

	from, to := hexutil.MustDecode("0x0100000000000009"), hexutil.MustDecode("0x0100000000000014") // 9, 20
	it, err := hc.RangeAsOf(ctx, 14, from, to, order.Asc, -1, roTx)
	require.NoError(t, err)

	for it.HasNext() {
		k, v, err := it.Next()
		require.NoError(t, err)
		keys = append(keys, fmt.Sprintf("%x", k))
		vals = append(vals, fmt.Sprintf("%x", v))
	}

	db2, h2, _ := filledHistory(t, true, logger)
	collateAndMergeHistory(t, db2, h2, 200, true)

	roTx2, err := db2.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx2.Rollback()
	hc2 := h2.BeginFilesRo()
	defer hc2.Close()

	var keys2, vals2 []string
	it2, err := hc2.RangeAsOf(ctx, 14, from, to, order.Asc, -1, roTx2)
	require.NoError(t, err)

	for it2.HasNext() {
		k, v, err := it2.Next()
		require.NoError(t, err)
		keys2 = append(keys2, fmt.Sprintf("%x", k))
		vals2 = append(vals2, fmt.Sprintf("%x", v))
	}

	require.Equal(t, keys, keys2)
	require.Equal(t, vals, vals2)
}

func TestHistoryRange1(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()

	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		require := require.New(t)

		collateAndMergeHistory(t, db, h, txs, true)

		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()
		var keys, vals []string
		ic := h.BeginFilesRo()
		defer ic.Close()

		it, err := ic.HistoryRange(2, 20, order.Asc, -1, tx)
		require.NoError(err)
		for it.HasNext() {
			k, v, err := it.Next()
			require.NoError(err)
			keys = append(keys, fmt.Sprintf("%x", k))
			vals = append(vals, fmt.Sprintf("%x", v))
		}
		require.Equal([]string{
			"0100000000000001",
			"0100000000000002",
			"0100000000000003",
			"0100000000000004",
			"0100000000000005",
			"0100000000000006",
			"0100000000000007",
			"0100000000000008",
			"0100000000000009",
			"010000000000000a",
			"010000000000000b",
			"010000000000000c",
			"010000000000000d",
			"010000000000000e",
			"010000000000000f",
			"0100000000000010",
			"0100000000000011",
			"0100000000000012",
			"0100000000000013"}, keys)
		require.Equal([]string{
			"ff00000000000001",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			""}, vals)

		it, err = ic.HistoryRange(995, 1000, order.Asc, -1, tx)
		require.NoError(err)
		keys, vals = keys[:0], vals[:0]
		for it.HasNext() {
			k, v, err := it.Next()
			require.NoError(err)
			keys = append(keys, fmt.Sprintf("%x", k))
			vals = append(vals, fmt.Sprintf("%x", v))
		}
		require.Equal([]string{
			"0100000000000001",
			"0100000000000002",
			"0100000000000003",
			"0100000000000004",
			"0100000000000005",
			"0100000000000006",
			"0100000000000009",
			"010000000000000c",
			"010000000000001b",
		}, keys)

		require.Equal([]string{
			"ff000000000003e2",
			"ff000000000001f1",
			"ff0000000000014b",
			"ff000000000000f8",
			"ff000000000000c6",
			"ff000000000000a5",
			"ff0000000000006e",
			"ff00000000000052",
			"ff00000000000024"}, vals)

		// no upper bound
		it, err = ic.HistoryRange(995, -1, order.Asc, -1, tx)
		require.NoError(err)
		keys, vals = keys[:0], vals[:0]
		for it.HasNext() {
			k, v, err := it.Next()
			require.NoError(err)
			keys = append(keys, fmt.Sprintf("%x", k))
			vals = append(vals, fmt.Sprintf("%x", v))
		}
		require.Equal([]string{"0100000000000001", "0100000000000002", "0100000000000003", "0100000000000004", "0100000000000005", "0100000000000006", "0100000000000008", "0100000000000009", "010000000000000a", "010000000000000c", "0100000000000014", "0100000000000019", "010000000000001b"}, keys)
		require.Equal([]string{"ff000000000003e2", "ff000000000001f1", "ff0000000000014b", "ff000000000000f8", "ff000000000000c6", "ff000000000000a5", "ff0000000000007c", "ff0000000000006e", "ff00000000000063", "ff00000000000052", "ff00000000000031", "ff00000000000027", "ff00000000000024"}, vals)

		// no upper bound, limit=2
		it, err = ic.HistoryRange(995, -1, order.Asc, 2, tx)
		require.NoError(err)
		keys, vals = keys[:0], vals[:0]
		for it.HasNext() {
			k, v, err := it.Next()
			require.NoError(err)
			keys = append(keys, fmt.Sprintf("%x", k))
			vals = append(vals, fmt.Sprintf("%x", v))
		}
		require.Equal([]string{"0100000000000001", "0100000000000002"}, keys)
		require.Equal([]string{"ff000000000003e2", "ff000000000001f1"}, vals)

		// no lower bound, limit=2
		it, err = ic.HistoryRange(-1, 1000, order.Asc, 2, tx)
		require.NoError(err)
		keys, vals = keys[:0], vals[:0]
		for it.HasNext() {
			k, v, err := it.Next()
			require.NoError(err)
			keys = append(keys, fmt.Sprintf("%x", k))
			vals = append(vals, fmt.Sprintf("%x", v))
		}
		require.Equal([]string{"0100000000000001", "0100000000000002"}, keys)
		require.Equal([]string{"ff000000000003cf", "ff000000000001e7"}, vals)

	}
	t.Run("large_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
	})
	t.Run("small_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, false, logger)
		test(t, h, db, txs)
	})
}

func TestHistoryRange2(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()

	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		roTx, err := db.BeginRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()

		type testCase struct {
			k, v  string
			txNum uint64
		}
		testCases := []testCase{
			{txNum: 0, k: "0100000000000001", v: ""},
			{txNum: 99, k: "00000000000063", v: ""},
			{txNum: 199, k: "00000000000063", v: "d1ce000000000383"},
			{txNum: 900, k: "0100000000000001", v: "ff00000000000383"},
			{txNum: 1000, k: "0100000000000001", v: "ff000000000003e7"},
		}
		var firstKey [8]byte
		binary.BigEndian.PutUint64(firstKey[:], 1)
		firstKey[0] = 1 //mark key to simplify debug

		var keys, vals []string
		t.Run("before merge", func(t *testing.T) {
			hc, require := h.BeginFilesRo(), require.New(t)
			defer hc.Close()

			{ //check IdxRange
				idxIt, err := hc.IdxRange(firstKey[:], -1, -1, order.Asc, -1, roTx)
				require.NoError(err)
				cnt, err := stream.CountU64(idxIt)
				require.NoError(err)
				require.Equal(1000, cnt)

				idxIt, err = hc.IdxRange(firstKey[:], 2, 20, order.Asc, -1, roTx)
				require.NoError(err)
				idxItDesc, err := hc.IdxRange(firstKey[:], 19, 1, order.Desc, -1, roTx)
				require.NoError(err)
				descArr, err := stream.ToArrayU64(idxItDesc)
				require.NoError(err)
				stream.ExpectEqualU64(t, idxIt, stream.ReverseArray(descArr))
			}

			it, err := hc.HistoryRange(2, 20, order.Asc, -1, roTx)
			require.NoError(err)
			for it.HasNext() {
				k, v, err := it.Next()
				require.NoError(err)
				keys = append(keys, fmt.Sprintf("%x", k))
				vals = append(vals, fmt.Sprintf("%x", v))
			}
			require.NoError(err)
			require.Equal([]string{
				"0100000000000001",
				"0100000000000002",
				"0100000000000003",
				"0100000000000004",
				"0100000000000005",
				"0100000000000006",
				"0100000000000007",
				"0100000000000008",
				"0100000000000009",
				"010000000000000a",
				"010000000000000b",
				"010000000000000c",
				"010000000000000d",
				"010000000000000e",
				"010000000000000f",
				"0100000000000010",
				"0100000000000011",
				"0100000000000012",
				"0100000000000013"}, keys)
			require.Equal([]string{
				"ff00000000000001",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				""}, vals)
			keys, vals = keys[:0], vals[:0]

			it, err = hc.HistoryRange(995, 1000, order.Asc, -1, roTx)
			require.NoError(err)
			for it.HasNext() {
				k, v, err := it.Next()
				require.NoError(err)
				keys = append(keys, fmt.Sprintf("%x", k))
				vals = append(vals, fmt.Sprintf("%x", v))
			}
			require.NoError(err)
			require.Equal([]string{
				"0100000000000001",
				"0100000000000002",
				"0100000000000003",
				"0100000000000004",
				"0100000000000005",
				"0100000000000006",
				"0100000000000009",
				"010000000000000c",
				"010000000000001b",
			}, keys)

			require.Equal([]string{
				"ff000000000003e2",
				"ff000000000001f1",
				"ff0000000000014b",
				"ff000000000000f8",
				"ff000000000000c6",
				"ff000000000000a5",
				"ff0000000000006e",
				"ff00000000000052",
				"ff00000000000024"}, vals)

			// single Get test-cases
			tx, err := db.BeginRo(ctx)
			require.NoError(err)
			defer tx.Rollback()

			v, ok, err := hc.HistorySeek(hexutil.MustDecodeHex("0100000000000001"), 900, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal(hexutil.MustDecodeHex("ff00000000000383"), v)
			v, ok, err = hc.HistorySeek(hexutil.MustDecodeHex("0100000000000001"), 0, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal([]byte{}, v)
			v, ok, err = hc.HistorySeek(hexutil.MustDecodeHex("0100000000000001"), 1000, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal(hexutil.MustDecodeHex("ff000000000003e7"), v)
			_ = testCases
		})
		t.Run("after merge", func(t *testing.T) {
			collateAndMergeHistory(t, db, h, txs, true)
			hc, require := h.BeginFilesRo(), require.New(t)
			defer hc.Close()

			keys = keys[:0]
			it, err := hc.HistoryRange(2, 20, order.Asc, -1, roTx)
			require.NoError(err)
			for it.HasNext() {
				k, _, err := it.Next()
				require.NoError(err)
				keys = append(keys, fmt.Sprintf("%x", k))
			}
			require.NoError(err)
			require.Equal([]string{
				"0100000000000001",
				"0100000000000002",
				"0100000000000003",
				"0100000000000004",
				"0100000000000005",
				"0100000000000006",
				"0100000000000007",
				"0100000000000008",
				"0100000000000009",
				"010000000000000a",
				"010000000000000b",
				"010000000000000c",
				"010000000000000d",
				"010000000000000e",
				"010000000000000f",
				"0100000000000010",
				"0100000000000011",
				"0100000000000012",
				"0100000000000013"}, keys)

			// single Get test-cases
			tx, err := db.BeginRo(ctx)
			require.NoError(err)
			defer tx.Rollback()

			v, ok, err := hc.HistorySeek(hexutil.MustDecodeHex("0100000000000001"), 900, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal(hexutil.MustDecodeHex("ff00000000000383"), v)
			v, ok, err = hc.HistorySeek(hexutil.MustDecodeHex("0100000000000001"), 0, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal([]byte{}, v)
			v, ok, err = hc.HistorySeek(hexutil.MustDecodeHex("0100000000000001"), 1000, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal(hexutil.MustDecodeHex("ff000000000003e7"), v)
		})
	}
	t.Run("large_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
	})
	t.Run("small_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, false, logger)
		test(t, h, db, txs)
	})
}

func TestHistoryKeyTrace_SmallVals(t *testing.T) {
	testHistoryKeyTrace(t, false)
}

func TestHistoryKeyTrace_LargeVals(t *testing.T) {
	testHistoryKeyTrace(t, true)
}

func testHistoryKeyTrace(t *testing.T, largeVals bool) {
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()

	db, h, _ := filledHistory(t, largeVals, logger)
	collateAndMergeHistory(t, db, h, 32, true)

	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	hc := h.BeginFilesRo()
	defer hc.Close()

	hc2 := h.BeginFilesRo()
	defer hc2.Close() // need to have different HistoryFilesRo for HistorySeek (because it changes internal state)

	randfn := func(till uint64) uint64 { return 1 + (rand.Uint64() % till) } // [1,till]

	key := randfn(20) // [1-20] random key
	//key := uint64(2)
	t.Logf("using key: %d", key)
	keyBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(keyBytes, key)
	keyBytes[0] = 1
	from, to := uint64(1), uint64(0)
	for from >= to {
		from, to = randfn(100), randfn(1000)
	}

	// from, to := uint64(10), uint64(21)
	t.Logf("from: %d, to: %d", from, to)
	it, err := hc.DebugHistoryKeyTrace(ctx, keyBytes, from, to, roTx)
	require.NoError(t, err)
	defer it.Close()

	i := uint64(math.Ceil(float64(from)/float64(key))) * key
	count := uint64(0)
	for it.HasNext() {
		txNum, val, err := it.Next()
		require.NoError(t, err)

		require.Equal(t, i, txNum)
		sval, ok, err := hc2.HistorySeek(keyBytes, txNum, roTx)
		require.NoError(t, err)
		require.True(t, ok)
		//fmt.Println(hexutil.Encode(val), hexutil.Encode(sval))
		require.Equal(t, sval, val)
		i += key
		count++
	}

	require.Equal(t, (to-1)/key-(from-1)/key, count)
}
