package receipts

import (
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
)

func TestGasUsedBelowReceiptHistory(t *testing.T) {
	saved, assertEnabled := statecfg.Schema.RCacheDomain, dbg.AssertEnabled
	statecfg.EnableHistoricalRCache()
	dbg.AssertEnabled = false
	t.Cleanup(func() { statecfg.Schema.RCacheDomain, dbg.AssertEnabled = saved, assertEnabled })
	_, tx := temporaltest.NewTestTx(t)
	require.NoError(t, rawdbv3.TxNums.Append(tx, 0, 1))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 1, 7))
	doms, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer doms.Close()

	// Block 1 starts at txNum 2. Only the cumulative totals from its third
	// user transaction onwards survive in the receipt history.
	put := doms.AsPutDel(tx)
	require.NoError(t, put.DomainPut(kv.ReceiptDomain, rawtemporaldb.LogIndexAfterTxKey, []byte{1}, 3, nil))
	require.NoError(t, put.DomainPut(kv.ReceiptDomain, rawtemporaldb.CumulativeGasUsedInBlockKey,
		binary.AppendUvarint(nil, 600), 5, binary.AppendUvarint(nil, 300)))
	require.NoError(t, rawtemporaldb.AppendReceiptMetadata(put, 0, 1000, 0, 6))
	require.NoError(t, doms.Flush(t.Context(), tx))
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 1))
	frame := &types.FrameTransaction{}
	header := &types.Header{Number: *uint256.NewInt(1)}
	block := types.NewBlockFromStorage(header.Hash(), header, types.Transactions{frame, frame, frame, frame}, nil, nil, nil)
	g := newTestGenerator(t)

	check := func(t *testing.T, wantFirst uint64) {
		t.Helper()
		first, err := g.GetReceiptGasUsed(t.Context(), tx, header, frame, 0)
		require.NoError(t, err)
		require.Equal(t, wantFirst, first)
		last, err := g.GetReceiptGasUsed(t.Context(), tx, header, frame, 3)
		require.NoError(t, err)
		require.Equal(t, uint64(400), last)
		byIndex, err := g.GetReceiptsGasUsed(t.Context(), tx, block, true)
		require.NoError(t, err)
		if wantFirst == 0 {
			require.Nil(t, byIndex[0])
		} else {
			require.Equal(t, wantFirst, byIndex[0].GasUsed)
		}
		require.Nil(t, byIndex[1])
		require.Equal(t, uint64(400), byIndex[3].GasUsed)
		_, err = g.GetReceiptsGasUsed(t.Context(), tx, block, false)
		require.ErrorIs(t, err, state.ErrPruned)
	}
	t.Run("metadata only", func(t *testing.T) { check(t, 0) })
	t.Run("persisted receipt", func(t *testing.T) {
		receipt := &types.Receipt{Type: types.FrameTxType, Payer: &common.Address{1}, GasUsed: 100, CumulativeGasUsed: 100}
		require.NoError(t, rawdb.WriteReceiptCacheV2(put, receipt, 3))
		require.NoError(t, rawdb.WriteReceiptCacheV2(put, nil, 4))
		require.NoError(t, doms.Flush(t.Context(), tx))
		check(t, 100)

		// A later block also has a receipt at index zero. Its row cannot
		// supply this block's receipt once the older values are pruned.
		encoded, err := rlp.EncodeToBytes((*types.ReceiptForStorage)(receipt))
		require.NoError(t, err)
		require.NoError(t, tx.ClearTable(kv.TblRCacheHistoryVals))
		require.NoError(t, put.DomainPut(kv.RCacheDomain, rawtemporaldb.ReceiptCacheKey, []byte{}, 10, encoded))
		require.NoError(t, doms.Flush(t.Context(), tx))
		check(t, 0)
	})
}

func TestFirstReceiptAtGasHistoryStart(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	require.NoError(t, rawdbv3.TxNums.Append(tx, 0, 1))
	doms, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer doms.Close()

	// The first surviving change is the second transaction. Its previous
	// cumulative total is the entire gas used by the first transaction.
	require.NoError(t, doms.AsPutDel(tx).DomainPut(kv.ReceiptDomain, rawtemporaldb.CumulativeGasUsedInBlockKey,
		binary.AppendUvarint(nil, 300), 4, binary.AppendUvarint(nil, 100)))
	require.NoError(t, doms.Flush(t.Context(), tx))
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 1))
	block := types.NewBlockFromStorage(common.Hash{1}, &types.Header{Number: *uint256.NewInt(1)},
		types.Transactions{&types.FrameTransaction{}, &types.LegacyTx{}}, nil, nil, nil)
	got, err := newTestGenerator(t).GetReceiptsGasUsed(t.Context(), tx, block, false)
	require.NoError(t, err)
	require.Equal(t, types.Receipts{{GasUsed: 100}, {GasUsed: 200}}, got)
}

type receiptReadCounter struct {
	kv.TemporalTx
	reads []uint64
}

func (tx *receiptReadCounter) GetAsOf(domain kv.Domain, key []byte, ts uint64) ([]byte, bool, error) {
	tx.reads = append(tx.reads, ts)
	return tx.TemporalTx.GetAsOf(domain, key, ts)
}

func TestGetReceiptsGasUsedMixedBlock(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	require.NoError(t, rawdbv3.TxNums.Append(tx, 0, 1))
	doms, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer doms.Close()

	frame, ordinary := &types.FrameTransaction{}, &types.LegacyTx{}
	txns := types.Transactions{frame, ordinary, ordinary, frame, frame, ordinary}
	block := types.NewBlockFromStorage(common.Hash{1}, &types.Header{Number: *uint256.NewInt(1)}, txns, nil, nil, nil)
	var cumulative uint64
	for i := range txns {
		cumulative += uint64(i+1) * 100
		require.NoError(t, rawtemporaldb.AppendReceiptMetadata(doms.AsPutDel(tx), 0, cumulative, 0, 3+uint64(i)))
	}
	require.NoError(t, doms.Flush(t.Context(), tx))

	g := newTestGenerator(t)
	t.Run("before execution", func(t *testing.T) {
		got, err := g.GetReceiptsGasUsed(t.Context(), tx, block, true)
		require.NoError(t, err)
		require.Empty(t, got)
	})
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, block.NumberU64()))
	for _, tc := range []struct {
		name       string
		framesOnly bool
		reads      []uint64
		want       types.Receipts
	}{
		{
			name: "frames", framesOnly: true,
			reads: []uint64{4, 6, 7, 8},
			want:  types.Receipts{{GasUsed: 100}, nil, nil, {GasUsed: 400}, {GasUsed: 500}, nil},
		},
		{
			name:  "all",
			reads: []uint64{4, 5, 6, 7, 8, 9},
			want:  types.Receipts{{GasUsed: 100}, {GasUsed: 200}, {GasUsed: 300}, {GasUsed: 400}, {GasUsed: 500}, {GasUsed: 600}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counted := &receiptReadCounter{TemporalTx: tx}
			got, err := g.GetReceiptsGasUsed(t.Context(), counted, block, tc.framesOnly)
			require.NoError(t, err)
			require.Equal(t, tc.reads, counted.reads)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestAssertEqualReceiptsBlobGas(t *testing.T) {
	t.Parallel()

	for name, txn := range map[string]types.Transaction{
		"blob":  &types.BlobTx{BlobVersionedHashes: []common.Hash{{1}}},
		"frame": &types.FrameTransaction{BlobVersionedHashes: []common.Hash{{1}}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			receipt := &types.Receipt{
				Type:              txn.Type(),
				Status:            types.ReceiptStatusSuccessful,
				CumulativeGasUsed: params.TxGas,
				GasUsed:           params.TxGas,
				BlobGasUsed:       params.GasPerBlob,
				TxHash:            txn.Hash(),
				BlockNumber:       uint256.NewInt(1),
			}
			if txn.Type() == types.FrameTxType {
				receipt.Payer = &common.Address{1}
				receipt.FrameReceipts = []types.FrameReceipt{{Status: types.ReceiptStatusSuccessful}}
			}

			data, err := rlp.EncodeToBytes((*types.ReceiptForStorage)(receipt))
			require.NoError(t, err)
			var stored types.ReceiptForStorage
			require.NoError(t, rlp.DecodeBytes(data, &stored))
			fromDB := (*types.Receipt)(&stored)
			fromDB.DeriveFieldsV4ForCachedReceipt(receipt.BlockHash, receipt.BlockNumber.Uint64(), txn.Hash(), true)
			require.Zero(t, fromDB.BlobGasUsed)

			g := &Generator{}
			require.NotPanics(t, func() { g.assertEqualReceipts(receipt, fromDB, txn) })

			receipt.BlobGasUsed++
			require.Panics(t, func() { g.assertEqualReceipts(receipt, fromDB, txn) })
		})
	}
}
