package receipts

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

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
			got, err := g.GetReceiptsGasUsed(t.Context(), counted, block, rawdbv3.TxNums, tc.framesOnly)
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
