package receipts

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

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
