// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package graph

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/graphql/graph/model"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/types/ethutils"
	"github.com/erigontech/erigon/rpc/jsonrpc"
)

// graphqlReceipt builds a receipt the way buildBlockDetailsResponse does.
func graphqlReceipt(receipt *types.Receipt, txn types.Transaction, header *types.Header) *jsonrpc.GraphQLReceipt {
	transaction := &jsonrpc.GraphQLReceipt{
		RPCReceipt: ethutils.MarshalReceipt(receipt, txn, chain.TestChainOsakaConfig, header, txn.Hash(), true, false),
		Nonce:      txn.GetNonce(),
		Value:      txn.GetValue(),
		Data:       txn.GetData(),
		Logs:       receipt.Logs,
		Gas:        txn.GetGasLimit(),
		AccessList: txn.GetAccessList(),
	}
	txType := txn.Type()
	if txType == types.DynamicFeeTxType || txType == types.SetCodeTxType || txType == types.BlobTxType {
		transaction.MaxFeePerGas = txn.GetFeeCap()
		transaction.MaxPriorityFeePerGas = txn.GetTipCap()
	}
	if blobTx, ok := txn.(*types.BlobTx); ok {
		transaction.MaxFeePerBlobGas = (*hexutil.U256)(new(uint256.Int).Set(&blobTx.MaxFeePerBlobGas))
	}
	return transaction
}

func TestBuildTransactionReadsMarshalledReceipt(t *testing.T) {
	t.Parallel()

	sender := common.HexToAddress("0xAbCdEf0123456789aBcDeF0123456789AbCdEf03")
	to := common.HexToAddress("0xAbCdEf0123456789aBcDeF0123456789AbCdEf04")
	contract := common.HexToAddress("0xAbCdEf0123456789aBcDeF0123456789AbCdEf05")
	excessBlobGas := uint64(0)
	header := &types.Header{Number: *uint256.NewInt(7), BaseFee: uint256.NewInt(50), ExcessBlobGas: &excessBlobGas}
	block := &model.Block{Number: 7}

	t.Run("blob", func(t *testing.T) {
		t.Parallel()
		txn := &types.BlobTx{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx:   types.CommonTx{Nonce: 3, GasLimit: 21000, To: &to, Value: *uint256.NewInt(5), Data: []byte{0xde, 0xad}},
				ChainID:    *uint256.NewInt(1337),
				TipCap:     *uint256.NewInt(2),
				FeeCap:     *uint256.NewInt(100),
				AccessList: types.AccessList{{Address: to, StorageKeys: []common.Hash{{0x0a}}}},
			},
			MaxFeePerBlobGas:    *uint256.NewInt(9),
			BlobVersionedHashes: []common.Hash{{0x01}, {0x02}},
		}
		txn.SetSender(accounts.InternAddress(sender))
		receipt := &types.Receipt{
			Status:            types.ReceiptStatusSuccessful,
			CumulativeGasUsed: 42_000,
			Logs:              types.Logs{{Address: to, Topics: []common.Hash{{0x0b}}, Data: []byte{0xbe, 0xef}, Index: 4}},
			TxHash:            txn.Hash(),
			ContractAddress:   contract,
			GasUsed:           21_000,
			BlockNumber:       uint256.NewInt(7),
			TransactionIndex:  3,
		}

		got := (&queryResolver{}).buildTransaction(block, graphqlReceipt(receipt, txn, header))

		require.Equal(t, uint64(21000), got.Gas)
		require.Equal(t, "0xdead", got.InputData)
		require.Equal(t, "0x3", got.Nonce)
		require.Equal(t, "0x5", got.Value)
		require.Equal(t, "0x34", got.GasPrice)
		require.Equal(t, "0x34", *got.EffectiveGasPrice)
		require.Equal(t, "0x64", *got.MaxFeePerGas)
		require.Equal(t, "0x2", *got.MaxPriorityFeePerGas)
		require.Equal(t, "0x9", *got.MaxFeePerBlobGas)
		require.Equal(t, uint64(262144), *got.BlobGasUsed)
		require.Equal(t, "0x1", *got.BlobGasPrice)
		require.Equal(t, uint64(42_000), *got.CumulativeGasUsed)
		require.Equal(t, uint64(21_000), *got.GasUsed)
		require.Equal(t, txn.Hash().Hex(), got.Hash)
		require.Equal(t, uint64(3), *got.Index)
		require.Equal(t, uint64(1), *got.Status)
		require.Equal(t, uint64(types.BlobTxType), *got.Type)
		require.Equal(t, "0xabcdef0123456789abcdef0123456789abcdef03", got.From.Address)
		require.Equal(t, "0xabcdef0123456789abcdef0123456789abcdef04", got.To.Address)
		require.Equal(t, "0xabcdef0123456789abcdef0123456789abcdef05", got.CreatedContract.Address)
		require.Equal(t, []*model.AccessTuple{{
			Address:     "0xabcdef0123456789abcdef0123456789abcdef04",
			StorageKeys: []string{common.Hash{0x0a}.Hex()},
		}}, got.AccessList)
		require.Len(t, got.Logs, 1)
		require.Equal(t, uint64(4), got.Logs[0].Index)
		require.Equal(t, "0xbeef", got.Logs[0].Data)
		require.Equal(t, "0xabcdef0123456789abcdef0123456789abcdef04", got.Logs[0].Account.Address)
		require.Equal(t, []string{common.Hash{0x0b}.Hex()}, got.Logs[0].Topics)
	})

	// A pre-Byzantium creation has no status, recipient, fee caps or access list.
	t.Run("legacyCreation", func(t *testing.T) {
		t.Parallel()
		txn := &types.LegacyTx{CommonTx: types.CommonTx{GasLimit: 53000}, GasPrice: *uint256.NewInt(60)}
		txn.SetSender(accounts.InternAddress(sender))
		receipt := &types.Receipt{PostState: []byte{0x0b}, BlockNumber: uint256.NewInt(7), TxHash: txn.Hash()}

		got := (&queryResolver{}).buildTransaction(block, graphqlReceipt(receipt, txn, header))

		require.Equal(t, "0x", got.InputData)
		require.Equal(t, "0x0", got.Nonce)
		require.Equal(t, "0x0", got.Value)
		require.Nil(t, got.Status)
		require.Nil(t, got.To)
		require.Nil(t, got.CreatedContract)
		require.Nil(t, got.MaxFeePerGas)
		require.Nil(t, got.MaxPriorityFeePerGas)
		require.Nil(t, got.MaxFeePerBlobGas)
		require.Nil(t, got.BlobGasUsed)
		require.Nil(t, got.BlobGasPrice)
		require.Empty(t, got.AccessList)
		require.Empty(t, got.Logs)
	})
}
