// Copyright 2025 The Erigon Authors
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

package executiontests

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
)

func TestEngineApiGeneratedPayloadIncludesBlockAccessList(t *testing.T) {
	if !dbg.Exec3Parallel {
		t.Skip("requires parallel exec")
	}
	eat := DefaultEngineApiTester(t)
	receiver := common.HexToAddress("0x333")
	eat.Run(t, func(ctx context.Context, t *testing.T, eat EngineApiTester) {
		sender := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)
		txn, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, big.NewInt(1))
		require.NoError(t, err)

		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, txn.Hash())
		require.NoError(t, err)

		balBytes := payload.ExecutionPayload.BlockAccessList
		require.NotNil(t, balBytes)
		require.NotEmpty(t, *balBytes)

		bal, err := types.DecodeBlockAccessListBytes(*balBytes)
		require.NoError(t, err)
		require.NoError(t, bal.Validate())
		require.NotEmpty(t, bal)

		blockNumber := rpc.BlockNumber(payload.ExecutionPayload.BlockNumber)
		block, err := eat.RpcApiClient.GetBlockByNumber(ctx, blockNumber, false)
		require.NoError(t, err)
		require.NotNil(t, block)
		require.Equal(t, payload.ExecutionPayload.BlockHash, block.Hash)
		require.NotNil(t, block.BlockAccessListHash)
		require.Equal(t, bal.Hash(), *block.BlockAccessListHash)

		senderChanges := findAccountChanges(bal, accounts.InternAddress(sender))
		receiverChanges := findAccountChanges(bal, accounts.InternAddress(receiver))
		require.NotNil(t, senderChanges)
		require.NotNil(t, receiverChanges)

		receipt, err := eat.RpcApiClient.GetTransactionReceipt(ctx, txn.Hash())
		require.NoError(t, err)
		require.NotNil(t, receipt)

		balIndex := uint16(receipt.TransactionIndex + 1)

		senderBalance, err := eat.RpcApiClient.GetBalance(sender, rpc.LatestBlock)
		require.NoError(t, err)
		receiverBalance, err := eat.RpcApiClient.GetBalance(receiver, rpc.LatestBlock)
		require.NoError(t, err)
		senderNonce, err := eat.RpcApiClient.GetTransactionCount(sender, rpc.LatestBlock)
		require.NoError(t, err)

		senderBalanceChange := findBalanceChange(senderChanges, balIndex)
		require.NotNilf(t, senderBalanceChange, "missing sender balance change at index %d\n%s", balIndex, bal.DebugString())
		expectedSenderBalance, overflow := uint256.FromBig(senderBalance)
		require.False(t, overflow)
		require.True(t, senderBalanceChange.Value.Eq(expectedSenderBalance))

		receiverBalanceChange := findBalanceChange(receiverChanges, balIndex)
		require.NotNilf(t, receiverBalanceChange, "missing receiver balance change at index %d\n%s", balIndex, bal.DebugString())
		expectedReceiverBalance, overflow := uint256.FromBig(receiverBalance)
		require.False(t, overflow)
		require.True(t, receiverBalanceChange.Value.Eq(expectedReceiverBalance))

		senderNonceChange := findNonceChange(senderChanges, balIndex)
		require.NotNilf(t, senderNonceChange, "missing sender nonce change at index %d\n%s", balIndex, bal.DebugString())
		require.Equal(t, senderNonce.Uint64(), senderNonceChange.Value)
	})
}

func findAccountChanges(bal types.BlockAccessList, addr accounts.Address) *types.AccountChanges {
	for _, ac := range bal {
		if ac != nil && ac.Address == addr {
			return ac
		}
	}
	return nil
}

func findBalanceChange(ac *types.AccountChanges, index uint16) *types.BalanceChange {
	if ac == nil {
		return nil
	}
	for _, change := range ac.BalanceChanges {
		if change != nil && change.Index == index {
			return change
		}
	}
	return nil
}

func findNonceChange(ac *types.AccountChanges, index uint16) *types.NonceChange {
	if ac == nil {
		return nil
	}
	for _, change := range ac.NonceChanges {
		if change != nil && change.Index == index {
			return change
		}
	}
	return nil
}
