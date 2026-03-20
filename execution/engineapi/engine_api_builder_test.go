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

package engineapi_test

import (
	"context"
	"encoding/binary"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/abi/bind"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

func TestEngineApiBuiltBlockStateMatchesValidation(t *testing.T) {
	eat := engineapitester.DefaultEngineApiTester(t)
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		receiver := common.HexToAddress("0x42")
		sender := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)

		// Submit transfer.
		amount := big.NewInt(1_000_000)
		txn, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, amount)
		require.NoError(t, err)

		// Build canonical block (ForkchoiceUpdated + GetPayload + NewPayload + FCU).
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		// Verify tx inclusion.
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, txn.Hash())
		require.NoError(t, err)

		// Verify receiver balance via RPC.
		receiverBalance, err := eat.RpcApiClient.GetBalance(receiver, rpc.LatestBlock)
		require.NoError(t, err)
		require.Equal(t, amount, receiverBalance)

		// Verify sender nonce via RPC.
		senderNonce, err := eat.RpcApiClient.GetTransactionCount(sender, rpc.LatestBlock)
		require.NoError(t, err)
		require.Equal(t, uint64(1), senderNonce.Uint64())
	})
}

func TestEngineApiMultiBlockSequence(t *testing.T) {
	eat := engineapitester.DefaultEngineApiTester(t)
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		receiver := common.HexToAddress("0x42")

		for i := 0; i < 5; i++ {
			txn, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, big.NewInt(1000))
			require.NoError(t, err)

			payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
			require.NoError(t, err)

			err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, txn.Hash())
			require.NoError(t, err)
		}

		// Verify cumulative balance.
		balance, err := eat.RpcApiClient.GetBalance(receiver, rpc.LatestBlock)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(5000), balance)
	})
}

func TestEngineApiEmptyBlockProduction(t *testing.T) {
	eat := engineapitester.DefaultEngineApiTester(t)
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		// Build block with no pending transactions.
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		require.Empty(t, payload.ExecutionPayload.Transactions)
		// DefaultEngineApiTester already builds 1 empty block, so this is block 2.
		require.Equal(t, hexutil.Uint64(2), payload.ExecutionPayload.BlockNumber)
	})
}

func TestEngineApiBuiltBlockWithContractDeployAndCall(t *testing.T) {
	eat := engineapitester.DefaultEngineApiTester(t)
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		// Deploy Changer contract.
		transactOpts, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainId())
		require.NoError(t, err)
		transactOpts.GasLimit = params.MaxTxnGasLimit

		_, deployTx, changer, err := contracts.DeployChanger(transactOpts, eat.ContractBackend)
		require.NoError(t, err)

		// Build block with deployment.
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, deployTx.Hash())
		require.NoError(t, err)

		// Call the Change() method.
		changeTx, err := changer.Change(transactOpts)
		require.NoError(t, err)

		// Build block with the call.
		payload2, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload2.ExecutionPayload, changeTx.Hash())
		require.NoError(t, err)
	})
}

func TestEngineApiBuiltBlockReorgRecovery(t *testing.T) {
	eat := engineapitester.DefaultEngineApiTester(t)
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		receiver := common.HexToAddress("0x42")

		// Build canonical block 2 with a transfer.
		txn, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, big.NewInt(1000))
		require.NoError(t, err)
		b2, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b2.ExecutionPayload, txn.Hash())
		require.NoError(t, err)

		// Build canonical block 3 with another transfer.
		txn2, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, big.NewInt(2000))
		require.NoError(t, err)
		b3, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b3.ExecutionPayload, txn2.Hash())
		require.NoError(t, err)

		// Create an invalid fork at block 3 — tamper state root.
		b3Faulty := engineapitester.TamperMockClPayloadStateRoot(b3, common.HexToHash("0xbad"))
		status, err := eat.MockCl.InsertNewPayload(ctx, b3Faulty)
		require.NoError(t, err)
		require.Equal(t, enginetypes.InvalidStatus, status.Status)

		// Build block 4 on the canonical chain — proves recovery.
		txn3, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, big.NewInt(3000))
		require.NoError(t, err)
		b4, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b4.ExecutionPayload, txn3.Hash())
		require.NoError(t, err)

		// Verify cumulative balance.
		balance, err := eat.RpcApiClient.GetBalance(receiver, rpc.LatestBlock)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(6000), balance)
	})
}

func TestEngineApiBlockGasOverflowSpillsToNextBlock(t *testing.T) {
	genesis, coinbaseKey := engineapitester.DefaultEngineApiTesterGenesis(t)
	genesis.GasLimit = 150_000 // ~7 simple transfers at 21K gas each
	eat := engineapitester.InitialiseEngineApiTester(t, engineapitester.EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, log.LvlDebug),
		DataDir:     t.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
	})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		receiver := common.HexToAddress("0x42")

		// Submit 10 simple transfers.
		var txHashes []common.Hash
		for i := 0; i < 10; i++ {
			txn, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, big.NewInt(100))
			require.NoError(t, err)
			txHashes = append(txHashes, txn.Hash())
		}

		// Block 2: should be gas-limited, not all 10 fit.
		p1, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		b2TxCount := len(p1.ExecutionPayload.Transactions)
		require.Greater(t, b2TxCount, 0)
		require.Less(t, b2TxCount, 10)

		// Block 3: remaining transactions spill over.
		p2, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		b3TxCount := len(p2.ExecutionPayload.Transactions)
		require.Greater(t, b3TxCount, 0)

		// All 10 transactions should be included across the 2 blocks.
		require.Equal(t, 10, b2TxCount+b3TxCount)

		// Verify cumulative balance.
		balance, err := eat.RpcApiClient.GetBalance(receiver, rpc.LatestBlock)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(1000), balance) // 10 * 100
	})
}

func TestEngineApiSequentialNonceAdvancement(t *testing.T) {
	eat := engineapitester.DefaultEngineApiTester(t)
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		sender := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)
		receiver := common.HexToAddress("0x42")

		// Block 2: transfer with nonce 0.
		txn0, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, big.NewInt(100))
		require.NoError(t, err)
		p1, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, p1.ExecutionPayload, txn0.Hash())
		require.NoError(t, err)

		// Verify nonce is 1 after block 2.
		nonce, err := eat.RpcApiClient.GetTransactionCount(sender, rpc.LatestBlock)
		require.NoError(t, err)
		require.Equal(t, uint64(1), nonce.Uint64())

		// Block 3: transfer with nonce 1.
		txn1, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, big.NewInt(200))
		require.NoError(t, err)
		p2, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, p2.ExecutionPayload, txn1.Hash())
		require.NoError(t, err)

		// Verify nonce is 2 after block 3.
		nonce, err = eat.RpcApiClient.GetTransactionCount(sender, rpc.LatestBlock)
		require.NoError(t, err)
		require.Equal(t, uint64(2), nonce.Uint64())

		// Verify cumulative balance.
		balance, err := eat.RpcApiClient.GetBalance(receiver, rpc.LatestBlock)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(300), balance)
	})
}

func TestEngineApiMultipleSendersInBlock(t *testing.T) {
	genesis, coinbaseKey := engineapitester.DefaultEngineApiTesterGenesis(t)
	secondKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	secondAddr := crypto.PubkeyToAddress(secondKey.PublicKey)
	genesis.Alloc[secondAddr] = types.GenesisAccount{
		Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil), // 1000 ETH
	}

	eat := engineapitester.InitialiseEngineApiTester(t, engineapitester.EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, log.LvlDebug),
		DataDir:     t.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
	})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		receiver := common.HexToAddress("0x42")

		// Submit from coinbase (nonce 0).
		txn1, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, big.NewInt(100))
		require.NoError(t, err)
		// Submit from second account (nonce 0).
		txn2, err := eat.Transactor.SubmitSimpleTransfer(secondKey, receiver, big.NewInt(200))
		require.NoError(t, err)

		// Build block — should include both.
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload,
			txn1.Hash(), txn2.Hash())
		require.NoError(t, err)

		// Verify cumulative balance.
		balance, err := eat.RpcApiClient.GetBalance(receiver, rpc.LatestBlock)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(300), balance)

		// Verify both sender nonces advanced.
		coinbaseNonce, err := eat.RpcApiClient.GetTransactionCount(
			crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey), rpc.LatestBlock)
		require.NoError(t, err)
		require.Equal(t, uint64(1), coinbaseNonce.Uint64())

		secondNonce, err := eat.RpcApiClient.GetTransactionCount(secondAddr, rpc.LatestBlock)
		require.NoError(t, err)
		require.Equal(t, uint64(1), secondNonce.Uint64())
	})
}

func TestEngineApiHighGasContractsFillBlock(t *testing.T) {
	genesis, coinbaseKey := engineapitester.DefaultEngineApiTesterGenesis(t)
	genesis.GasLimit = 200_000 // tight budget for contracts + transfers
	eat := engineapitester.InitialiseEngineApiTester(t, engineapitester.EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, log.LvlDebug),
		DataDir:     t.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
	})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		transactOpts, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainId())
		require.NoError(t, err)
		transactOpts.GasLimit = 100_000 // contract deploy ~53K+

		// Deploy 2 contracts (~106K gas) + 1 transfer (21K) ≈ 127K.
		// With system call overhead, this should leave little room.
		_, deployTx1, _, err := contracts.DeployChanger(transactOpts, eat.ContractBackend)
		require.NoError(t, err)
		_, deployTx2, _, err := contracts.DeployChanger(transactOpts, eat.ContractBackend)
		require.NoError(t, err)

		receiver := common.HexToAddress("0x42")
		transfer1, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, big.NewInt(500))
		require.NoError(t, err)
		// This transfer may or may not fit in the same block.
		transfer2, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, big.NewInt(500))
		require.NoError(t, err)

		// Build block 2.
		p1, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		b2TxCount := len(p1.ExecutionPayload.Transactions)
		t.Logf("block 2: %d transactions", b2TxCount)

		// Build block 3 for any spillover.
		p2, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		b3TxCount := len(p2.ExecutionPayload.Transactions)
		t.Logf("block 3: %d transactions", b3TxCount)

		// All 4 txns should be included across the 2 blocks.
		totalTxCount := b2TxCount + b3TxCount
		require.Equal(t, 4, totalTxCount)

		// Verify all txns included across both blocks.
		allTxHashes := []common.Hash{deployTx1.Hash(), deployTx2.Hash(), transfer1.Hash(), transfer2.Hash()}
		for _, h := range allTxHashes {
			err1 := eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, p1.ExecutionPayload, h)
			err2 := eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, p2.ExecutionPayload, h)
			require.True(t, err1 == nil || err2 == nil, "tx %s not found in either block", h.Hex())
		}

		// Verify receiver balance.
		balance, err := eat.RpcApiClient.GetBalance(receiver, rpc.LatestBlock)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(1000), balance) // 500 + 500
	})
}

// TestEngineApiBuiltBlockWithWithdrawalRequest sends a transaction to the EIP-7002
// withdrawal request system contract and verifies the builder produces a block that
// passes validation via NewPayload (ExecV3). This exercises the builder's state root
// computation when system calls during finalization read state written by user txns.
func TestEngineApiBuiltBlockWithWithdrawalRequest(t *testing.T) {
	eat := engineapitester.DefaultEngineApiTester(t)
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		sender := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)

		// Build calldata: 48-byte validator pubkey + 8-byte amount (little-endian).
		// Use a fake pubkey (all 0x01) and amount=0 (full exit).
		var calldata []byte
		pubkey := make([]byte, 48)
		for i := range pubkey {
			pubkey[i] = 0x01
		}
		amount := make([]byte, 8) // 0 = full exit
		calldata = append(calldata, pubkey...)
		calldata = append(calldata, amount...)

		// Get current nonce and gas price.
		nonce, err := eat.RpcApiClient.GetTransactionCount(sender, rpc.PendingBlock)
		require.NoError(t, err)
		gasPrice, err := eat.RpcApiClient.GasPrice()
		require.NoError(t, err)
		gasPriceU256, _ := uint256.FromBig(gasPrice)

		// Send tx to withdrawal request contract with 0.5 ETH.
		withdrawalRequestAddr := params.WithdrawalRequestAddress.Value()
		txn := &types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce:    nonce.Uint64(),
				GasLimit: 1_000_000,
				To:       &withdrawalRequestAddr,
				Value:    *uint256.NewInt(500_000_000_000_000_000), // 0.5 ETH
				Data:     calldata,
			},
			GasPrice: *gasPriceU256,
		}
		signer := types.LatestSignerForChainID(eat.ChainConfig.ChainID)
		signedTxn, err := types.SignTx(txn, *signer, eat.CoinbaseKey)
		require.NoError(t, err)

		_, err = eat.RpcApiClient.SendTransaction(signedTxn)
		require.NoError(t, err)

		// Build canonical block — this builds via the builder AND validates via NewPayload.
		// If the builder's ComputeCommitment produces a different state root than ExecV3,
		// InsertNewPayload will return INVALID and BuildCanonicalBlock will fail.
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		// Verify the withdrawal request tx was included.
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, signedTxn.Hash())
		require.NoError(t, err)

		// Verify execution requests are present in the payload (Prague includes withdrawal requests).
		require.NotNil(t, payload.ExecutionRequests)

		// Verify withdrawal request content — the system contract should have
		// dequeued the request we submitted and included it in the block.
		var foundWithdrawalRequest bool
		for _, req := range payload.ExecutionRequests {
			if len(req) == 0 || req[0] != types.WithdrawalRequestType {
				continue
			}
			requestData := []byte(req[1:])
			// A withdrawal request is: 20-byte source address + 48-byte pubkey + 8-byte LE amount.
			require.Equal(t, types.WithdrawalRequestDataLen, len(requestData),
				"withdrawal request should be exactly %d bytes", types.WithdrawalRequestDataLen)

			sourceAddr := common.BytesToAddress(requestData[:20])
			gotPubkey := requestData[20:68]
			gotAmount := binary.LittleEndian.Uint64(requestData[68:76])

			require.Equal(t, sender, sourceAddr,
				"withdrawal request source address should be the sender")
			require.Equal(t, pubkey, gotPubkey,
				"withdrawal request pubkey should match the one we sent")
			require.Equal(t, uint64(0), gotAmount,
				"withdrawal request amount should be 0 (full exit)")
			foundWithdrawalRequest = true
		}
		require.True(t, foundWithdrawalRequest,
			"should find at least one withdrawal request in execution requests")
	})
}
