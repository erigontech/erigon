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
	"crypto/ecdsa"
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
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/abi/bind"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
)

func TestEngineApiBuiltBlockStateMatchesValidation(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
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
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
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
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		// Build block with no pending transactions.
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		require.Empty(t, payload.ExecutionPayload.Transactions)
		// DefaultEngineApiTester already builds 1 empty block, so this is block 2.
		require.Equal(t, hexutil.Uint64(2), payload.ExecutionPayload.BlockNumber)
	})
}

// TestEngineApiBuiltBlockEmptyRequestsHash verifies that a built block with an
// empty EIP-7685 request set carries empty.RequestsHash (SHA256 of empty input)
// in its header, not a zero hash.
func TestEngineApiBuiltBlockEmptyRequestsHash(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		// An empty block has no deposit/withdrawal/consolidation requests.
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		require.Empty(t, payload.ExecutionPayload.Transactions)
		require.Empty(t, payload.ExecutionRequests)

		block, err := eat.RpcApiClient.GetBlockByNumber(ctx, rpc.LatestBlockNumber, false)
		require.NoError(t, err)
		require.Equal(t, payload.ExecutionPayload.BlockHash, block.Hash)
		require.NotNil(t, block.RequestsHash)
		require.Equal(t, empty.RequestsHash, *block.RequestsHash)
	})
}

func TestEngineApiBuiltBlockWithContractDeployAndCall(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
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
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
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
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	genesis.GasLimit = 1_300_000 // ~7 account-creating transfers at ~184K state gas each
	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger:      logger,
		DataDir:     t.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		// Distinct fresh recipients so every transfer is account-creating and costs the same.
		receivers := make([]common.Address, 10)
		for i := range receivers {
			receivers[i] = common.BigToAddress(big.NewInt(int64(0x1000 + i)))
		}
		// Submit 10 simple transfers.
		for _, receiver := range receivers {
			_, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, big.NewInt(100))
			require.NoError(t, err)
		}
		// Block 2: gas-limited to 7 of the 10.
		p1, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		b2TxCount := len(p1.ExecutionPayload.Transactions)
		require.Equal(t, 7, b2TxCount)
		// Block 3: the remaining 3 spill over.
		p2, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		b3TxCount := len(p2.ExecutionPayload.Transactions)
		require.Equal(t, 3, b3TxCount)
		// All 10 transactions should be included across the 2 blocks.
		require.Equal(t, 10, b2TxCount+b3TxCount)
		// Verify cumulative balance across the distinct recipients.
		total := big.NewInt(0)
		for _, receiver := range receivers {
			balance, err := eat.RpcApiClient.GetBalance(receiver, rpc.LatestBlock)
			require.NoError(t, err)
			total.Add(total, balance)
		}
		require.Equal(t, big.NewInt(1000), total) // 10 * 100
	})
}

// TestEngineApiV4TargetGasLimitOverridesMinerGasLimit checks that a CL-supplied
// targetGasLimit in PayloadAttributesV4 (engine_forkchoiceUpdatedV4) overrides
// the EL's static --miner.gaslimit when building a block — and that the
// resulting block respects the CL target as a cap.
//
// Setup picks numbers so the two values produce distinguishable block contents:
//   - parent gas limit = 367_400 (room for two account-creating transfers)
//   - static --miner.gaslimit = 225_000 (would cap the block at one transfer)
//   - CL targetGasLimit = 367_400 (room for two transfers)
//
// Three transfers are submitted; only two must fit. If the static target won,
// the block would gas-limit at ~367_040 and contain a single transfer.
// See https://github.com/ethereum/execution-apis/pull/796.
func TestEngineApiV4TargetGasLimitOverridesMinerGasLimit(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	const targetGasLimit uint64 = 367_400
	const minerGasLimit uint64 = 225_000
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	genesis.GasLimit = targetGasLimit
	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger:      logger,
		DataDir:     t.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			gl := minerGasLimit
			config.Builder.GasLimit = &gl
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		receivers := []common.Address{
			common.HexToAddress("0x42"),
			common.HexToAddress("0x43"),
			common.HexToAddress("0x44"),
		}
		for _, receiver := range receivers {
			_, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, big.NewInt(1))
			require.NoError(t, err)
		}
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		// Block gas limit follows the CL target — not the EL's --miner.gaslimit.
		require.Equal(t, hexutil.Uint64(targetGasLimit), payload.ExecutionPayload.GasLimit)
		require.Len(t, payload.ExecutionPayload.Transactions, 2)
	})
}

func TestEngineApiSequentialNonceAdvancement(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
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
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	secondKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	secondAddr := crypto.PubkeyToAddress(secondKey.PublicKey)
	genesis.Alloc[secondAddr] = types.GenesisAccount{
		Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil), // 1000 ETH
	}

	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger:      logger,
		DataDir:     t.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
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
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	genesis.Config.AmsterdamTime = nil // EIP-8037 state gas changes intrinsic costs; test pre-Amsterdam
	genesis.GasLimit = 200_000         // tight budget for contracts + transfers
	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger:      logger,
		DataDir:     t.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
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
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
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

// TestEngineApiBALGlamsterdamCreate2OntoFundedAddress pins block-access-list
// consistency for the glamsterdam-devnet-5 invalid-block pattern: a Disperse
// contract credits many still-empty addresses, then a Gnosis-Safe-style Proxy is
// CREATE2-deployed onto each in the same block. EIP-7610 preserves the balance, so
// the credit is the only balance change; the builder's embedded BAL must match the
// parallel validator's recomputed one, or BuildCanonicalBlock fails with a BAL
// mismatch. Independent senders let the parallel executor race each credit against
// its reincarnation.
func TestEngineApiBALGlamsterdamCreate2OntoFundedAddress(t *testing.T) {
	if !dbg.Exec3Parallel {
		t.Skip("requires parallel exec")
	}
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlInfo)
	const numProxies = 12
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	fund := new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)
	disperserKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	genesis.Alloc[crypto.PubkeyToAddress(disperserKey.PublicKey)] = types.GenesisAccount{Balance: fund}
	deployerKeys := make([]*ecdsa.PrivateKey, numProxies)
	for i := range deployerKeys {
		deployerKeys[i], err = crypto.GenerateKey()
		require.NoError(t, err)
		genesis.Alloc[crypto.PubkeyToAddress(deployerKeys[i].PublicKey)] = types.GenesisAccount{Balance: fund}
	}
	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger: logger, DataDir: t.TempDir(), Genesis: genesis, CoinbaseKey: coinbaseKey,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		chainID := eat.ChainId()
		disperse, factory, factoryAddr := deployBALWorkloadContracts(ctx, t, eat)
		// Proxy carries no constructor args, so its CREATE2 init code is exactly
		// ProxyBin and each proxy address is deterministic from factory and salt.
		proxyInit := common.FromHex(contracts.ProxyBin)
		proxies := make([]common.Address, numProxies)
		salts := make([][32]byte, numProxies)
		recipients := make([]common.Address, numProxies)
		values := make([]*big.Int, numProxies)
		for i := range proxies {
			salts[i][31] = byte(i + 1)
			proxies[i] = create2Addr(factoryAddr, salts[i], proxyInit)
			recipients[i] = proxies[i]
			values[i] = creditWei.ToBig()
		}
		// One disperse tx credits every still-empty proxy, then independent senders
		// CREATE2-deploy each proxy in the same block.
		disperseAuth, err := bind.NewKeyedTransactorWithChainID(disperserKey, chainID)
		require.NoError(t, err)
		disperseAuth.GasLimit = 3_000_000
		disperseAuth.Value = new(big.Int).Mul(creditWei.ToBig(), big.NewInt(numProxies))
		_, err = disperse.DisperseEther(disperseAuth, recipients, values)
		require.NoError(t, err)
		for i := range proxies {
			deployAuth, err := bind.NewKeyedTransactorWithChainID(deployerKeys[i], chainID)
			require.NoError(t, err)
			deployAuth.GasLimit = 500_000
			_, err = factory.CreateProxy(deployAuth, salts[i])
			require.NoError(t, err)
		}
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err, "proposer vs validator BAL mismatch: disperse credit + same-block CREATE2 of funded proxies")
		bal := decodeAndValidateBAL(t, payload)
		for _, proxy := range proxies {
			requireProxyCreditPreserved(t, eat, bal, proxy)
		}
	})
}

// creditWei is the fixed amount dispersed to each proxy while it is still empty
// (0.000256 ETH, the constant seen on glamsterdam-devnet-5).
var creditWei = uint256.NewInt(256_000_000_000_000)

func deployBALWorkloadContracts(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) (*contracts.Disperse, *contracts.ProxyFactory, common.Address) {
	t.Helper()
	chainID := eat.ChainId()
	disperseAuth, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, chainID)
	require.NoError(t, err)
	disperseAuth.GasLimit = params.MaxTxnGasLimit
	disperseAddr, _, disperse, err := contracts.DeployDisperse(disperseAuth, eat.ContractBackend)
	require.NoError(t, err)
	_, err = eat.MockCl.BuildCanonicalBlock(ctx)
	require.NoError(t, err)
	factoryAuth, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, chainID)
	require.NoError(t, err)
	factoryAuth.GasLimit = params.MaxTxnGasLimit
	factoryAddr, _, factory, err := contracts.DeployProxyFactory(factoryAuth, eat.ContractBackend)
	require.NoError(t, err)
	_, err = eat.MockCl.BuildCanonicalBlock(ctx)
	require.NoError(t, err)
	disperseCode, err := eat.RpcApiClient.GetCode(disperseAddr, rpc.LatestBlock)
	require.NoError(t, err)
	require.NotEmpty(t, disperseCode, "disperse must be deployed")
	factoryCode, err := eat.RpcApiClient.GetCode(factoryAddr, rpc.LatestBlock)
	require.NoError(t, err)
	require.NotEmpty(t, factoryCode, "factory must be deployed")
	return disperse, factory, factoryAddr
}

func requireProxyCreditPreserved(t *testing.T, eat engineapitester.EngineApiTester, bal types.BlockAccessList, proxy common.Address) {
	t.Helper()
	onChain, err := eat.RpcApiClient.GetBalance(proxy, rpc.LatestBlock)
	require.NoError(t, err)
	require.Equalf(t, creditWei.ToBig(), onChain, "proxy %s must retain the dispersed credit", proxy)
	code, err := eat.RpcApiClient.GetCode(proxy, rpc.LatestBlock)
	require.NoError(t, err)
	require.NotEmptyf(t, code, "proxy %s must be CREATE2-deployed", proxy)
	cc := findAccountChanges(bal, accounts.InternAddress(proxy))
	require.NotNilf(t, cc, "proxy %s missing from BAL\n%s", proxy, bal.DebugString())
	credit := lastBalanceChange(cc)
	require.NotNilf(t, credit, "proxy %s has no balance change in BAL (credit dropped)\n%s", proxy, bal.DebugString())
	want, _ := uint256.FromBig(onChain)
	require.Truef(t, credit.Value.Eq(want), "proxy %s BAL balance %s != on-chain %s\n%s", proxy, credit.Value.Hex(), want.Hex(), bal.DebugString())
	require.NotEmptyf(t, cc.CodeChanges, "proxy %s missing code change\n%s", proxy, bal.DebugString())
	require.Lessf(t, credit.Index, cc.CodeChanges[0].Index, "proxy %s credit must precede its CREATE2 deploy\n%s", proxy, bal.DebugString())
}

func create2Addr(factory common.Address, salt [32]byte, initCode []byte) common.Address {
	buf := make([]byte, 0, 85)
	buf = append(buf, 0xff)
	buf = append(buf, factory[:]...)
	buf = append(buf, salt[:]...)
	buf = append(buf, crypto.Keccak256(initCode)...)
	return common.BytesToAddress(crypto.Keccak256(buf)[12:])
}

func lastBalanceChange(ac *types.AccountChanges) *types.BalanceChange {
	var last *types.BalanceChange
	for _, b := range ac.BalanceChanges {
		if b != nil && (last == nil || b.Index >= last.Index) {
			last = b
		}
	}
	return last
}
