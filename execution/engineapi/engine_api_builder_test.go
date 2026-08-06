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

func TestEngineApiClearsFreshTouchedEmptyAccount(t *testing.T) {
	tests := []struct {
		name            string
		experimentalBAL bool
	}{
		{name: "serial builder"},
		{name: "experimental BAL builder", experimentalBAL: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testEngineApiClearsFreshTouchedEmptyAccount(t, test.experimentalBAL)
		})
	}
}

func testEngineApiClearsFreshTouchedEmptyAccount(t *testing.T, experimentalBAL bool) {
	parallel := dbg.Exec3Parallel
	dbg.Exec3Parallel = true
	t.Cleanup(func() { dbg.Exec3Parallel = parallel })

	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlError)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	genesis.Config.AmsterdamTime = nil

	senderKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(senderKey.PublicKey)
	genesis.Alloc[senderAddr] = types.GenesisAccount{
		Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil),
	}

	emptyKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	emptyAddr := crypto.PubkeyToAddress(emptyKey.PublicKey)

	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger: logger, DataDir: t.TempDir(), Genesis: genesis, CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(cfg *ethconfig.Config) {
			cfg.ExperimentalBAL = experimentalBAL
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		chainID := eat.ChainId()
		signer := types.LatestSignerForChainID(chainID)
		rpcClient := eat.Transactor.RpcClient()
		feeCap := uint256.NewInt(1_000_000_000)

		send := func(txn types.Transaction) {
			signed, err := types.SignTx(txn, *signer, senderKey)
			require.NoError(t, err)
			_, err = rpcClient.SendTransaction(signed)
			require.NoError(t, err)
		}

		// Deploy runtime code that pushes emptyAddr and executes SELFDESTRUCT.
		initCode := append([]byte{0x75, 0x73}, emptyAddr[:]...)
		initCode = append(initCode, 0xff, 0x60, 0x00, 0x52, 0x60, 0x16, 0x60, 0x0a, 0xf3)
		send(&types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: 0, GasLimit: 200_000, Data: initCode},
			ChainID:  *chainID, TipCap: *feeCap, FeeCap: *feeCap,
		})
		_, err = eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		selfDestructor := types.CreateAddress(senderAddr, 0)
		send(&types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: 1, GasLimit: 200_000, To: &selfDestructor},
			ChainID:  *chainID, TipCap: *feeCap, FeeCap: *feeCap,
		})
		auth, err := types.SignAuthorization(emptyKey, *chainID, common.Address{0xaa}, 0)
		require.NoError(t, err)
		send(&types.SetCodeTransaction{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{Nonce: 2, GasLimit: 500_000, To: &senderAddr},
				ChainID:  *chainID, TipCap: *feeCap, FeeCap: *feeCap,
			},
			Authorizations: []types.Authorization{auth},
		})

		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		const gasUsedWithoutAuthorityExistenceRefund = uint64(74_603)
		require.Equal(t, gasUsedWithoutAuthorityExistenceRefund, uint64(payload.ExecutionPayload.GasUsed))
	})
}

func TestEngineApiClearsFreshZeroAmountWithdrawal(t *testing.T) {
	parallel := dbg.Exec3Parallel
	dbg.Exec3Parallel = true
	t.Cleanup(func() { dbg.Exec3Parallel = parallel })

	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlError)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)

	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger: logger, DataDir: t.TempDir(), Genesis: genesis, CoinbaseKey: coinbaseKey,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		fresh := common.Address{0xbe, 0xef}
		withdrawals := []*types.Withdrawal{{Index: 1, Validator: 1, Address: fresh, Amount: 0}}
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx, engineapitester.WithWithdrawals(withdrawals))
		require.NoError(t, err)

		bal := decodeAndValidateBAL(t, payload)
		changes := findAccountChanges(bal, accounts.InternAddress(fresh))
		require.NotNilf(t, changes, "zero-amount withdrawal recipient must remain an access-only BAL entry\n%s", bal.DebugString())
		require.Empty(t, changes.StorageChanges)
		require.Empty(t, changes.StorageReads)
		require.Empty(t, changes.BalanceChanges)
		require.Empty(t, changes.NonceChanges)
		require.Empty(t, changes.CodeChanges)
	})
}

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

		for range 5 {
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
	genesis.GasLimit = 1_400_000 // ~7 account-creating transfers at ~200K state gas each
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
//   - parent gas limit = 490_000 (room for two account-creating transfers)
//   - static --miner.gaslimit = 225_000 (would cap the block at one transfer)
//   - CL targetGasLimit = 490_000 (room for two transfers)
//
// Three transfers are submitted; only two must fit. If the static target won,
// the block would gas-limit at ~489_520 and contain a single transfer.
// See https://github.com/ethereum/execution-apis/pull/796.
func TestEngineApiV4TargetGasLimitOverridesMinerGasLimit(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	const targetGasLimit uint64 = 490_000
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

// TestEngineApiEIP8246PreservedBalanceSurvivesCreate2Recreate builds one block
// with two calls to SelfDestructFactory using the same salt: each CREATE2-
// deploys SelfDestructInConstructor, whose constructor self-destructs to
// itself, so under EIP-8246 the first call leaves a balance-only account and
// the second re-creates over it with more value. The assembler runs both txs
// on one shared IntraBlockState, so a stale selfdestructed marker from tx1
// would drop the preserved balance in tx2's CREATE2 and produce an invalid
// block: newPayload validation (fresh state per tx) computes the preserved sum
// and rejects the assembled root.
func TestEngineApiEIP8246PreservedBalanceSurvivesCreate2Recreate(t *testing.T) {
	if !dbg.Exec3Parallel {
		t.Skip("requires parallel exec")
	}
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	senderKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(senderKey.PublicKey)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	genesis.Alloc[senderAddr] = types.GenesisAccount{
		Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil), // 100 ETH
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
	valueTx1 := big.NewInt(1_000_000)
	valueTx2 := big.NewInt(500_000)
	salt := [32]byte{0x82, 0x46}
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		chainID := eat.ChainId()
		factoryAuth, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, chainID)
		require.NoError(t, err)
		factoryAuth.GasLimit = params.MaxTxnGasLimit
		factoryAddr, _, factory, err := contracts.DeploySelfDestructFactory(factoryAuth, eat.ContractBackend)
		require.NoError(t, err)
		_, err = eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		deployAuth, err := bind.NewKeyedTransactorWithChainID(senderKey, chainID)
		require.NoError(t, err)
		deployAuth.GasLimit = 1_000_000
		deployAuth.Value = valueTx1
		txn1, err := factory.Deploy(deployAuth, salt)
		require.NoError(t, err)
		deployAuth.Value = valueTx2
		txn2, err := factory.Deploy(deployAuth, salt)
		require.NoError(t, err)
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err, "assembled block must validate: a dropped preserved balance surfaces as an INVALID payload")
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, txn1.Hash(), txn2.Hash())
		require.NoError(t, err)
		createdAddr := create2Addr(factoryAddr, salt, common.FromHex(contracts.SelfDestructInConstructorBin))
		balance, err := eat.RpcApiClient.GetBalance(createdAddr, rpc.LatestBlock)
		require.NoError(t, err)
		expected := new(big.Int).Add(valueTx1, valueTx2)
		require.Equal(t, expected, balance,
			"EIP-8246: tx1's preserved balance must survive tx2's CREATE2 at the same address")
		code, err := eat.RpcApiClient.GetCode(createdAddr, rpc.LatestBlock)
		require.NoError(t, err)
		require.Empty(t, code, "the self-destructed account is balance-only")
	})
}

func TestEngineApiAccountLifecycleFinalizationConsistency(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	genesis.Config.AmsterdamTime = nil

	salt := [32]byte{}
	factoryAddr := types.CreateAddress(crypto.PubkeyToAddress(coinbaseKey.PublicKey), 0)
	coinbase := create2Addr(factoryAddr, salt, common.FromHex(contracts.SelfDestructInConstructorBin))
	genesis.Coinbase = coinbase

	senderKeys := make([]*ecdsa.PrivateKey, 3)
	funds := new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)
	for i := range senderKeys {
		senderKeys[i], err = crypto.GenerateKey()
		require.NoError(t, err)
		genesis.Alloc[crypto.PubkeyToAddress(senderKeys[i].PublicKey)] = types.GenesisAccount{Balance: funds}
	}

	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger: logger, DataDir: t.TempDir(), Genesis: genesis, CoinbaseKey: coinbaseKey,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		chainID := eat.ChainId()
		deployAuth, err := bind.NewKeyedTransactorWithChainID(coinbaseKey, chainID)
		require.NoError(t, err)
		deployAuth.GasLimit = params.MaxTxnGasLimit
		deployedFactoryAddr, _, factory, err := contracts.DeploySelfDestructFactory(deployAuth, eat.ContractBackend)
		require.NoError(t, err)
		require.Equal(t, factoryAddr, deployedFactoryAddr)
		_, err = eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		disperseAuth, err := bind.NewKeyedTransactorWithChainID(coinbaseKey, chainID)
		require.NoError(t, err)
		disperseAuth.GasLimit = params.MaxTxnGasLimit
		_, _, disperse, err := contracts.DeployDisperse(disperseAuth, eat.ContractBackend)
		require.NoError(t, err)
		_, err = eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		destroyAuth, err := bind.NewKeyedTransactorWithChainID(senderKeys[0], chainID)
		require.NoError(t, err)
		destroyAuth.GasLimit = 1_000_000
		destroyAuth.GasPrice = big.NewInt(5_000_000_000)
		destroyTx, err := factory.Deploy(destroyAuth, salt)
		require.NoError(t, err)

		fundAuth, err := bind.NewKeyedTransactorWithChainID(senderKeys[1], chainID)
		require.NoError(t, err)
		fundAuth.GasLimit = 1_000_000
		fundAuth.GasPrice = big.NewInt(4_000_000_000)
		fundAuth.Value = big.NewInt(7)
		fundTx, err := disperse.DisperseEther(fundAuth, []common.Address{coinbase}, []*big.Int{big.NewInt(7)})
		require.NoError(t, err)

		recreateAuth, err := bind.NewKeyedTransactorWithChainID(senderKeys[2], chainID)
		require.NoError(t, err)
		recreateAuth.GasLimit = 1_000_000
		recreateAuth.GasPrice = big.NewInt(3_000_000_000)
		recreateTx, err := factory.Deploy(recreateAuth, salt)
		require.NoError(t, err)

		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		gotHashes := make([]common.Hash, len(payload.ExecutionPayload.Transactions))
		for i, encoded := range payload.ExecutionPayload.Transactions {
			txn, err := types.DecodeTransaction(encoded)
			require.NoError(t, err)
			gotHashes[i] = txn.Hash()
		}
		require.Equal(t, []common.Hash{destroyTx.Hash(), fundTx.Hash(), recreateTx.Hash()}, gotHashes)
	})
}

// TestEngineApiAccountLifecycleFinalizationConsistencyEIP8246 is the Amsterdam
// counterpart of TestEngineApiAccountLifecycleFinalizationConsistency. Under
// EIP-8246 a self-destructed account that still holds a balance is preserved
// (balance-only) rather than deleted, so the tip credited to a same-block
// created+self-destructed coinbase must survive — the builder and the parallel
// validator must still agree on the assembled block.
func TestEngineApiAccountLifecycleFinalizationConsistencyEIP8246(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	require.NotNil(t, genesis.Config.AmsterdamTime, "genesis must keep Amsterdam enabled to exercise the EIP-8246 path")

	salt := [32]byte{}
	factoryAddr := types.CreateAddress(crypto.PubkeyToAddress(coinbaseKey.PublicKey), 0)
	coinbase := create2Addr(factoryAddr, salt, common.FromHex(contracts.SelfDestructInConstructorBin))
	genesis.Coinbase = coinbase

	senderKeys := make([]*ecdsa.PrivateKey, 3)
	funds := new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)
	for i := range senderKeys {
		senderKeys[i], err = crypto.GenerateKey()
		require.NoError(t, err)
		genesis.Alloc[crypto.PubkeyToAddress(senderKeys[i].PublicKey)] = types.GenesisAccount{Balance: funds}
	}

	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger: logger, DataDir: t.TempDir(), Genesis: genesis, CoinbaseKey: coinbaseKey,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		chainID := eat.ChainId()
		deployAuth, err := bind.NewKeyedTransactorWithChainID(coinbaseKey, chainID)
		require.NoError(t, err)
		deployAuth.GasLimit = params.MaxTxnGasLimit
		deployedFactoryAddr, _, factory, err := contracts.DeploySelfDestructFactory(deployAuth, eat.ContractBackend)
		require.NoError(t, err)
		require.Equal(t, factoryAddr, deployedFactoryAddr)
		_, err = eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		disperseAuth, err := bind.NewKeyedTransactorWithChainID(coinbaseKey, chainID)
		require.NoError(t, err)
		disperseAuth.GasLimit = params.MaxTxnGasLimit
		_, _, disperse, err := contracts.DeployDisperse(disperseAuth, eat.ContractBackend)
		require.NoError(t, err)
		_, err = eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		destroyAuth, err := bind.NewKeyedTransactorWithChainID(senderKeys[0], chainID)
		require.NoError(t, err)
		destroyAuth.GasLimit = 1_000_000
		destroyAuth.GasPrice = big.NewInt(5_000_000_000)
		destroyTx, err := factory.Deploy(destroyAuth, salt)
		require.NoError(t, err)

		fundAuth, err := bind.NewKeyedTransactorWithChainID(senderKeys[1], chainID)
		require.NoError(t, err)
		fundAuth.GasLimit = 1_000_000
		fundAuth.GasPrice = big.NewInt(4_000_000_000)
		fundAuth.Value = big.NewInt(7)
		fundTx, err := disperse.DisperseEther(fundAuth, []common.Address{coinbase}, []*big.Int{big.NewInt(7)})
		require.NoError(t, err)

		recreateAuth, err := bind.NewKeyedTransactorWithChainID(senderKeys[2], chainID)
		require.NoError(t, err)
		recreateAuth.GasLimit = 1_000_000
		recreateAuth.GasPrice = big.NewInt(3_000_000_000)
		recreateTx, err := factory.Deploy(recreateAuth, salt)
		require.NoError(t, err)

		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		gotHashes := make([]common.Hash, len(payload.ExecutionPayload.Transactions))
		for i, encoded := range payload.ExecutionPayload.Transactions {
			txn, err := types.DecodeTransaction(encoded)
			require.NoError(t, err)
			gotHashes[i] = txn.Hash()
		}
		require.Equal(t, []common.Hash{destroyTx.Hash(), fundTx.Hash(), recreateTx.Hash()}, gotHashes)

		// EIP-8246: the created+self-destructed+recreated coinbase is preserved as
		// a balance-only account (empty code, non-zero accrued tips + funding), so
		// the delayed tip is credited rather than burned.
		code, err := eat.RpcApiClient.GetCode(coinbase, rpc.LatestBlock)
		require.NoError(t, err)
		require.Empty(t, code, "EIP-8246: self-destructed coinbase is balance-only")
		balance, err := eat.RpcApiClient.GetBalance(coinbase, rpc.LatestBlock)
		require.NoError(t, err)
		require.Positive(t, balance.Sign(), "EIP-8246: coinbase tip must be preserved, not burned")
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

// TestEngineApiEmptyAccountClearingConsistency pins builder/validator agreement
// when one transaction leaves an account empty and a later EIP-7702
// authorization reads its existence in the same block.
func TestEngineApiEmptyAccountClearingConsistency(t *testing.T) {
	for _, tc := range []emptyAccountClearingCase{
		{name: "touch_only", touch: true, parallel: true},
		{name: "authorization_only", authorize: true, parallel: true},
		{name: "combined_serial", touch: true, authorize: true},
		{name: "combined_parallel", touch: true, authorize: true, parallel: true},
		{name: "preexisting_combined_serial", touch: true, authorize: true, preexisting: true},
		{name: "preexisting_combined_parallel", touch: true, authorize: true, preexisting: true, parallel: true},
		{name: "combined_amsterdam_parallel", touch: true, authorize: true, amsterdam: true, parallel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runEmptyAccountClearingCase(t, tc)
		})
	}
}

type emptyAccountClearingCase struct {
	name        string
	touch       bool
	authorize   bool
	preexisting bool
	amsterdam   bool
	parallel    bool
}

func runEmptyAccountClearingCase(t *testing.T, tc emptyAccountClearingCase) {
	prev := dbg.Exec3Parallel
	dbg.Exec3Parallel = tc.parallel
	t.Cleanup(func() { dbg.Exec3Parallel = prev })

	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlError)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	if !tc.amsterdam {
		genesis.Config.AmsterdamTime = nil
	}

	senderKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(senderKey.PublicKey)
	genesis.Alloc[senderAddr] = types.GenesisAccount{
		Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil),
	}

	emptyKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	emptyAddr := crypto.PubkeyToAddress(emptyKey.PublicKey)
	if tc.preexisting {
		genesis.Alloc[emptyAddr] = types.GenesisAccount{Balance: new(big.Int)}
	}

	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger: logger, DataDir: t.TempDir(), Genesis: genesis, CoinbaseKey: coinbaseKey,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		chainID := eat.ChainId()
		signer := types.LatestSignerForChainID(chainID)
		rpcClient := eat.Transactor.RpcClient()
		feeCap, tipCap := uint256.NewInt(1_000_000_000), uint256.NewInt(1_000_000_000)
		gasLimit := func(preAmsterdam uint64) uint64 {
			if tc.amsterdam {
				return params.MaxTxnGasLimit
			}
			return preAmsterdam
		}

		send := func(txn types.Transaction) {
			signed, err := types.SignTx(txn, *signer, senderKey)
			require.NoError(t, err)
			_, err = rpcClient.SendTransaction(signed)
			require.NoError(t, err)
		}

		// Deploy in its own block so EIP-6780 keeps the contract alive and the
		// SELFDESTRUCT only forwards its (zero) balance.
		// runtime: PUSH20 <empty account> ; SELFDESTRUCT
		// init:    PUSH22 <runtime> ; PUSH1 0 ; MSTORE ; PUSH1 0x16 ; PUSH1 0x0a ; RETURN
		initCode := append([]byte{0x75, 0x73}, emptyAddr[:]...)
		initCode = append(initCode, 0xff, 0x60, 0x00, 0x52, 0x60, 0x16, 0x60, 0x0a, 0xf3)
		send(&types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: 0, GasLimit: gasLimit(200_000), Data: initCode},
			ChainID:  *chainID, TipCap: *tipCap, FeeCap: *feeCap,
		})
		_, err = eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		selfDestructor := types.CreateAddress(senderAddr, 0)
		code, err := rpcClient.GetCode(selfDestructor, rpc.LatestBlock)
		require.NoError(t, err)
		require.NotEmpty(t, code)

		nonce := uint64(1)
		if tc.touch {
			send(&types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{Nonce: nonce, GasLimit: gasLimit(200_000), To: &selfDestructor},
				ChainID:  *chainID, TipCap: *tipCap, FeeCap: *feeCap,
			})
			nonce++
		}
		if tc.authorize {
			send(&types.SetCodeTransaction{
				DynamicFeeTransaction: types.DynamicFeeTransaction{
					CommonTx: types.CommonTx{Nonce: nonce, GasLimit: gasLimit(500_000), To: &senderAddr},
					ChainID:  *chainID, TipCap: *tipCap, FeeCap: *feeCap,
				},
				Authorizations: []types.Authorization{
					signAuthorization(t, emptyKey, chainID, common.Address{0xaa}, 0),
				},
			})
		}

		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		if tc.amsterdam {
			want := uint64(params.StateGasNewAccount + params.StateGasAuthBase)
			require.Equal(t, want, uint64(payload.ExecutionPayload.GasUsed))
			return
		}

		var want uint64
		if tc.touch {
			want += 28603 // 21000 + PUSH20 + SELFDESTRUCT + cold beneficiary
		}
		if tc.authorize {
			want += 46000 // 21000 + PerEmptyAccountCost, no existing-account refund
		}
		require.Equal(t, want, uint64(payload.ExecutionPayload.GasUsed))
	})
}

// TestEngineApiEmptyCoinbaseTipped pins builder/validator agreement when a
// transaction touches an empty fee recipient. EIP-161 judges emptiness after the
// tip lands, so the transaction's own write-set — which predates it — must not
// settle the fee recipient's fate either way: it survives a tip and is cleared
// without one.
func TestEngineApiEmptyCoinbaseTipped(t *testing.T) {
	for _, tc := range []emptyCoinbaseCase{
		{name: "touched_serial", touchFeeRecipient: true},
		{name: "touched_parallel", touchFeeRecipient: true, parallel: true},
		{name: "untouched_parallel", parallel: true},
		{name: "touched_zero_tip_serial", touchFeeRecipient: true, zeroTip: true},
		{name: "touched_zero_tip_parallel", touchFeeRecipient: true, zeroTip: true, parallel: true},
		{name: "touched_amsterdam_serial", touchFeeRecipient: true, amsterdam: true},
		{name: "touched_amsterdam_parallel", touchFeeRecipient: true, amsterdam: true, parallel: true},
		{name: "touched_zero_tip_amsterdam_parallel", touchFeeRecipient: true, zeroTip: true, amsterdam: true, parallel: true},
		{name: "system_address_zero_tip_amsterdam_serial", zeroTip: true, systemAddress: true, amsterdam: true},
		{name: "system_address_zero_tip_amsterdam_parallel", zeroTip: true, systemAddress: true, amsterdam: true, parallel: true},
		{name: "touched_zero_tip_then_authorized_amsterdam_serial", touchFeeRecipient: true, zeroTip: true, authorizeFeeRecipient: true, amsterdam: true},
		{name: "touched_zero_tip_then_authorized_amsterdam_parallel", touchFeeRecipient: true, zeroTip: true, authorizeFeeRecipient: true, amsterdam: true, parallel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runEmptyCoinbaseCase(t, tc)
		})
	}
}

type emptyCoinbaseCase struct {
	name                  string
	touchFeeRecipient     bool
	zeroTip               bool
	systemAddress         bool
	authorizeFeeRecipient bool
	amsterdam             bool
	parallel              bool
}

func runEmptyCoinbaseCase(t *testing.T, tc emptyCoinbaseCase) {
	touchFeeRecipient, zeroTip, parallel := tc.touchFeeRecipient, tc.zeroTip, tc.parallel
	prev := dbg.Exec3Parallel
	dbg.Exec3Parallel = parallel
	t.Cleanup(func() { dbg.Exec3Parallel = prev })

	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlError)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	if !tc.amsterdam {
		genesis.Config.AmsterdamTime = nil
	}

	feeRecipient := genesis.Coinbase
	if tc.systemAddress {
		feeRecipient = params.SystemAddress.Value()
		genesis.Coinbase = feeRecipient
	}
	genesis.Alloc[feeRecipient] = types.GenesisAccount{Balance: new(big.Int)}

	senderKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(senderKey.PublicKey)
	genesis.Alloc[senderAddr] = types.GenesisAccount{
		Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil),
	}

	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger: logger, DataDir: t.TempDir(), Genesis: genesis, CoinbaseKey: coinbaseKey,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		chainID := eat.ChainId()
		signer := types.LatestSignerForChainID(chainID)
		rpcClient := eat.Transactor.RpcClient()

		to := &senderAddr
		if touchFeeRecipient {
			to = &feeRecipient
		}
		tipCap := uint256.NewInt(1_000_000_000)
		if zeroTip {
			tipCap = uint256.NewInt(0)
		}
		txn, err := types.SignTx(&types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: 0, GasLimit: 100_000, To: to},
			ChainID:  *chainID,
			TipCap:   *tipCap,
			FeeCap:   *uint256.NewInt(1_000_000_000),
		}, *signer, senderKey)
		require.NoError(t, err)
		_, err = rpcClient.SendTransaction(txn)
		require.NoError(t, err)

		if tc.authorizeFeeRecipient {
			txn, err = types.SignTx(&types.SetCodeTransaction{
				DynamicFeeTransaction: types.DynamicFeeTransaction{
					CommonTx: types.CommonTx{Nonce: 1, GasLimit: params.MaxTxnGasLimit, To: &senderAddr},
					ChainID:  *chainID,
					TipCap:   *uint256.NewInt(0),
					FeeCap:   *uint256.NewInt(1_000_000_000),
				},
				Authorizations: []types.Authorization{
					signAuthorization(t, coinbaseKey, chainID, common.Address{0xaa}, 0),
				},
			}, *signer, senderKey)
			require.NoError(t, err)
			_, err = rpcClient.SendTransaction(txn)
			require.NoError(t, err)
		}

		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		wantTxnCount := 1
		if tc.authorizeFeeRecipient {
			wantTxnCount++
		}
		require.Len(t, payload.ExecutionPayload.Transactions, wantTxnCount)

		if tc.amsterdam {
			// EIP-7928: the assembler's and the validator's account lists must
			// agree on the fee recipient too.
			bal := decodeAndValidateBAL(t, payload)
			block, err := eat.RpcApiClient.GetBlockByNumber(ctx, rpc.BlockNumber(payload.ExecutionPayload.BlockNumber), false)
			require.NoError(t, err)
			require.NotNil(t, block.BlockAccessListHash)
			require.Equal(t, bal.Hash(), *block.BlockAccessListHash)
			if tc.systemAddress {
				changes := findAccountChanges(bal, params.SystemAddress)
				require.NotNil(t, changes)
				require.Empty(t, changes.StorageChanges)
				require.Empty(t, changes.StorageReads)
				require.Empty(t, changes.BalanceChanges)
				require.Empty(t, changes.NonceChanges)
				require.Empty(t, changes.CodeChanges)
			}
		}
		if tc.authorizeFeeRecipient {
			wantGasUsed := uint64(params.StateGasNewAccount + params.StateGasAuthBase)
			require.Equal(t, wantGasUsed, uint64(payload.ExecutionPayload.GasUsed))
			code, err := rpcClient.GetCode(feeRecipient, rpc.LatestBlock)
			require.NoError(t, err)
			require.NotEmpty(t, code)
		}

		balance, err := rpcClient.GetBalance(feeRecipient, rpc.LatestBlock)
		require.NoError(t, err)
		if zeroTip {
			require.Zero(t, balance.Sign(), "an untipped fee recipient must not receive a balance")
			return
		}
		require.Positive(t, balance.Sign(), "fee recipient must keep the tip it was credited")
	})
}

// TestEngineApiZeroAmountWithdrawal pins the block access list of a zero-amount
// withdrawal: EIP-7928 lists the recipient regardless of the amount, with no
// balance change, and the validator must accept the block it built. An empty
// recipient is also cleared per EIP-161, whether fresh or pre-existing.
func TestEngineApiZeroAmountWithdrawal(t *testing.T) {
	for _, tc := range []zeroWithdrawalCase{
		{name: "preexisting_serial", preexisting: true},
		{name: "preexisting_parallel", preexisting: true, parallel: true},
		{name: "fresh_serial"},
		{name: "fresh_parallel", parallel: true},
		{name: "funded_serial", funded: true},
		{name: "funded_parallel", funded: true, parallel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runZeroWithdrawalCase(t, tc)
		})
	}
}

type zeroWithdrawalCase struct {
	name        string
	preexisting bool
	funded      bool
	parallel    bool
}

func runZeroWithdrawalCase(t *testing.T, tc zeroWithdrawalCase) {
	prev := dbg.Exec3Parallel
	dbg.Exec3Parallel = tc.parallel
	t.Cleanup(func() { dbg.Exec3Parallel = prev })

	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlError)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)

	target := common.HexToAddress("0x2222222222222222222222222222222222222222")
	if tc.preexisting {
		genesis.Alloc[target] = types.GenesisAccount{Balance: new(big.Int)}
	}
	if tc.funded {
		genesis.Alloc[target] = types.GenesisAccount{Balance: big.NewInt(1)}
	}

	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger: logger, DataDir: t.TempDir(), Genesis: genesis, CoinbaseKey: coinbaseKey,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx, engineapitester.WithWithdrawals([]*types.Withdrawal{
			{Index: 0, Validator: 0, Address: target, Amount: 0},
		}))
		require.NoError(t, err)

		bal := decodeAndValidateBAL(t, payload)
		changes := findAccountChanges(bal, accounts.InternAddress(target))
		require.NotNil(t, changes, "a withdrawal recipient must appear in the block access list")
		require.Empty(t, changes.BalanceChanges,
			"a zero-amount withdrawal must not produce a balance change")
		block, err := eat.RpcApiClient.GetBlockByNumber(ctx, rpc.BlockNumber(payload.ExecutionPayload.BlockNumber), false)
		require.NoError(t, err)
		require.NotNil(t, block.BlockAccessListHash)
		require.Equal(t, bal.Hash(), *block.BlockAccessListHash)

		balance, err := eat.Transactor.RpcClient().GetBalance(target, rpc.LatestBlock)
		require.NoError(t, err)
		if tc.funded {
			require.Equal(t, int64(1), balance.Int64(), "the recipient's balance must be unchanged")
			return
		}
		require.Zero(t, balance.Sign(), "an account left empty must not gain a balance")
	})
}
