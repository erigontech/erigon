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

package engineapi_test

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/protocol/params"
	stateContracts "github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc/contracts"
)

// TestEngineApiBALMultiSenderBlock packs transfers from many independent senders
// into a single block. Because the senders are independent the parallel executor
// speculatively executes them concurrently, exercising the coinbase-balance
// strip→rebase→merge path in finalizeWithIBS. Any divergence between the
// assembler's BAL (sequential) and the parallel executor's BAL surfaces as a
// BAL hash mismatch returned by ProcessBAL.
func TestEngineApiBALMultiSenderBlock(t *testing.T) {
	if !dbg.Exec3Parallel {
		t.Skip("requires parallel exec")
	}
	const numSenders = 10
	senderKeys := make([]*ecdsa.PrivateKey, numSenders)
	for i := range senderKeys {
		key, err := crypto.GenerateKey()
		require.NoError(t, err)
		senderKeys[i] = key
	}

	genesis, coinbaseKey := engineapitester.DefaultEngineApiTesterGenesis(t)
	for _, key := range senderKeys {
		addr := crypto.PubkeyToAddress(key.PublicKey)
		genesis.Alloc[addr] = types.GenesisAccount{
			Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil), // 100 ETH each
		}
	}

	eat := engineapitester.InitialiseEngineApiTester(t, engineapitester.EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, log.LvlDebug),
		DataDir:     t.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
	})

	receiver := common.HexToAddress("0xaaaa")

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		// Submit one transfer from each independent sender.
		txHashes := make([]common.Hash, numSenders)
		for i, key := range senderKeys {
			txn, err := eat.Transactor.SubmitSimpleTransfer(key, receiver, big.NewInt(1))
			require.NoError(t, err)
			txHashes[i] = txn.Hash()
		}

		// BuildCanonicalBlock assembles (sequential) then validates via
		// newPayload (parallel executor). A BAL hash mismatch will surface
		// as an INVALID payload status, which BuildCanonicalBlock returns
		// as an error.
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, txHashes...)
		require.NoError(t, err)

		bal := decodeAndValidateBAL(t, payload)

		blockNumber := rpc.BlockNumber(payload.ExecutionPayload.BlockNumber)
		block, err := eat.RpcApiClient.GetBlockByNumber(ctx, blockNumber, false)
		require.NoError(t, err)
		require.NotNil(t, block.BlockAccessListHash)
		require.Equal(t, bal.Hash(), *block.BlockAccessListHash)
	})
}

func TestEngineApiGeneratedPayloadIncludesBlockAccessList(t *testing.T) {
	if !dbg.Exec3Parallel {
		t.Skip("requires parallel exec")
	}
	eat := engineapitester.DefaultEngineApiTester(t)
	receiver := common.HexToAddress("0x333")
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		sender := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)
		txn, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, big.NewInt(1))
		require.NoError(t, err)

		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, txn.Hash())
		require.NoError(t, err)

		balBytes := payload.ExecutionPayload.BlockAccessList
		require.NotNil(t, balBytes)
		require.NotEmpty(t, balBytes)

		bal, err := types.DecodeBlockAccessListBytes(balBytes)
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

		balIndex := uint32(receipt.TransactionIndex + 1)

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

func TestEngineApiBALContractCreation(t *testing.T) {
	if !dbg.Exec3Parallel {
		t.Skip("requires parallel exec")
	}
	eat := engineapitester.DefaultEngineApiTester(t)
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		sender := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)

		// Deploy Token contract via ContractBackend
		auth, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainConfig.ChainID)
		require.NoError(t, err)

		contractAddr, deployTx, _, err := contracts.DeployToken(auth, eat.ContractBackend, sender)
		require.NoError(t, err)

		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, deployTx.Hash())
		require.NoError(t, err)

		bal := decodeAndValidateBAL(t, payload)

		// Verify contract address has CodeChange
		contractChanges := findAccountChanges(bal, accounts.InternAddress(contractAddr))
		require.NotNilf(t, contractChanges, "missing contract account changes\n%s", bal.DebugString())

		receipt, err := eat.RpcApiClient.GetTransactionReceipt(ctx, deployTx.Hash())
		require.NoError(t, err)
		require.NotNil(t, receipt)
		balIndex := uint32(receipt.TransactionIndex + 1)

		codeChange := findCodeChange(contractChanges, balIndex)
		require.NotNilf(t, codeChange, "missing code change at index %d\n%s", balIndex, bal.DebugString())
		require.NotEmpty(t, codeChange.Bytecode, "code change bytecode should not be empty")

		// Verify deployed code matches what's on chain
		deployedCode, err := eat.RpcApiClient.GetCode(contractAddr, rpc.LatestBlock)
		require.NoError(t, err)
		require.Equal(t, []byte(deployedCode), codeChange.Bytecode)

		// Verify sender has balance + nonce changes
		senderChanges := findAccountChanges(bal, accounts.InternAddress(sender))
		require.NotNilf(t, senderChanges, "missing sender account changes\n%s", bal.DebugString())
		require.NotNil(t, findBalanceChange(senderChanges, balIndex), "missing sender balance change")
		require.NotNil(t, findNonceChange(senderChanges, balIndex), "missing sender nonce change")
	})
}

func TestEngineApiBALStorageWrites(t *testing.T) {
	if !dbg.Exec3Parallel {
		t.Skip("requires parallel exec")
	}
	eat := engineapitester.DefaultEngineApiTester(t)
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		sender := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)
		mintReceiver := common.HexToAddress("0x444")

		// Block 1: deploy contract
		auth, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainConfig.ChainID)
		require.NoError(t, err)
		contractAddr, _, tokenContract, err := contracts.DeployToken(auth, eat.ContractBackend, sender)
		require.NoError(t, err)
		_, err = eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		// Block 2: mint tokens (writes to storage: balanceOf[receiver], totalSupply)
		auth, err = bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainConfig.ChainID)
		require.NoError(t, err)
		mintTx, err := tokenContract.Mint(auth, mintReceiver, big.NewInt(1000))
		require.NoError(t, err)

		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, mintTx.Hash())
		require.NoError(t, err)

		bal := decodeAndValidateBAL(t, payload)

		// Contract account should have storage changes from mint
		contractChanges := findAccountChanges(bal, accounts.InternAddress(contractAddr))
		require.NotNilf(t, contractChanges, "missing contract account changes\n%s", bal.DebugString())
		require.NotEmpty(t, contractChanges.StorageChanges,
			"contract should have storage changes from mint\n%s", bal.DebugString())

		receipt, err := eat.RpcApiClient.GetTransactionReceipt(ctx, mintTx.Hash())
		require.NoError(t, err)
		balIndex := uint32(receipt.TransactionIndex + 1)

		// Verify at least one storage slot was written at the mint tx index
		foundStorageChange := false
		for _, slotChange := range contractChanges.StorageChanges {
			if findStorageChange(slotChange, balIndex) != nil {
				foundStorageChange = true
				break
			}
		}
		require.Truef(t, foundStorageChange,
			"expected storage change at index %d\n%s", balIndex, bal.DebugString())

		// Sender should have balance + nonce changes
		senderChanges := findAccountChanges(bal, accounts.InternAddress(sender))
		require.NotNilf(t, senderChanges, "missing sender account changes\n%s", bal.DebugString())
		require.NotNil(t, findBalanceChange(senderChanges, balIndex), "missing sender balance change")
		require.NotNil(t, findNonceChange(senderChanges, balIndex), "missing sender nonce change")
	})
}

func TestEngineApiBALMultiTxBlock(t *testing.T) {
	if !dbg.Exec3Parallel {
		t.Skip("requires parallel exec")
	}
	eat := engineapitester.DefaultEngineApiTester(t)
	transferReceiver := common.HexToAddress("0x555")
	mintReceiver := common.HexToAddress("0x666")
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		sender := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)

		// Block 1: deploy contract (needed for block 2)
		auth, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainConfig.ChainID)
		require.NoError(t, err)
		contractAddr, _, tokenContract, err := contracts.DeployToken(auth, eat.ContractBackend, sender)
		require.NoError(t, err)
		_, err = eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		// Block 2: submit multiple transactions of different types
		// Tx 1: simple ETH transfer
		transferTx, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, transferReceiver, big.NewInt(1))
		require.NoError(t, err)

		// Tx 2: mint tokens (contract call with storage writes)
		auth, err = bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainConfig.ChainID)
		require.NoError(t, err)
		mintTx, err := tokenContract.Mint(auth, mintReceiver, big.NewInt(500))
		require.NoError(t, err)

		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, transferTx.Hash(), mintTx.Hash())
		require.NoError(t, err)

		bal := decodeAndValidateBAL(t, payload)

		// Verify block-level BAL hash
		blockNumber := rpc.BlockNumber(payload.ExecutionPayload.BlockNumber)
		block, err := eat.RpcApiClient.GetBlockByNumber(ctx, blockNumber, false)
		require.NoError(t, err)
		require.NotNil(t, block.BlockAccessListHash)
		require.Equal(t, bal.Hash(), *block.BlockAccessListHash)

		// Get receipts to determine BAL indices
		transferReceipt, err := eat.RpcApiClient.GetTransactionReceipt(ctx, transferTx.Hash())
		require.NoError(t, err)
		mintReceipt, err := eat.RpcApiClient.GetTransactionReceipt(ctx, mintTx.Hash())
		require.NoError(t, err)
		transferIdx := uint32(transferReceipt.TransactionIndex + 1)
		mintIdx := uint32(mintReceipt.TransactionIndex + 1)
		require.NotEqual(t, transferIdx, mintIdx, "transactions should have different indices")

		// Sender: should have balance+nonce changes at BOTH indices
		senderChanges := findAccountChanges(bal, accounts.InternAddress(sender))
		require.NotNilf(t, senderChanges, "missing sender changes\n%s", bal.DebugString())
		require.NotNilf(t, findBalanceChange(senderChanges, transferIdx),
			"missing sender balance change at transfer index %d\n%s", transferIdx, bal.DebugString())
		require.NotNilf(t, findBalanceChange(senderChanges, mintIdx),
			"missing sender balance change at mint index %d\n%s", mintIdx, bal.DebugString())
		require.NotNilf(t, findNonceChange(senderChanges, transferIdx),
			"missing sender nonce change at transfer index %d\n%s", transferIdx, bal.DebugString())
		require.NotNilf(t, findNonceChange(senderChanges, mintIdx),
			"missing sender nonce change at mint index %d\n%s", mintIdx, bal.DebugString())

		// Transfer receiver: balance change at transfer index
		receiverChanges := findAccountChanges(bal, accounts.InternAddress(transferReceiver))
		require.NotNilf(t, receiverChanges, "missing transfer receiver changes\n%s", bal.DebugString())
		require.NotNilf(t, findBalanceChange(receiverChanges, transferIdx),
			"missing receiver balance change at index %d\n%s", transferIdx, bal.DebugString())

		// Contract: storage changes at mint index
		contractChanges := findAccountChanges(bal, accounts.InternAddress(contractAddr))
		require.NotNilf(t, contractChanges, "missing contract changes\n%s", bal.DebugString())
		foundStorageChange := false
		for _, slotChange := range contractChanges.StorageChanges {
			if findStorageChange(slotChange, mintIdx) != nil {
				foundStorageChange = true
				break
			}
		}
		require.Truef(t, foundStorageChange,
			"expected contract storage change at mint index %d\n%s", mintIdx, bal.DebugString())

		// Verify final balances match BAL entries
		senderBalance, err := eat.RpcApiClient.GetBalance(sender, rpc.LatestBlock)
		require.NoError(t, err)
		expectedSenderBal, overflow := uint256.FromBig(senderBalance)
		require.False(t, overflow)

		// The last BAL entry for the sender should reflect the final balance
		lastSenderBalChange := findBalanceChange(senderChanges, mintIdx)
		require.NotNil(t, lastSenderBalChange)
		require.Truef(t, lastSenderBalChange.Value.Eq(expectedSenderBal),
			"sender balance mismatch: BAL=%s expected=%s", lastSenderBalChange.Value.Hex(), expectedSenderBal.Hex())
	})
}

// TestEngineApiBALMixedBlock packs a variety of transaction types into a single block
// to exercise multiple state write paths through the BAL:
//   - Simple ETH transfer (balance, nonce)
//   - Contract deployment (code change)
//   - Contract call with storage writes (storage changes)
//   - Withdrawals (CL-side balance increases via system transaction)
//   - System transactions at block boundaries (beacon root contract)
//
// The test uses two blocks:
//   - Block 1: deploy Changer contract
//   - Block 2: ETH transfer + Token deploy + Changer.Change() + withdrawals
func TestEngineApiBALMixedBlock(t *testing.T) {
	eat := engineapitester.DefaultEngineApiTester(t)
	transferReceiver := common.HexToAddress("0x777")
	withdrawalReceiver := common.HexToAddress("0x888")
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		sender := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)
		chainId := eat.ChainConfig.ChainID

		// Block 1: deploy Changer contract (need it alive for block 2 storage writes)
		auth, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, chainId)
		require.NoError(t, err)
		auth.GasLimit = params.MaxTxnGasLimit
		changerAddr, _, changerContract, err := stateContracts.DeployChanger(auth, eat.ContractBackend)
		require.NoError(t, err)
		_, err = eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		// --- Block 2: the mixed block ---

		// Tx A: simple ETH transfer
		transferTx, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, transferReceiver, big.NewInt(42))
		require.NoError(t, err)

		// Tx B: deploy Token contract (contract creation with code change)
		auth, err = bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, chainId)
		require.NoError(t, err)
		tokenAddr, deployTx, _, err := contracts.DeployToken(auth, eat.ContractBackend, sender)
		require.NoError(t, err)

		// Tx C: call Changer.Change() — writes storage (x=1, y=2, z=3)
		auth, err = bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, chainId)
		require.NoError(t, err)
		auth.GasLimit = params.MaxTxnGasLimit
		changeTx, err := changerContract.Change(auth)
		require.NoError(t, err)

		// Build block with withdrawals
		withdrawals := []*types.Withdrawal{
			{Index: 0, Validator: 1, Address: withdrawalReceiver, Amount: 1000}, // 1000 Gwei
		}
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx, engineapitester.WithWithdrawals(withdrawals))
		require.NoError(t, err)

		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload,
			transferTx.Hash(), deployTx.Hash(), changeTx.Hash())
		require.NoError(t, err)

		bal := decodeAndValidateBAL(t, payload)

		// Verify block hash includes BAL
		blockNumber := rpc.BlockNumber(payload.ExecutionPayload.BlockNumber)
		block, err := eat.RpcApiClient.GetBlockByNumber(ctx, blockNumber, false)
		require.NoError(t, err)
		require.NotNil(t, block.BlockAccessListHash)
		require.Equal(t, bal.Hash(), *block.BlockAccessListHash)

		// Get receipts to determine BAL indices
		transferReceipt, err := eat.RpcApiClient.GetTransactionReceipt(ctx, transferTx.Hash())
		require.NoError(t, err)
		deployReceipt, err := eat.RpcApiClient.GetTransactionReceipt(ctx, deployTx.Hash())
		require.NoError(t, err)
		changeReceipt, err := eat.RpcApiClient.GetTransactionReceipt(ctx, changeTx.Hash())
		require.NoError(t, err)

		transferIdx := uint32(transferReceipt.TransactionIndex + 1)
		deployIdx := uint32(deployReceipt.TransactionIndex + 1)
		changeIdx := uint32(changeReceipt.TransactionIndex + 1)

		// --- Verify sender: balance+nonce changes at all tx indices ---
		senderChanges := findAccountChanges(bal, accounts.InternAddress(sender))
		require.NotNilf(t, senderChanges, "missing sender changes\n%s", bal.DebugString())
		for _, idx := range []uint32{transferIdx, deployIdx, changeIdx} {
			require.NotNilf(t, findBalanceChange(senderChanges, idx),
				"missing sender balance change at index %d", idx)
			require.NotNilf(t, findNonceChange(senderChanges, idx),
				"missing sender nonce change at index %d", idx)
		}

		// --- Verify transfer receiver: balance change ---
		receiverChanges := findAccountChanges(bal, accounts.InternAddress(transferReceiver))
		require.NotNilf(t, receiverChanges, "missing transfer receiver\n%s", bal.DebugString())
		require.NotNil(t, findBalanceChange(receiverChanges, transferIdx))

		// --- Verify Token contract: code change at deploy index ---
		tokenChanges := findAccountChanges(bal, accounts.InternAddress(tokenAddr))
		require.NotNilf(t, tokenChanges, "missing token contract\n%s", bal.DebugString())
		require.NotNil(t, findCodeChange(tokenChanges, deployIdx), "missing code change for token deploy")

		// --- Verify Changer contract: storage changes at change index ---
		changerChanges := findAccountChanges(bal, accounts.InternAddress(changerAddr))
		require.NotNilf(t, changerChanges, "missing changer contract\n%s", bal.DebugString())
		foundStorageAtChange := false
		for _, slotChange := range changerChanges.StorageChanges {
			if findStorageChange(slotChange, changeIdx) != nil {
				foundStorageAtChange = true
				break
			}
		}
		require.Truef(t, foundStorageAtChange,
			"expected storage change at change index %d\n%s", changeIdx, bal.DebugString())

		// --- Verify withdrawal receiver: balance change ---
		withdrawalChanges := findAccountChanges(bal, accounts.InternAddress(withdrawalReceiver))
		require.NotNilf(t, withdrawalChanges, "missing withdrawal receiver\n%s", bal.DebugString())
		require.NotEmpty(t, withdrawalChanges.BalanceChanges, "withdrawal receiver should have balance changes")

		withdrawalBalance, err := eat.RpcApiClient.GetBalance(withdrawalReceiver, rpc.LatestBlock)
		require.NoError(t, err)
		expectedWithdrawalWei := new(big.Int).Mul(big.NewInt(1000), big.NewInt(1e9))
		require.Equal(t, expectedWithdrawalWei, withdrawalBalance)

		// --- Verify system contracts appear in BAL ---
		beaconRootChanges := findAccountChanges(bal, params.BeaconRootsAddress)
		require.NotNilf(t, beaconRootChanges,
			"beacon root contract should appear in BAL (system tx at block start)\n%s", bal.DebugString())
		require.NotEmpty(t, beaconRootChanges.StorageChanges,
			"beacon root contract should have storage changes")
	})
}

// TestEngineApiBALParallelConsistencyStress exercises the parallel executor's
// BAL computation under heavy concurrent write pressure to surface any
// divergence from the block assembler's BAL (proposer side, serial). The
// engineapi round-trip — BuildCanonicalBlock → InsertNewPayload → validate —
// fails with an INVALID payload status if the two BALs disagree, converting
// cross-path non-determinism into a test failure.
//
// Pattern mirrors the spamoor workload on the bal-devnet-3 Kurtosis cluster
// where BAL mismatches have been observed:
//   - Many independent senders → parallel exec schedules them concurrently.
//   - Mixed tx types (transfer / deploy / storage-write) → different finalize
//     and coinbase-rebase paths per tx.
//   - Multiple blocks → accumulated state, cross-block dependencies.
//   - Shared contract targets → storage-conflict retry in the parallel executor.
//
// If this test flakes, it's the same class of bug that makes the glamsterdam
// assertoor suite fail. Runs under -race should eventually expose non-
// determinism sources that survive a single execution.
func TestEngineApiBALParallelConsistencyStress(t *testing.T) {
	if !dbg.Exec3Parallel {
		t.Skip("requires parallel exec")
	}
	const (
		numSenders = 20
		numBlocks  = 3
	)
	senderKeys := make([]*ecdsa.PrivateKey, numSenders)
	for i := range senderKeys {
		key, err := crypto.GenerateKey()
		require.NoError(t, err)
		senderKeys[i] = key
	}

	genesis, coinbaseKey := engineapitester.DefaultEngineApiTesterGenesis(t)
	for _, key := range senderKeys {
		addr := crypto.PubkeyToAddress(key.PublicKey)
		genesis.Alloc[addr] = types.GenesisAccount{
			Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil), // 100 ETH each
		}
	}

	eat := engineapitester.InitialiseEngineApiTester(t, engineapitester.EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, log.LvlInfo),
		DataDir:     t.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
	})

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		chainId := eat.ChainConfig.ChainID

		// Block 1: deploy a Changer contract used by subsequent blocks for
		// shared-target storage writes (exercises parallel conflict retry).
		auth, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, chainId)
		require.NoError(t, err)
		auth.GasLimit = params.MaxTxnGasLimit
		_, _, sharedChanger, err := stateContracts.DeployChanger(auth, eat.ContractBackend)
		require.NoError(t, err)
		_, err = eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err, "block 1 (deploy shared Changer) must build cleanly")

		for blockIdx := 0; blockIdx < numBlocks; blockIdx++ {
			// Each sender does one simple transfer (fully independent state).
			for i, key := range senderKeys {
				// Distinct receiver per (sender, block) to avoid nonce/collision.
				receiver := common.HexToAddress(fmt.Sprintf("0x%040x", (blockIdx*numSenders+i)+0x1000))
				_, err := eat.Transactor.SubmitSimpleTransfer(key, receiver, big.NewInt(1))
				require.NoErrorf(t, err, "block %d sender %d: submit transfer", blockIdx, i)
			}
			// Coinbase also deploys a Token (CREATE path, emits code change).
			coinbaseAuth, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, chainId)
			require.NoError(t, err)
			_, _, _, err = contracts.DeployToken(coinbaseAuth, eat.ContractBackend, crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey))
			require.NoError(t, err)
			// Coinbase also hits the shared Changer (storage-write conflict target).
			coinbaseAuth, err = bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, chainId)
			require.NoError(t, err)
			coinbaseAuth.GasLimit = params.MaxTxnGasLimit
			_, err = sharedChanger.Change(coinbaseAuth)
			require.NoError(t, err)

			// The payoff: BuildCanonicalBlock assembles (serial, proposer BAL)
			// and then validates via newPayload (parallel, validator BAL).
			// A hash mismatch surfaces here as an error.
			payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
			require.NoErrorf(t, err, "block %d: assembler vs parallel-executor BAL mismatch", blockIdx+2)

			// Also assert the header hash ties out to the decoded BAL body,
			// catching any encoding/decoding asymmetry.
			bal := decodeAndValidateBAL(t, payload)
			blockNumber := rpc.BlockNumber(payload.ExecutionPayload.BlockNumber)
			block, err := eat.RpcApiClient.GetBlockByNumber(ctx, blockNumber, false)
			require.NoError(t, err)
			require.NotNil(t, block.BlockAccessListHash)
			require.Equalf(t, bal.Hash(), *block.BlockAccessListHash,
				"block %d: BAL body hash doesn't match header BAL hash", blockIdx+2)
		}
	})
}

// TestEngineApiBALCreateSSTOREThenSelfdestructInInitCode is a targeted
// regression test for the proposer/validator BAL divergence that manifested
// as the glamsterdam assertoor flake.
//
// Scenario: a single CREATE transaction whose init code does SSTORE followed
// by SELFDESTRUCT (of the just-created contract). The SSTORE's EIP-2200
// gas calculation records a StoragePath read on the new contract; the
// SELFDESTRUCT then marks the state object as deleted. Pre-fix behavior
// differed between the two execution paths:
//
//   - Block assembler (serial, proposer): FinalizeTx iterated
//     sdb.journal.dirties after the tx and, for any stateObject with
//     `so.deleted == true`, dropped its whole versionedReads entry:
//     if so.deleted { delete(sdb.versionedReads, addr) }
//     That removed the new contract's SSTORE-gas-calc read from the
//     per-tx read set before it was merged into balIO.
//
//   - Parallel executor (validator): txtask.Execute calls SoftFinalise +
//     MakeWriteSet, which never had the corresponding delete. The read
//     survived into result.TxIn and ended up in blockIO as a
//     storageReads=[0x01] entry on the ephemeral contract's address.
//
// BuildCanonicalBlock ran the block through both paths (assembler-signed
// header BAL hash, parallel-executor validator BAL hash). Divergence →
// engine_newPayload returned INVALID → this test fails. The fix (remove
// the delete from FinalizeTx) makes both paths retain the read, matching
// EIP-7928's access-list semantics.
//
// The bytecode is 7 bytes of handwritten EVM. Init code:
//
//	6001600155  SSTORE slot 0x01 = 1   (records StoragePath read on slot 0x01)
//	33          CALLER
//	FF          SELFDESTRUCT            (EIP-6780 actually destroys because
//	                                     the contract was created this tx)
//
// There is no deployed code (no `f3` RETURN), so the create returns no
// bytecode — the only output the test needs is the BAL agreement.
func TestEngineApiBALCreateSSTOREThenSelfdestructInInitCode(t *testing.T) {
	if !dbg.Exec3Parallel {
		t.Skip("requires parallel exec")
	}
	eat := engineapitester.DefaultEngineApiTester(t)
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		signer := types.LatestSignerForChainID(eat.ChainConfig.ChainID)
		coinbaseAddr := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)

		nonce, err := eat.RpcApiClient.GetTransactionCount(coinbaseAddr, rpc.PendingBlock)
		require.NoError(t, err)
		gasPrice, err := eat.RpcApiClient.GasPrice()
		require.NoError(t, err)
		gasPriceU256, overflow := uint256.FromBig(gasPrice)
		require.False(t, overflow, "gas price overflows uint256")

		// SSTORE-then-SELFDESTRUCT init code. See the docstring above for
		// the bytecode breakdown.
		initCode := []byte{
			0x60, 0x01, // PUSH1 0x01 (value)
			0x60, 0x01, // PUSH1 0x01 (slot)
			0x55, // SSTORE
			0x33, // CALLER
			0xff, // SELFDESTRUCT
		}

		createTx := &types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce:    nonce.Uint64(),
				GasLimit: 1_000_000,
				To:       nil, // CREATE
				Value:    uint256.Int{},
				Data:     initCode,
			},
			GasPrice: *gasPriceU256,
		}
		signedCreateTx, err := types.SignTx(createTx, *signer, eat.CoinbaseKey)
		require.NoError(t, err)
		_, err = eat.RpcApiClient.SendTransaction(signedCreateTx)
		require.NoError(t, err)

		// BuildCanonicalBlock drives proposer (serial) + validator (parallel)
		// through the full engine_newPayload roundtrip. If their BAL hashes
		// diverge, this call errors with "block access list mismatch".
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err, "BAL divergence: block assembler vs parallel executor")
		require.NoError(t, eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx,
			payload.ExecutionPayload, signedCreateTx.Hash()))

		// Sanity-check the BAL body decodes and validates.
		_ = decodeAndValidateBAL(t, payload)
	})
}

// TestEngineApiBALSelfDestruct tests BAL tracking when a contract self-destructs.
// Exercises storage writes followed by self-destruct in the same block.
func TestEngineApiBALSelfDestruct(t *testing.T) {
	if !dbg.Exec3Parallel {
		t.Skip("requires parallel exec")
	}
	eat := engineapitester.DefaultEngineApiTester(t)
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		chainId := eat.ChainConfig.ChainID

		// Block 1: deploy Selfdestruct contract
		auth, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, chainId)
		require.NoError(t, err)
		auth.GasLimit = params.MaxTxnGasLimit
		sdAddr, _, sdContract, err := stateContracts.DeploySelfdestruct(auth, eat.ContractBackend)
		require.NoError(t, err)
		_, err = eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		sdCode, err := eat.RpcApiClient.GetCode(sdAddr, rpc.LatestBlock)
		require.NoError(t, err)
		require.NotEmpty(t, sdCode, "selfdestruct contract should have code")

		// Block 2: change storage then self-destruct in same block
		auth, err = bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, chainId)
		require.NoError(t, err)
		auth.GasLimit = params.MaxTxnGasLimit
		changeTx, err := sdContract.Change(auth)
		require.NoError(t, err)

		auth, err = bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, chainId)
		require.NoError(t, err)
		auth.GasLimit = params.MaxTxnGasLimit
		destructTx, err := sdContract.Destruct(auth)
		require.NoError(t, err)

		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload,
			changeTx.Hash(), destructTx.Hash())
		require.NoError(t, err)

		bal := decodeAndValidateBAL(t, payload)

		sdChanges := findAccountChanges(bal, accounts.InternAddress(sdAddr))
		require.NotNilf(t, sdChanges, "missing selfdestruct contract\n%s", bal.DebugString())
	})
}

func decodeAndValidateBAL(t *testing.T, payload *engineapitester.MockClPayload) types.BlockAccessList {
	t.Helper()
	balBytes := payload.ExecutionPayload.BlockAccessList
	require.NotNil(t, balBytes)
	require.NotEmpty(t, balBytes)

	bal, err := types.DecodeBlockAccessListBytes(balBytes)
	require.NoError(t, err)
	require.NoError(t, bal.Validate())
	require.NotEmpty(t, bal)
	return bal
}

func findAccountChanges(bal types.BlockAccessList, addr accounts.Address) *types.AccountChanges {
	for _, ac := range bal {
		if ac != nil && ac.Address == addr {
			return ac
		}
	}
	return nil
}

func findBalanceChange(ac *types.AccountChanges, index uint32) *types.BalanceChange {
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

func findNonceChange(ac *types.AccountChanges, index uint32) *types.NonceChange {
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

func findCodeChange(ac *types.AccountChanges, index uint32) *types.CodeChange {
	if ac == nil {
		return nil
	}
	for _, change := range ac.CodeChanges {
		if change != nil && change.Index == index {
			return change
		}
	}
	return nil
}

func findStorageChange(sc *types.SlotChanges, index uint32) *types.StorageChange {
	if sc == nil {
		return nil
	}
	for _, change := range sc.Changes {
		if change != nil && change.Index == index {
			return change
		}
	}
	return nil
}
