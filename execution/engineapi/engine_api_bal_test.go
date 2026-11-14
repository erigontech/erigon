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
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/abi/bind"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
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
// fee-aggregation path in finalizeTxSimple. Any divergence between the
// assembler's BAL (sequential) and the parallel executor's BAL surfaces as a
// BAL hash mismatch returned by bal.Process.
func TestEngineApiBALMultiSenderBlock(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	const numSenders = 10
	senderKeys := make([]*ecdsa.PrivateKey, numSenders)
	for i := range senderKeys {
		key, err := crypto.GenerateKey()
		require.NoError(t, err)
		senderKeys[i] = key
	}

	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	for _, key := range senderKeys {
		addr := crypto.PubkeyToAddress(key.PublicKey)
		genesis.Alloc[addr] = types.GenesisAccount{
			Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil), // 100 ETH each
		}
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
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
	receiver := common.HexToAddress("0x333")
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		sender := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)
		txn, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, big.NewInt(1))
		require.NoError(t, err)

		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, txn.Hash())
		require.NoError(t, err)

		balPtr := payload.ExecutionPayload.BlockAccessList
		require.NotNil(t, balPtr)
		require.NotEmpty(t, *balPtr)

		bal, err := types.DecodeBlockAccessListBytes(*balPtr)
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
		require.True(t, senderBalanceChange.Value.Eq(senderBalance))

		receiverBalanceChange := findBalanceChange(receiverChanges, balIndex)
		require.NotNilf(t, receiverBalanceChange, "missing receiver balance change at index %d\n%s", balIndex, bal.DebugString())
		require.True(t, receiverBalanceChange.Value.Eq(receiverBalance))

		senderNonceChange := findNonceChange(senderChanges, balIndex)
		require.NotNilf(t, senderNonceChange, "missing sender nonce change at index %d\n%s", balIndex, bal.DebugString())
		require.Equal(t, senderNonce.Uint64(), senderNonceChange.Value)
	})
}

func TestEngineApiBALContractCreation(t *testing.T) {
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
			if findStorageChange(&slotChange, balIndex) != nil {
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

// TestEngineApiBALStorageNoOpWriteOmitted exercises the EIP-7928 no-op rule
// end-to-end through the parallel executor: a write storing the value a slot
// already holds must not appear in storage_changes. The contract does
// SSTORE(0,2);SSTORE(0,1) so the final write is recorded even though it equals
// the slot's prior value — a plain same-value SSTORE would be skipped before
// reaching the access list. Called twice in one block, the second call is a
// no-op whose write must be excluded.
func TestEngineApiBALStorageNoOpWriteOmitted(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		signer := types.LatestSignerForChainID(eat.ChainConfig.ChainID)
		coinbaseAddr := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)
		gasPrice, err := eat.RpcApiClient.GasPrice()
		require.NoError(t, err)

		// init: 600b600c600039600b6000f3   -> return the 11-byte runtime
		// code: 6002600055 6001600055 00   -> SSTORE(0,2); SSTORE(0,1); STOP
		initCode := common.FromHex("600b600c600039600b6000f36002600055600160005500")

		signTx := func(nonce uint64, to *common.Address, data []byte, gas uint64) types.Transaction {
			tx, err := types.SignTx(&types.LegacyTx{
				CommonTx: types.CommonTx{Nonce: nonce, GasLimit: gas, To: to, Value: uint256.Int{}, Data: data},
				GasPrice: *gasPrice,
			}, *signer, eat.CoinbaseKey)
			require.NoError(t, err)
			return tx
		}

		// Block 1: deploy the contract.
		deployNonce, err := eat.RpcApiClient.GetTransactionCount(coinbaseAddr, rpc.PendingBlock)
		require.NoError(t, err)
		deployTx := signTx(deployNonce.Uint64(), nil, initCode, 1_000_000)
		_, err = eat.RpcApiClient.SendTransaction(deployTx)
		require.NoError(t, err)
		_, err = eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		contractAddr := types.CreateAddress(coinbaseAddr, deployNonce.Uint64())
		code, err := eat.RpcApiClient.GetCode(contractAddr, rpc.LatestBlock)
		require.NoError(t, err)
		require.NotEmpty(t, code, "contract must be deployed")

		// Block 2: two calls to the contract in the same block. Same sender, so
		// they execute in nonce order — fully deterministic, no scheduling race.
		callNonce, err := eat.RpcApiClient.GetTransactionCount(coinbaseAddr, rpc.PendingBlock)
		require.NoError(t, err)
		firstCall := signTx(callNonce.Uint64(), &contractAddr, nil, 1_000_000)
		secondCall := signTx(callNonce.Uint64()+1, &contractAddr, nil, 1_000_000)
		_, err = eat.RpcApiClient.SendTransaction(firstCall)
		require.NoError(t, err)
		_, err = eat.RpcApiClient.SendTransaction(secondCall)
		require.NoError(t, err)

		// A node accepts its own BAL, so the block stays VALID even without the
		// fix (assembler and parallel validator compute the same wrong BAL); the
		// storage_changes assertion below is what catches the leaked no-op write.
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		require.NoError(t, eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, firstCall.Hash(), secondCall.Hash()))

		bal := decodeAndValidateBAL(t, payload)
		cc := findAccountChanges(bal, accounts.InternAddress(contractAddr))
		require.NotNilf(t, cc, "contract must appear in BAL\n%s", bal.DebugString())
		require.Lenf(t, cc.StorageChanges, 1, "contract writes exactly one slot\n%s", bal.DebugString())
		require.Lenf(t, cc.StorageChanges[0].Changes, 1,
			"slot 0 must record only the first call's real change (0->1); the second call leaves the value unchanged, so the no-op write must not appear in storage_changes\n%s",
			bal.DebugString())
	})
}

func TestEngineApiBALMultiTxBlock(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
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
			if findStorageChange(&slotChange, mintIdx) != nil {
				foundStorageChange = true
				break
			}
		}
		require.Truef(t, foundStorageChange,
			"expected contract storage change at mint index %d\n%s", mintIdx, bal.DebugString())

		// Verify final balances match BAL entries
		expectedSenderBal, err := eat.RpcApiClient.GetBalance(sender, rpc.LatestBlock)
		require.NoError(t, err)

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
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
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
			if findStorageChange(&slotChange, changeIdx) != nil {
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
		expectedWithdrawalWei := uint256.NewInt(1000 * 1e9)
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
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlInfo)
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

	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	for _, key := range senderKeys {
		addr := crypto.PubkeyToAddress(key.PublicKey)
		genesis.Alloc[addr] = types.GenesisAccount{
			Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil), // 100 ETH each
		}
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

		for blockIdx := range numBlocks {
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
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		signer := types.LatestSignerForChainID(eat.ChainConfig.ChainID)
		coinbaseAddr := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)

		nonce, err := eat.RpcApiClient.GetTransactionCount(coinbaseAddr, rpc.PendingBlock)
		require.NoError(t, err)
		gasPrice, err := eat.RpcApiClient.GasPrice()
		require.NoError(t, err)

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
			GasPrice: *gasPrice,
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
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
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

// TestEngineApiBALIncludesSystemAddressOnSelfdestructToItWithZeroBalance asserts
// the EIP-7928 rule that a zero-value SELFDESTRUCT to SystemAddress is still
// recorded in the BAL: the SELFDESTRUCT is a state access on the beneficiary,
// so the entry is present even with no value transferred and every change-set
// empty.
func TestEngineApiBALIncludesSystemAddressOnSelfdestructToItWithZeroBalance(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)

	senderKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	eat := newSelfdestructBALTester(ctx, t, logger, crypto.PubkeyToAddress(senderKey.PublicKey))
	require.True(t, eat.ChainConfig.IsAmsterdam(0), "Amsterdam must be active for EIP-7928")

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		initCode := pushAddrSelfdestruct(params.SystemAddress.Value())
		createTx := signCreateTx(t, eat, senderKey, uint256.Int{}, initCode)
		_, err := eat.RpcApiClient.SendTransaction(createTx)
		require.NoError(t, err)

		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		require.NoError(t, eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, createTx.Hash()))

		// The init code runs SELFDESTRUCT before returning any deploy bytes,
		// so the CREATE still succeeds and deploys no code.
		receipt, err := eat.RpcApiClient.GetTransactionReceipt(ctx, createTx.Hash())
		require.NoError(t, err)
		require.Equal(t, types.ReceiptStatusSuccessful, receipt.Status,
			"SELFDESTRUCT-in-init-code CREATE tx should succeed")

		bal := decodeAndValidateBAL(t, payload)

		sysEntry := findAccountChanges(bal, params.SystemAddress)
		require.NotNilf(t, sysEntry,
			"BAL must include a SystemAddress entry: EIP-7928 records SELFDESTRUCT as an access on the beneficiary even when no value is transferred\n%s",
			bal.DebugString())

		require.Empty(t, sysEntry.StorageChanges, "SystemAddress entry should have no storage changes")
		require.Empty(t, sysEntry.StorageReads, "SystemAddress entry should have no storage reads")
		require.Empty(t, sysEntry.BalanceChanges, "SystemAddress entry should have no balance changes (zero transfer)")
		require.Empty(t, sysEntry.NonceChanges, "SystemAddress entry should have no nonce changes")
		require.Empty(t, sysEntry.CodeChanges, "SystemAddress entry should have no code changes")
	})
}

// TestEngineApiBALIncludesSystemAddressOnSelfdestructToItWithNonZeroBalance
// guards the non-zero variant: the SystemAddress beneficiary records a balance
// change in addition to the access already recorded by the SELFDESTRUCT.
func TestEngineApiBALIncludesSystemAddressOnSelfdestructToItWithNonZeroBalance(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)

	senderKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	eat := newSelfdestructBALTester(ctx, t, logger, crypto.PubkeyToAddress(senderKey.PublicKey))

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		// Same init code as the zero-balance test, but the CREATE tx sends
		// 1 wei to the new contract so its balance is non-zero at SELFDESTRUCT.
		initCode := pushAddrSelfdestruct(params.SystemAddress.Value())
		createTx := signCreateTx(t, eat, senderKey, *uint256.NewInt(1), initCode)
		_, err := eat.RpcApiClient.SendTransaction(createTx)
		require.NoError(t, err)

		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		require.NoError(t, eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, createTx.Hash()))

		receipt, err := eat.RpcApiClient.GetTransactionReceipt(ctx, createTx.Hash())
		require.NoError(t, err)
		require.Equal(t, types.ReceiptStatusSuccessful, receipt.Status)

		bal := decodeAndValidateBAL(t, payload)

		sysEntry := findAccountChanges(bal, params.SystemAddress)
		require.NotNilf(t, sysEntry, "BAL must include SystemAddress entry on non-zero-balance SELFDESTRUCT to it\n%s", bal.DebugString())
		require.NotEmptyf(t, sysEntry.BalanceChanges,
			"BAL must record a balance change on the SystemAddress beneficiary when SELFDESTRUCT transfers non-zero value to it\n%s", bal.DebugString())
	})
}

// TestEngineApiBALIncludesSystemAddressOnZeroAmountWithdrawal asserts the
// EIP-7928 rule that a withdrawal to SystemAddress records it in the BAL even
// when the amount is zero: the withdrawal is a state access, and SystemAddress
// is excluded only as the caller of system calls (ethereum/execution-specs#3681).
func TestEngineApiBALIncludesSystemAddressOnZeroAmountWithdrawal(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)

	senderKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	eat := newSelfdestructBALTester(ctx, t, logger, crypto.PubkeyToAddress(senderKey.PublicKey))

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		withdrawals := []*types.Withdrawal{
			{Index: 0, Validator: 0, Address: params.SystemAddress.Value(), Amount: 0},
		}
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx, engineapitester.WithWithdrawals(withdrawals))
		require.NoError(t, err)

		bal := decodeAndValidateBAL(t, payload)

		sysEntry := findAccountChanges(bal, params.SystemAddress)
		require.NotNilf(t, sysEntry,
			"BAL must include SystemAddress when it is a withdrawal recipient, even with a zero amount\n%s",
			bal.DebugString())
		require.Empty(t, sysEntry.StorageChanges, "SystemAddress entry should have no storage changes")
		require.Empty(t, sysEntry.StorageReads, "SystemAddress entry should have no storage reads")
		require.Empty(t, sysEntry.BalanceChanges, "SystemAddress entry should have no balance changes (zero amount)")
		require.Empty(t, sysEntry.NonceChanges, "SystemAddress entry should have no nonce changes")
		require.Empty(t, sysEntry.CodeChanges, "SystemAddress entry should have no code changes")
	})
}

// TestEngineApiBALIncludesSystemAddressAsDelegationTarget asserts the EIP-7928
// rule that calling an EOA delegated (EIP-7702) to SystemAddress records
// SystemAddress in the BAL: resolving the delegation loads the target's code,
// which is a state access (ethereum/execution-specs#3681).
func TestEngineApiBALIncludesSystemAddressAsDelegationTarget(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)

	senderKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(senderKey.PublicKey)
	eat := newSelfdestructBALTester(ctx, t, logger, senderAddr)

	authorityKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	authority := crypto.PubkeyToAddress(authorityKey.PublicKey)

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		chainID := eat.ChainId()
		signer := types.LatestSignerForChainID(chainID)
		feeCap := uint256.NewInt(1_000_000_000)
		nonce, err := eat.RpcApiClient.GetTransactionCount(senderAddr, rpc.PendingBlock)
		require.NoError(t, err)

		auth, err := types.SignAuthorization(authorityKey, *chainID, params.SystemAddress.Value(), 0)
		require.NoError(t, err)
		tx, err := types.SignTx(&types.SetCodeTransaction{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{Nonce: nonce.Uint64(), GasLimit: 500_000, To: &authority},
				ChainID:  *chainID, TipCap: *feeCap, FeeCap: *feeCap,
			},
			Authorizations: []types.Authorization{auth},
		}, *signer, senderKey)
		require.NoError(t, err)
		_, err = eat.RpcApiClient.SendTransaction(tx)
		require.NoError(t, err)

		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		require.NoError(t, eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, tx.Hash()))

		bal := decodeAndValidateBAL(t, payload)

		authorityEntry := findAccountChanges(bal, accounts.InternAddress(authority))
		require.NotNilf(t, authorityEntry, "BAL must include the authority\n%s", bal.DebugString())
		require.NotEmptyf(t, authorityEntry.CodeChanges, "authority must record the delegation code change\n%s", bal.DebugString())

		sysEntry := findAccountChanges(bal, params.SystemAddress)
		require.NotNilf(t, sysEntry,
			"BAL must include SystemAddress when a called EOA delegates to it\n%s", bal.DebugString())
		require.Empty(t, sysEntry.StorageChanges, "SystemAddress entry should have no storage changes")
		require.Empty(t, sysEntry.StorageReads, "SystemAddress entry should have no storage reads")
		require.Empty(t, sysEntry.BalanceChanges, "SystemAddress entry should have no balance changes")
		require.Empty(t, sysEntry.NonceChanges, "SystemAddress entry should have no nonce changes")
		require.Empty(t, sysEntry.CodeChanges, "SystemAddress entry should have no code changes")
	})
}

// TestEngineApiBALIncludesOrdinaryBeneficiaryOnSelfdestructWithZeroBalance
// guards that ordinary EOA beneficiaries also appear in the BAL on a
// zero-balance SELFDESTRUCT.
func TestEngineApiBALIncludesOrdinaryBeneficiaryOnSelfdestructWithZeroBalance(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)

	senderKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	eat := newSelfdestructBALTester(ctx, t, logger, crypto.PubkeyToAddress(senderKey.PublicKey))

	// An arbitrary non-SystemAddress beneficiary EOA, distinct from the
	// sender / coinbase / system contracts so it is identifiable in the BAL.
	beneficiary := common.HexToAddress("0x00000000000000000000000000000000000000aa")

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		initCode := pushAddrSelfdestruct(beneficiary)
		createTx := signCreateTx(t, eat, senderKey, uint256.Int{}, initCode)
		_, err := eat.RpcApiClient.SendTransaction(createTx)
		require.NoError(t, err)

		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		require.NoError(t, eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, createTx.Hash()))

		receipt, err := eat.RpcApiClient.GetTransactionReceipt(ctx, createTx.Hash())
		require.NoError(t, err)
		require.Equal(t, types.ReceiptStatusSuccessful, receipt.Status)

		bal := decodeAndValidateBAL(t, payload)
		require.NotNilf(t, findAccountChanges(bal, accounts.InternAddress(beneficiary)),
			"ordinary EOA beneficiary must appear in BAL on SELFDESTRUCT-to-it\n%s", bal.DebugString())
	})
}

// newSelfdestructBALTester brings up an engine API tester whose genesis funds
// senderAddr, used by the SELFDESTRUCT-beneficiary BAL tests below.
func newSelfdestructBALTester(ctx context.Context, t *testing.T, logger log.Logger, senderAddr common.Address) engineapitester.EngineApiTester {
	t.Helper()
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
	t.Cleanup(func() { require.NoError(t, eat.Close()) })
	return eat
}

// signCreateTx builds and signs a CREATE (To==nil) legacy transaction carrying
// initCode, using the sender's current pending nonce and the node's gas price.
func signCreateTx(t *testing.T, eat engineapitester.EngineApiTester, key *ecdsa.PrivateKey, value uint256.Int, initCode []byte) types.Transaction {
	t.Helper()
	signer := types.LatestSignerForChainID(eat.ChainConfig.ChainID)
	addr := crypto.PubkeyToAddress(key.PublicKey)
	nonce, err := eat.RpcApiClient.GetTransactionCount(addr, rpc.PendingBlock)
	require.NoError(t, err)
	gasPrice, err := eat.RpcApiClient.GasPrice()
	require.NoError(t, err)
	tx, err := types.SignTx(&types.LegacyTx{
		CommonTx: types.CommonTx{Nonce: nonce.Uint64(), GasLimit: 1_000_000, To: nil, Value: value, Data: initCode},
		GasPrice: *gasPrice,
	}, *signer, key)
	require.NoError(t, err)
	return tx
}

// pushAddrSelfdestruct returns init code that pushes addr and runs SELFDESTRUCT:
// PUSH20 <addr>; SELFDESTRUCT  (0x73 || 20 bytes || 0xff).
func pushAddrSelfdestruct(addr common.Address) []byte {
	out := make([]byte, 0, 22)
	out = append(out, 0x73)
	out = append(out, addr[:]...)
	out = append(out, 0xff)
	return out
}

func decodeAndValidateBAL(t *testing.T, payload *engineapitester.MockClPayload) types.BlockAccessList {
	t.Helper()
	balPtr := payload.ExecutionPayload.BlockAccessList
	require.NotNil(t, balPtr)
	require.NotEmpty(t, *balPtr)

	bal, err := types.DecodeBlockAccessListBytes(*balPtr)
	require.NoError(t, err)
	require.NoError(t, bal.Validate())
	require.NotEmpty(t, bal)
	return bal
}

func findAccountChanges(bal types.BlockAccessList, addr accounts.Address) *types.AccountChanges {
	for i := range bal {
		if bal[i].Address == addr.Value() {
			return &bal[i]
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

// TestEngineApiNewPayloadBALInvalid pins that both an undecodable blockAccessList
// and a decodable one violating EIP-7928 make the block invalid.
func TestEngineApiNewPayloadBALInvalid(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
	receiver := common.HexToAddress("0x333")
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		_, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, big.NewInt(1))
		require.NoError(t, err)
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		executionRequests := payload.ExecutionRequests
		if executionRequests == nil {
			executionRequests = []hexutil.Bytes{}
		}
		setBlockHash := func(elPayload *enginetypes.ExecutionPayload) {
			response := *payload.GetPayloadResponse
			response.ExecutionPayload = elPayload
			payloadCopy := *payload
			payloadCopy.GetPayloadResponse = &response
			header := engineapitester.MockClPayloadToHeader(&payloadCopy)
			balHash := crypto.Keccak256Hash(*elPayload.BlockAccessList)
			header.BlockAccessListHash = &balHash
			elPayload.BlockHash = header.Hash()
		}
		sendWithBAL := func(bal hexutil.Bytes) (*enginetypes.PayloadStatus, error) {
			elPayload := *payload.ExecutionPayload
			elPayload.BlockAccessList = &bal
			setBlockHash(&elPayload)
			return eat.EngineApiClient.NewPayloadV5(ctx, &elPayload, []common.Hash{}, payload.ParentBeaconBlockRoot, executionRequests)
		}
		for name, malformed := range map[string]hexutil.Bytes{
			"empty byte string": {},
			"string not list":   {0x80},
			"truncated list":    {0xc1},
		} {
			status, err := sendWithBAL(malformed)
			require.NoErrorf(t, err, "%s: expected INVALID status, got error: %v", name, err)
			require.Equalf(t, enginetypes.InvalidStatus, status.Status, "%s", name)
			require.NotNilf(t, status.ValidationError, "%s", name)
			require.ErrorContainsf(t, status.ValidationError.Error(), types.ErrInvalidBlockAccessList.Error(), "%s", name)
		}
		account := append([]byte{0xda, 0x94}, make([]byte, 20)...)
		account = append(account, 0xc0, 0xc0, 0xc0, 0xc0, 0xc0)
		duplicateAccounts := make([]byte, 0, 1+2*len(account))
		duplicateAccounts = append(duplicateAccounts, 0xc0+byte(2*len(account)))
		duplicateAccounts = append(duplicateAccounts, account...)
		duplicateAccounts = append(duplicateAccounts, account...)
		status, err := sendWithBAL(duplicateAccounts)
		require.NoError(t, err)
		require.Equal(t, enginetypes.InvalidStatus, status.Status)
		require.NotNil(t, status.ValidationError)
		require.ErrorContains(t, status.ValidationError.Error(), "access list",
			"INVALID must originate from block-access-list validation, not e.g. a block-hash mismatch")

		unknownParentPayload := *payload.ExecutionPayload
		unknownParentPayload.ParentHash = common.Hash{0xff}
		unknownParentBAL := hexutil.Bytes(duplicateAccounts)
		unknownParentPayload.BlockAccessList = &unknownParentBAL
		setBlockHash(&unknownParentPayload)
		status, err = eat.EngineApiClient.NewPayloadV5(ctx, &unknownParentPayload, []common.Hash{}, payload.ParentBeaconBlockRoot, executionRequests)
		require.NoError(t, err)
		require.Equal(t, enginetypes.InvalidStatus, status.Status)
		require.NotNil(t, status.ValidationError)
		require.ErrorContains(t, status.ValidationError.Error(), "access list")

		oversized, err := types.EncodeBlockAccessListBytes(types.BlockAccessList{{
			Address: common.Address{1},
		}})
		require.NoError(t, err)
		oversizedBytes := hexutil.Bytes(oversized)
		elPayload := *payload.ExecutionPayload
		elPayload.GasLimit = hexutil.Uint64(types.BalItemCost - 1)
		elPayload.Transactions = []hexutil.Bytes{}
		elPayload.BlockAccessList = &oversizedBytes
		setBlockHash(&elPayload)
		status, err = eat.EngineApiClient.NewPayloadV5(ctx, &elPayload, []common.Hash{}, payload.ParentBeaconBlockRoot, executionRequests)
		require.NoError(t, err)
		require.Equal(t, enginetypes.InvalidStatus, status.Status)
		require.NotNil(t, status.ValidationError)
		require.ErrorContains(t, status.ValidationError.Error(), "block access list too large")

		// The transaction passes the count bound but not the gas limit, and its INVALID wins over the too large access list.
		gasLimit := mdgas.MinTxBlockGas(true, true, true)
		overBudget := make(types.BlockAccessList, gasLimit/types.BalItemCost+1)
		for i := range overBudget {
			overBudget[i].Address = common.Address{byte(i + 1)}
		}
		overBudgetEncoded, err := types.EncodeBlockAccessListBytes(overBudget)
		require.NoError(t, err)
		overBudgetBytes := hexutil.Bytes(overBudgetEncoded)
		elPayload = *payload.ExecutionPayload
		elPayload.GasLimit = hexutil.Uint64(gasLimit)
		elPayload.BlockAccessList = &overBudgetBytes
		setBlockHash(&elPayload)
		status, err = eat.EngineApiClient.NewPayloadV5(ctx, &elPayload, []common.Hash{}, payload.ParentBeaconBlockRoot, executionRequests)
		require.NoError(t, err)
		require.Equal(t, enginetypes.InvalidStatus, status.Status)
		require.NotNil(t, status.ValidationError)
		require.EqualError(t, status.ValidationError.Error(), "gas limit reached")

		balWithNonceChangeAt := func(index uint32) hexutil.Bytes {
			encoded, err := types.EncodeBlockAccessListBytes(types.BlockAccessList{{
				Address:      common.Address{1},
				NonceChanges: []*types.NonceChange{{Index: index}},
			}})
			require.NoError(t, err)
			return encoded
		}
		postExecutionIndex := uint32(len(payload.ExecutionPayload.Transactions)) + 1
		status, err = sendWithBAL(balWithNonceChangeAt(postExecutionIndex + 1))
		require.NoError(t, err)
		require.Equal(t, enginetypes.InvalidStatus, status.Status)
		require.NotNil(t, status.ValidationError)
		require.ErrorContains(t, status.ValidationError.Error(), "block access index",
			"an index beyond len(transactions)+1 must be rejected before the block is inserted or executed")
		status, err = sendWithBAL(balWithNonceChangeAt(postExecutionIndex))
		require.NoError(t, err)
		require.Equal(t, enginetypes.InvalidStatus, status.Status)
		require.NotNil(t, status.ValidationError)
		require.ErrorContains(t, status.ValidationError.Error(), "block access list mismatch",
			"an in-range index is only refuted by the post-execution comparison")
	})
}

// TestEngineApiBALUnchangedClaimLieInvalid pins that a BAL listing a written account with reads and no changes makes
// the block invalid, also when a later tx of the block reads what an earlier one wrote.
func TestEngineApiBALUnchangedClaimLieInvalid(t *testing.T) {
	forceParallelExec(t)
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
	mintReceiver := common.HexToAddress("0x777")
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		sender := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)
		auth, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainConfig.ChainID)
		require.NoError(t, err)
		contractAddr, _, tokenContract, err := contracts.DeployToken(auth, eat.ContractBackend, sender)
		require.NoError(t, err)
		_, err = eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		// The second mint reads totalSupply and balanceOf[mintReceiver] that the first one wrote.
		for _, amount := range []int64{1000, 500} {
			auth, err = bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainConfig.ChainID)
			require.NoError(t, err)
			_, err = tokenContract.Mint(auth, mintReceiver, big.NewInt(amount))
			require.NoError(t, err)
		}
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		require.Len(t, payload.ExecutionPayload.Transactions, 2)

		bal := decodeAndValidateBAL(t, payload)
		token := findAccountChanges(bal, accounts.InternAddress(contractAddr))
		require.NotNil(t, token)
		require.NotEmpty(t, token.StorageChanges)
		hideStorageWrites(token)
		requireBALMismatch(t, newPayloadWithBAL(ctx, t, eat, payload, bal))
	})
}

// TestEngineApiBALUnchangedClaimLieShapesInvalid pins that a BAL hiding a committed storage write makes the block
// invalid in each shape of the lie, and that a net-zero write inside one tx is an honest read.
func TestEngineApiBALUnchangedClaimLieShapesInvalid(t *testing.T) {
	forceParallelExec(t)
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
	slot0 := common.Hash{}
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		sender := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)

		t.Run("write after read", func(t *testing.T) {
			contractAddr, payload, bal := buildBALLieBlock(ctx, t, eat, balLieCall{}, balLieCall{slotValuePairs: []uint64{0, 5}})
			c := findAccountChanges(bal, accounts.InternAddress(contractAddr))
			require.NotNil(t, c)
			require.Len(t, c.StorageChanges, 1)
			hideStorageWrites(c)
			requireBALMismatch(t, newPayloadWithBAL(ctx, t, eat, payload, bal))
		})

		t.Run("original value restored by a later tx", func(t *testing.T) {
			contractAddr, payload, bal := buildBALLieBlock(ctx, t, eat, balLieCall{slotValuePairs: []uint64{0, 7}}, balLieCall{slotValuePairs: []uint64{0, 1}})
			c := findAccountChanges(bal, accounts.InternAddress(contractAddr))
			require.NotNil(t, c)
			require.Len(t, c.StorageChanges, 1)
			require.Len(t, c.StorageChanges[0].Changes, 2)
			hideStorageWrites(c)
			requireBALMismatch(t, newPayloadWithBAL(ctx, t, eat, payload, bal))
		})

		t.Run("original value restored inside one tx", func(t *testing.T) {
			contractAddr, payload, bal := buildBALLieBlock(ctx, t, eat, balLieCall{slotValuePairs: []uint64{0, 2, 0, 1}})
			c := findAccountChanges(bal, accounts.InternAddress(contractAddr))
			require.NotNil(t, c)
			require.Empty(t, c.StorageChanges)
			require.Equal(t, []common.Hash{slot0}, c.StorageReads)

			t.Run("read omitted", func(t *testing.T) {
				c.StorageReads = nil
				requireBALMismatch(t, newPayloadWithBAL(ctx, t, eat, payload, bal))
			})
			t.Run("listed as a change", func(t *testing.T) {
				c.StorageReads = nil
				c.StorageChanges = []types.SlotChanges{{Slot: accounts.InternKey(slot0), Changes: []*types.StorageChange{{Index: 1, Value: *uint256.NewInt(1)}}}}
				requireBALMismatch(t, newPayloadWithBAL(ctx, t, eat, payload, bal))
			})
		})

		t.Run("written slot listed under another account", func(t *testing.T) {
			contractAddr, payload, bal := buildBALLieBlock(ctx, t, eat, balLieCall{slotValuePairs: []uint64{0, 5}})
			c := findAccountChanges(bal, accounts.InternAddress(contractAddr))
			require.NotNil(t, c)
			require.Len(t, c.StorageChanges, 1)
			s := findAccountChanges(bal, accounts.InternAddress(sender))
			require.NotNil(t, s)
			require.Empty(t, s.StorageChanges)
			s.StorageChanges = c.StorageChanges
			hideStorageWrites(c)
			requireBALMismatch(t, newPayloadWithBAL(ctx, t, eat, payload, bal))
		})

		t.Run("hidden write on an account with a declared balance change", func(t *testing.T) {
			contractAddr, payload, bal := buildBALLieBlock(ctx, t, eat, balLieCall{value: 1, slotValuePairs: []uint64{0, 5}})
			c := findAccountChanges(bal, accounts.InternAddress(contractAddr))
			require.NotNil(t, c)
			require.Len(t, c.StorageChanges, 1)
			require.NotEmpty(t, c.BalanceChanges)
			hideStorageWrites(c)
			requireBALMismatch(t, newPayloadWithBAL(ctx, t, eat, payload, bal))
		})
	})
}

// balLieContractInitCode deploys a runtime that stores each 32-byte (slot, value) calldata pair in order and reads slot 0
// on empty calldata.
const balLieContractInitCode = "6020600c60003960206000f3" + "3615601a5760005b8060200135813555604001803611600757005b6000545000"

type balLieCall struct {
	value          uint64
	slotValuePairs []uint64
}

// buildBALLieBlock deploys the balLieContractInitCode contract with slot 0 set to 1, then builds a block with calls from
// one sender in order.
func buildBALLieBlock(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester, calls ...balLieCall) (common.Address, *engineapitester.MockClPayload, types.BlockAccessList) {
	t.Helper()
	slotValueData := func(slotValuePairs []uint64) []byte {
		var data []byte
		for _, v := range slotValuePairs {
			word := uint256.NewInt(v).Bytes32()
			data = append(data, word[:]...)
		}
		return data
	}
	send := balLieSender(t, eat)
	contractAddr := deployBALLieContract(ctx, t, eat, send, balLieContractInitCode, slotValueData([]uint64{0, 1}))

	txHashes := make([]common.Hash, 0, len(calls))
	for _, c := range calls {
		txHashes = append(txHashes, send(&contractAddr, c.value, slotValueData(c.slotValuePairs)).Hash())
	}
	payload, bal := buildBALLiePayload(ctx, t, eat, txHashes)
	return contractAddr, payload, bal
}

// balLieSender returns a func that submits a legacy tx from the coinbase key, with consecutive nonces.
func balLieSender(t *testing.T, eat engineapitester.EngineApiTester) func(to *common.Address, value uint64, data []byte) types.Transaction {
	t.Helper()
	signer := types.LatestSignerForChainID(eat.ChainConfig.ChainID)
	sender := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)
	gasPrice, err := eat.RpcApiClient.GasPrice()
	require.NoError(t, err)
	pendingNonce, err := eat.RpcApiClient.GetTransactionCount(sender, rpc.PendingBlock)
	require.NoError(t, err)
	nonce := pendingNonce.Uint64()
	return func(to *common.Address, value uint64, data []byte) types.Transaction {
		tx, err := types.SignTx(&types.LegacyTx{
			CommonTx: types.CommonTx{Nonce: nonce, GasLimit: 500_000, To: to, Value: *uint256.NewInt(value), Data: data},
			GasPrice: *gasPrice,
		}, *signer, eat.CoinbaseKey)
		require.NoError(t, err)
		nonce++
		_, err = eat.RpcApiClient.SendTransaction(tx)
		require.NoError(t, err)
		return tx
	}
}

// deployBALLieContract deploys initCode and calls it once with setupData in a block of its own.
func deployBALLieContract(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester, send func(to *common.Address, value uint64, data []byte) types.Transaction, initCode string, setupData []byte) common.Address {
	t.Helper()
	deploy := send(nil, 0, common.FromHex(initCode))
	contractAddr := types.CreateAddress(crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey), deploy.GetNonce())
	if setupData != nil {
		send(&contractAddr, 0, setupData)
	}
	_, err := eat.MockCl.BuildCanonicalBlock(ctx)
	require.NoError(t, err)
	code, err := eat.RpcApiClient.GetCode(contractAddr, rpc.LatestBlock)
	require.NoError(t, err)
	require.NotEmpty(t, code)
	return contractAddr
}

// buildBALLiePayload builds a block that holds exactly the txs with txHashes, in order.
func buildBALLiePayload(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester, txHashes []common.Hash) (*engineapitester.MockClPayload, types.BlockAccessList) {
	t.Helper()
	payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
	require.NoError(t, err)
	require.Len(t, payload.ExecutionPayload.Transactions, len(txHashes))
	require.NoError(t, eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, txHashes...))
	return payload, decodeAndValidateBAL(t, payload)
}

// hideStorageWrites lists the slots ac writes as reads only.
func hideStorageWrites(ac *types.AccountChanges) {
	for _, sc := range ac.StorageChanges {
		ac.StorageReads = append(ac.StorageReads, sc.Slot.Value())
	}
	ac.StorageChanges = nil
	slices.SortFunc(ac.StorageReads, func(a, b common.Hash) int { return a.Cmp(b) })
}

// newPayloadWithBAL submits payload with bal as its access list and a block hash that commits to it.
func newPayloadWithBAL(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester, payload *engineapitester.MockClPayload, bal types.BlockAccessList) *enginetypes.PayloadStatus {
	t.Helper()
	require.NoError(t, bal.Validate())
	encoded, err := types.EncodeBlockAccessListBytes(bal)
	require.NoError(t, err)
	balBytes := hexutil.Bytes(encoded)

	executionRequests := payload.ExecutionRequests
	if executionRequests == nil {
		executionRequests = []hexutil.Bytes{}
	}
	elPayload := *payload.ExecutionPayload
	elPayload.BlockAccessList = &balBytes
	response := *payload.GetPayloadResponse
	response.ExecutionPayload = &elPayload
	payloadCopy := *payload
	payloadCopy.GetPayloadResponse = &response
	header := engineapitester.MockClPayloadToHeader(&payloadCopy)
	balHash := crypto.Keccak256Hash(balBytes)
	header.BlockAccessListHash = &balHash
	elPayload.BlockHash = header.Hash()

	status, err := engineapitester.RetryEngine(ctx, []enginetypes.EngineStatus{enginetypes.SyncingStatus}, nil,
		func() (*enginetypes.PayloadStatus, enginetypes.EngineStatus, error) {
			r, err := eat.EngineApiClient.NewPayloadV5(ctx, &elPayload, []common.Hash{}, payload.ParentBeaconBlockRoot, executionRequests)
			if err != nil {
				return nil, "", err
			}
			return r, r.Status, nil
		})
	require.NoError(t, err)
	return status
}

func requireBALMismatch(t *testing.T, status *enginetypes.PayloadStatus) {
	t.Helper()
	require.Equalf(t, enginetypes.InvalidStatus, status.Status, "validation error: %v", status.ValidationError)
	require.NotNil(t, status.ValidationError)
	require.ErrorContains(t, status.ValidationError.Error(), "block access list mismatch")
}

// TestEngineApiBALSameTxSelfdestructLieInvalid pins the honest BAL of an account that two txs each create and
// self-destruct, the second one reading the slot the first wrote, and that claiming the account unchanged while a tx
// changes it makes the block invalid.
func TestEngineApiBALSameTxSelfdestructLieInvalid(t *testing.T) {
	forceParallelExec(t)
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
	slot0 := common.Hash{}
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		t.Run("no balance left", func(t *testing.T) {
			child, payload, bal := buildSameTxSelfdestructBlock(ctx, t, eat, 0x33, sameTxSelfdestructCall{}, sameTxSelfdestructCall{})
			c := findAccountChanges(bal, accounts.InternAddress(child))
			requireOnlyStorageReadsAndBalances(t, c, []common.Hash{slot0})
			require.Equal(t, enginetypes.ValidStatus, newPayloadWithBAL(ctx, t, eat, payload, bal).Status)

			c.StorageReads = nil
			requireBALMismatch(t, newPayloadWithBAL(ctx, t, eat, payload, bal))
		})

		t.Run("no balance left, funded by a later tx", func(t *testing.T) {
			child, payload, bal := buildSameTxSelfdestructBlock(ctx, t, eat, 0x33, sameTxSelfdestructCall{}, sameTxSelfdestructCall{}, sameTxSelfdestructCall{value: 1, fundChild: true})
			c := findAccountChanges(bal, accounts.InternAddress(child))
			requireOnlyStorageReadsAndBalances(t, c, []common.Hash{slot0}, 3, 1)
			require.Equal(t, enginetypes.ValidStatus, newPayloadWithBAL(ctx, t, eat, payload, bal).Status)

			c.BalanceChanges = nil
			requireBALMismatch(t, newPayloadWithBAL(ctx, t, eat, payload, bal))
		})

		t.Run("balance kept", func(t *testing.T) {
			child, payload, bal := buildSameTxSelfdestructBlock(ctx, t, eat, 0x30, sameTxSelfdestructCall{value: 1}, sameTxSelfdestructCall{value: 1})
			c := findAccountChanges(bal, accounts.InternAddress(child))
			requireOnlyStorageReadsAndBalances(t, c, []common.Hash{slot0}, 1, 1, 2, 2)
			require.Equal(t, enginetypes.ValidStatus, newPayloadWithBAL(ctx, t, eat, payload, bal).Status)

			c.BalanceChanges = nil
			requireBALMismatch(t, newPayloadWithBAL(ctx, t, eat, payload, bal))
		})
	})
}

// create2FactoryInitCode deploys a runtime that runs its calldata as CREATE2 init code with salt 0 and the call value.
const create2FactoryInitCode = "600f600c60003960" + "0f6000f3" + "366000600037600036600034f55000"

type sameTxSelfdestructCall struct {
	value     uint64
	fundChild bool
}

// buildSameTxSelfdestructBlock deploys a create2FactoryInitCode factory, then builds a block where each call either
// creates the child through the factory or sends value to the child. The child init code reads slot 0, stores 5 in it
// and self-destructs to beneficiaryOp (CALLER or ADDRESS).
func buildSameTxSelfdestructBlock(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester, beneficiaryOp byte, calls ...sameTxSelfdestructCall) (common.Address, *engineapitester.MockClPayload, types.BlockAccessList) {
	t.Helper()
	childInitCode := []byte{0x60, 0x00, 0x54, 0x50, 0x60, 0x05, 0x60, 0x00, 0x55, beneficiaryOp, 0xff}
	send := balLieSender(t, eat)
	factory := deployBALLieContract(ctx, t, eat, send, create2FactoryInitCode, nil)
	child := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(childInitCode)))

	txHashes := make([]common.Hash, 0, len(calls))
	for _, c := range calls {
		if c.fundChild {
			txHashes = append(txHashes, send(&child, c.value, nil).Hash())
		} else {
			txHashes = append(txHashes, send(&factory, c.value, childInitCode).Hash())
		}
	}
	payload, bal := buildBALLiePayload(ctx, t, eat, txHashes)
	return child, payload, bal
}

// requireOnlyStorageReadsAndBalances checks that c lists the reads and the (index, balance) pairs and no other change.
func requireOnlyStorageReadsAndBalances(t *testing.T, c *types.AccountChanges, reads []common.Hash, indexBalancePairs ...uint64) {
	t.Helper()
	require.NotNil(t, c)
	require.Equal(t, reads, c.StorageReads)
	require.Empty(t, c.StorageChanges)
	require.Empty(t, c.NonceChanges)
	require.Empty(t, c.CodeChanges)
	require.Len(t, c.BalanceChanges, len(indexBalancePairs)/2)
	for i, bc := range c.BalanceChanges {
		require.Equal(t, indexBalancePairs[2*i], uint64(bc.Index))
		require.Equal(t, indexBalancePairs[2*i+1], bc.Value.Uint64())
	}
}

// TestEngineApiBALUnchangedAccountLieInvalid pins that a BAL listing an account with no change while a tx changes it
// makes the block invalid when a later tx of the block reads that account.
func TestEngineApiBALUnchangedAccountLieInvalid(t *testing.T) {
	forceParallelExec(t)
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := eat.Close()
		require.NoError(t, err)
	})
	childInitCode := common.FromHex(balLieContractInitCode)
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		t.Run("hidden balance change", func(t *testing.T) {
			send := balLieSender(t, eat)
			probe := deployBALLieContract(ctx, t, eat, send, accountProbeInitCode, nil)
			receiver := common.HexToAddress("0x7e1")
			payload, bal := buildBALLiePayload(ctx, t, eat, []common.Hash{send(&receiver, 1, nil).Hash(), probeAccount(send, probe, receiver)})
			c := findAccountChanges(bal, accounts.InternAddress(receiver))
			require.NotNil(t, c)
			require.NotEmpty(t, c.BalanceChanges)
			c.BalanceChanges = nil
			requireBALMismatch(t, newPayloadWithBAL(ctx, t, eat, payload, bal))
		})

		t.Run("hidden nonce change", func(t *testing.T) {
			send := balLieSender(t, eat)
			probe := deployBALLieContract(ctx, t, eat, send, accountProbeInitCode, nil)
			factory := deployBALLieContract(ctx, t, eat, send, create2FactoryInitCode, nil)
			payload, bal := buildBALLiePayload(ctx, t, eat, []common.Hash{send(&factory, 0, childInitCode).Hash(), probeAccount(send, probe, factory)})
			c := findAccountChanges(bal, accounts.InternAddress(factory))
			require.NotNil(t, c)
			require.NotEmpty(t, c.NonceChanges)
			c.NonceChanges = nil
			requireUnchanged(t, c)
			requireBALMismatch(t, newPayloadWithBAL(ctx, t, eat, payload, bal))
		})

		t.Run("create over an address listed unchanged", func(t *testing.T) {
			child, payload, bal := buildCreateThenProbeBlock(ctx, t, eat, childInitCode, false)
			c := findAccountChanges(bal, accounts.InternAddress(child))
			require.NotNil(t, c)
			*c = types.AccountChanges{Address: c.Address, StorageReads: c.StorageReads}
			requireBALMismatch(t, newPayloadWithBAL(ctx, t, eat, payload, bal))
		})

		t.Run("create collision over an address listed unchanged", func(t *testing.T) {
			child, payload, bal := buildCreateThenProbeBlock(ctx, t, eat, childInitCode, true)
			require.Equal(t, enginetypes.ValidStatus, newPayloadWithBAL(ctx, t, eat, payload, bal).Status)
			c := findAccountChanges(bal, accounts.InternAddress(child))
			require.NotNil(t, c)
			*c = types.AccountChanges{Address: c.Address, StorageReads: c.StorageReads}
			requireBALMismatch(t, newPayloadWithBAL(ctx, t, eat, payload, bal))
		})
	})
}

// accountProbeInitCode deploys a runtime that reads BALANCE, EXTCODEHASH and EXTCODESIZE of the calldata address.
const accountProbeInitCode = "600c600c600039600c6000f3" + "600035803150803f503b5000"

func probeAccount(send func(to *common.Address, value uint64, data []byte) types.Transaction, probe, target common.Address) common.Hash {
	return send(&probe, 0, common.LeftPadBytes(target[:], 32)).Hash()
}

// buildCreateThenProbeBlock builds a block where a create2FactoryInitCode factory creates a child from initCode and a
// later tx reads the child, or creates it again when recreate is set.
func buildCreateThenProbeBlock(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester, initCode []byte, recreate bool) (common.Address, *engineapitester.MockClPayload, types.BlockAccessList) {
	t.Helper()
	send := balLieSender(t, eat)
	factory := deployBALLieContract(ctx, t, eat, send, create2FactoryInitCode, nil)
	child := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode)))
	var probe common.Address
	if !recreate {
		probe = deployBALLieContract(ctx, t, eat, send, accountProbeInitCode, nil)
	}
	txHashes := []common.Hash{send(&factory, 0, initCode).Hash()}
	if recreate {
		txHashes = append(txHashes, send(&factory, 0, initCode).Hash())
	} else {
		txHashes = append(txHashes, probeAccount(send, probe, child))
	}
	payload, bal := buildBALLiePayload(ctx, t, eat, txHashes)
	return child, payload, bal
}

func requireUnchanged(t *testing.T, c *types.AccountChanges) {
	t.Helper()
	require.Empty(t, c.StorageChanges)
	require.Empty(t, c.BalanceChanges)
	require.Empty(t, c.NonceChanges)
	require.Empty(t, c.CodeChanges)
}

func forceParallelExec(t *testing.T) {
	prev := dbg.Exec3Parallel
	dbg.Exec3Parallel = true
	t.Cleanup(func() { dbg.Exec3Parallel = prev })
}
