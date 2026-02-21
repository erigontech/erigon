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

package jsonrpc_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/abi/bind"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
)

// TestReceiptRootValidationAfterReorg may reproduce the receipt root mismatch bug from GitHub issue #17351.
// Root cause: after a re-org, orphaned block data (header, body, HeaderNumber index) persists in the DB.
// When receipts are requested for an orphaned block by hash:
//  1. HeaderNumber(hash) still returns the block number
//  2. BlockWithSenders(hash, number) still returns the full block
//  3. CheckBlockExecuted passes (checks block NUMBER, not HASH — the canonical chain has executed past such number)
//  4. CreateHistoryStateReader reads state from the CANONICAL chain's txNums
//  5. Re-execution of the orphaned block's transactions against the canonical state produces incorrect receipts
//     with different gas usage
//  6. Without non-canonical block hash rejection: wrong receipts are cached in the LRU and served to all subsequent callers
//  7. Client validates DeriveSha(receipts) vs block.ReceiptHash → mismatch
//
// This test creates two competing chains that deploy the same contract but diverge in storage state (chain A calls
// Change() in block 2, chain B does not). After re-orging from chain A to chain B, querying receipts for chain
// A's orphaned block 3 (which calls Change()) against chain B's state produces different SSTORE gas costs, hnece
// different receipt root.
//
// Without the fix: wrong receipts are silently returned → test FAILS
// With the fix: non-canonical block error is returned → test PASSES
func TestReceiptRootValidationAfterReorg(t *testing.T) {
	maxReorgDepth := uint64(64)
	logLvl := log.LvlDebug
	receiver := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f2bD18")
	sharedGenesis, coinbaseKey := engineapitester.DefaultEngineApiTesterGenesis(t)

	// ==============================================================================================================
	// Chain A: Deploy Changer + call Change() in block 2, call Change() again in block 3 (no-op SSTOREs, cheap gas)
	// ==============================================================================================================
	// After init, all testers share identical block 1 (empty). So the fork point is at block 1,
	// and chains diverge starting at block 2.
	chainA := make([]*engineapitester.MockClPayload, 2) // blocks 2 and 3
	eatA := engineapitester.InitialiseEngineApiTester(t, engineapitester.EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, logLvl),
		DataDir:     t.TempDir(),
		Genesis:     sharedGenesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = maxReorgDepth
		},
	})
	eatA.Run(t, func(ctx context.Context, t *testing.T, eatA engineapitester.EngineApiTester) {
		transactOpts, err := bind.NewKeyedTransactorWithChainID(eatA.CoinbaseKey, eatA.ChainId())
		require.NoError(t, err)
		transactOpts.GasLimit = params.MaxTxnGasLimit

		// Block 2: deploy Changer (nonce 0) + call Change() (nonce 1)
		// After this block: Changer storage = {x:1, y:2, z:3}
		_, deployTxn, changer, err := contracts.DeployChanger(transactOpts, eatA.ContractBackend)
		require.NoError(t, err)
		changeTxn1, err := changer.Change(transactOpts)
		require.NoError(t, err)
		block2A, err := eatA.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eatA.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, block2A.ExecutionPayload, deployTxn.Hash(), changeTxn1.Hash())
		require.NoError(t, err)
		chainA[0] = block2A

		// Block 3: call Change() again (nonce 2)
		// Changer storage is already {1,2,3} → SSTOREs are no-ops (100 gas each)
		changeTxn2, err := changer.Change(transactOpts)
		require.NoError(t, err)
		block3A, err := eatA.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eatA.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, block3A.ExecutionPayload, changeTxn2.Hash())
		require.NoError(t, err)
		chainA[1] = block3A
	})

	// ==============================================================================================================
	// Chain B: Deploy Changer + ETH transfer in block 2 (no Change() call), ETH transfer in block 3
	// ==============================================================================================================
	// Same tx count per block as chain A (2 txs in block 2, 1 tx in block 3) to keep coinbase nonce aligned.
	// But Changer.Change() is never called, so Changer storage remains {x:0, y:0, z:0}.
	chainB := make([]*engineapitester.MockClPayload, 2) // blocks 2 and 3
	eatB := engineapitester.InitialiseEngineApiTester(t, engineapitester.EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, logLvl),
		DataDir:     t.TempDir(),
		Genesis:     sharedGenesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = maxReorgDepth
		},
	})
	eatB.Run(t, func(ctx context.Context, t *testing.T, eatB engineapitester.EngineApiTester) {
		transactOpts, err := bind.NewKeyedTransactorWithChainID(eatB.CoinbaseKey, eatB.ChainId())
		require.NoError(t, err)
		transactOpts.GasLimit = params.MaxTxnGasLimit

		// Block 2: deploy Changer (nonce 0) + ETH transfer (nonce 1)
		// After this block: Changer storage = {x:0, y:0, z:0} (Change never called)
		_, deployTxn, _, err := contracts.DeployChanger(transactOpts, eatB.ContractBackend)
		require.NoError(t, err)
		ethTxn1, err := eatB.Transactor.SubmitSimpleTransfer(eatB.CoinbaseKey, receiver, big.NewInt(1e18))
		require.NoError(t, err)
		block2B, err := eatB.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eatB.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, block2B.ExecutionPayload, deployTxn.Hash(), ethTxn1.Hash())
		require.NoError(t, err)
		chainB[0] = block2B

		// Block 3: ETH transfer (nonce 2)
		ethTxn2, err := eatB.Transactor.SubmitSimpleTransfer(eatB.CoinbaseKey, receiver, big.NewInt(1e18))
		require.NoError(t, err)
		block3B, err := eatB.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eatB.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, block3B.ExecutionPayload, ethTxn2.Hash())
		require.NoError(t, err)
		chainB[1] = block3B
	})

	// ==============================================================================================================
	// Sync tester: process chain A, then re-org to chain B, then query receipts for orphaned block 3(A)
	// ==============================================================================================================
	eatSync := engineapitester.InitialiseEngineApiTester(t, engineapitester.EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, logLvl),
		DataDir:     t.TempDir(),
		Genesis:     sharedGenesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = maxReorgDepth
		},
	})
	eatSync.Run(t, func(ctx context.Context, t *testing.T, eatSync engineapitester.EngineApiTester) {
		// Insert and commit chain A (blocks 2(A) and 3(A))
		for _, payload := range chainA {
			status, err := eatSync.MockCl.InsertNewPayload(ctx, payload)
			require.NoError(t, err)
			require.Equal(t, enginetypes.ValidStatus, status.Status, "chain A block should be valid")
		}
		err := eatSync.MockCl.UpdateForkChoice(ctx, chainA[len(chainA)-1])
		require.NoError(t, err)

		// Save block 3(A) hash for querying AFTER re-org.
		block3AHash := chainA[1].ExecutionPayload.BlockHash

		// Insert chain B's blocks
		for _, payload := range chainB {
			status, err := eatSync.MockCl.InsertNewPayload(ctx, payload)
			require.NoError(t, err)
			require.Equal(t, enginetypes.ValidStatus, status.Status, "chain B block should be valid")
		}

		// Re-org to chain B — this unwinds blocks 3(A) and 2(A), then executes 2(B) and 3(B). After this:
		// - Block 3(A) is orphaned but its data persists in the DB
		// - The canonical chain's state at block 3 reflects chain B
		// - Changer storage on canonical chain = {0,0,0} (never called)
		err = eatSync.MockCl.UpdateForkChoice(ctx, chainB[len(chainB)-1])
		require.NoError(t, err)

		// ==========================================================================================================
		// Query receipts for non-canonical, still findable-after-reorg block 3(A) by hash
		// ==========================================================================================================
		// CheckBlockExecuted(tx, 3) passes because execution progress >= 3 (the canonical chain B has executed
		// block 3(B) at the same height).
		// CreateHistoryStateReader uses block NUMBER (3) to look up txNums, which now correspond to chain B's
		// execution. The Changer's storage on chain B is {0,0,0}, so re-executing Change() does fresh SSTORE opcodes
		// (0→1, 0→2, 0→3 at 20000 gas each) instead of no-op SSTORE opcodes (1→1, 2→2, 3→3 at 100 gas each).
		// This produces a receipt with ~59700 more gas used → different receipt root.
		receiptsAfterReorg, err := eatSync.RpcApiClient.GetBlockReceipts(ctx,
			rpc.BlockNumberOrHashWithHash(block3AHash, false))

		// When the issue happens, wrong receipts with inflated gas are silently returned.
		if err == nil && len(receiptsAfterReorg) > 0 {
			computedRoot := types.DeriveSha(receiptsAfterReorg)
			expectedRoot := chainA[1].ExecutionPayload.ReceiptsRoot
			require.Equal(t, expectedRoot, computedRoot,
				"receipt root mismatch detected — GetBlockReceipts returned incorrect receipts for orphaned block 3(A) after re-org.")
		}
	})
}
