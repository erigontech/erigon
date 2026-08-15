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
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
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

// stateChurnReorgDepthBudget sits well above the deepest unwind these tests
// perform, so forkchoice never rejects one as a too-deep reorg.
const stateChurnReorgDepthBudget = 256

// TestEngineApiUnwindRedoStateChurnPreservesState builds a chain that churns
// storage via the StateChurn contract (a third of the writes delete a slot by
// setting it to 0, and slots are continually recreated), recording the on-chain
// trackedSum at every height. It then repeatedly unwinds (fcu to an earlier
// block) and redoes (re-insert + fcu forward) across shallow and deep targets,
// asserting after each that the canonical head, the in-EVM consistency check,
// and the live trackedSum all match the value originally recorded at that
// height. This catches unwinds that move the head pointer without actually
// rolling back state, and any storage-clearing slot that fails to be restored.
func TestEngineApiUnwindRedoStateChurnPreservesState(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger:      logger,
		DataDir:     t.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = stateChurnReorgDepthBudget
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, eat.Close())
	})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		const pokes = 22
		payloads, _, churn, sums := buildChurnChain(ctx, t, eat, pokes, func(k int) int64 { return int64(k) })
		tip := uint64(2 + pokes)
		at := func(h uint64) *engineapitester.MockClPayload { return payloads[h-2] }
		sumAt := func(h uint64) *big.Int { return sums[h-2] }

		// The freshly built tip must already be self-consistent.
		assertChurnState(ctx, t, eat, churn, at(tip), sumAt(tip))

		// A walk of head targets exercising shallow/deep unwinds and redos.
		sequence := []uint64{tip - 1, tip, tip - 6, tip, tip - 14, tip, 3, tip, 5, 9, 4, tip}
		head := tip
		for _, target := range sequence {
			for h := head + 1; h <= target; h++ { // only runs when redoing forward
				status, err := eat.MockCl.InsertNewPayload(ctx, at(h))
				require.NoError(t, err)
				require.Equalf(t, enginetypes.ValidStatus, status.Status, "re-insert of block %d while redoing", h)
			}
			require.NoErrorf(t, eat.MockCl.UpdateForkChoice(ctx, at(target)), "fcu to block %d", target)
			assertChurnState(ctx, t, eat, churn, at(target), sumAt(target))
			head = target
		}
	})
}

// TestEngineApiReorgToSideChainSwitchesStateChurn syncs a victim node to a
// canonical chain of storage churn, then feeds it a competing side chain that
// shares the prefix up to a fork point but churns different storage afterwards
// (the divergence is forced through the poke seed, so the side blocks are
// genuinely distinct). A forkchoice update to the side tip must reorg the victim
// off the canonical suffix and onto the side suffix; afterwards the victim's
// live state must match the side chain's independently-computed state, proving
// the unwind discarded the canonical-suffix storage writes and re-applied the
// side ones correctly.
func TestEngineApiReorgToSideChainSwitchesStateChurn(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	sharedGenesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	const totalPokes = 12
	const forkPoke = 6 // pokes [0,forkPoke) are shared; [forkPoke,totalPokes) diverge
	tweak := func(config *ethconfig.Config) { config.MaxReorgDepth = stateChurnReorgDepthBudget }

	canonPayloads, canonAddr, canonRef := buildRecordedChurnChain(ctx, t, logger, sharedGenesis, coinbaseKey, tweak, totalPokes, func(k int) int64 { return int64(k) })
	sidePayloads, sideAddr, sideRef := buildRecordedChurnChain(ctx, t, logger, sharedGenesis, coinbaseKey, tweak, totalPokes, func(k int) int64 {
		if k < forkPoke {
			return int64(k)
		}
		return int64(k) + 1_000_000
	})

	eatVictim, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger: logger, DataDir: t.TempDir(), Genesis: sharedGenesis, CoinbaseKey: coinbaseKey, EthConfigTweaker: tweak,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eatVictim.Close()) })
	eatVictim.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		require.Equal(t, canonAddr, sideAddr, "contract must deploy at the same address on both chains")
		canonTip := canonPayloads[len(canonPayloads)-1]
		sideTip := sidePayloads[len(sidePayloads)-1]
		require.NotEqual(t, canonTip.ExecutionPayload.BlockHash, sideTip.ExecutionPayload.BlockHash, "the two chains must genuinely differ at the tip")

		churn, err := contracts.NewStateChurn(canonAddr, eat.ContractBackend)
		require.NoError(t, err)

		// Sync the full canonical chain and confirm its state.
		for _, payload := range canonPayloads {
			status, err := eat.MockCl.InsertNewPayload(ctx, payload)
			require.NoError(t, err)
			require.Equal(t, enginetypes.ValidStatus, status.Status)
		}
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, canonTip))
		assertChurnState(ctx, t, eat, churn, canonTip, canonRef)

		// Insert the divergent side suffix (the shared prefix is already present)
		// and reorg to it; the victim must end up on the side chain's state.
		for i := forkPoke + 1; i < len(sidePayloads); i++ {
			status, err := eat.MockCl.InsertNewPayload(ctx, sidePayloads[i])
			require.NoError(t, err)
			require.Equalf(t, enginetypes.ValidStatus, status.Status, "insert of side suffix payload %d", i)
		}
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, sideTip))
		assertChurnState(ctx, t, eat, churn, sideTip, sideRef)
	})
}

// TestEngineApiForkBounceStateChurn bounces a node between two competing
// churn forks — canonical → side → canonical → side — asserting the full
// churn state after every switch, and then keeps building on the final fork.
// Each bounce unwinds and re-executes the same height range under the
// opposite fork's hashes, so per-block data keyed only by height (diffsets,
// commitment changesets, caches) that survives from the losing fork corrupts
// the winner.
func TestEngineApiForkBounceStateChurn(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	sharedGenesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	const totalPokes = 12
	const forkPoke = 6
	tweak := func(config *ethconfig.Config) { config.MaxReorgDepth = stateChurnReorgDepthBudget }

	canonPayloads, canonAddr, canonRef := buildRecordedChurnChain(ctx, t, logger, sharedGenesis, coinbaseKey, tweak, totalPokes, func(k int) int64 { return int64(k) })
	sidePayloads, sideAddr, sideRef := buildRecordedChurnChain(ctx, t, logger, sharedGenesis, coinbaseKey, tweak, totalPokes, func(k int) int64 {
		if k < forkPoke {
			return int64(k)
		}
		return int64(k) + 1_000_000
	})

	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger: logger, DataDir: t.TempDir(), Genesis: sharedGenesis, CoinbaseKey: coinbaseKey, EthConfigTweaker: tweak,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		require.Equal(t, canonAddr, sideAddr, "contract must deploy at the same address on both chains")
		canonTip := canonPayloads[len(canonPayloads)-1]
		sideTip := sidePayloads[len(sidePayloads)-1]
		require.NotEqual(t, canonTip.ExecutionPayload.BlockHash, sideTip.ExecutionPayload.BlockHash, "the two chains must genuinely differ at the tip")
		churn, err := contracts.NewStateChurn(canonAddr, eat.ContractBackend)
		require.NoError(t, err)

		for _, payload := range canonPayloads {
			status, err := eat.MockCl.InsertNewPayload(ctx, payload)
			require.NoError(t, err)
			require.Equal(t, enginetypes.ValidStatus, status.Status)
		}
		for i := forkPoke + 1; i < len(sidePayloads); i++ {
			status, err := eat.MockCl.InsertNewPayload(ctx, sidePayloads[i])
			require.NoError(t, err)
			require.Equalf(t, enginetypes.ValidStatus, status.Status, "insert of side suffix payload %d", i)
		}

		bounces := []struct {
			name string
			tip  *engineapitester.MockClPayload
			ref  *big.Int
		}{
			{"canon", canonTip, canonRef},
			{"side", sideTip, sideRef},
			{"back to canon", canonTip, canonRef},
			{"back to side", sideTip, sideRef},
		}
		for _, b := range bounces {
			require.NoErrorf(t, eat.MockCl.UpdateForkChoice(ctx, b.tip), "fcu bounce to %s", b.name)
			assertChurnState(ctx, t, eat, churn, b.tip, b.ref)
		}

		// The final fork must remain fully usable: keep churning on top of it.
		churnAndAssert(ctx, t, eat, churn, 3, func(k int) int64 { return int64(2_000 + k) })
	})
}

// buildChurnChain deploys StateChurn at block 2 on the given tester and then
// applies one poke per block, using seed(k) for the k-th poke. payloads[0] is
// the deploy block (height 2) and payloads[k+1] is the block applying poke k
// (height 3+k). sums[i] is the on-chain trackedSum observed at payloads[i] and
// is the ground truth a later unwind/reorg must reproduce.
func buildChurnChain(
	ctx context.Context,
	t *testing.T,
	eat engineapitester.EngineApiTester,
	pokes int,
	seed func(k int) int64,
	tweakOpts ...func(*bind.TransactOpts),
) (payloads []*engineapitester.MockClPayload, addr common.Address, churn *contracts.StateChurn, sums []*big.Int) {
	transactOpts, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainId())
	require.NoError(t, err)
	transactOpts.GasLimit = params.MaxTxnGasLimit
	for _, tweak := range tweakOpts {
		tweak(transactOpts)
	}

	addr, deployTxn, churn, err := contracts.DeployStateChurn(transactOpts, eat.ContractBackend)
	require.NoError(t, err)
	deployBlock, err := eat.MockCl.BuildCanonicalBlock(ctx)
	require.NoError(t, err)
	require.NoError(t, eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, deployBlock.ExecutionPayload, deployTxn.Hash()))
	payloads = append(payloads, deployBlock)
	sums = append(sums, recordChurnSum(ctx, t, churn))

	for k := range pokes {
		txn, err := churn.Poke(transactOpts, big.NewInt(seed(k)))
		require.NoError(t, err)
		block, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		require.NoError(t, eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, block.ExecutionPayload, txn.Hash()))
		payloads = append(payloads, block)
		sums = append(sums, recordChurnSum(ctx, t, churn))
	}
	return payloads, addr, churn, sums
}

// churnAndAssert applies pokes one block at a time on the tester's current
// head, asserting the full churn invariant after every block. A poke reads its
// slots before writing, so any stale value left behind by a preceding
// unwind/reorg/restart/prune makes the transaction revert or the invariant
// trip right here.
func churnAndAssert(
	ctx context.Context,
	t *testing.T,
	eat engineapitester.EngineApiTester,
	churn *contracts.StateChurn,
	pokes int,
	seed func(k int) int64,
) (payloads []*engineapitester.MockClPayload, sums []*big.Int) {
	transactOpts, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainId())
	require.NoError(t, err)
	transactOpts.GasLimit = params.MaxTxnGasLimit
	coinbaseAddr := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)
	for k := range pokes {
		// Pin the nonce to the head state: the txpool's own pending-nonce view
		// can be stale right after a reorg/restart (re-injected dead-fork txns,
		// https://github.com/erigontech/erigon/issues/22299), and submission is
		// retried while the pool digests the head change.
		nonce, err := eat.RpcApiClient.GetTransactionCount(coinbaseAddr, rpc.LatestBlock)
		require.NoError(t, err)
		transactOpts.Nonce = nonce
		var txn types.Transaction
		require.Eventually(t, func() bool {
			var err error
			txn, err = churn.Poke(transactOpts, big.NewInt(seed(k)))
			if err != nil {
				t.Logf("poke %d submission retry: %v", k, err)
			}
			return err == nil
		}, 30*time.Second, 100*time.Millisecond, "poke submission did not settle")
		block, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		require.NoError(t, eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, block.ExecutionPayload, txn.Hash()))
		sum := recordChurnSum(ctx, t, churn)
		assertChurnState(ctx, t, eat, churn, block, sum)
		payloads = append(payloads, block)
		sums = append(sums, sum)
	}
	return payloads, sums
}

// buildRecordedChurnChain builds a churn chain on a throwaway node and returns
// its payloads, contract address and final trackedSum. The node is closed
// before returning; only the recorded payloads are needed by the caller.
func buildRecordedChurnChain(
	ctx context.Context,
	t *testing.T,
	logger log.Logger,
	genesis *types.Genesis,
	coinbaseKey *ecdsa.PrivateKey,
	tweak func(*ethconfig.Config),
	pokes int,
	seed func(k int) int64,
	tweakOpts ...func(*bind.TransactOpts),
) (payloads []*engineapitester.MockClPayload, addr common.Address, ref *big.Int) {
	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger: logger, DataDir: t.TempDir(), Genesis: genesis, CoinbaseKey: coinbaseKey, EthConfigTweaker: tweak,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		var sums []*big.Int
		payloads, addr, _, sums = buildChurnChain(ctx, t, eat, pokes, seed, tweakOpts...)
		ref = sums[len(sums)-1]
	})
	require.NoError(t, eat.Close())
	return payloads, addr, ref
}

// recordChurnSum reads and returns the contract's live trackedSum. poke reverts
// unless trackedSum equals the sum recomputed from storage, and VerifyTxnsInclusion
// rejects a reverted poke, so a successfully built block already proves that
// invariant — no extra computedSum/check read is needed here.
func recordChurnSum(ctx context.Context, t *testing.T, churn *contracts.StateChurn) *big.Int {
	opts := &bind.CallOpts{Context: ctx}
	var tracked *big.Int
	require.Eventually(t, func() bool {
		var err error
		tracked, err = churn.TrackedSum(opts)
		return err == nil
	}, 10*time.Second, 25*time.Millisecond, "trackedSum read did not succeed")
	return tracked
}

// assertChurnState waits for the canonical head to settle on want (matching both
// number and hash) and then asserts the contract's live state is self-consistent,
// equal to wantSum, and at the cursor expected for want's height.
func assertChurnState(
	ctx context.Context,
	t *testing.T,
	eat engineapitester.EngineApiTester,
	churn *contracts.StateChurn,
	want *engineapitester.MockClPayload,
	wantSum *big.Int,
) {
	height := want.ExecutionPayload.BlockNumber.Uint64()
	assertCanonicalHead(ctx, t, eat, want)

	// fcu-to-ancestor unwinds for real only while the target stays above the finalized
	// block (the mock CL pins finalized to genesis).
	if fin, err := eat.RpcApiClient.GetBlockByNumber(ctx, rpc.FinalizedBlockNumber, false); err == nil && fin != nil {
		require.Greaterf(t, height, fin.Number.Uint64(), "fcu target %d must stay above finalized %d", height, fin.Number.Uint64())
	}

	tracked, computed, cursor, ok := readChurn(ctx, t, churn)
	require.Truef(t, ok, "in-EVM check() must hold at height %d: tracked=%s computed=%s", height, tracked, computed)
	require.Equalf(t, wantSum, tracked, "live trackedSum must match the recorded value at height %d", height)
	// cursor increments once per poke (height-2 at each block), so a head that moved
	// without rolling back state would read the wrong value here.
	require.Equalf(t, height-2, cursor.Uint64(), "live cursor must equal pokes applied at height %d", height)
}

// assertCanonicalHead blocks until the canonical head reported over JSON-RPC
// matches want's number and hash, failing the test if it does not settle. This
// is both the readiness gate (forkchoice persistence is asynchronous) and a
// correctness check that the head moved to exactly the expected block.
func assertCanonicalHead(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester, want *engineapitester.MockClPayload) {
	wantHash := want.ExecutionPayload.BlockHash
	wantNum := want.ExecutionPayload.BlockNumber.Uint64()
	require.Eventuallyf(t, func() bool {
		head, err := eat.RpcApiClient.GetBlockByNumber(ctx, rpc.LatestBlockNumber, false)
		return err == nil && head != nil && head.Number.Uint64() == wantNum && head.Hash == wantHash
	}, 30*time.Second, 25*time.Millisecond, "canonical head did not settle at block %d %s", wantNum, wantHash)
}

// readChurn reads trackedSum, computedSum, cursor and check() from the live state,
// retrying only on transient RPC errors (never on a value), so the returned
// values can be asserted strictly by the caller.
func readChurn(ctx context.Context, t *testing.T, churn *contracts.StateChurn) (tracked, computed, cursor *big.Int, ok bool) {
	opts := &bind.CallOpts{Context: ctx}
	require.Eventually(t, func() bool {
		var err error
		if tracked, err = churn.TrackedSum(opts); err != nil {
			return false
		}
		if computed, err = churn.ComputedSum(opts); err != nil {
			return false
		}
		if cursor, err = churn.Cursor(opts); err != nil {
			return false
		}
		ok, err = churn.Check(opts)
		return err == nil
	}, 10*time.Second, 25*time.Millisecond, "state churn reads did not succeed")
	return tracked, computed, cursor, ok
}
