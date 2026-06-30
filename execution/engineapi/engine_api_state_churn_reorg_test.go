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
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/abi/bind"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state/contracts"
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

	var canonPayloads []*engineapitester.MockClPayload
	var canonAddr common.Address
	var canonRef *big.Int
	eatCanon, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger: logger, DataDir: t.TempDir(), Genesis: sharedGenesis, CoinbaseKey: coinbaseKey, EthConfigTweaker: tweak,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eatCanon.Close()) })
	eatCanon.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		var sums []*big.Int
		canonPayloads, canonAddr, _, sums = buildChurnChain(ctx, t, eat, totalPokes, func(k int) int64 { return int64(k) })
		canonRef = sums[len(sums)-1]
	})
	require.NoError(t, eatCanon.Close()) // free the canon node; only its recorded payloads are needed below

	var sidePayloads []*engineapitester.MockClPayload
	var sideAddr common.Address
	var sideRef *big.Int
	eatSide, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger: logger, DataDir: t.TempDir(), Genesis: sharedGenesis, CoinbaseKey: coinbaseKey, EthConfigTweaker: tweak,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eatSide.Close()) })
	eatSide.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		var sums []*big.Int
		sidePayloads, sideAddr, _, sums = buildChurnChain(ctx, t, eat, totalPokes, func(k int) int64 {
			if k < forkPoke {
				return int64(k)
			}
			return int64(k) + 1_000_000
		})
		sideRef = sums[len(sums)-1]
	})
	require.NoError(t, eatSide.Close()) // free the side node; only its recorded payloads are needed below

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
) (payloads []*engineapitester.MockClPayload, addr common.Address, churn *contracts.StateChurn, sums []*big.Int) {
	transactOpts, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainId())
	require.NoError(t, err)
	transactOpts.GasLimit = params.MaxTxnGasLimit

	addr, deployTxn, churn, err := contracts.DeployStateChurn(transactOpts, eat.ContractBackend)
	require.NoError(t, err)
	deployBlock, err := eat.MockCl.BuildCanonicalBlock(ctx)
	require.NoError(t, err)
	require.NoError(t, eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, deployBlock.ExecutionPayload, deployTxn.Hash()))
	payloads = append(payloads, deployBlock)
	sums = append(sums, recordChurnSum(ctx, t, churn))

	for k := 0; k < pokes; k++ {
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
// number and hash) and then asserts the contract's live state is self-consistent
// and equal to wantSum.
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
	tracked, computed, ok := readChurn(ctx, t, churn)
	require.Truef(t, ok, "in-EVM check() must hold at height %d: tracked=%s computed=%s", height, tracked, computed)
	require.Equalf(t, wantSum, tracked, "live trackedSum must match the recorded value at height %d", height)
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

// readChurn reads trackedSum, computedSum and check() from the live state,
// retrying only on transient RPC errors (never on a value), so the returned
// values can be asserted strictly by the caller.
func readChurn(ctx context.Context, t *testing.T, churn *contracts.StateChurn) (tracked, computed *big.Int, ok bool) {
	opts := &bind.CallOpts{Context: ctx}
	require.Eventually(t, func() bool {
		var err error
		if tracked, err = churn.TrackedSum(opts); err != nil {
			return false
		}
		if computed, err = churn.ComputedSum(opts); err != nil {
			return false
		}
		ok, err = churn.Check(opts)
		return err == nil
	}, 10*time.Second, 25*time.Millisecond, "state churn reads did not succeed")
	return tracked, computed, ok
}
