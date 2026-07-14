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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/kv/prune"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/node/ethconfig"
)

// TestEngineApiReorgWithPruningInterference churns storage on a node whose
// history pruning runs with a distance small enough to bite within the test
// chain (every forkchoice update prunes in the foreground). Reorgs then
// interleave with pruning: a shallow unwind inside the retained window must
// reproduce the recorded state exactly, a deep unwind beyond it must be
// rejected cleanly rather than silently corrupt, and the node must keep
// churning correctly afterwards.
func TestEngineApiReorgWithPruningInterference(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlError)
	dataDir := newSmallStepDataDir(t)

	// Standard prune modes use distances in the 100k+ block range and would
	// never delete anything at test scale, so shrink the distance to make
	// pruning genuinely interfere with the reorgs below.
	const pruneDistance = 24
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger:      logger,
		DataDir:     dataDir,
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(c *ethconfig.Config) {
			c.Snapshot.ProduceE3 = true
			c.Prune = prune.Mode{
				Initialised:       true,
				Blocks:            prune.Distance(pruneDistance),
				History:           prune.Distance(pruneDistance),
				CommitmentHistory: prune.KeepAllBlocksPruneMode,
			}
			c.MaxReorgDepth = 90
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		const pokes = 120
		payloads, _, churn, sums := buildChurnChain(ctx, t, eat, pokes, func(k int) int64 { return int64(k) })
		tip := uint64(2 + pokes)

		waitForDomainFilesSettled(ctx, t, eat.StateAgg)
		t.Logf("domain files settled: %v", eat.StateAgg.FilesAmount())

		// Shallow unwind, well inside the retained window: pruning must not
		// have eaten the history/diffsets this unwind needs.
		shallow := tip - 8
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, payloads[shallow-2]))
		assertChurnState(ctx, t, eat, churn, payloads[shallow-2], sums[shallow-2])

		// Redo forward to the tip and confirm the original state is restored.
		for h := shallow + 1; h <= tip; h++ {
			status, err := eat.MockCl.InsertNewPayload(ctx, payloads[h-2])
			require.NoError(t, err)
			require.Equalf(t, enginetypes.ValidStatus, status.Status, "re-insert of block %d while redoing", h)
		}
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, payloads[tip-2]))
		assertChurnState(ctx, t, eat, churn, payloads[tip-2], sums[tip-2])

		// Give pruning further forkchoice cycles to advance while the chain
		// keeps churning.
		extraPayloads, extraSums := churnAndAssert(ctx, t, eat, churn, 20, func(k int) int64 { return int64(3_000 + k) })
		head := extraPayloads[len(extraPayloads)-1]
		headSum := extraSums[len(extraSums)-1]

		// Deep unwind reaching into prune territory (but within the engine's
		// reorg budget): either the history needed is still retained — then the
		// unwind must be performed fully and correctly — or it is gone and the
		// forkchoice must fail loudly. A silently no-op'd or partial unwind
		// that leaves phantom state behind fails the churn asserts below.
		deep := tip - 60
		deepErr := eat.MockCl.UpdateForkChoice(ctx, payloads[deep-2])
		if deepErr == nil {
			assertChurnState(ctx, t, eat, churn, payloads[deep-2], sums[deep-2])
			for h := deep + 1; h <= tip; h++ {
				status, err := eat.MockCl.InsertNewPayload(ctx, payloads[h-2])
				require.NoError(t, err)
				require.Equalf(t, enginetypes.ValidStatus, status.Status, "re-insert of block %d after deep unwind", h)
			}
			for _, p := range extraPayloads {
				status, err := eat.MockCl.InsertNewPayload(ctx, p)
				require.NoError(t, err)
				require.Equal(t, enginetypes.ValidStatus, status.Status)
			}
		} else {
			t.Logf("deep unwind to %d rejected: %v", deep, deepErr)
		}
		// Either way the node must remain fully functional at the head.
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, head))
		assertChurnState(ctx, t, eat, churn, head, headSum)

		// Pruning keeps running while the chain keeps churning.
		churnAndAssert(ctx, t, eat, churn, 4, func(k int) int64 { return int64(4_000 + k) })
	})
}

// TestEngineApiUnwindBeyondRetainedChangesetsRejectedCleanly runs without
// AlwaysGenerateChangesets, so once churn history is collated into snapshot
// files the changesets an unwind would need are pruned away. A forkchoice to
// a block below what is still unwindable must then be rejected loudly — a
// silently partial unwind would leave phantom state — while a shallow unwind
// and continued churn keep working on the same node.
func TestEngineApiUnwindBeyondRetainedChangesetsRejectedCleanly(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlError)
	dataDir := newSmallStepDataDir(t)

	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger:      logger,
		DataDir:     dataDir,
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(c *ethconfig.Config) {
			c.Snapshot.ProduceE3 = true
			c.MaxReorgDepth = 400 // deeper than the rejection target: the changeset check must fire, not the depth cap
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		const pokes = 300
		payloads, _, churn, sums := buildChurnChain(ctx, t, eat, pokes, func(k int) int64 { return int64(k) })
		tip := uint64(2 + pokes)

		waitForDomainFilesSettled(ctx, t, eat.StateAgg)
		t.Logf("domain files settled: %v", eat.StateAgg.FilesAmount())

		// A shallow unwind at the tip must still work.
		shallow := tip - 8
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, payloads[shallow-2]))
		assertChurnState(ctx, t, eat, churn, payloads[shallow-2], sums[shallow-2])
		for h := shallow + 1; h <= tip; h++ {
			status, err := eat.MockCl.InsertNewPayload(ctx, payloads[h-2])
			require.NoError(t, err)
			require.Equalf(t, enginetypes.ValidStatus, status.Status, "re-insert of block %d while redoing", h)
		}
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, payloads[tip-2]))
		assertChurnState(ctx, t, eat, churn, payloads[tip-2], sums[tip-2])

		// Deep below what remains unwindable: must fail loudly, not silently
		// no-op or partially apply.
		deep := uint64(20)
		deepErr := eat.MockCl.UpdateForkChoice(ctx, payloads[deep-2])
		require.Errorf(t, deepErr, "unwind to block %d must be rejected once its history is gone", deep)
		t.Logf("deep unwind to %d rejected: %v", deep, deepErr)

		// The head must remain restorable and the state readable and correct.
		// Block production after this rejection is still broken (the
		// SeekCommitment wedge, https://github.com/erigontech/erigon/issues/22301),
		// so this test stops at the read-side contract.
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, payloads[tip-2]))
		assertChurnState(ctx, t, eat, churn, payloads[tip-2], sums[tip-2])
	})
}
