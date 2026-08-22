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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/node/ethconfig"
)

// waitForDomainFilesSettled blocks until background building has produced at
// least one domain file and any in-flight build/merge has drained, so the
// snapshot boundary is stable before the test unwinds across it.
func waitForDomainFilesSettled(ctx context.Context, t *testing.T, agg *state.Aggregator) {
	t.Helper()
	require.NotNil(t, agg, "StateAgg is nil: ChainDB did not expose a *state.Aggregator")
	// First wait for a file to appear; FilesAmount()>0 is the signal that the
	// background builder has run at least once.
	require.Eventually(t, func() bool {
		for _, n := range agg.FilesAmount() {
			if n > 0 {
				return true
			}
		}
		return false
	}, 60*time.Second, 100*time.Millisecond, "no domain files were built")
	// Then let the current build/merge finish so the file set stops changing.
	<-agg.WaitForBuildAndMerge(ctx)
}

// newSmallStepDataDir returns a datadir whose erigondb.toml shrinks domain
// steps so that snapshot files build within a few hundred test blocks.
func newSmallStepDataDir(t *testing.T) string {
	t.Helper()
	dataDir := t.TempDir()
	snapDir := filepath.Join(dataDir, "snapshots")
	require.NoError(t, os.MkdirAll(snapDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(snapDir, "erigondb.toml"),
		[]byte("step_size = 32\nsteps_in_frozen_file = 256\n"), 0o644))
	return dataDir
}

func TestEngineApiUnwindAcrossDomainStepBoundaries(t *testing.T) {
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
			c.Snapshot.ProduceE3 = true       // build domain snapshot files
			c.AlwaysGenerateChangesets = true // keep changesets so the unwind is permitted
			c.MaxReorgDepth = 90              // engine reorg cap, comfortably above the 60-block unwind below
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })

	var churnAddr common.Address
	var targetPayload *engineapitester.MockClPayload
	var targetSum *big.Int
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		const pokes = 300
		payloads, addr, churn, sums := buildChurnChain(ctx, t, eat, pokes, func(k int) int64 { return int64(k) })
		tip := uint64(2 + pokes)
		churnAddr = addr

		waitForDomainFilesSettled(ctx, t, eat.StateAgg)
		t.Logf("domain files settled: %v", eat.StateAgg.FilesAmount())

		// Unwind a 60-block range. With snapshot files present it crosses several
		// domain-step boundaries, so a key modified at multiple steps in the range
		// must be restored to its value at the unwind target — the path fixed in the
		// domain unwind. AlwaysGenerateChangesets retains the diffsets, so the unwind
		// is permitted.
		target := tip - 60
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, payloads[target-2]))
		assertChurnState(ctx, t, eat, churn, payloads[target-2], sums[target-2])
		targetPayload = payloads[target-2]
		targetSum = sums[target-2]
	})

	// Restart the node on the same datadir. The unwind's correctness must be
	// durable: the in-RAM overlay and unwind-changeset that could mask a wrong
	// MDBX/files state die with the process, so the restarted node reads what
	// was actually persisted. A deletion whose tombstone was lost by the unwind
	// resurrects here as a stale value from the snapshot files.
	mockClState := eat.MockCl.State()
	require.NoError(t, eat.Close())
	eat2, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger:      logger,
		DataDir:     dataDir,
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(c *ethconfig.Config) {
			c.Snapshot.ProduceE3 = true
			c.AlwaysGenerateChangesets = true
			c.MaxReorgDepth = 90
		},
		MockClState:   mockClState,
		NoEmptyBlock1: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat2.Close()) })
	eat2.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		churn, err := contracts.NewStateChurn(churnAddr, eat.ContractBackend)
		require.NoError(t, err)
		assertChurnState(ctx, t, eat, churn, targetPayload, targetSum)
		// Keep churning across the restarted node: each poke re-reads its ring
		// slots from the persisted state, so a resurrected or missing value
		// reverts the transaction or trips the in-EVM check.
		churnAndAssert(ctx, t, eat, churn, 4, func(k int) int64 { return int64(1_000 + k) })
	})
}

// TestEngineApiUnwindToSnapshotBoundaryPreservesDeletedSlots unwinds to just
// above the visible snapshot-file boundary, after pruning has evacuated the
// filed range from MDBX. Post-unwind reads of the churned slots must then
// resolve through the snapshot files — the state the shallower unwind of
// TestEngineApiUnwindAcrossDomainStepBoundaries never reaches because MDBX
// still holds every pre-range value there. Any domain read path that
// mishandles file-resident values or deletion markers surfaces here.
func TestEngineApiUnwindToSnapshotBoundaryPreservesDeletedSlots(t *testing.T) {
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
			c.AlwaysGenerateChangesets = true
			c.MaxReorgDepth = 400 // the boundary sits deep below the tip
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		const pokes = 300
		payloads, _, churn, sums := buildChurnChain(ctx, t, eat, pokes, func(k int) int64 { return int64(k) })
		tip := uint64(2 + pokes)

		waitForDomainFilesSettled(ctx, t, eat.StateAgg)
		// The last background build may have finished after the final forkchoice,
		// so no prune pass has evacuated the freshly filed range from MDBX yet —
		// and lingering MDBX entries (including original deletion tombstones)
		// would shadow the file reads this test is about. A few more forkchoice
		// cycles let the foreground collate+prune catch up.
		extraPayloads, extraSums := churnAndAssert(ctx, t, eat, churn, 6, func(k int) int64 { return int64(9_000 + k) })
		payloads = append(payloads, extraPayloads...)
		sums = append(sums, extraSums...)
		tip += 6
		<-eat.StateAgg.WaitForBuildAndMerge(ctx)
		boundaryTxNum := eat.StateAgg.EndTxNumMinimax()
		t.Logf("domain files settled: %v, boundary txNum: %d", eat.StateAgg.FilesAmount(), boundaryTxNum)

		// Locate the first block fully above the visible-files boundary. Each
		// block spans len(txs)+2 txNums (block-begin + txns + block-end); the
		// +2 slack below absorbs the genesis anchoring.
		cum := uint64(2) // genesis
		cum += 2         // block 1, the tester's initial empty block
		target := uint64(0)
		for i, p := range payloads {
			cum += uint64(len(p.ExecutionPayload.Transactions)) + 2
			if cum > boundaryTxNum {
				target = uint64(i) + 2 // payloads[i] is height i+2
				break
			}
		}
		require.NotZero(t, target, "no block above the files boundary — files cover the whole chain?")
		target += 2 // slack for the genesis/system-txn anchoring above
		require.Greater(t, target, uint64(2), "target must stay above the deploy block")
		require.Less(t, target, tip, "target must be a real unwind")
		t.Logf("unwinding to block %d (tip %d, depth %d)", target, tip, tip-target)

		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, payloads[target-2]))
		assertChurnState(ctx, t, eat, churn, payloads[target-2], sums[target-2])

		// Keep churning: every poke re-reads its ring slots from MDBX+files.
		churnAndAssert(ctx, t, eat, churn, 4, func(k int) int64 { return int64(5_000 + k) })
	})
}
