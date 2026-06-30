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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
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

func TestEngineApiUnwindAcrossDomainStepBoundaries(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlError)
	dataDir := t.TempDir()
	snapDir := filepath.Join(dataDir, "snapshots")
	require.NoError(t, os.MkdirAll(snapDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(snapDir, "erigondb.toml"),
		[]byte("step_size = 32\nsteps_in_frozen_file = 256\n"), 0o644))

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

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		const pokes = 300
		payloads, _, churn, sums := buildChurnChain(ctx, t, eat, pokes, func(k int) int64 { return int64(k) })
		tip := uint64(2 + pokes)

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
	})
}
