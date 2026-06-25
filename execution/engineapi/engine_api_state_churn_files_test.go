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
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/node/ethconfig"
)

// countDomainKVFiles returns the number of built domain value (.kv) files.
func countDomainKVFiles(snapDir string) int {
	n := 0
	_ = filepath.WalkDir(snapDir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && filepath.Ext(p) == ".kv" {
			n++
		}
		return nil
	})
	return n
}

// waitForDomainFilesSettled blocks until background domain-file building has
// produced at least one .kv file and the count has stopped changing, so the
// snapshot-file boundary is stable before the test unwinds across it.
func waitForDomainFilesSettled(t *testing.T, snapDir string) {
	t.Helper()
	prev, stable := -1, 0
	require.Eventually(t, func() bool {
		n := countDomainKVFiles(snapDir)
		if n > 0 && n == prev {
			stable++
		} else {
			stable = 0
		}
		prev = n
		return n > 0 && stable >= 3
	}, 60*time.Second, 300*time.Millisecond, "domain .kv files never settled")
}

func TestEngineApiUnwindAcrossSnapshotFileBoundary(t *testing.T) {
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
			c.MaxReorgDepth = 90              // below the ~96-block file-boundary lag: unwinds stay above filed state
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		const pokes = 300
		payloads, _, churn, sums := buildChurnChain(ctx, t, eat, pokes, func(k int) int64 { return int64(k) })
		tip := uint64(2 + pokes)

		waitForDomainFilesSettled(t, snapDir)
		t.Logf("domain .kv files settled at %d", countDomainKVFiles(snapDir))

		// Unwind a 60-block range. With snapshot files present this crosses
		// several domain-step boundaries, so a key modified at multiple steps in
		// the range must be restored to its value at the unwind target — the path
		// fixed in the domain unwind. The depth stays under MaxReorgDepth (and the
		// file-boundary lag), so the unwind is permitted and targets supported state.
		target := tip - 60
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, payloads[target-2]))
		assertChurnState(ctx, t, eat, churn, payloads[target-2], sums[target-2])
	})
}
