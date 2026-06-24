package engineapi_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/node/ethconfig"
)

// TestEngineApiHistoricalStateConsistencyAcrossFiles builds a churning chain that
// produces domain snapshot files, then reads the contract's self-consistency
// (computed == tracked) and ground-truth trackedSum at every historical block via
// archive eth_call. Historical reads reconstruct state from the history/snapshot
// files (a different path than live getLatest), so a collation/prune that corrupts
// the files surfaces here as a stale/inconsistent historical read.
func TestEngineApiHistoricalStateConsistencyAcrossFiles(t *testing.T) {
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
			c.Snapshot.ProduceE3 = true
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = eat.Close() })

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		const pokes = 300
		_, _, churn, sums := buildChurnChain(ctx, t, eat, pokes, func(k int) int64 { return int64(k) })
		tip := uint64(2 + pokes)

		waitForDomainFilesSettled(t, snapDir)
		t.Logf("domain .kv files settled at %d", countDomainKVFiles(snapDir))

		bad := 0
		for h := uint64(2); h <= tip; h++ {
			opts := &bind.CallOpts{Context: ctx, BlockNumber: uint256.NewInt(h)}
			tracked, err := churn.TrackedSum(opts)
			require.NoError(t, err)
			computed, err := churn.ComputedSum(opts)
			require.NoError(t, err)
			ok, err := churn.Check(opts)
			require.NoError(t, err)
			if !ok || tracked.Cmp(computed) != 0 || tracked.Cmp(sums[h-2]) != 0 {
				bad++
				t.Logf("HISTBAD block %d: tracked=%s computed=%s ok=%v want=%s", h, tracked, computed, ok, sums[h-2])
			}
		}
		require.Zero(t, bad, "historical state inconsistent/stale at %d of %d blocks", bad, tip-1)
	})
}
