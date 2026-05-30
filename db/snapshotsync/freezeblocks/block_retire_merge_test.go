package freezeblocks_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

// dumpTenSegments generates a chain and dumps it into ten 1k block segments
// covering [0, 10000), then opens the snapshot folder. Returns the BlockRetire
// (wired with the given shared build semaphore) and the snapshot dir.
func dumpTenSegments(t *testing.T, sema *semaphore.Weighted) (*freezeblocks.BlockRetire, string) {
	t.Helper()
	const chainSize = 10_000 // ten 1k segments → triggers the first 10k merge step

	m := createDumpTestKV(t, chain.AllProtocolChanges, chainSize)
	logger := log.New()
	snConfig, _ := snapcfg.KnownCfg(networkname.Mainnet)

	// Dump ten separate 1k segments [0,1000)..[9000,10000); the last one ends on
	// a 10k boundary, so the merger combines them into a single [0,10000) file.
	for from := uint64(0); from < chainSize; from += 1000 {
		require.NoError(t, freezeblocks.DumpBlocks(m.Ctx, from, from+1000, m.ChainConfig, m.Dirs.Tmp, m.Dirs.Snap, m.DB, 1, log.LvlInfo, logger, m.BlockReader, snConfig))
		// Reopen so the next segment's firstTxNum is computed from the prior one.
		require.NoError(t, m.BlockSnapshots.OpenFolder())
	}
	require.GreaterOrEqual(t, m.BlockSnapshots.BlocksAvailable(), uint64(chainSize-1))

	br := freezeblocks.NewBlockRetire(1, m.Dirs, m.BlockReader, blockio.NewBlockWriter(), m.DB, nil, nil, m.ChainConfig, &ethconfig.Defaults, nil, sema, logger)
	return br, m.Dirs.Snap
}

// TestBlockMergeRunsWithoutSemaphore is the guard for the "merge off the
// semaphore" change: a block merge must run to completion even when the shared
// snapshot-build semaphore is fully held by another party (e.g. state
// collation). If a future change makes the merge acquire the semaphore, this
// deadlocks and the timeout fails the test.
func TestBlockMergeRunsWithoutSemaphore(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	sema := semaphore.NewWeighted(1)
	br, snapDir := dumpTenSegments(t, sema)
	ctx := t.Context()

	// Occupy the shared build semaphore, as state collation would.
	require.NoError(t, sema.Acquire(ctx, 1))
	defer sema.Release(1)

	type mergeResult struct {
		merged bool
		err    error
	}
	done := make(chan mergeResult, 1)
	go func() {
		merged, err := br.MergeBlocks(ctx, log.LvlInfo, downloader.NoopSeederClient{})
		done <- mergeResult{merged, err}
	}()

	select {
	case res := <-done:
		require.NoError(t, res.err)
		if !res.merged {
			entries, _ := os.ReadDir(snapDir)
			names := make([]string, 0, len(entries))
			for _, e := range entries {
				names = append(names, e.Name())
			}
			t.Fatalf("expected a merge to happen; snapDir=%v", names)
		}
	case <-time.After(120 * time.Second):
		t.Fatal("block merge blocked on the shared build semaphore")
	}

	require.FileExists(t, filepath.Join(snapDir, "v1.1-000000-000010-transactions.seg"))
}

// TestRetireBlocksInBackgroundReleasesSemaphore verifies the background retire
// path releases the shared build semaphore after the dump phase and does not
// leak it across the (now off-semaphore) merge.
func TestRetireBlocksInBackgroundReleasesSemaphore(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	sema := semaphore.NewWeighted(1)
	br, _ := dumpTenSegments(t, sema)
	ctx := t.Context()

	retireDone := make(chan struct{})
	started := br.RetireBlocksInBackground(ctx, 0, 10_000, log.LvlInfo, downloader.NoopSeederClient{},
		func() error { return nil },
		func() { close(retireDone) })
	require.True(t, started)

	select {
	case <-retireDone:
	case <-time.After(120 * time.Second):
		t.Fatal("background retire did not finish")
	}
	br.WaitForMerges()

	// Semaphore must be fully available again — nothing leaked it.
	require.True(t, sema.TryAcquire(1), "shared build semaphore was not released")
	sema.Release(1)
}
