// Copyright 2024 The Erigon Authors
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

package stages

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type recordingBlobHistoryDownloader struct {
	mu       sync.Mutex
	headSlot uint64
	notify   *network.BlobBackfilledNotifier
	started  bool
}

func (d *recordingBlobHistoryDownloader) SetHeadSlot(slot uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.headSlot = slot
}

func (d *recordingBlobHistoryDownloader) SetNotifyBlobBackfilled(notify *network.BlobBackfilledNotifier) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.notify = notify
}

func (d *recordingBlobHistoryDownloader) Start() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.started = true
}

func TestBlobHistoryDownloadStartsOnlyAfterBlockHistoryFinishes(t *testing.T) {
	downloader := &recordingBlobHistoryDownloader{}

	startBlobHistoryDownload(false, downloader, 99, func(bool) {})
	require.False(t, downloader.started)
	require.Zero(t, downloader.headSlot)
	require.Nil(t, downloader.notify)

	startBlobHistoryDownload(true, downloader, 99, func(bool) {})
	require.True(t, downloader.started)
	require.Equal(t, uint64(99), downloader.headSlot)
	require.NotNil(t, downloader.notify)
}

func TestBlobHistoryDownloadIgnoresNilConfiguredDownloader(t *testing.T) {
	var downloader *network.BlobHistoryDownloader
	require.NotPanics(t, func() {
		startBlobHistoryDownload(true, downloader, 99, func(bool) {})
	})
}

type failingHistoryDownloader struct {
	err      error
	finished bool
}

func (d *failingHistoryDownloader) Finished() bool                             { return d.finished }
func (*failingHistoryDownloader) Progress() uint64                             { return 1 }
func (d *failingHistoryDownloader) RequestMore(context.Context) error          { return d.err }
func (*failingHistoryDownloader) SetBlockChecker(network.BlockChecker)         {}
func (*failingHistoryDownloader) SetBlockReader(network.BeaconBlockBodyReader) {}
func (*failingHistoryDownloader) SetExpectedRoot(common.Hash)                  {}
func (*failingHistoryDownloader) SetNeverSkip(bool)                            {}
func (*failingHistoryDownloader) SetOnNewBlock(network.OnNewBlock)             {}
func (*failingHistoryDownloader) SetOnInitialGloasBlock(common.Hash, func(*cltypes.SignedBeaconBlock) error) {
}
func (*failingHistoryDownloader) SetSlotToDownload(uint64)  {}
func (*failingHistoryDownloader) SetThrottle(time.Duration) {}

// clampProgress must never report a total below processed nor underflow, even
// when the floor and current counters drift past the frozen highestBlockSeen.
// The last case mirrors the field report where the live EL head advanced past
// the frozen top and previously underflowed the denominator to ~2^64.
func TestClampProgress(t *testing.T) {
	cases := []struct {
		name                     string
		highest, floor, current  uint64
		wantProcessed, wantTotal uint64
	}{
		{"normal", 100, 20, 60, 40, 80},
		{"floor above top", 100, 150, 60, 40, 40},
		{"current above top", 100, 20, 200, 0, 80},
		{"current below floor grows total", 100, 20, 5, 95, 95},
		{"el head past frozen tip", 23_000_000, 23_123_953, 22_983_559, 16_441, 16_441},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			processed, total := clampProgress(tc.highest, tc.floor, tc.current)
			if processed != tc.wantProcessed || total != tc.wantTotal {
				t.Fatalf("clampProgress(%d,%d,%d) = (%d,%d), want (%d,%d)",
					tc.highest, tc.floor, tc.current, processed, total, tc.wantProcessed, tc.wantTotal)
			}
			if processed > total {
				t.Fatalf("processed (%d) exceeds total (%d)", processed, total)
			}
		})
	}
}

// Post-merge the EL block number exceeds the beacon slot, so a snapshot-gap
// floor must be compared against EL block progress, not the slot.
func TestELBackfillFinished_GapUsesBlockNotSlot(t *testing.T) {
	const (
		bellatrixSlot = uint64(4_636_672) // a real beacon-slot floor
		frozenBlock   = uint64(25_073_000)
		headSlot      = uint64(14_460_640)
		headBlock     = uint64(25_224_522)
	)
	destBlock := frozenBlock - 1

	if elBackfillFinished(headSlot, headBlock, bellatrixSlot, destBlock) {
		t.Fatalf("backfill reported finished at the tip (slot=%d block=%d) while gap down to block %d is unfilled",
			headSlot, headBlock, destBlock)
	}

	// Once EL block progress has descended to the frozen tip, it is finished.
	if !elBackfillFinished(headSlot-150_000, destBlock, bellatrixSlot, destBlock) {
		t.Fatalf("backfill should be finished once EL block progress reaches the frozen tip (block %d)", destBlock)
	}
}

// Without a snapshot gap, the EL block floor is unset and completion is driven
// purely by the beacon-slot floor (normal Deneb backfill toward the merge).
func TestELBackfillFinished_NoGapUsesSlotFloor(t *testing.T) {
	const bellatrixSlot = uint64(4_636_672)
	noBlockFloor := uint64(math.MaxUint64)

	if elBackfillFinished(bellatrixSlot+1, 20_000_000, bellatrixSlot, noBlockFloor) {
		t.Fatal("backfill must continue while still above the beacon-slot floor")
	}
	if !elBackfillFinished(bellatrixSlot, 20_000_000, bellatrixSlot, noBlockFloor) {
		t.Fatal("backfill must finish once the beacon-slot floor is reached")
	}
}

func TestSpawnStageHistoryDownloadReturnsDownloaderFailure(t *testing.T) {
	wantErr := errors.New("canonical successor unavailable")
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err := SpawnStageHistoryDownload(StageHistoryReconstructionCfg{
		beaconCfg:    &clparams.MainnetBeaconConfig,
		downloader:   &failingHistoryDownloader{err: wantErr},
		startingSlot: 1,
	}, ctx, log.Root())
	require.ErrorIs(t, err, wantErr)
}

func TestWaitForHistoryDownloadJoinsFinishedWorker(t *testing.T) {
	wantErr := errors.New("commit history progress")
	historyDone := make(chan error, 1)
	historyDone <- wantErr

	err := waitForHistoryDownload(t.Context(), StageHistoryReconstructionCfg{
		downloader: &failingHistoryDownloader{finished: true},
	}, 0, historyDone)
	require.ErrorIs(t, err, wantErr)
}

type initialGloasHistoryDownloader struct {
	failingHistoryDownloader
	block   *cltypes.SignedBeaconBlock
	persist func(*cltypes.SignedBeaconBlock) error
}

func (d *initialGloasHistoryDownloader) SetOnInitialGloasBlock(_ common.Hash, persist func(*cltypes.SignedBeaconBlock) error) {
	d.persist = persist
}

func (d *initialGloasHistoryDownloader) RequestMore(context.Context) error {
	if d.persist == nil {
		return errors.New("initial Gloas callback was not configured")
	}
	if err := d.persist(d.block); err != nil {
		return err
	}
	return d.err
}

func TestHistoryDownloadPersistsInitialGloasBlockWithoutPayloadClassification(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.GloasForkEpoch = 0
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	block.Block.Slot = 10
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	t.Cleanup(db.Close)
	stop := errors.New("stop after anchor persistence")
	downloader := &initialGloasHistoryDownloader{failingHistoryDownloader: failingHistoryDownloader{err: stop}, block: block}
	err = SpawnStageHistoryDownload(StageHistoryReconstructionCfg{beaconCfg: &cfg, downloader: downloader, startingRoot: root, startingSlot: 10, indiciesDB: db, logger: log.New()}, t.Context(), log.New())
	require.ErrorIs(t, err, stop)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		got, err := beacon_indicies.ReadCanonicalBlockRoot(tx, 10)
		require.NoError(t, err)
		require.Equal(t, common.Hash(root), got)
		executionHash, err := beacon_indicies.ReadExecutionBlockHash(tx, root)
		require.NoError(t, err)
		require.Equal(t, common.Hash{}, executionHash)
		return nil
	}))
}

type firstBlockHistoryDownloader struct {
	failingHistoryDownloader
	onNewBlock network.OnNewBlock
	deliver    func(network.OnNewBlock)
}

func (d *firstBlockHistoryDownloader) SetOnNewBlock(onNewBlock network.OnNewBlock) {
	d.onNewBlock = onNewBlock
}

func (d *firstBlockHistoryDownloader) RequestMore(context.Context) error {
	d.deliver(d.onNewBlock)
	return d.err
}

func writeUnindexedBeaconBlocks(t *testing.T, dirs datadir.Dirs, from, to uint64) {
	t.Helper()
	f, _, ok := snaptype.ParseFileName(dirs.Snap, snaptype.BeaconBlocks.FileName(version.ZeroVersion, from, to))
	require.True(t, ok)
	c, err := seg.NewCompressor(t.Context(), "test beaconblocks", f.Path, dirs.Tmp, seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	defer c.Close()
	for range to - from {
		require.NoError(t, c.AddWord(nil))
	}
	require.NoError(t, c.Compress())
}

func TestHistoryDownloadIndexesCaplinSegmentsBeforeFirstBlock(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit
	writeSegments := func(t *testing.T, dirs datadir.Dirs) {
		writeUnindexedBeaconBlocks(t, dirs, 0, limit)
		writeUnindexedBeaconBlocks(t, dirs, limit, 2*limit)
	}
	for _, tc := range []struct {
		name   string
		engine func(*testing.T, datadir.Dirs) execution_client.ExecutionEngine
	}{
		{"standalone", func(t *testing.T, dirs datadir.Dirs) execution_client.ExecutionEngine {
			writeSegments(t, dirs)
			return nil
		}},
		{"embedded", func(t *testing.T, dirs datadir.Dirs) execution_client.ExecutionEngine {
			engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
			engine.EXPECT().SupportInsertion().Return(true).AnyTimes()
			polls := 0
			engine.EXPECT().Ready(gomock.Any()).DoAndReturn(func(context.Context) (bool, error) {
				polls++
				if polls == 1 {
					writeSegments(t, dirs)
					return false, nil
				}
				return true, nil
			}).AnyTimes()
			return engine
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			cfg := clparams.MainnetBeaconConfig
			sn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
			t.Cleanup(sn.Close)
			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			t.Cleanup(db.Close)

			block := cltypes.NewSignedBeaconBlock(&cfg, clparams.Phase0Version)
			block.Block.Slot = limit
			var blocksAvailable uint64
			var finished bool
			var blockErr error
			stop := errors.New("stop after the first block")
			downloader := &firstBlockHistoryDownloader{failingHistoryDownloader: failingHistoryDownloader{err: stop}}
			downloader.deliver = func(onNewBlock network.OnNewBlock) {
				assert.Eventually(t, func() bool { return sn.BlocksAvailable() == 2*limit-1 }, 10*time.Second, 10*time.Millisecond)
				blocksAvailable = sn.BlocksAvailable()
				finished, blockErr = onNewBlock(block, nil)
			}

			err := SpawnStageHistoryDownload(StageHistoryReconstructionCfg{
				beaconCfg:          &cfg,
				downloader:         downloader,
				sn:                 sn,
				engine:             tc.engine(t, dirs),
				waitForAllRoutines: true,
				startingSlot:       2 * limit,
				indiciesDB:         db,
				caplinConfig:       clparams.CaplinConfig{ArchiveBlocks: true},
				logger:             log.New(),
			}, t.Context(), log.New())
			if err != nil {
				require.ErrorIs(t, err, stop)
			}
			require.NoError(t, blockErr)
			require.Equal(t, uint64(2*limit-1), blocksAvailable, "caplin segments must be indexed before the first block")
			require.Equal(t, uint64(2*limit-1), sn.SegmentsMax())
			require.True(t, finished, "the archive backfill must stop at the snapshot tip")
			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
				root, err := beacon_indicies.ReadCanonicalBlockRoot(tx, limit)
				require.NoError(t, err)
				require.Equal(t, common.Hash{}, root, "a block held by snapshots must not be written to the DB")
				return nil
			}))
		})
	}
}

type countingHistoryDownloader struct {
	failingHistoryDownloader
	requests int
}

func (d *countingHistoryDownloader) RequestMore(context.Context) error {
	d.requests++
	return d.err
}

var failingIndexBuilder = snaptype.IndexBuilderFunc(func(context.Context, snaptype.FileInfo, uint32, *chain.Config, string, *background.Progress, log.Lvl, log.Logger) error {
	return errors.New("no space left in tmpdir")
})

func TestHistoryDownloadStartsDownloaderWhenIndexBuildFails(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	writeUnindexedBeaconBlocks(t, dirs, 0, snaptype.CaplinMergeLimit)
	cfg := clparams.MainnetBeaconConfig
	sn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(sn.Close)
	var builds atomic.Int32
	sn.SetIndexBuilder(snaptype.BeaconBlocks, snaptype.IndexBuilderFunc(func(context.Context, snaptype.FileInfo, uint32, *chain.Config, string, *background.Progress, log.Lvl, log.Logger) error {
		builds.Add(1)
		return errors.New("no space left in tmpdir")
	}))

	stop := errors.New("stop after the first request")
	downloader := &countingHistoryDownloader{failingHistoryDownloader: failingHistoryDownloader{err: stop}}
	for pass := 1; pass <= 2; pass++ {
		err := SpawnStageHistoryDownload(StageHistoryReconstructionCfg{
			beaconCfg:    &cfg,
			downloader:   downloader,
			sn:           sn,
			startingSlot: 2 * snaptype.CaplinMergeLimit,
			caplinConfig: clparams.CaplinConfig{ArchiveBlocks: true},
			logger:       log.New(),
		}, t.Context(), log.New())
		require.Equal(t, pass, downloader.requests, "a failed index build must not keep the downloader from starting")
		require.ErrorIs(t, err, stop)
	}
	require.Eventually(t, func() bool { return builds.Load() == 2 }, 10*time.Second, 10*time.Millisecond)
}

func TestHistoryDownloadKeepsBackfillingBelowUnindexedLeadingSegment(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit
	dirs := datadir.New(t.TempDir())
	writeUnindexedBeaconBlocks(t, dirs, 0, limit)
	writeUnindexedBeaconBlocks(t, dirs, limit, 2*limit)
	indexed, _, ok := snaptype.ParseFileName(dirs.Snap, snaptype.BeaconBlocks.FileName(version.ZeroVersion, limit, 2*limit))
	require.True(t, ok)
	require.NoError(t, snapshotsync.BeaconSimpleIdx(t.Context(), indexed, 0, dirs.Tmp, &background.Progress{}, log.LvlDebug, log.New()))

	cfg := clparams.MainnetBeaconConfig
	sn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(sn.Close)
	sn.SetIndexBuilder(snaptype.BeaconBlocks, failingIndexBuilder)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	t.Cleanup(db.Close)

	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.Phase0Version)
	block.Block.Slot = limit
	var segmentsMax uint64
	var finished bool
	var blockErr error
	stop := errors.New("stop after the first block")
	downloader := &firstBlockHistoryDownloader{failingHistoryDownloader: failingHistoryDownloader{err: stop}}
	downloader.deliver = func(onNewBlock network.OnNewBlock) {
		assert.Eventually(t, func() bool { return sn.SegmentsMax() == 2*limit-1 }, 10*time.Second, 10*time.Millisecond)
		segmentsMax = sn.SegmentsMax()
		finished, blockErr = onNewBlock(block, nil)
	}

	err := SpawnStageHistoryDownload(StageHistoryReconstructionCfg{
		beaconCfg:          &cfg,
		downloader:         downloader,
		sn:                 sn,
		waitForAllRoutines: true,
		startingSlot:       2 * limit,
		indiciesDB:         db,
		caplinConfig:       clparams.CaplinConfig{ArchiveBlocks: true},
		logger:             log.New(),
	}, t.Context(), log.New())
	require.ErrorIs(t, err, stop)
	require.NoError(t, blockErr)
	require.Equal(t, uint64(2*limit-1), segmentsMax)
	require.False(t, finished, "slots below an unindexed leading segment must still be backfilled")
}

func historyDownloadFirstBlock(t *testing.T, sn *freezeblocks.CaplinSnapshots, visibleTip, slot uint64) (finished bool, beaconBlockRows uint64) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	t.Cleanup(db.Close)
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.Phase0Version)
	block.Block.Slot = slot
	var blockErr error
	stop := errors.New("stop after the first block")
	downloader := &firstBlockHistoryDownloader{failingHistoryDownloader: failingHistoryDownloader{err: stop}}
	downloader.deliver = func(onNewBlock network.OnNewBlock) {
		assert.Eventually(t, func() bool { return sn.BlocksAvailable() == visibleTip }, 10*time.Second, 10*time.Millisecond)
		finished, blockErr = onNewBlock(block, nil)
	}

	err := SpawnStageHistoryDownload(StageHistoryReconstructionCfg{
		beaconCfg:          &cfg,
		downloader:         downloader,
		sn:                 sn,
		waitForAllRoutines: true,
		startingSlot:       2 * snaptype.CaplinMergeLimit,
		indiciesDB:         db,
		caplinConfig:       clparams.CaplinConfig{ArchiveBlocks: true},
		logger:             log.New(),
	}, t.Context(), log.New())
	require.ErrorIs(t, err, stop)
	require.NoError(t, blockErr)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		beaconBlockRows, err = tx.Count(kv.BeaconBlocks)
		return err
	}))
	return finished, beaconBlockRows
}

func TestHistoryDownloadServesIndexedPrefixWhenIndexBuildFails(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit
	dirs := datadir.New(t.TempDir())
	writeUnindexedBeaconBlocks(t, dirs, 0, limit)
	writeUnindexedBeaconBlocks(t, dirs, limit, 2*limit)
	indexed, _, ok := snaptype.ParseFileName(dirs.Snap, snaptype.BeaconBlocks.FileName(version.ZeroVersion, 0, limit))
	require.True(t, ok)
	require.NoError(t, snapshotsync.BeaconSimpleIdx(t.Context(), indexed, 0, dirs.Tmp, &background.Progress{}, log.LvlDebug, log.New()))
	sn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	sn.SetIndexBuilder(snaptype.BeaconBlocks, failingIndexBuilder)

	finished, rows := historyDownloadFirstBlock(t, sn, limit-1, limit/2)
	require.Zero(t, rows, "a slot an indexed segment serves must not be written to the DB")
	require.True(t, finished, "the archive backfill must stop at the indexed snapshot tip")
}

func TestHistoryDownloadBackfillsBelowAbsentLeadingRange(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit
	for _, tc := range []struct {
		name     string
		slot     uint64
		wantRows uint64
	}{
		{"slot the indexed segment serves", limit, 0},
		{"slot below the indexed segment", limit / 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			writeUnindexedBeaconBlocks(t, dirs, limit, 2*limit)
			sn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
			t.Cleanup(sn.Close)

			finished, rows := historyDownloadFirstBlock(t, sn, 2*limit-1, tc.slot)
			require.Equal(t, uint64(2*limit-1), sn.SegmentsMax())
			require.Equal(t, tc.wantRows, rows)
			require.False(t, finished, "slots below a beaconblocks range absent from disk must still be backfilled")
		})
	}
}

func TestHistoryDownloadStartsDownloaderWhileEngineIsNotReady(t *testing.T) {
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	engine.EXPECT().SupportInsertion().Return(true).AnyTimes()
	engine.EXPECT().Ready(gomock.Any()).Return(false, nil).AnyTimes()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	requested := make(chan struct{})
	downloader := &firstBlockHistoryDownloader{failingHistoryDownloader: failingHistoryDownloader{err: errors.New("stop after the first request")}}
	downloader.deliver = func(network.OnNewBlock) { close(requested) }
	go func() {
		_ = SpawnStageHistoryDownload(StageHistoryReconstructionCfg{
			beaconCfg:    &clparams.MainnetBeaconConfig,
			downloader:   downloader,
			engine:       engine,
			startingSlot: 1,
			caplinConfig: clparams.CaplinConfig{ArchiveBlocks: true},
			logger:       log.New(),
		}, ctx, log.New())
	}()
	select {
	case <-requested:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "the backward downloader must start while the engine is not ready")
	}
}

func TestHistoryDownloadServesRangeIndexedAfterEngineReady(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit
	dirs := datadir.New(t.TempDir())
	writeUnindexedBeaconBlocks(t, dirs, 0, limit)
	cfg := clparams.MainnetBeaconConfig
	sn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(sn.Close)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	t.Cleanup(db.Close)

	var ready atomic.Bool
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	engine.EXPECT().SupportInsertion().Return(true).AnyTimes()
	engine.EXPECT().Ready(gomock.Any()).DoAndReturn(func(context.Context) (bool, error) { return ready.Load(), nil }).AnyTimes()

	before := cltypes.NewSignedBeaconBlock(&cfg, clparams.Phase0Version)
	before.Block.Slot = limit/2 + 1
	after := cltypes.NewSignedBeaconBlock(&cfg, clparams.Phase0Version)
	after.Block.Slot = limit / 2
	var beforeErr, afterErr error
	stop := errors.New("stop after the first request")
	downloader := &firstBlockHistoryDownloader{failingHistoryDownloader: failingHistoryDownloader{err: stop}}
	downloader.deliver = func(onNewBlock network.OnNewBlock) {
		_, beforeErr = onNewBlock(before, nil)
		ready.Store(true)
		assert.Eventually(t, func() bool { return sn.BlocksAvailable() == limit-1 }, 10*time.Second, 10*time.Millisecond, "the build must run once the engine is ready")
		_, afterErr = onNewBlock(after, nil)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	err := SpawnStageHistoryDownload(StageHistoryReconstructionCfg{
		beaconCfg:          &cfg,
		downloader:         downloader,
		sn:                 sn,
		engine:             engine,
		waitForAllRoutines: true,
		startingSlot:       2 * limit,
		indiciesDB:         db,
		caplinConfig:       clparams.CaplinConfig{ArchiveBlocks: true},
		logger:             log.New(),
	}, ctx, log.New())
	require.NoError(t, err)
	require.NoError(t, beforeErr)
	require.NoError(t, afterErr)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		for _, tc := range []struct {
			block *cltypes.SignedBeaconBlock
			row   bool
		}{{before, true}, {after, false}} {
			root, err := tc.block.Block.HashSSZ()
			require.NoError(t, err)
			v, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(tc.block.Block.Slot, root))
			require.NoError(t, err)
			require.Equal(t, tc.row, v != nil, "slot %d: only a block downloaded before the build lands is written to the DB", tc.block.Block.Slot)
		}
		return nil
	}))
}

func TestHistoryDownloadSkipsEngineWaitWithoutBlocksArchive(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	writeUnindexedBeaconBlocks(t, dirs, 0, snaptype.CaplinMergeLimit)
	sn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	var builds atomic.Int32
	sn.SetIndexBuilder(snaptype.BeaconBlocks, snaptype.IndexBuilderFunc(func(context.Context, snaptype.FileInfo, uint32, *chain.Config, string, *background.Progress, log.Lvl, log.Logger) error {
		builds.Add(1)
		return errors.New("no space left in tmpdir")
	}))
	var readyCalls atomic.Int32
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	engine.EXPECT().SupportInsertion().Return(true).AnyTimes()
	engine.EXPECT().Ready(gomock.Any()).DoAndReturn(func(context.Context) (bool, error) {
		readyCalls.Add(1)
		return true, nil
	}).AnyTimes()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	downloader := &countingHistoryDownloader{failingHistoryDownloader: failingHistoryDownloader{err: errors.New("stop after the first request")}}
	err := SpawnStageHistoryDownload(StageHistoryReconstructionCfg{
		beaconCfg:          &clparams.MainnetBeaconConfig,
		downloader:         downloader,
		sn:                 sn,
		engine:             engine,
		waitForAllRoutines: true,
		startingSlot:       1,
		logger:             log.New(),
	}, ctx, log.New())
	require.Equal(t, 1, downloader.requests, "a node that archives no blocks must not wait for the engine")
	require.NoError(t, err)
	require.Never(t, func() bool { return readyCalls.Load() > 0 || builds.Load() > 0 }, time.Second, 10*time.Millisecond, "a node that archives no blocks must neither poll the engine nor build caplin indexes")
}

func TestWaitForHistoryCompletion(t *testing.T) {
	t.Run("asynchronous caller returns immediately", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		require.NoError(t, waitForHistoryCompletion(ctx, make(chan struct{}), false))
	})

	t.Run("synchronous caller waits for completion", func(t *testing.T) {
		finishCh := make(chan struct{})
		close(finishCh)

		require.NoError(t, waitForHistoryCompletion(context.Background(), finishCh, true))
	})

	t.Run("synchronous caller remains owned until cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- waitForHistoryCompletion(ctx, make(chan struct{}), true)
		}()
		cancel()

		require.ErrorIs(t, <-done, context.Canceled)
	})

	t.Run("cancellation wins when completion is also ready", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		finishCh := make(chan struct{})
		close(finishCh)

		require.ErrorIs(t, waitForHistoryCompletion(ctx, finishCh, true), context.Canceled)
	})
}
