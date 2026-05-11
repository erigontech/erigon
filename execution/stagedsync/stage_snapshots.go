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

package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/stats"
	"github.com/erigontech/erigon/diagnostics/diaglib"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/shards"
)

type SnapshotsCfg struct {
	db                 kv.TemporalRwDB
	chainConfig        *chain.Config
	dirs               datadir.Dirs
	blockRetire        services.BlockRetire
	snapshotDownloader downloader.Client
	blockReader        services.FullBlockReader
	notifier           *shards.Notifications
	caplin             bool
	blobs              bool
	caplinState        bool
	syncConfig         ethconfig.Sync
	prune              prune.Mode
	// Called once after snapshot downloads complete on the first sync cycle.
	afterDownload func(ctx context.Context) error
	// manifestReady is closed when P2P manifest discovery completes (--snap.p2p-manifest).
	// Nil when P2P manifest mode is not enabled.
	manifestReady <-chan struct{}
}

// Returns a seeder client for block management, a noop implementation if no downloader is attached.
func (me *SnapshotsCfg) getSeederClient() downloader.SeederClient {
	if me.snapshotDownloader == nil {
		return downloader.NoopSeederClient{}
	}
	return me.snapshotDownloader
}

func StageSnapshotsCfg(db kv.TemporalRwDB,
	chainConfig *chain.Config,
	syncConfig ethconfig.Sync,
	dirs datadir.Dirs,
	blockRetire services.BlockRetire,
	snapshotDownloader downloader.Client,
	blockReader services.FullBlockReader,
	notifier *shards.Notifications,
	caplin bool,
	blobs bool,
	caplinState bool,
	prune prune.Mode,
	afterDownload func(ctx context.Context) error,
	manifestReady <-chan struct{},
) SnapshotsCfg {
	cfg := SnapshotsCfg{
		db:                 db,
		chainConfig:        chainConfig,
		dirs:               dirs,
		blockRetire:        blockRetire,
		snapshotDownloader: snapshotDownloader,
		blockReader:        blockReader,
		notifier:           notifier,
		caplin:             caplin,
		syncConfig:         syncConfig,
		blobs:              blobs,
		prune:              prune,
		caplinState:        caplinState,
		afterDownload:      afterDownload,
		manifestReady:      manifestReady,
	}

	return cfg
}

func SpawnStageSnapshots(s *StageState, ctx context.Context, tx kv.RwTx, cfg SnapshotsCfg, logger log.Logger) (err error) {
	if err := DownloadAndIndexSnapshotsIfNeed(s, ctx, tx, cfg, logger); err != nil {
		return err
	}
	var minProgress uint64
	for _, stage := range []stages.SyncStage{stages.Headers, stages.Bodies, stages.Senders, stages.TxLookup} {
		progress, err := stages.GetStageProgress(tx, stage)
		if err != nil {
			return err
		}
		if minProgress == 0 || progress < minProgress {
			minProgress = progress
		}

		if stage == stages.SyncStage(cfg.syncConfig.BreakAfterStage) {
			break
		}
	}

	if minProgress > s.BlockNumber {
		if err = s.Update(tx, minProgress); err != nil {
			return err
		}
	}

	// call this after the tx is commited otherwise observing
	// components see an inconsistent db view
	if !cfg.blockReader.Snapshots().DownloadReady() {
		cfg.blockReader.Snapshots().DownloadComplete()
	}
	if cfg.chainConfig.Bor != nil && !cfg.blockReader.BorSnapshots().DownloadReady() {
		cfg.blockReader.BorSnapshots().DownloadComplete()
	}

	return nil
}

func DownloadAndIndexSnapshotsIfNeed(s *StageState, ctx context.Context, tx kv.RwTx, cfg SnapshotsCfg, logger log.Logger) error {
	if !s.CurrentSyncCycle.IsFirstCycle {
		return nil
	}

	cstate := snapshotsync.NoCaplin
	if cfg.caplin {
		cstate = snapshotsync.AlsoCaplin
	}

	log.Info("[OtterSync] Starting Ottersync")

	// If P2P manifest mode is enabled, wait for chain.toml discovery before
	// building download requests. Without this, the preverified registry is
	// empty and OtterSync would complete instantly with nothing to download.
	//
	// Bounded wait: if discovery never succeeds (no peers with chain-toml ENR,
	// unreachable info-hash, etc.) we fall through to the centralized preverified
	// registry rather than stalling the sync indefinitely.
	const manifestReadyTimeout = 5 * time.Minute
	if cfg.manifestReady != nil {
		log.Info(fmt.Sprintf("[%s] Waiting for P2P manifest discovery (timeout %s)...", s.LogPrefix(), manifestReadyTimeout))
		select {
		case <-cfg.manifestReady:
			log.Info(fmt.Sprintf("[%s] P2P manifest ready, proceeding with download", s.LogPrefix()))
		case <-time.After(manifestReadyTimeout):
			log.Warn(fmt.Sprintf("[%s] P2P manifest discovery timed out after %s — falling back to preverified registry", s.LogPrefix(), manifestReadyTimeout))
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	agg := cfg.db.(*temporal.DB).Agg().(*state.Aggregator)
	// Download only the snapshots that are for the header chain.

	// How do we get to the real Downloader if we need? Get the stack trace.
	//panic("here")

	if err := snapshotsync.SyncSnapshots(
		ctx,
		s.LogPrefix(),
		"header-chain",
		true, /*headerChain=*/
		cfg.blobs,
		cfg.caplinState,
		cfg.prune,
		cstate,
		tx,
		cfg.blockReader,
		cfg.chainConfig,
		cfg.snapshotDownloader,
		cfg.syncConfig,
		agg.StepSize(),
	); err != nil {
		return err
	}

	// Reload erigondb settings: the downloader should have provided the real erigondb.toml
	// during header-chain phase, which may have a different stepSize than the default.
	if err := agg.ReloadErigonDBSettings(cfg.snapshotDownloader == nil); err != nil {
		return err
	}

	// Erigon can start on datadir with broken files `transactions.seg` files and Downloader will
	// fix them, but only if Erigon call `.Add()` for broken files. But `headerchain` feature
	// calling `.Add()` only for header/body files (not for `transactions.seg`) and `.OpenFolder()` will fail
	if err := cfg.blockReader.Snapshots().OpenSegments([]snaptype.Type{snaptype2.Headers, snaptype2.Bodies}, true, false); err != nil {
		err = fmt.Errorf("error opening segments after syncing header chain: %w", err)
		return err
	}

	diaglib.Send(diaglib.CurrentSyncSubStage{SubStage: "Download snapshots"})
	if err := snapshotsync.SyncSnapshots(
		ctx,
		s.LogPrefix(),
		"remaining snapshots",
		false, /*headerChain=*/
		cfg.blobs,
		cfg.caplinState,
		cfg.prune,
		cstate,
		tx,
		cfg.blockReader,
		cfg.chainConfig,
		cfg.snapshotDownloader,
		cfg.syncConfig,
		agg.StepSize(),
	); err != nil {
		return err
	}

	if cfg.afterDownload != nil {
		if err := cfg.afterDownload(ctx); err != nil {
			return fmt.Errorf("after snapshot download: %w", err)
		}
	}

	{ // Now can open all files
		if err := cfg.blockReader.Snapshots().OpenFolder(); err != nil {
			return err
		}

		if cfg.chainConfig.Bor != nil {
			if err := cfg.blockReader.BorSnapshots().OpenFolder(); err != nil {
				return err
			}
		}
		if err := agg.OpenFolder(); err != nil {
			return err
		}

		// After opening files: check for state/block snapshot misalignment.
		if err := alignStateToBlockSnapshots(ctx, agg, cfg, s.LogPrefix(), logger); err != nil {
			return fmt.Errorf("align state to block snapshots: %w", err)
		}

		if err := firstNonGenesisCheck(tx, cfg.blockReader.Snapshots(), s.LogPrefix(), cfg.dirs); err != nil {
			return err
		}
	}

	// All snapshots are downloaded. Now commit the preverified.toml file so we load the same set of
	// hashes next time.
	err := downloadercfg.SaveSnapshotHashes(cfg.dirs, cfg.chainConfig.ChainName)
	if err != nil {
		err = fmt.Errorf("saving snapshot hashes: %w", err)
		return err
	}

	if cfg.notifier.Events != nil {
		cfg.notifier.Events.OnNewSnapshot()
	}

	headersProgress, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return fmt.Errorf("getting headers progress for indexing decision: %w", err)
	}

	if err := buildOrDeferE2Indices(ctx, s, cfg, headersProgress); err != nil {
		return err
	}

	if err := buildOrDeferE3Accessors(ctx, s, cfg, agg, headersProgress); err != nil {
		return err
	}

	if temporal, ok := tx.(*temporal.RwTx); ok {
		temporal.ForceReopenAggCtx() // otherwise next stages will not see just-indexed-files
	}

	// It's ok to notify before tx.Commit(), because RPCDaemon does read list of files by gRPC (not by reading from db)
	if cfg.notifier.Events != nil {
		cfg.notifier.Events.OnNewSnapshot()
	}

	frozenBlocks := cfg.blockReader.FrozenBlocks()
	if s.BlockNumber < frozenBlocks { // allow genesis
		if err := s.Update(tx, frozenBlocks); err != nil {
			return err
		}
		s.BlockNumber = frozenBlocks
	}

	diaglib.Send(diaglib.CurrentSyncSubStage{SubStage: "Fill DB"})
	if err := rawdbreset.FillDBFromSnapshots(s.LogPrefix(), ctx, tx, cfg.dirs, cfg.blockReader, logger); err != nil {
		return fmt.Errorf("FillDBFromSnapshots: %w", err)
	}

	if temporal, ok := tx.(*temporal.RwTx); ok {
		temporal.ForceReopenAggCtx() // otherwise next stages will not see just-indexed-files
	}

	// In E3, the post-execution state is in domain files. After FillDBFromSnapshots,
	// snapshot domain state may be ahead of the Execution stage progress (which is 0
	// on a fresh node until ExecV3 runs and self-corrects via SeekCommitment). During
	// that startup window the RPC layer can resolve `latest` to genesis (exec=0) while
	// the cached state reader serves snapshot-tip state — a state-vs-rules mismatch
	// that crashed eth_call on Gnosis with "invalid opcode: SHR" (#21066).
	// Bump Execution stage progress to the snapshot commitment block so RPC sees a
	// consistent view immediately, matching what ExecV3 would set on its first run.
	if commitBlock := readCommitmentBlockFromDB(ctx, cfg.db); commitBlock > 0 {
		execProgress, err := stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return fmt.Errorf("get Execution stage progress: %w", err)
		}
		if execProgress < commitBlock {
			if err := stages.SaveStageProgress(tx, stages.Execution, commitBlock); err != nil {
				return fmt.Errorf("advance Execution stage to snapshot commitment block: %w", err)
			}
		}
	}

	{
		cfg.blockReader.Snapshots().LogStat("download")
		txNumsReader := cfg.blockReader.TxnumReader()
		aggtx := state.AggTx(tx)
		stats.LogStats(aggtx, tx, logger, func(endTxNumMinimax uint64) (uint64, error) {
			histBlockNumProgress, _, err := txNumsReader.FindBlockNum(ctx, tx, endTxNumMinimax)
			return histBlockNumProgress, err
		})
	}

	return nil
}

// buildOrDeferE2Indices decides whether to build E2 block snapshot indices synchronously
// or defer them to background processing.
// On restart (headersProgress > 0), E2 indexing is skipped at startup. Missing indices
// will be built in the background via RetireBlocksInBackground (called from SnapshotsPrune
// on every sync cycle).
// Exception: Bor chains always index synchronously because RetireBlocks has an early-exit
// guard for Bor data readiness that may skip BuildMissedIndicesIfNeed.
func buildOrDeferE2Indices(ctx context.Context, s *StageState, cfg SnapshotsCfg, headersProgress uint64) error {
	isBor := cfg.chainConfig.Bor != nil
	canDefer := headersProgress > 0 && !isBor

	diaglib.Send(diaglib.CurrentSyncSubStage{SubStage: "E2 Indexing"})
	if !canDefer {
		if err := cfg.blockRetire.BuildMissedIndicesIfNeed(ctx, s.LogPrefix(), cfg.notifier.Events); err != nil {
			return err
		}
	} else {
		log.Debug(fmt.Sprintf("[%s] Deferring E2 indexing to background", s.LogPrefix()), "reason", "restart", "headersProgress", headersProgress)
	}
	return nil
}

// buildOrDeferE3Accessors decides whether to build E3 state accessors synchronously
// or defer them to background processing.
// On restart (headersProgress > 0), E3 indexing is skipped at startup. Missing accessors
// will be built in the background via BuildMissedAccessorsInBackground (called from
// SnapshotsPrune on every sync cycle).
// Unindexed state files are safely excluded from visible files by checkForVisibility
// (which checks accessor presence), so queries correctly reflect only indexed data.
// Note: unlike E2, there is no Bor exception — the background path calls
// BuildMissedAccessors directly without any Bor-specific early-exit guards.
func buildOrDeferE3Accessors(ctx context.Context, s *StageState, cfg SnapshotsCfg, agg *state.Aggregator, headersProgress uint64) error {
	canDefer := headersProgress > 0

	indexWorkers := estimate.IndexSnapshot.Workers()
	diaglib.Send(diaglib.CurrentSyncSubStage{SubStage: "E3 Indexing"})
	if !canDefer {
		if err := agg.BuildMissedAccessors(ctx, indexWorkers); err != nil {
			return err
		}
	} else {
		log.Debug(fmt.Sprintf("[%s] Deferring E3 indexing to background", s.LogPrefix()), "reason", "restart", "headersProgress", headersProgress)
	}
	return nil
}

func firstNonGenesisCheck(tx kv.RwTx, snapshots services.BlockSnapshots, logPrefix string, dirs datadir.Dirs) error {
	firstNonGenesis, err := rawdbv3.SecondKey(tx, kv.Headers)
	if err != nil {
		return err
	}
	if firstNonGenesis != nil {
		firstNonGenesisBlockNumber := binary.BigEndian.Uint64(firstNonGenesis)
		if snapshots.SegmentsMax()+1 < firstNonGenesisBlockNumber {
			log.Warn(fmt.Sprintf("[%s] Some blocks are not in snapshots and not in db. This could have happened because the node was stopped at the wrong time; you can fix this with 'rm -rf %s' (this is not equivalent to a full resync)", logPrefix, dirs.Chaindata), "max_in_snapshots", snapshots.SegmentsMax(), "min_in_db", firstNonGenesisBlockNumber)
			return fmt.Errorf("some blocks are not in snapshots and not in db. This could have happened because the node was stopped at the wrong time; you can fix this with 'rm -rf %s' (this is not equivalent to a full resync)", dirs.Chaindata)
		}
	}
	return nil
}

func pruneCanonicalMarkers(ctx context.Context, tx kv.RwTx, blockReader services.FullBlockReader) error {
	pruneThreshold := rawdbreset.GetPruneMarkerSafeThreshold(blockReader)
	if pruneThreshold == 0 {
		return nil
	}

	c, err := tx.RwCursor(kv.HeaderCanonical) // Number -> Hash
	if err != nil {
		return err
	}
	defer c.Close()
	var tdKey [40]byte
	for k, v, err := c.First(); k != nil && err == nil; k, v, err = c.Next() {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum == 0 { // Do not prune genesis marker
			continue
		}
		if blockNum >= pruneThreshold {
			break
		}
		if err := tx.Delete(kv.HeaderNumber, v); err != nil {
			return err
		}
		if dbg.PruneTotalDifficulty() {
			copy(tdKey[:], k)
			copy(tdKey[8:], v)
			if err := tx.Delete(kv.HeaderTD, tdKey[:]); err != nil {
				return err
			}
		}
		if err := c.DeleteCurrent(); err != nil {
			return err
		}
	}
	return nil
}

// SnapshotsPrune moving block data from db into snapshots, removing old snapshots (if --prune.* enabled)
func SnapshotsPrune(s *PruneState, cfg SnapshotsCfg, ctx context.Context, tx kv.RwTx, logger log.Logger) (err error) {
	if dbg.NoPrune() {
		return nil
	}
	freezingCfg := cfg.blockReader.FreezingCfg()
	if freezingCfg.ProduceE2 && !dbg.NoBackgroundMaintenance() {
		//TODO: initialSync maybe save files progress here

		var minBlockNumber uint64

		if s.CurrentSyncCycle.IsInitialCycle {
			cfg.blockRetire.SetWorkers(estimate.CompressSnapshot.Workers())
		} else {
			cfg.blockRetire.SetWorkers(1)
		}

		started := cfg.blockRetire.RetireBlocksInBackground(
			ctx,
			minBlockNumber,
			s.ForwardProgress,
			log.LvlDebug,
			cfg.getSeederClient(),
			func() error {
				filesDeleted, err := pruneBlockSnapshots(ctx, cfg, logger)
				if filesDeleted && cfg.notifier != nil {
					cfg.notifier.Events.OnNewSnapshot()
				}
				return err
			},
			func() {
				if cfg.notifier != nil {
					cfg.notifier.Events.OnRetirementDone()
				}
			})
		if cfg.notifier != nil {
			cfg.notifier.Events.OnRetirementStart(started)
		}
	}

	pruneLimit := 10
	pruneTimeout := 125 * time.Millisecond
	if s.CurrentSyncCycle.IsInitialCycle {
		pruneLimit = 10_000
		pruneTimeout = time.Hour
	}
	if _, err := cfg.blockRetire.PruneAncientBlocks(tx, pruneLimit, pruneTimeout); err != nil {
		return err
	}
	if err := pruneCanonicalMarkers(ctx, tx, cfg.blockReader); err != nil {
		return err
	}
	return nil
}

func pruneBlockSnapshots(ctx context.Context, cfg SnapshotsCfg, logger log.Logger) (bool, error) {
	tx, err := cfg.db.BeginRo(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	// Prune snapshots if necessary (remove .segs or idx files appropriately)
	headNumber := cfg.blockReader.FrozenBlocks()
	executionProgress, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return false, err
	}
	// If we are behind the execution stage, we should not prune snapshots
	if headNumber > executionProgress || !cfg.prune.Blocks.Enabled() {
		return false, nil
	}

	// Keep at least 2 block snapshots as we do not want FrozenBlocks to be 0
	pruneTo := cfg.prune.Blocks.PruneTo(headNumber)

	if pruneTo > executionProgress {
		return false, nil
	}

	//TODO: push-down this logic into `blockRetire`: instead of work on raw file names - we must work on dirtySegments. Instead of calling downloader.Del(file) we must call `downloader.Del(dirtySegment.Paths(snapDir)`
	snapshotFileNames := cfg.blockReader.FrozenFiles()
	filesDeleted := false
	// Prune blocks snapshots if necessary
	for _, file := range snapshotFileNames {
		if !cfg.prune.Blocks.Enabled() || headNumber == 0 || !strings.Contains(file, "transactions") {
			continue
		}

		// take the snapshot file name and parse it to get the "from"
		info, _, ok := snaptype.ParseFileName(cfg.dirs.Snap, file)
		if !ok {
			continue
		}
		if info.To >= pruneTo {
			continue
		}
		if info.To-info.From != snaptype.Erigon2MergeLimit {
			continue
		}
		err = cfg.getSeederClient().Delete(ctx, []string{file})
		if err != nil {
			return filesDeleted, err
		}
		if err := cfg.blockReader.Snapshots().Delete(file); err != nil {
			return filesDeleted, err
		}
		filesDeleted = true
	}
	return filesDeleted, nil
}

// alignStateToBlockSnapshots detects when state domain files imply a
// commitment block past the current frozen block snapshots (for example due to
// a preverified snapshot publication mismatch) and removes the highest state
// files until the state view no longer extends beyond the block snapshots.
// Without this, SeekCommitment can return a block number past what TxNums
// covers, causing "behind commitment" errors.
//
// Algorithm: read the commitment block from the current state files (via the
// already-open aggregator) and compare with cfg.blockReader.FrozenBlocks().
// If ahead, remove the highest state files (all domains), de-register them
// from the downloader, strip from preverified.toml, reopen, and repeat.
func alignStateToBlockSnapshots(ctx context.Context, agg *state.Aggregator, cfg SnapshotsCfg, logPrefix string, logger log.Logger) error {
	frozenBlocks := cfg.blockReader.FrozenBlocks()
	if frozenBlocks == 0 {
		return nil
	}

	// Only align on fresh start. If MDBX already has commitment data
	// (written by execution, not by OtterSync indexing), the node
	// previously executed past any snapshot misalignment.
	// Re-running alignment on restart would remove snapshot files that
	// the downloader re-downloaded, cascading until all state is gone.
	if roTx, err := cfg.db.BeginRo(ctx); err == nil {
		// Check if the commitment domain values table has any entries.
		// This table is only populated by execution, not by OtterSync.
		if cursor, cErr := roTx.Cursor(kv.TblCommitmentVals); cErr == nil {
			k, _, _ := cursor.First()
			cursor.Close()
			roTx.Rollback()
			if k != nil {
				return nil // execution has run — skip alignment
			}
		} else {
			roTx.Rollback()
		}
	}

	dirs := cfg.dirs
	totalRemoved := 0

	for {
		commitBlock := readCommitmentBlockFromDB(ctx, cfg.db)
		logger.Debug(fmt.Sprintf("[%s] alignment check", logPrefix),
			"commitBlock", commitBlock, "frozenBlocks", frozenBlocks, "totalRemoved", totalRemoved)

		if commitBlock == 0 || commitBlock <= frozenBlocks {
			if totalRemoved > 0 {
				logger.Info(fmt.Sprintf("[%s] state/block snapshot alignment complete", logPrefix),
					"commitBlock", commitBlock, "frozenBlocks", frozenBlocks, "filesRemoved", totalRemoved)
			}
			return nil
		}

		// Commitment past block boundary. Remove the highest state files.
		highestStart, found := findHighestStateFileStartStep(dirs)
		if !found {
			logger.Warn(fmt.Sprintf("[%s] state files misaligned but no removable files found", logPrefix),
				"commitBlock", commitBlock, "frozenBlocks", frozenBlocks)
			return nil
		}

		logger.Info(fmt.Sprintf("[%s] state/block misalignment detected, removing step %d", logPrefix, highestStart),
			"commitBlock", commitBlock, "frozenBlocks", frozenBlocks)

		removedFiles := removeStateFilesFromStep(dirs, highestStart, logger, logPrefix)
		totalRemoved += removedFiles.count
		if removedFiles.count == 0 {
			return nil
		}

		// Tell the downloader to de-register deleted files so the torrent
		// client doesn't panic trying to serve them to peers.
		if len(removedFiles.names) > 0 {
			seeder := cfg.getSeederClient()
			if err := seeder.Delete(ctx, removedFiles.names); err != nil {
				logger.Warn(fmt.Sprintf("[%s] failed to de-register deleted files from downloader", logPrefix), "err", err)
			}
		}

		// Reopen aggregator files with the reduced set.
		if err := agg.OpenFolder(); err != nil {
			return err
		}
	}
}

// readCommitmentBlockFromDB reads the commitment domain's "state" key via a
// temporary RO tx. The RwTx from the snapshot stage is not temporal, so we
// need a separate temporal RO tx to read domain data from snapshot files.
// The value format: txNum(8 bytes) + blockNum(8 bytes) + trie state.
func readCommitmentBlockFromDB(ctx context.Context, db kv.TemporalRwDB) uint64 {
	roTx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return 0
	}
	defer roTx.Rollback()
	v, _, err := roTx.GetLatest(kv.CommitmentDomain, commitmentdb.KeyCommitmentState)
	if err != nil || len(v) < 16 {
		return 0
	}
	return binary.BigEndian.Uint64(v[8:16])
}

// findHighestStateFileStartStep finds the start step of the highest state
// domain file. Returns (step, true) or (0, false) if no state files exist.
func findHighestStateFileStartStep(dirs datadir.Dirs) (kv.Step, bool) {
	var highest kv.Step
	found := false
	entries, err := os.ReadDir(dirs.SnapDomain)
	if err != nil {
		return 0, false
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, ".torrent") {
			continue
		}
		if !isStateDomainFile(name) {
			continue
		}
		startStep, _, ok := parseFileStepRange(name)
		if !ok {
			continue
		}
		if !found || startStep > highest {
			highest = startStep
			found = true
		}
	}
	return highest, found
}

// isStateDomainFile returns true for state snapshot data files (accounts,
// storage, code, commitment, receipt, rcache) and false for block snapshot
// files (bodies, headers, transactions) to avoid accidentally deleting
// block-level indices.
func isStateDomainFile(name string) bool {
	// State domain files contain these domain names in their filename.
	domains := []string{"accounts", "storage", "code", "commitment", "receipt", "rcache",
		"logaddrs", "logtopics", "tracesfrom", "tracesto"}
	lower := strings.ToLower(name)
	for _, d := range domains {
		if strings.Contains(lower, d) {
			return true
		}
	}
	return false
}

type removedFilesResult struct {
	count int
	names []string // relative paths for downloader de-registration
}

// removeStateFilesFromStep removes all state files whose start step >= fromStep
// and strips matching entries from preverified.toml so the downloader doesn't
// re-download them.
func removeStateFilesFromStep(dirs datadir.Dirs, fromStep kv.Step, logger log.Logger, logPrefix string) removedFilesResult {
	result := removedFilesResult{}

	for _, snapDir := range []string{dirs.SnapDomain, dirs.SnapIdx, dirs.SnapHistory, dirs.SnapAccessors} {
		entries, err := os.ReadDir(snapDir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if strings.HasSuffix(name, ".torrent") {
				continue
			}
			if !isStateDomainFile(name) {
				continue
			}
			startStep, _, ok := parseFileStepRange(name)
			if !ok || startStep < fromStep {
				continue
			}
			dataPath := filepath.Join(snapDir, name)
			torrentPath := dataPath + ".torrent"
			if err := dir.RemoveFile(dataPath); err != nil && !os.IsNotExist(err) {
				continue
			}
			dir.RemoveFile(torrentPath) //nolint:errcheck
			relDir := filepath.Base(snapDir)
			result.names = append(result.names, filepath.Join(relDir, name))
			logger.Debug(fmt.Sprintf("[%s] removed misaligned file", logPrefix), "file", name, "startStep", startStep)
			result.count++
		}
	}

	// Strip removed step ranges from preverified.toml so the downloader
	// doesn't re-fetch them on next startup.
	pvPath := filepath.Join(dirs.Snap, "preverified.toml")
	if data, err := os.ReadFile(pvPath); err == nil {
		lines := strings.Split(string(data), "\n")
		var kept []string
		stripped := 0
		for _, line := range lines {
			startStep, _, ok := parseFileStepRange(line)
			if ok && startStep >= fromStep && isStateDomainFile(line) {
				stripped++
				continue
			}
			kept = append(kept, line)
		}
		if stripped > 0 {
			if err := os.Chmod(pvPath, 0644); err != nil {
				logger.Warn(fmt.Sprintf("[%s] failed to chmod preverified.toml", logPrefix), "err", err)
			} else if err := os.WriteFile(pvPath, []byte(strings.Join(kept, "\n")), 0644); err != nil {
				logger.Warn(fmt.Sprintf("[%s] failed to write preverified.toml", logPrefix), "err", err)
			} else {
				logger.Info(fmt.Sprintf("[%s] stripped %d entries from preverified.toml", logPrefix, stripped))
			}
		}
	}

	if result.count > 0 {
		logger.Info(fmt.Sprintf("[%s] removed state files from step %d", logPrefix, fromStep), "removed", result.count)
	}
	return result
}

// parseFileStepRange extracts start and end steps from a state snapshot filename.
// Filenames: "v2.0-accounts.8192-8704.kv" → start=8192, end=8704
func parseFileStepRange(name string) (start, end kv.Step, ok bool) {
	parts := strings.Split(name, ".")
	for _, part := range parts {
		if idx := strings.Index(part, "-"); idx > 0 {
			var s, e uint64
			if _, err := fmt.Sscanf(part, "%d-%d", &s, &e); err == nil && e > s {
				return kv.Step(s), kv.Step(e), true
			}
		}
	}
	return 0, 0, false
}
