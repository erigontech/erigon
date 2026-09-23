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
	"time"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/stats"
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
	blockRetire        dbservices.BlockRetire
	snapshotDownloader dbservices.DownloaderClient
	blockReader        dbservices.FullBlockReader
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
	// lifecycleDrivenByStorage gates the stage's index/accessor build
	// calls. When true, the storage component's lifecycle driver
	// owns those transitions; the stage no-ops. Set via
	// SetLifecycleDrivenByStorage by production wiring (backend.go);
	// defaults false. See ethconfig.BlocksFreezing.LifecycleDrivenByStorage
	// for the operator-facing flag.
	lifecycleDrivenByStorage bool

	// initialStateReady is the orchestrator's "minimal-set ready"
	// signal. When lifecycleDrivenByStorage is true AND this channel
	// is non-nil, OtterSync skips its own SyncSnapshots calls
	// entirely (storage owns download) and waits on this channel
	// before resuming with file-open / agg-reload bookkeeping. The
	// downstream stages (BlockHashes/Senders/Execution/...) can then
	// run concurrently with the long-tail historical download still
	// in progress. Set via SetInitialStateReady by production wiring
	// (backend.go) when the storage orchestrator is constructed.
	// Defaults nil — existing callers see no behaviour change.
	initialStateReady <-chan struct{}

	// publishRetirementStart / publishRetirementDone bridge block
	// retirement signals onto the storage component's event bus. The
	// legacy shards.Events fan-out (OnRetirementStart/Done) carries no
	// range info; these hooks do. Production wiring (backend.go) sets
	// them to provider.Bus().Publish(flow.RetirementStarted{...} /
	// flow.RetirementDone{...}). Tests/tools that don't run the storage
	// component leave them nil (the publish call site null-checks).
	publishRetirementStart func(fromBlock, toBlock uint64)
	publishRetirementDone  func(fromBlock, toBlock uint64)
}

// SetLifecycleDrivenByStorage opts the stage out of driving
// index/accessor builds. When set, the storage component's lifecycle
// driver runs BuildMissedIndices and BuildMissedAccessors autonomously;
// stage_snapshots.go's buildOrDeferE2Indices and buildOrDeferE3Accessors
// become no-ops.
//
// Production wiring sources the value from
// ethconfig.BlocksFreezing.LifecycleDrivenByStorage. Tests and tools
// that don't run the storage component leave it false (the default).
func (cfg *SnapshotsCfg) SetLifecycleDrivenByStorage(b bool) {
	cfg.lifecycleDrivenByStorage = b
}

// SetInitialStateReady installs the orchestrator's minimal-set-ready
// channel. With it set (and LifecycleDrivenByStorage=true), OtterSync
// skips its own download calls and waits on this channel instead.
// Production wiring constructs and sets this when the storage
// orchestrator comes up; tests/tools that don't run the storage
// component leave it nil.
func (cfg *SnapshotsCfg) SetInitialStateReady(ch <-chan struct{}) {
	cfg.initialStateReady = ch
}

// SetRetirementPublishers installs hooks called when block retirement
// starts and finishes, with the [fromBlock, toBlock] range RetireBlocks
// was driven against. Production wiring (backend.go) bridges these
// onto the storage component's event bus (flow.RetirementStarted /
// flow.RetirementDone). Tests/tools that don't run the storage
// component leave them nil — the legacy shards.Events fan-out continues
// to fire either way (this is purely additive for dual-working).
func (cfg *SnapshotsCfg) SetRetirementPublishers(onStart, onDone func(fromBlock, toBlock uint64)) {
	cfg.publishRetirementStart = onStart
	cfg.publishRetirementDone = onDone
}

// Returns a seeder client for block management, a noop implementation if no downloader is attached.
func (me *SnapshotsCfg) getSeederClient() dbservices.SeederClient {
	if me.snapshotDownloader == nil {
		return dbservices.NoopSeederClient{}
	}
	return me.snapshotDownloader
}

func StageSnapshotsCfg(db kv.TemporalRwDB,
	chainConfig *chain.Config,
	syncConfig ethconfig.Sync,
	dirs datadir.Dirs,
	blockRetire dbservices.BlockRetire,
	snapshotDownloader dbservices.DownloaderClient,
	blockReader dbservices.FullBlockReader,
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

// mustReopenUnderlyingFilesTx refreshes the tx's pinned block-files/aggregator
// view so files opened earlier in this stage are visible to reads made through
// this tx. Panics rather than silently skipping: a tx that can't reopen would
// reintroduce stale-view bugs (e.g. minimal-mode history pruning downloading all files).
func mustReopenUnderlyingFilesTx(tx kv.RwTx) {
	reopener, ok := tx.(kv.CanReopenUnderlyingFilesTx)
	if !ok {
		panic(fmt.Sprintf("snapshots stage requires a tx that can ForceReopenUnderlyingFilesTx, got %T", tx))
	}
	reopener.ForceReopenUnderlyingFilesTx()
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
		if err := s.Update(tx, minProgress); err != nil {
			return err
		}
	}

	// call this after the tx is commited otherwise observing
	// components see an inconsistent db view
	if !cfg.blockReader.Snapshots().DownloadReady() {
		cfg.blockReader.Snapshots().DownloadComplete()
	}
	return nil
}

// Overridden by tests to avoid waiting whole intervals for a publish.
var snapshotDownloadProgressInterval = 2 * time.Second

// startSnapshotDownloadProgressReporter periodically republishes sync state so
// eth_syncing reports progress during the (long) snapshot download. The reply
// switches to download-based progress only once the first sample arrives, so a
// downloader with nothing to report never changes the reply shape. Returns a
// stop func that, on a successful download, pins progress at the commitment
// block to bridge the handoff to execution — or clears it when no
// commitment block came with the snapshots (execution restarts from genesis).
// No-op when the downloader can't report progress or the target is unknown.
func startSnapshotDownloadProgressReporter(ctx context.Context, cfg SnapshotsCfg) func(downloadErr error, downloadCompleted bool, commitBlock uint64) {
	noop := func(error, bool, uint64) {}
	provider, ok := cfg.snapshotDownloader.(dbservices.DownloadProgressProvider)
	if !ok {
		return noop
	}
	reporter := provider.DownloadProgress()
	if reporter == nil {
		return noop
	}
	// No state files means no commitment block: execution restarts from genesis,
	// so a byte ratio mapped onto the headers tip would climb to ~tip and then
	// reset to 0. Keep the stage-list reply instead.
	if cfg.blockReader.FreezingCfg().DisableDownloadE3 {
		return noop
	}
	var target uint64
	if c, known := snapcfg.KnownCfg(cfg.chainConfig.ChainName); known {
		toBlock := cfg.syncConfig.SnapshotDownloadToBlock
		// Not ExpectBlocks: it also counts slot-numbered CL segments, which can
		// exceed the EL tip. The headers files bound the blocks this download
		// covers; a capped download retains only the files up to the cap, so the
		// byte total covers that set and the target has to match its top boundary.
		for _, info := range c.PreverifiedParsed {
			if info == nil || info.Ext != ".seg" || info.Type == nil || info.Type.Enum() != snaptype2.Enums.Headers {
				continue
			}
			if !snapshotsync.BlockFileRetainedUnderCap(info.To, toBlock) {
				continue
			}
			target = max(target, info.To)
		}
		if target > 0 {
			target--
		}
	}
	if target == 0 {
		return noop
	}

	reporter.ResetProgress()

	publishState := func() {
		if cfg.notifier.Events == nil {
			return
		}
		if err := cfg.db.View(ctx, func(tx kv.Tx) error {
			return cfg.notifier.PublishSyncState(tx, cfg.blockReader.FrozenBlocks())
		}); err != nil {
			log.Warn("[OtterSync] sync-state publish failed", "err", err)
		}
	}

	// Owned by the ticker goroutine.
	var lastDone, lastTotal uint64
	publish := func() {
		done, total := reporter.Completed()
		if total == 0 {
			return
		}
		// The downloader refreshes its sample on a slower backoff than the tick,
		// so an unchanged one would open a ro-tx just to build an identical reply.
		if done == lastDone && total == lastTotal {
			return
		}
		lastDone, lastTotal = done, total
		cfg.notifier.SetSnapshotDownloading(done, total, target)
		publishState()
	}

	stopCtx, cancel := context.WithCancel(ctx)
	stopped := make(chan struct{})
	go func() {
		defer dbg.LogPanic()
		defer close(stopped)
		t := time.NewTicker(snapshotDownloadProgressInterval)
		defer t.Stop()
		for {
			select {
			case <-stopCtx.Done():
				return
			case <-t.C:
				publish()
			}
		}
	}()

	return func(downloadErr error, downloadCompleted bool, commitBlock uint64) {
		cancel()
		<-stopped
		// A failed download must not claim 100%, and clearing would fabricate 0:
		// keep the last honest sample. A failure after the download completed is
		// different — the sample would outlive a node that syncs on regardless,
		// so fall through to the same clear-or-pin as on success.
		if downloadErr != nil && !downloadCompleted {
			return
		}
		// Without a commitment block execution starts from genesis, so a handoff
		// pin would claim the frozen tip for the whole re-execution. Clear instead.
		if commitBlock == 0 {
			cfg.notifier.ClearSnapshotDownload()
			publishState()
			return
		}
		// Pin at the commitment block, where execution resumes, instead of
		// clearing: clearing would report currentBlock=0 until the first-cycle
		// commit, i.e. a 100%→0% dip. The last in-flight sample can map above it
		// when the snapshot set's commitment lags the headers tip; stepping back
		// to the block execution resumes from is the honest correction.
		cfg.notifier.SetSnapshotDownloadHandoff(commitBlock)
		publishState()
	}
}

func DownloadAndIndexSnapshotsIfNeed(s *StageState, ctx context.Context, tx kv.RwTx, cfg SnapshotsCfg, logger log.Logger) (err error) {
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
	//
	// 2 minutes balances two costs: too short and we fall through to
	// preverified before devp2p has even established staticpeer connections
	// (observed 2026-05-07 multi-consumer fleet test: 30s timeout fired
	// before publisher's RLPx handshake completed under host CPU
	// contention). Too long and a fresh consumer with no reachable
	// publisher waits unnecessarily before falling back. With the forced
	// discv5-ping-on-connect fix, the actual ENR-Resolve latency once
	// devp2p IS connected is sub-second; the budget is dominated by the
	// devp2p connection establishment itself.
	const manifestReadyTimeout = 2 * time.Minute
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

	// Storage-owned download: when the orchestrator drives the
	// lifecycle, OtterSync delegates download entirely. We skip both
	// SyncSnapshots calls and instead wait on the orchestrator's
	// initialStateReady signal before proceeding to the post-download
	// bookkeeping (file open, agg reload). Stages 2-6 then run
	// concurrently with whatever historical-tail download remains, per
	// the V2 architectural target (gap (d) in
	// .claude/plans/time-to-get-back-generic-mist.md).
	storageDriven := cfg.lifecycleDrivenByStorage && cfg.initialStateReady != nil
	if storageDriven {
		// (C) Step 4: OtterSync is a pure wait-and-return in
		// storageDriven mode. The storage component owns ALL the
		// post-download bookkeeping (OpenFolder, Aggregator.OpenFolder,
		// FillDBFromSnapshots, etc.) and runs them via the orchestrator's
		// postIndexed callback BEFORE InitialStateReady fires.
		//
		// The wait MUST happen with NO MDBX writer-tx held. Erigon's
		// framework opens an RW tx in ProcessFrozenBlocks that wraps
		// the entire stage loop including this stage; if we hold that
		// tx here while waiting on initialStateReady, the orchestrator's
		// postIndexed (running in its own goroutine) deadlocks trying
		// to acquire its own MDBX writer slot. The fix lives in
		// ProcessFrozenBlocks (executor.go): it waits on initialStateReady
		// BEFORE BeginTemporalRw. By the time control reaches this stage,
		// the signal has already fired and the channel-receive below
		// returns immediately — no deadlock, no held lock, OtterSync
		// returns instantly.
		select {
		case <-cfg.initialStateReady:
			log.Info(fmt.Sprintf("[%s] Storage signalled minimal set ready, OtterSync DONE", s.LogPrefix()))
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
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
		cfg.dirs.Snap,
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
	if err := cfg.blockReader.Snapshots().OpenSegments([]snaptype.Type{snaptype2.Headers, snaptype2.Bodies}, false); err != nil {
		err = fmt.Errorf("error opening segments after syncing header chain: %w", err)
		return err
	}
	// Only this phase reports progress: the header-chain phase above downloads a
	// small subset, so its ratio would jump backwards once the full set is known.
	var commitBlock uint64
	var downloadCompleted bool
	if !storageDriven {
		mustReopenUnderlyingFilesTx(tx)

		stopReporter := startSnapshotDownloadProgressReporter(ctx, cfg)
		defer func() {
			// A panic unwinds with err == nil; stopping as a success would clear the
			// last honest sample. Stop as a failure instead, then keep unwinding.
			if r := recover(); r != nil {
				stopReporter(fmt.Errorf("snapshots stage panic: %v", r), downloadCompleted, 0)
				panic(r)
			}
			stopReporter(err, downloadCompleted, commitBlock)
		}()

		if err := snapshotsync.SyncSnapshots(
			ctx,
			s.LogPrefix(),
			"snapshots",
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
			cfg.dirs.Snap,
		); err != nil {
			return err
		}
		downloadCompleted = true

		if cfg.afterDownload != nil {
			if err := cfg.afterDownload(ctx); err != nil {
				return fmt.Errorf("after snapshot download: %w", err)
			}
		}

		{ // Now can open all files
			// (A) bridge for the InitialStateReady race: the orchestrator
			// fires the signal when state-domain downloads are complete, but
			// the per-file lifecycle's Indexing transitions for block-header
			// .seg files (which produce .idx accessors) race the signal by
			// ~hundreds of ms in practice. OpenFolder's openSegments path
			// lstat's each .idx; if any is mid-build it fails the whole stage
			// and the publisher errors out at startup. Retry until either
			// OpenFolder succeeds (the .idx files have landed) or a generous
			// budget expires. End-state (see
			// docs/plans/20260518-storage-owns-post-download-pipeline.md) is
			// for storage to run OpenFolder itself as part of the pre-signal
			// pipeline; this retry is the stopgap.
			const openFolderTimeout = 30 * time.Second
			const openFolderBackoff = 100 * time.Millisecond
			openFolderDeadline := time.Now().Add(openFolderTimeout)
			for {
				err := cfg.blockReader.Snapshots().OpenFolder()
				if err == nil {
					break
				}
				if time.Now().After(openFolderDeadline) {
					return err
				}
				log.Debug(fmt.Sprintf("[%s] OpenFolder retry (lifecycle still indexing)", s.LogPrefix()), "err", err)
				select {
				case <-time.After(openFolderBackoff):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if err := cfg.db.OpenStateSnapshots(ctx); err != nil {
				return err
			}

			if err := firstNonGenesisCheck(tx, cfg.blockReader.Snapshots(), s.LogPrefix(), cfg.dirs); err != nil {
				return err
			}
		}
	}

	// All snapshots are downloaded. Now commit the preverified.toml file so we load the same set of
	// hashes next time.
	err = downloadercfg.SaveSnapshotHashes(cfg.dirs, cfg.chainConfig.ChainName)
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

	// a state file missing its accessor is excluded from the visible set, so E3 accessors
	// must be rebuilt before execution — there is no background rebuild path
	if err := cfg.db.Debug().BuildMissedAccessors(ctx, estimate.IndexSnapshot.Workers(), kv.SkipCoveredAccessors); err != nil {
		return err
	}

	mustReopenUnderlyingFilesTx(tx) // otherwise next stages will not see just-indexed-files

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

	if err := rawdbreset.FillDBFromSnapshots(s.LogPrefix(), ctx, tx, cfg.dirs, cfg.blockReader, logger); err != nil {
		return fmt.Errorf("FillDBFromSnapshots: %w", err)
	}

	mustReopenUnderlyingFilesTx(tx) // otherwise next stages will not see just-indexed-files

	// In E3, the post-execution state is in domain files. After FillDBFromSnapshots,
	// snapshot domain state may be ahead of the Execution stage progress (which is 0
	// on a fresh node until ExecV3 runs and self-corrects via SeekCommitment). During
	// that startup window the RPC layer can resolve `latest` to genesis (exec=0) while
	// the cached state reader serves snapshot-tip state — a state-vs-rules mismatch
	// that crashed eth_call on Gnosis with "invalid opcode: SHR" (#21066).
	// Bump Execution stage progress to the snapshot commitment block so RPC sees a
	// consistent view immediately, matching what ExecV3 would set on its first run.
	// Plain assignment: the deferred stopReporter reads this variable, and a
	// := here would shadow it and silently disable the handoff pin.
	commitBlock = readCommitmentBlockFromDB(ctx, cfg.db)
	if commitBlock > 0 {
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
// will be built in the background via BuildFilesInBackground (called from SnapshotsPrune
// on every sync cycle).
func buildOrDeferE2Indices(ctx context.Context, s *StageState, cfg SnapshotsCfg, headersProgress uint64) error {
	// When the storage component owns the import lifecycle its
	// lifecycle.Driver runs BuildMissedIndices on its own clock, so
	// building here would pre-empt it.
	if cfg.lifecycleDrivenByStorage {
		return nil
	}
	if headersProgress == 0 {
		if err := cfg.blockRetire.BuildMissedIndicesIfNeed(ctx, s.LogPrefix(), cfg.notifier.Events); err != nil {
			return err
		}
	} else {
		log.Debug(fmt.Sprintf("[%s] Deferring E2 indexing to background", s.LogPrefix()), "reason", "restart", "headersProgress", headersProgress)
	}
	return nil
}

// buildOrDeferE3Accessors decides whether to build E3 state accessors synchronously
// or defer them to background processing. Restart skips E3 indexing at startup;
// missing accessors get built later by BuildMissedAccessorsInBackground.
func buildOrDeferE3Accessors(ctx context.Context, s *StageState, cfg SnapshotsCfg, agg *state.Aggregator, headersProgress uint64) error {
	if cfg.lifecycleDrivenByStorage {
		return nil
	}
	canDefer := headersProgress > 0

	indexWorkers := estimate.IndexSnapshot.Workers()
	if !canDefer {
		if err := agg.BuildMissedAccessors(ctx, cfg.db, indexWorkers); err != nil {
			return err
		}
	} else {
		log.Debug(fmt.Sprintf("[%s] Deferring E3 indexing to background", s.LogPrefix()), "reason", "restart", "headersProgress", headersProgress)
	}
	return nil
}

func firstNonGenesisCheck(tx kv.RwTx, snapshots dbservices.BlockSnapshots, logPrefix string, dirs datadir.Dirs) error {
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

func pruneCanonicalMarkers(ctx context.Context, tx kv.RwTx, blockReader dbservices.FullBlockReader) error {
	pruneThreshold := rawdbreset.GetPruneMarkerSafeThreshold(blockReader)
	if pruneThreshold == 0 {
		return nil
	}

	c, err := tx.RwCursor(kv.HeaderCanonical) // Number -> Hash
	if err != nil {
		return err
	}
	defer c.Close()
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
		// TD is deliberately NOT pruned with the marker. Mode-B/D unwind
		// to an arbitrary historical target and the first insert after it
		// reads the target's TD; snapshots do not carry TD, so removing it
		// here makes such an unwind unrecoverable ("parent's total
		// difficulty not found"). FillDBFromSnapshots writes TD for every
		// frozen header for that reason — pruning it back out of the same
		// range put the two in direct contradiction. ~32 bytes/block.
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
		if s.CurrentSyncCycle.IsInitialCycle {
			cfg.blockRetire.SetWorkers(estimate.CompressSnapshot.Workers())
		} else {
			cfg.blockRetire.SetWorkers(1)
		}

		// Capture the range for the bus-event bridge so the onDone closure
		// can report [fromBlock, toBlock] back to the storage event bus.
		retireFromBlock, retireToBlock := cfg.blockReader.FrozenBlocks(), s.ForwardProgress
		started := cfg.blockRetire.BuildFilesInBackground(
			ctx,
			0,
			s.FinalityCtx,
			log.LvlDebug,
			cfg.getSeederClient(),
			func() error {
				filesDeleted, err := retireBlockSnapshots(ctx, cfg, logger)
				if filesDeleted && cfg.notifier != nil {
					cfg.notifier.Events.OnNewSnapshot()
				}
				return err
			},
			func() {
				if cfg.notifier != nil {
					cfg.notifier.Events.OnRetirementDone()
				}
				if cfg.publishRetirementDone != nil {
					cfg.publishRetirementDone(retireFromBlock, retireToBlock)
				}
			})
		if cfg.notifier != nil {
			cfg.notifier.Events.OnRetirementStart(started)
		}
		if started && cfg.publishRetirementStart != nil {
			cfg.publishRetirementStart(retireFromBlock, retireToBlock)
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

func retireBlockSnapshots(ctx context.Context, cfg SnapshotsCfg, logger log.Logger) (bool, error) {
	if dbg.NoRetire() {
		return false, nil
	}
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

	pruneTo := cfg.prune.Blocks.PruneTo(headNumber)
	if pruneTo > executionProgress {
		return false, nil
	}

	return cfg.blockRetire.RetireTransactionFiles(pruneTo, func(files []string) error {
		return cfg.getSeederClient().Delete(ctx, files)
	})
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
	v, _, err := roTx.GetLatest(kv.CommitmentDomain, commitmentdb.KeyCommitmentState, kv.GetLatestOptions{})
	if err != nil || len(v) < 16 {
		return 0
	}
	return binary.BigEndian.Uint64(v[8:16])
}
