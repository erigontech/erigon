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

// Package storage provides the Storage Provider — the component extracted from
// backend.go responsible for owning the chain database, snapshot reader/writer,
// genesis state, block retirement, and file-change callbacks.
//
// Notifications (state-change events) are NOT owned by storage — they are an
// execution-layer concern and will move to the execution component. Storage
// receives Notifications as a dep for BlockRetire and snapshot event forwarding.
//
// Sequencing: Initialize must be called early in backend.New(), after
// OpenDatabase and SetUpBlockReader, and before any component that needs
// ChainDB or BlockReader.
package storage

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/preverified"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	snaptypelib "github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/app/workerpool"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/lifecycle"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/validation"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
)

// Provider holds the storage component runtime state.
//
// After Initialize, all public fields are ready for consumers.
type Provider struct {
	// Public outputs — available after Initialize.
	ChainDB              kv.TemporalRwDB
	BlockReader          *freezeblocks.BlockReader
	BlockWriter          *blockio.BlockWriter
	AllSnapshots         *freezeblocks.RoSnapshots
	AllBorSnapshots      *heimdall.RoSnapshots // nil if not Bor
	BridgeStore          bridge.Store          // nil if not Bor
	HeimdallStore        heimdall.Store        // nil if not Bor
	ChainConfig          *chain.Config
	Genesis              *types.Block
	GenesisHash          common.Hash
	CurrentBlockNumber   uint64
	SegmentsBuildLimiter *semaphore.Weighted
	BlockRetire          services.BlockRetire

	// Inventory is the storage component's metadata registry of all
	// known snapshot files (local + remote, per the snapshot-flow PR).
	// Optional — nil for tools and tests that don't run the full
	// snapshot-flow component. Populated by Deps.Inventory in
	// Initialize. See node/components/storage/snapshot/inventory.go.
	Inventory *snapshot.Inventory

	// LifecycleDriver runs the storage-owned import lifecycle (per
	// docs/plans/20260501-storage-lifecycle-spec.md). Created and
	// started in Initialize ONLY when both Deps.Inventory is non-nil
	// AND Config.Snapshot.LifecycleDrivenByStorage is true. Stopped
	// via Provider.Stop. Nil otherwise.
	LifecycleDriver *lifecycle.Driver

	// eventBus + Orchestrator + InitialStateReady are constructed
	// together in Initialize when storage owns the lifecycle. The
	// channel is exposed so backend.go can plumb it into
	// cfg.Snapshot.InitialStateReady; the bus + orchestrator stay
	// internal. NOTE: until the framework wires sentry/downloader
	// to a shared bus, the orchestrator's input events
	// (PeerManifestReceived / DownloadComplete) never arrive, so
	// InitialStateReady will not fire in production today — see
	// memory/shared-bus-wiring-todo.md.
	eventBus          event.EventBus
	eventPool         *workerpool.WorkerPool
	Orchestrator      *flow.Orchestrator
	InitialStateReady <-chan struct{}

	// BlockHeadersReady closes the first time the storage component
	// observes the tip *-headers.seg reach LifecycleIndexed (i.e. the
	// .seg + .idx are on disk and BlockReader can serve headers from
	// them). At that point the Provider calls OpenSegments(Headers)
	// on the EL's blockReader and publishes flow.BlockHeadersReady on
	// the storage event bus.
	//
	// Caplin's RunCaplinService waits on this channel before starting
	// its clstages loop so the historical-blocks download stage reads
	// a meaningful FrozenBlocks() tip and doesn't race the EL's
	// snapshot-open by walking back to Bellatrix epoch.
	//
	// Architectural rule: only the storage component is aware of the
	// downloader / snapshot lifecycle; Caplin (and every other
	// non-storage consumer) depends on the storage bus event, never
	// on the downloader directly.
	BlockHeadersReady <-chan struct{}

	// bootstrapChainName is set in Initialize when
	// --snap.bootstrap-from-preverified is configured. The actual
	// synthetic-manifest publish is deferred until backend.go calls
	// BootstrapFromPreverified() — after BindBus wires the downloader
	// to the bus. Without the deferral, DownloadRequested events fire
	// into a bus whose downloader-side subscriber doesn't yet exist
	// and the wedge symptom is the same as the gap the bootstrap is
	// meant to fix. Empty string disables the bootstrap publish.
	bootstrapChainName string

	// bootstrapPruneMode is the prune.Mode passed in via Deps.Config.Prune.
	// Used to filter the preverified manifest down to the subset this
	// node will actually download — without this, a --prune.mode=minimal
	// publisher pulls the full archive (~1.9 TB of state history) it
	// would just prune anyway. See bug N in
	// docs/plans/time-to-get-back-generic-mist.md.
	bootstrapPruneMode prune.Mode

	// pausedCommitmentCache is the partial-block-pause cache shared
	// by CommitmentDomainValidator. Held here so the ChangeSet
	// subscriber can drop entries for files that get retired before
	// the cache map accumulates dead names.
	pausedCommitmentCache *PausedCommitmentCache

	// indexBuilder is the productionIndexBuilder constructed alongside
	// LifecycleDriver. The watchTipHeaderForOpenSegments goroutine
	// invokes it before OpenSegments(Headers) so block-file .idx
	// accessors are produced — block files arrive in the inventory
	// with empty Dependencies, so BuildOnIndexing's deps-already-local
	// short-circuit advances them straight to LifecycleIndexed without
	// ever invoking the builder. Without this, headers .seg downloads
	// land but no .idx is built, RecalcVisibleSegments skips them
	// (IsIndexed()==false), and FrozenBlocks() stays at 0.
	indexBuilder lifecycle.IndexBuilder

	logger log.Logger
}

// Deps holds all external dependencies needed by Initialize.
// backend.go calls SetUpBlockReader and passes the results here.
type Deps struct {
	Ctx context.Context

	// Outputs from SetUpBlockReader (called in backend.go).
	ChainDB         kv.TemporalRwDB
	BlockReader     *freezeblocks.BlockReader
	BlockWriter     *blockio.BlockWriter
	AllSnapshots    *freezeblocks.RoSnapshots
	AllBorSnapshots *heimdall.RoSnapshots // nil if not Bor
	BridgeStore     bridge.Store          // nil if not Bor
	HeimdallStore   heimdall.Store        // nil if not Bor

	// Genesis and chain config (resolved in backend.go).
	ChainConfig *chain.Config
	Genesis     *types.Block

	// Config for snapshot and downloader settings.
	Config *ethconfig.Config

	// DBEventNotifier — NOT owned by storage. Passed in so BlockRetire and
	// file-change callbacks can forward snapshot events. Currently backed by
	// shards.Events; will migrate to the framework event bus.
	DBEventNotifier services.DBEventNotifier

	// Downloader client for file-change callbacks (may be nil).
	DownloaderClient downloader.Client

	// RepublishChainToml is called after retire/merge produces new
	// snapshot files (via OnFilesChange) so the publisher's
	// chain.toml manifest reflects the freshest local files. V2
	// consumers connecting after the re-publish see the larger /
	// fresher manifest and can skip executing blocks the publisher
	// has already retired into snapshots.
	//
	// Without this, the publisher's chain.toml stays at whatever
	// state was generated by the initial 30-s post-startup publish
	// + the post-DownloadSnapshots publish, even as retire produces
	// fresher files. Consumers connecting later get a stale manifest
	// and have to re-execute the same blocks the publisher already
	// did.
	//
	// Production wiring: backend.go passes
	// `backend.components.Downloader.Downloader.PublishLocalChainToml`
	// here. May be nil for tests / tools / non-publishing nodes —
	// the OnFilesChange callback null-checks before invoking.
	RepublishChainToml func() error

	// Inventory is the storage component's metadata registry (per the
	// snapshot-flow PR). Optional — nil for tools and tests that
	// don't run the snapshot-flow component. When non-nil AND
	// Config.Snapshot.LifecycleDrivenByStorage is true, Initialize
	// constructs and starts a lifecycle.Driver.
	Inventory *snapshot.Inventory

	// Aggregator is the state Aggregator (db/state). Optional;
	// productionIndexBuilder uses it to build E3 (state) accessors
	// when the lifecycle driver runs. nil → only E2 (block) indexes
	// are built via the storage-driven path; E3 stays on the stage
	// path until this is wired.
	Aggregator *dbstate.Aggregator

	// IndexWorkers is the parallelism for accessor builds. Sourced
	// from estimate.IndexSnapshot.Workers() in production. Zero →
	// productionIndexBuilder skips the E3 build (Aggregator's
	// BuildMissedAccessors interprets workers strictly).
	IndexWorkers int

	// PostIndexedSeed, when set, runs as part of the orchestrator's
	// postIndexed pipeline AFTER snapshots.OpenFolder +
	// aggregator.OpenFolder and BEFORE InitialStateReady fires. The
	// callback owns its own RW tx; the storage component is agnostic to
	// what gets seeded. Production wires this from backend.go to a
	// closure invoking rawdbreset.FillDBFromSnapshots, which populates
	// kv.HeaderTD, canonical-hash pointers, etc. from frozen headers —
	// without it, Caplin's BlockCollector.Flush fails with "parent's
	// total difficulty not found" for blocks at the snapshot tip. See
	// docs/plans/20260518-storage-owns-post-download-pipeline.md (C
	// Step 3). nil → no-op (tests / tools that don't need MDBX seeding).
	PostIndexedSeed func(ctx context.Context) error

	SegmentsBuildLimiter *semaphore.Weighted
	Logger               log.Logger
}

// Initialize wires up file-change callbacks, reads the current block number,
// and creates the block retire service.
func (p *Provider) Initialize(deps Deps) error {
	ctx := deps.Ctx
	config := deps.Config
	logger := deps.Logger
	p.logger = logger

	// Store direct references.
	p.ChainDB = deps.ChainDB
	p.BlockReader = deps.BlockReader
	p.BlockWriter = deps.BlockWriter
	p.AllSnapshots = deps.AllSnapshots
	p.AllBorSnapshots = deps.AllBorSnapshots
	p.BridgeStore = deps.BridgeStore
	p.HeimdallStore = deps.HeimdallStore
	p.ChainConfig = deps.ChainConfig
	p.Genesis = deps.Genesis
	p.GenesisHash = deps.Genesis.Hash()
	p.SegmentsBuildLimiter = deps.SegmentsBuildLimiter
	p.Inventory = deps.Inventory

	// Read current block number. Use deps.Ctx so cancellation/shutdown
	// propagates into this lookup instead of masking it with Background.
	var currentBlock *types.Block
	if err := p.ChainDB.View(ctx, func(tx kv.Tx) error {
		var viewErr error
		currentBlock, viewErr = p.BlockReader.CurrentBlock(tx)
		return viewErr
	}); err != nil {
		return fmt.Errorf("storage: read current block: %w", err)
	}
	if currentBlock != nil {
		p.CurrentBlockNumber = currentBlock.NumberU64()
	}

	// BlockRetire — heimdallStore and bridgeStore may be nil for non-Bor chains.
	p.BlockRetire = freezeblocks.NewBlockRetire(1, config.Dirs, p.BlockReader, p.BlockWriter, p.ChainDB, p.HeimdallStore, p.BridgeStore, p.ChainConfig, config, deps.DBEventNotifier, p.SegmentsBuildLimiter, logger)

	// Serialize retirement's chain-DB reads against Aggregator commit+prune.
	// Without this, retirement's db.View RO txs can overlap a commit and pin
	// MDBX freelist pages, preventing reclamation (observed as openTxs>1 at
	// commit time).
	if hasAgg, ok := p.ChainDB.(dbstate.HasAgg); ok {
		if agg, ok := hasAgg.Agg().(*dbstate.Aggregator); ok && agg != nil {
			p.BlockRetire.(*freezeblocks.BlockRetire).SetCommitGate(agg.CommitGate())
		}
	}

	// Wire file-change callbacks so completed snapshots are seeded and
	// deleted snapshots are removed from the swarm.
	notifications := deps.DBEventNotifier
	downloaderClient := deps.DownloaderClient
	inv := deps.Inventory // may be nil for tools/tests
	p.ChainDB.OnFilesChange(
		func(frozenFileNames []string) {
			p.logger.Debug("files changed...sending notification")
			notifications.OnNewSnapshot()

			// Reflect the post-build / post-recalc state into Inventory:
			// OnFilesChange fires AFTER recalcVisibleFiles, so by the time
			// we see these names they are local AND indexed AND visible —
			// the lifecycle's "Advertisable" condition. Drives the
			// storage-component import-lifecycle state machine; harmless
			// no-op when Inventory is nil.
			if inv != nil {
				for _, name := range frozenFileNames {
					if state, ok := inv.LifecycleState(name); ok {
						if state < snapshot.LifecycleAdvertisable {
							inv.AdvanceTo(name, snapshot.LifecycleAdvertisable)
						}
					} else {
						// First time we hear about this file. Minimal
						// entry — Domain/Kind/Size land via the disk
						// scan path when the lifecycle driver populates
						// Inventory more thoroughly.
						inv.AddFile(&snapshot.FileEntry{
							Name:         name,
							Local:        true,
							Advertisable: true,
						})
					}
				}
			}

			// Re-publish chain.toml so V2 consumers see the freshly
			// retired/aggregated files. Runs unconditionally on every
			// OnFilesChange (fires for retire output AND aggregator
			// state files); the inner generator-and-save path is
			// idempotent when nothing's changed. Skipped only when
			// the callback isn't wired (consumer-only nodes,
			// tests/tools).
			//
			// Placed BEFORE the downloader-specific early returns
			// because state-aggregation events can fire OnFilesChange
			// with empty frozenFileNames; those events still produce
			// new local files that should be reflected in the
			// advertised chain.toml. The downloader.Seed call below
			// remains gated on frozenFileNames non-empty (no point
			// telling the downloader to seed nothing).
			if deps.RepublishChainToml != nil {
				if err := deps.RepublishChainToml(); err != nil {
					p.logger.Warn("[snapshots] re-publish chain.toml after retire", "err", err)
				}
			}

			if config.Downloader != nil && config.Downloader.ChainName == "" {
				return
			}
			if config.Snapshot.NoDownloader || downloaderClient == nil || len(frozenFileNames) == 0 {
				return
			}
			if err := downloaderClient.Seed(ctx, frozenFileNames); err != nil {
				p.logger.Warn("[snapshots] downloader.Seed", "err", err)
			}
		},
		func(deletedFiles []string) {
			// Drop deleted files from Inventory. RemoveFile defers the
			// drop when held views still reference the file, so reads
			// in flight stay coherent.
			if inv != nil {
				for _, name := range deletedFiles {
					inv.RemoveFile(name)
				}
			}

			// Re-publish chain.toml after deletions too — files that
			// got merged out of existence shouldn't appear in the
			// advertised manifest.
			if deps.RepublishChainToml != nil {
				if err := deps.RepublishChainToml(); err != nil {
					p.logger.Warn("[snapshots] re-publish chain.toml after merge", "err", err)
				}
			}

			if config.Downloader != nil && config.Downloader.ChainName == "" {
				return
			}
			if config.Snapshot.NoDownloader || downloaderClient == nil || len(deletedFiles) == 0 {
				return
			}
			if err := downloaderClient.Delete(ctx, deletedFiles); err != nil {
				p.logger.Warn("[snapshots] downloader.Delete", "err", err)
			}
		},
	)

	// When the lifecycle driver advances files to Advertisable
	// (LifecycleDrivenByStorage path), the legacy ChainDB.OnFilesChange
	// callback above doesn't fire — those files came in via the disk
	// scan + lifecycle handlers, not via retire's old "frozen files"
	// notification. Without something firing Seed for them, the
	// downloader never builds .torrent metadata for the new
	// retire/merge output, GenerateChainToml has nothing to add to
	// chain.toml, and V2 consumers don't see the publisher's fresh
	// files.
	//
	// Subscribe to inventory ChangeSets to bridge the gap: when files
	// reach LifecycleAdvertisable, call Seed (which builds the
	// .torrent + adds the torrent to the client for seeding) and then
	// re-publish chain.toml so consumers see the entry. This is the
	// last-mile wiring that makes the V2 architectural advantage
	// visible — without it, chain.toml stays at the
	// post-DownloadSnapshots size while retire silently produces files
	// that never make it into the advertised manifest.
	if inv != nil && downloaderClient != nil {
		sub, unsub := inv.Subscribe()
		go func() {
			defer unsub()

			// alreadySeeded tracks names we've handed to the downloader
			// to avoid re-Seeding on every periodic scan (Seed is
			// idempotent at the downloader level, but the periodic
			// scan would still walk the whole inventory and fan out
			// hundreds of no-op RPC calls).
			alreadySeeded := make(map[string]struct{})

			// scanAndSeed walks the inventory and Seeds every entry
			// with Local=true that hasn't yet been handed to the
			// downloader. The architectural rule (per docs) is that
			// every node seeds every file it knows about; the
			// inventory is the authoritative source of truth for what
			// the node knows, so the bridge enumerates it directly
			// rather than relying on ChangeSet replay alone.
			//
			// ChangeSet-driven seeding alone is insufficient because
			// the inventory's Subscribe() channel is buffered at 16,
			// and discoverNewFiles fires hundreds of AddFile events
			// in a startup burst — the overflow drops silently. The
			// periodic re-scan catches anything ChangeSets dropped
			// AND covers the startup ordering case where the bridge
			// goroutine starts before discoverNewFiles populates the
			// inventory.
			scanAndSeed := func() (republished bool) {
				if config.Snapshot.NoDownloader {
					return
				}
				snapDir := config.Dirs.Snap
				var toSeed []string
				visit := func(e *snapshot.FileEntry) {
					if e == nil || !e.Local {
						return
					}
					relPath := snapshot.RelPathForName(e.Name)
					if _, seen := alreadySeeded[relPath]; seen {
						return
					}
					// Inventory-vs-disk drift guard: if inventory says
					// Local but the file is missing from disk (typical
					// after a merge supersedes a step file and the
					// deletedFiles callback hasn't run yet, or after an
					// out-of-band cleanup), don't hand the missing path
					// to the downloader. Without this guard,
					// BuildTorrentIfNeed's lstat fails inside Seed and
					// the batch-rollback below re-queues every file in
					// the batch every 10s, producing a noisy WARN loop
					// that hides the real inventory-vs-disk drift.
					// Treat the discovery as authoritative: remove the
					// stale entry so the inventory converges to the
					// on-disk truth.
					if _, err := os.Lstat(filepath.Join(snapDir, relPath)); os.IsNotExist(err) {
						p.logger.Debug("[snapshots] inventory entry missing on disk; removing", "name", e.Name)
						inv.RemoveFile(e.Name)
						return
					}
					alreadySeeded[relPath] = struct{}{}
					toSeed = append(toSeed, relPath)
				}
				for _, domain := range snapshot.AllDomains {
					for _, e := range inv.AllDomainFiles(domain) {
						visit(e)
					}
				}
				for _, e := range inv.BlockFiles() {
					visit(e)
				}
				for _, e := range inv.MetaFiles() {
					visit(e)
				}
				for _, e := range inv.SaltFiles() {
					visit(e)
				}
				if len(toSeed) == 0 {
					return
				}
				p.logger.Info("[snapshots] inventory scan seed", "count", len(toSeed))
				if err := downloaderClient.Seed(ctx, toSeed); err != nil {
					p.logger.Warn("[snapshots] inventory scan seed", "err", err, "count", len(toSeed))
					// Roll back so the next scan retries.
					for _, n := range toSeed {
						delete(alreadySeeded, n)
					}
					return
				}
				if deps.RepublishChainToml != nil {
					if err := deps.RepublishChainToml(); err != nil {
						p.logger.Warn("[snapshots] re-publish chain.toml after scan seed", "err", err)
					}
				}
				return true
			}

			// Periodic ticker re-scans the inventory every 10s. This
			// is the safety-net for the buffered-ChangeSet drop case
			// AND the timing case where this goroutine started before
			// the lifecycle driver populated the inventory. 10s
			// matches the chaintoml discovery cadence — V2 startup
			// is dominated by what these tickers do.
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()

			scanAndSeed()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					scanAndSeed()
				case cs, ok := <-sub:
					if !ok {
						return
					}
					var toSeed []string
					for _, name := range cs.Files {
						state, exists := inv.LifecycleState(name)
						if !exists {
							// File removed from the inventory (merge or
							// retraction). Drop dead entries from caches
							// so they don't accumulate over long-running
							// publisher uptime.
							delete(alreadySeeded, snapshot.RelPathForName(name))
							p.pausedCommitmentCache.Forget(name)
							continue
						}
						if state == snapshot.LifecycleAdvertisable {
							// Translate inventory basename into the
							// snap-dir-relative form the downloader
							// expects (e.g. "domain/foo.kv"). Without
							// this BuildTorrentIfNeed joins root +
							// basename and fails to find state files
							// that live in subdirs.
							relPath := snapshot.RelPathForName(name)
							if _, seen := alreadySeeded[relPath]; seen {
								continue
							}
							alreadySeeded[relPath] = struct{}{}
							toSeed = append(toSeed, relPath)
						}
					}
					if len(toSeed) == 0 {
						continue
					}
					if config.Snapshot.NoDownloader {
						continue
					}
					if err := downloaderClient.Seed(ctx, toSeed); err != nil {
						p.logger.Warn("[snapshots] post-advance Seed", "err", err)
						for _, n := range toSeed {
							delete(alreadySeeded, n)
						}
						continue
					}
					// Positive-confirmation signal — without this the only
					// way to know the bridge subscriber actually fired (vs
					// the legacy retire callback) is to grep Debug-level
					// torrent logs. Critical for the multi-day fleet test.
					p.logger.Info("[snapshots] post-advance seeded", "files", len(toSeed), "names", toSeed)
					if deps.RepublishChainToml != nil {
						if err := deps.RepublishChainToml(); err != nil {
							p.logger.Warn("[snapshots] re-publish chain.toml after seed", "err", err)
						}
					}
				}
			}
		}()
	}

	// Lifecycle driver — owns the import state machine when storage
	// drives. Construct + start only when both Inventory is provided
	// AND the operator has flipped the LifecycleDrivenByStorage flag.
	// Otherwise the existing stage-driven path remains authoritative;
	// see execution/stagedsync/stage_snapshots.go.
	//
	// OnIndexing wraps a productionIndexBuilder that coalesces
	// per-file calls into BlockRetire.BuildMissedIndicesIfNeed and
	// Aggregator.BuildMissedAccessors invocations.
	//
	// OnValidation runs PER-STEP batch validation: when an entry
	// reaches LifecycleIndexed the handler waits until every file in
	// its step group is also at Indexed, then runs the StepChain,
	// then atomically advances the whole step to Advertisable. The
	// default chain is just AllFilesPresent — a stat-level check
	// that every file is actually on disk. Heavier checks (Tier 2
	// content shape, Tier 3 format integrity) plug in via separate
	// flag-gated chains, not yet wired.
	//
	// Bootstrap files entry state depends on LifecycleDrivenByStorage:
	//   - flag OFF (legacy): files enter at LifecycleAdvertisable —
	//     no driver means no async validation path; back-compat with
	//     callers that don't run the validator chain at all.
	//   - flag ON (storage-driven): files enter at LifecycleIndexed
	//     so the lifecycle driver's OnValidation handler runs the
	//     cryptographic validator chain on them (HeaderChain, TxRoot,
	//     CommitmentDomain, ReceiptRoot). Catches the publisher-side
	//     gap-f class: commitment files inherited from preverified
	//     (or merged in place) whose internal KeyCommitmentState
	//     record encodes a (blockNum, txNum) pair the current
	//     txnum_index can't reconcile. Without this gate those files
	//     reached Advertisable on the assumption "visible-in-aggregator
	//     implies fully validated" — true for files this node retired
	//     under a working validator chain, NOT true for files we just
	//     downloaded from upstream / inherited across schema or chain
	//     shifts.
	//
	// Bootstrap Inventory with files already on disk so the lifecycle
	// driver doesn't start blind. Both AllSnapshots (block) and the
	// Aggregator (state) expose visible-file enumeration. Runs whenever
	// Inventory is set, regardless of LifecycleDrivenByStorage, because
	// non-driver consumers (manifest exchange, snapshot-flow) also
	// benefit from a populated registry.
	if deps.Inventory != nil {
		bootstrapInLifecycle := config.Snapshot.LifecycleDrivenByStorage
		addBootstrapFile := func(name string) {
			entry := &snapshot.FileEntry{
				Name:  filepath.Base(name),
				Local: true,
				// Advertisable depends on whether the lifecycle
				// driver is running. When it's on, files enter at
				// Indexed (the driver picks them up + validates);
				// when it's off, files enter at Advertisable
				// directly (no driver to advance them).
				Advertisable: !bootstrapInLifecycle,
			}
			// Populate step + domain + kind so cross-component
			// queries (per-step grouping, IsMinimum sort) work.
			snapshot.PopulateFromName(entry)
			_ = deps.Inventory.AddFile(entry)
			if bootstrapInLifecycle {
				// AddFile with (Local=true, Advertisable=false)
				// lands at LifecycleDownloaded. Advance to
				// LifecycleIndexed: the file is on disk AND
				// indexed (visibility in aggregator/AllSnapshots
				// implies indexes built). The driver's next
				// sweep dispatches OnValidation on it.
				deps.Inventory.AdvanceTo(entry.Name, snapshot.LifecycleIndexed)
			}
		}
		if p.AllSnapshots != nil {
			for _, name := range p.AllSnapshots.Files() {
				addBootstrapFile(name)
			}
		}
		if deps.Aggregator != nil {
			for _, fullpath := range deps.Aggregator.Files() {
				addBootstrapFile(fullpath)
			}
		}

		// Pre-register a (step, block) binding from the latest
		// bootstrap commitment file so the block-step wait gate
		// in BuildOnBatchValidation has something to anchor against
		// before the lifecycle driver's first async sweep completes.
		//
		// Under LifecycleDrivenByStorage=true, bootstrap commitment
		// files now enter at LifecycleIndexed and go through the
		// driver's OnValidation handler — which also registers
		// bindings on success. The seed call here is the fast-path
		// equivalent: it registers one binding synchronously so
		// block files don't have to wait for the first sweep.
		//
		// Under LifecycleDrivenByStorage=false (legacy), bootstrap
		// files go straight to Advertisable without any validator
		// running — the seed call is the ONLY thing registering a
		// binding for them. Failure to seed (e.g. partial-block tip
		// pause without a matching block .seg) means block files
		// stay un-bound until a future retire produces a commitment
		// that does seed; not blocking, just degraded.
		//
		// BlockToStep returns the smallest covering boundary, so a
		// single binding for the latest commitment step covers every
		// block range below it.
		if p.ChainDB != nil && p.BlockReader != nil {
			// Bootstrap anchor extraction: walk EVERY commitment file
			// in the inventory and run Phase A (ExtractCommitmentRecord)
			// to populate (step, block) bindings + Anchors on each.
			// Phase A only reads the file's KeyCommitmentState record
			// — no header, no body, no segment open required — so it
			// runs cleanly at startup before EL execution has opened
			// segments and regardless of prune mode (bodies < prune
			// horizon are intentionally absent under minimal mode).
			//
			// Why all files, not just the latest: the V2 manifest the
			// publisher will generate carries per-file ProofRoot /
			// AtBlock / AtTxNum anchors (see chaintoml_v2.go
			// DomainFileEntry) precisely so consumers can verify
			// without needing all files. To populate those anchors at
			// publish time, the inventory must hold them — which
			// requires running Phase A on every bootstrap commitment
			// file. The lifecycle driver then runs Phase B (header
			// check) + Phase C (body cross-check, may skip under
			// minimal mode) asynchronously while execution proceeds.
			//
			// EL execution and historical anchor extraction work in
			// opposite directions: execution moves forward from the
			// snapshot tip, anchor extraction works on every preverified
			// commitment file independently — they don't contend.
			extractBootstrapCommitmentAnchors(ctx, deps.Inventory,
				p.ChainDB, p.BlockReader, logger)
		}
	}

	if deps.Inventory != nil && config.Snapshot.LifecycleDrivenByStorage {
		builder := &productionIndexBuilder{
			blockRetire:  p.BlockRetire,
			agg:          deps.Aggregator,
			notifier:     deps.DBEventNotifier,
			logger:       logger,
			indexWorkers: deps.IndexWorkers,
		}
		p.indexBuilder = builder
		snapDir := config.Dirs.Snap
		batchChain := validation.DefaultStepChain(snapDir)
		// Stage-2 commitment-domain validator: opens commitment.kv on
		// commitment-step batches, asserts state is at end of block,
		// registers (step, block) binding. No-op for non-commitment
		// steps. Runs only when the underlying ChainDB + BlockReader
		// are available (tools / tests may construct a Provider
		// without them).
		if p.ChainDB != nil && p.BlockReader != nil {
			// State-domain validators (commitment, receipt) must not run
			// until the aggregator has finished OpenFolder and the state
			// domain is queryable — otherwise they read a half-loaded
			// domain and fail spuriously toward quarantine. The signal is
			// InitialStateReady; the closure reads p.InitialStateReady at
			// call time (it is assigned later in Initialize, before any
			// lifecycle sweep runs).
			stateReady := func() bool {
				ch := p.InitialStateReady
				if ch == nil {
					return false
				}
				select {
				case <-ch:
					return true
				default:
					return false
				}
			}
			// Order matters: header chain runs before commitment so the
			// commitment partial-block pause's blockNum lookup is on a
			// chain whose continuity is already verified.
			p.pausedCommitmentCache = NewPausedCommitmentCache()
			batchChain = append(batchChain,
				HeaderChainValidator{DB: p.ChainDB, BlockReader: p.BlockReader},
				TxRootValidator{DB: p.ChainDB, BlockReader: p.BlockReader},
				CommitmentDomainValidator{
					DB:          p.ChainDB,
					BlockReader: p.BlockReader,
					Inventory:   deps.Inventory,
					Logger:      logger,
					PausedCache: p.pausedCommitmentCache,
					StateReady:  stateReady,
					// PruneMode lets the validator classify
					// integrity.ErrAnchorBodyMissing: under minimal mode
					// the body for an anchor below
					// (BlockTip - 100K) is intentionally absent, so
					// Phase C (txnum-range cross-check) is skipped with
					// a logged warning. Phase A (file-internal record)
					// + Phase B (header.Root cross-check) still run.
					PruneMode: config.Prune,
				},
				ReceiptRootValidator{
					DB:          p.ChainDB,
					BlockReader: p.BlockReader,
					ChainConfig: p.ChainConfig,
					Logger:      logger,
					StateReady:  stateReady,
					// Same prune-mode treatment as
					// CommitmentDomainValidator: skip Phase C cleanly
					// when AtBlock body is intentionally absent under
					// minimal mode, rather than letting
					// CheckRCacheRootAtBlkRange's
					// TxnumReader.Min/Max fall back to the chain tip
					// and produce 5 spurious failures + quarantine.
					PruneMode: config.Prune,
				},
			)
		}
		p.LifecycleDriver = &lifecycle.Driver{
			Inv:          deps.Inventory,
			Logger:       logger,
			SnapDir:      snapDir,
			OnIndexing:   lifecycle.BuildOnIndexing(builder, deps.Inventory, logger),
			OnValidation: lifecycle.BuildOnBatchValidation(batchChain, deps.Inventory, logger),
		}
		if err := p.LifecycleDriver.Start(ctx); err != nil {
			return fmt.Errorf("storage: start lifecycle driver: %w", err)
		}

		// Real worker pool, not nil: async subscribers (downloader's
		// onDownloadRequested via SubscribeAsync, auto-publish) call
		// bus.execPool.Exec — a nil pool panics the moment the first
		// DownloadRequested fires. Sized to NumCPU like the framework's
		// root bus and the integration harness.
		p.eventPool = workerpool.New(runtime.NumCPU())
		p.eventBus = event.NewEventBus(execPoolAdapter{p.eventPool})
		p.InitialStateReady = flow.InitialStateReadyChannel(p.eventBus)
		// BlockHeadersReady channel closes when the storage component
		// publishes flow.BlockHeadersReady (see
		// watchTipHeaderForOpenSegments below). Caplin's RunCaplinService
		// receives this channel and blocks on it before starting its
		// clstages loop.
		blockHeadersReadyCh, _ := flow.BlockHeadersReadyChannel(p.eventBus)
		p.BlockHeadersReady = blockHeadersReadyCh
		p.Orchestrator = flow.NewWithStorage(
			p.eventBus,
			flow.NewInventoryStorage(deps.Inventory, validation.DefaultStage1ChainWithDisk(snapDir), snapDir),
			logger,
		)

		// (C) Step 2: wire the post-Indexed callback. Storage now owns
		// the file-open work that previously lived in
		// stage_snapshots.go (lines 339-350) — running it as a callback
		// the orchestrator invokes AFTER every phase-1 file reaches
		// LifecycleIndexed AND BEFORE InitialStateReady fires. This
		// closes the 2026-05-18 race where stage_snapshots.go's
		// OpenFolder lstat-failed on a not-yet-built .idx (the
		// lifecycle driver finished the Indexing transition ~150ms
		// after InitialStateReady fired under the older gate).
		// See docs/plans/20260518-storage-owns-post-download-pipeline.md.
		//
		// FillDBFromSnapshots (Step 3) will join this closure in a
		// later commit; for now stage_snapshots.go still owns it (its
		// own bounded-retry stopgap rides on top of this gate).
		blockReader := p.BlockReader
		aggregator := deps.Aggregator
		postIndexedSeed := deps.PostIndexedSeed
		if err := p.Orchestrator.SetPostIndexed(func(ctx context.Context) error {
			if blockReader != nil {
				if err := blockReader.Snapshots().OpenFolder(); err != nil {
					return fmt.Errorf("storage.postIndexed: snapshots.OpenFolder: %w", err)
				}
			}
			if aggregator != nil {
				if err := aggregator.OpenFolder(); err != nil {
					return fmt.Errorf("storage.postIndexed: aggregator.OpenFolder: %w", err)
				}
			}
			if postIndexedSeed != nil {
				if err := postIndexedSeed(ctx); err != nil {
					return fmt.Errorf("storage.postIndexed: seed: %w", err)
				}
			}
			logger.Info("[storage] postIndexed: OpenFolder + aggregator.OpenFolder + seed done")
			return nil
		}); err != nil {
			p.LifecycleDriver.Stop()
			return fmt.Errorf("storage: orchestrator.SetPostIndexed: %w", err)
		}

		if err := p.Orchestrator.Start(ctx); err != nil {
			p.LifecycleDriver.Stop() // unwind partial init
			return fmt.Errorf("storage: start orchestrator: %w", err)
		}

		// Watch the inventory for the tip *-headers.seg reaching
		// LifecycleIndexed. When it does, open EL header segments and
		// publish flow.BlockHeadersReady so Caplin's stage_history_download
		// reads a meaningful FrozenBlocks() tip on first call.
		// Architectural rule: only the storage component is aware of the
		// downloader / snapshot lifecycle; Caplin (and every other
		// non-storage consumer) waits on this bus event, not on the
		// downloader itself.
		if p.BlockReader != nil {
			go p.watchTipHeaderForOpenSegments(ctx, deps.Inventory)
		}

		// Record the chain name + prune.Mode so
		// BootstrapFromPreverified — invoked later by backend.go after
		// BindBus has wired the downloader — knows which preverified
		// registry to read AND how to filter it. Bootstrap can't fire
		// here: the orchestrator subscribes to PeerManifestReceived,
		// would process the synthetic manifest, and publish
		// DownloadRequested events into a bus whose downloader-side
		// subscriber doesn't exist yet (BindBus runs in backend.go
		// AFTER storage.Initialize returns). The events would be lost
		// and the wedge symptoms would be indistinguishable from the
		// no-peer-bootstrap case the path is meant to fix.
		//
		// Prune mode lives alongside chain name because the filter
		// must drop archive-only entries the running prune mode would
		// just prune anyway — bug N filled the disk with 1.3 TB of
		// .v history files a --prune.mode=minimal publisher would
		// never keep.
		if config.Snapshot.BootstrapFromPreverified {
			p.bootstrapChainName = config.Snapshot.ChainName
			p.bootstrapPruneMode = config.Prune
		}
	}

	return nil
}

// BootstrapFromPreverified seeds the orchestrator with a synthetic
// peer-manifest derived from preverified.toml so a fresh publisher can
// drive its initial download even when no V2-advertising peer is
// reachable on the swarm. Without this, the orchestrator only fires
// DownloadRequested on PeerManifestReceived events — and a bootstrap
// publisher (first publisher on a chain, or one restarting in a network
// whose V2 peers have all churned) has no such event to react to.
//
// Must be called AFTER all bus subscribers are wired (in particular,
// after downloader.BindBus). Backend.go owns the sequencing: it calls
// this method right after sentry/downloader BindBus completes.
//
// No-op when --snap.bootstrap-from-preverified wasn't set (the chain
// name is empty), the storage component isn't running an orchestrator
// (eventBus is nil), or the preverified registry doesn't know about
// the configured chain.
//
// Idempotent: re-firing the manifest is safe — requestGapsFor dedups
// against existing pending/local entries via haveLocally +
// coverageForRoleLocked. The intended call site fires exactly once,
// but no harm if backend.go invokes it again on resume.
// watchTipHeaderForOpenSegments subscribes to the inventory's ChangeSet
// stream and waits until the highest-range *-headers.seg file is at
// LifecycleIndexed (its .idx accessor is built and the EL's
// BlockReader can serve headers from it). When that happens, it calls
// OpenSegments(Headers) on the EL's BlockReader so FrozenBlocks()
// returns a meaningful tip, then publishes flow.BlockHeadersReady on
// the storage event bus.
//
// Caplin's RunCaplinService waits on Provider.BlockHeadersReady before
// starting clstages. Once published, Caplin's
// stage_history_download reads FrozenBlocks() = tip and the loop's
// destinationSlotForEL is computed against the real frozen tip instead
// of falling back to Bellatrix-fork-epoch — eliminating the 273h
// "Downloading Execution History" wedge.
//
// Single-fire: the goroutine exits once it has fired once, since the
// flow.BlockHeadersReady event uses sync.Once-guarded close
// (BlockHeadersReadyChannel). Later retire-driven tip advances are
// handled by the running EL's own snapshot reopens, not by this
// startup-bootstrap signal.
func (p *Provider) watchTipHeaderForOpenSegments(ctx context.Context, inv *snapshot.Inventory) {
	if inv == nil || p.BlockReader == nil || p.eventBus == nil {
		return
	}
	sub, unsubscribe := inv.Subscribe()
	defer unsubscribe()

	// Trigger once immediately in case the tip is already present on
	// disk at startup (e.g. on a resume where the EL just needs the
	// segments re-opened). Otherwise wait for ChangeSets as files land.
	if p.tryFireBlockHeadersReady(ctx, inv) {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-sub:
			if !ok {
				return
			}
			if p.tryFireBlockHeadersReady(ctx, inv) {
				return
			}
		}
	}
}

// tryFireBlockHeadersReady inspects the inventory for the highest-range
// *-headers.seg file. If it's at LifecycleDownloaded or beyond (i.e.
// the .seg is on disk), builds any missing block-file .idx accessors,
// calls OpenSegments(Headers) on the EL's BlockReader, and publishes
// flow.BlockHeadersReady. Returns true once the event has been fired
// (signalling the watcher goroutine to exit).
//
// Why the explicit BuildMissedIndices: header .seg files arrive in the
// bootstrap-from-preverified inventory with empty Dependencies. The
// lifecycle's BuildOnIndexing handler's deps-already-local short-circuit
// then advances them straight to LifecycleIndexed *without* invoking
// the index builder. RecalcVisibleSegments excludes any DirtySegment
// where !IsIndexed(), so a header.seg without its .idx is not in the
// visible set — FrozenBlocks() returns 0 and Caplin walks from genesis.
// OpenSegments does NOT build .idx files (it only opens existing ones
// via OpenIdxIfNeed), so we must invoke the builder ourselves. This
// mirrors what stage_snapshots.go does in the non-storage-driven path
// (`cfg.blockRetire.BuildMissedIndicesIfNeed` before OpenFolder).
//
// The builder is invoked unconditionally — BuildMissedIndicesIfNeed
// scans the snap dir and is a no-op when nothing needs building, so
// repeated calls from the ChangeSet loop are safe.
func (p *Provider) tryFireBlockHeadersReady(ctx context.Context, inv *snapshot.Inventory) bool {
	var tipHeader *snapshot.FileEntry
	for _, e := range inv.BlockFiles() {
		if !strings.HasSuffix(e.Name, "-headers.seg") {
			continue
		}
		if tipHeader == nil || e.ToBlock > tipHeader.ToBlock {
			tipHeader = e
		}
	}
	if tipHeader == nil || tipHeader.State < snapshot.LifecycleDownloaded {
		return false
	}

	// Two-step sequence required to expose headers to FrozenBlocks():
	//
	// 1. OpenFolder() to set SegmentsReady=true. BuildMissedIndices
	//    checks SegmentsReady() and returns "not all snapshot segments
	//    are available" if OpenFolder has never been called. OpenFolder
	//    is tolerant of missing .idx (OpenIdxIfNeed silently returns
	//    nil on os.ErrNotExist), so calling it with only .seg files on
	//    disk just flips the ready flag.
	//
	// 2. After BuildMissedIndices produces the headers .idx, call
	//    OpenSegments(Headers, allowGaps=true, alignMin=false). This
	//    is the bug Y fix: OpenFolder uses s.alignMin (default true),
	//    which calls RecalcVisibleSegments and then aligns the visible
	//    set across ALL types to the minimum max-block among them. With
	//    piece A landing block-header files in phase 1 alongside state
	//    domains, bodies/transactions arrive later (phase 2). During
	//    phase 1 they have 0 segments visible, so alignMin collapses
	//    EVERY type's visible set to empty — including headers — and
	//    FrozenBlocks() returns 0.
	//
	//    OpenSegments(Headers, true, false) opens only headers with
	//    alignMin=false, so headers become visible independent of
	//    body/tx state. This was the behaviour before the OpenFolder
	//    change; OpenFolder is still needed for SegmentsReady but
	//    we layer OpenSegments back on top to recover headers visibility.
	if err := p.BlockReader.Snapshots().OpenFolder(); err != nil {
		p.logger.Warn("[storage] OpenFolder failed — will retry on next inventory ChangeSet",
			"file", tipHeader.Name, "err", err)
		return false
	}

	if p.indexBuilder != nil {
		// Salt-arrival gate: BuildMissedIndices → GetIndexSalt reads
		// salt-blocks.txt from snapDir. During bootstrap-from-preverified,
		// the inventory's headers .seg arrives before salt-blocks.txt
		// (just-in-time download order is parallel + size-driven, not
		// dependency-aware). Every inventory ChangeSet that fires before
		// salt arrives would hit GetIndexSalt's ERROR + full-stack log
		// at type.go:134 — 630 ERROR-level lines observed during a single
		// ~77s startup window on 2026-05-17. Gate the build silently
		// until salt is on disk; the next ChangeSet after salt downloads
		// will pick it up.
		saltPath := filepath.Join(p.AllSnapshots.Dir(), "salt-blocks.txt")
		if _, err := os.Stat(saltPath); os.IsNotExist(err) {
			p.logger.Debug("[storage] BuildMissedIndices: waiting for salt to arrive", "file", tipHeader.Name)
			return false
		}
		if err := p.indexBuilder.BuildMissedIndices(ctx, tipHeader); err != nil {
			p.logger.Warn("[storage] BuildMissedIndices failed — will retry on next inventory ChangeSet",
				"file", tipHeader.Name, "err", err)
			return false
		}
	}

	if err := p.BlockReader.Snapshots().OpenSegments([]snaptypelib.Type{snaptype2.Headers}, true, false); err != nil {
		p.logger.Warn("[storage] OpenSegments(Headers, alignMin=false) failed — will retry on next inventory ChangeSet",
			"file", tipHeader.Name, "err", err)
		return false
	}

	tip := p.BlockReader.FrozenBlocks()
	if tip == 0 {
		// .idx still not on disk for the tip header (e.g. the builder's
		// scan was scheduled but a co-required .seg file from the same
		// build batch hasn't landed yet). Wait for the next ChangeSet.
		return false
	}
	p.logger.Info("[storage] tip header readable: OpenSegments(Headers) done, publishing BlockHeadersReady",
		"file", tipHeader.Name, "tip_block", tip)
	p.eventBus.Publish(flow.BlockHeadersReady{TipBlock: tip})
	return true
}

func (p *Provider) BootstrapFromPreverified() {
	if p == nil || p.bootstrapChainName == "" || p.eventBus == nil {
		return
	}
	if err := p.bootstrapFromPreverified(p.bootstrapChainName); err != nil {
		p.logger.Warn("[storage] bootstrap from preverified failed", "err", err)
	}
}

// bootstrapFromPreverified builds a synthetic flow.PeerManifestReceived
// from preverified.toml entries and publishes it on the storage event
// bus. The orchestrator processes it as a peer-claim manifest, dedups
// against the local inventory, and fires DownloadRequested for the
// gaps — exactly the same flow a real V2 peer would trigger, with no
// V2 peer required.
//
// Translation rules per entry:
//   - Hash parsed from hex; entries with malformed hashes are skipped
//     (preverified-toml-format invariant).
//   - Name + Kind + Domain + step/block range are populated via
//     snapshot.PopulateFromName so the orchestrator's role-coverage
//     check sees the same shape it would from a peer's V2 manifest.
//   - Trust is set to TrustNone — the orchestrator promotes to
//     TrustVerified on DownloadComplete, same path as for peer-fed
//     entries.
//   - Bucket routing mirrors manifest_exchange/convert.go's V2→peer
//     conversion: Domain != "" → Domains[domain], KindCaplin → Caplin,
//     KindMeta → Meta, KindSalt → Salt, else → Blocks.
//
// PeerID is the fixed sentinel "bootstrap-preverified" so peer-attribution
// (orchestrator.peerFiles) records the bootstrap source explicitly. A
// PeerDeparted for this ID is never fired — the bootstrap manifest is a
// once-per-lifetime seed.
func (p *Provider) bootstrapFromPreverified(chainName string) error {
	cfg, known := snapcfg.KnownCfg(chainName)
	if !known {
		return fmt.Errorf("no preverified config for chain %q", chainName)
	}

	manifest := buildBootstrapManifest(cfg.Preverified.Items, p.ChainConfig, p.bootstrapPruneMode, p.logger)

	totalEntries := len(manifest.Blocks) + len(manifest.Meta) + len(manifest.Salt) + len(manifest.Caplin)
	for _, d := range manifest.Domains {
		totalEntries += len(d)
	}
	p.logger.Info("[storage] bootstrap-from-preverified: seeding synthetic manifest",
		"chain", chainName,
		"total", totalEntries,
		"domains", len(manifest.Domains),
		"blocks", len(manifest.Blocks),
		"caplin", len(manifest.Caplin),
		"meta", len(manifest.Meta),
		"salt", len(manifest.Salt),
	)
	p.eventBus.Publish(manifest)
	return nil
}

// buildBootstrapManifest produces the synthetic
// flow.PeerManifestReceived for the bootstrap-from-preverified path.
// Composes the prune-mode filter (snapshotsync.FilterPreverifiedByPruneMode)
// with the per-kind bucket routing (KindMeta → Meta, KindSalt → Salt,
// KindCaplin → Caplin, Domain != "" → Domains[d], else → Blocks).
//
// Pure function: no event bus, no Provider state — testable in
// isolation against synthetic preverified inputs and synthetic
// prune.Mode values. Logging is optional via the passed logger.
//
// Filter is skipped when pruneMode.Initialised is false (defensive
// no-op for callers that don't yet wire the mode through).
func buildBootstrapManifest(
	items snapcfg.PreverifiedItems,
	chainConfig *chain.Config,
	pruneMode prune.Mode,
	logger log.Logger,
) flow.PeerManifestReceived {
	if pruneMode.Initialised {
		raw := len(items)
		items = snapshotsync.FilterPreverifiedByPruneMode(items, chainConfig, pruneMode)
		if logger != nil {
			logger.Info("[storage] bootstrap-from-preverified: filtered by prune mode",
				"prune.mode", pruneMode.String(),
				"raw", raw,
				"kept", len(items),
				"dropped", raw-len(items),
			)
		}
	}

	manifest := flow.PeerManifestReceived{
		PeerID:  "bootstrap-preverified",
		Domains: make(map[snapshot.Domain][]*snapshot.FileEntry),
	}
	for _, item := range items {
		entry := entryFromPreverifiedItem(item)
		if entry == nil {
			continue
		}
		switch {
		case entry.Kind == snapshot.KindMeta:
			manifest.Meta = append(manifest.Meta, entry)
		case entry.Kind == snapshot.KindSalt:
			manifest.Salt = append(manifest.Salt, entry)
		case entry.Kind == snapshot.KindCaplin:
			manifest.Caplin = append(manifest.Caplin, entry)
		case entry.Domain != "":
			manifest.Domains[entry.Domain] = append(manifest.Domains[entry.Domain], entry)
		default:
			manifest.Blocks = append(manifest.Blocks, entry)
		}
	}
	return manifest
}

// entryFromPreverifiedItem translates a preverified.Item (name + hex
// hash) into a snapshot.FileEntry suitable for inclusion in a
// flow.PeerManifestReceived. Returns nil for entries whose hash is
// malformed.
func entryFromPreverifiedItem(item preverified.Item) *snapshot.FileEntry {
	hashBytes, err := hex.DecodeString(item.Hash)
	if err != nil || len(hashBytes) != 20 {
		return nil
	}
	var hash [20]byte
	copy(hash[:], hashBytes)

	entry := &snapshot.FileEntry{
		Name:        item.Name,
		TorrentHash: hash,
		Trust:       snapshot.TrustNone,
	}
	// Populate Kind, Domain, FromStep/ToStep (state) or
	// FromBlock/ToBlock (block) from the filename. Returns silently for
	// unrecognised patterns; the entry still gets through with whatever
	// fields we set above.
	snapshot.PopulateFromName(entry)

	// Block files (Domain=="" with a parsed FromBlock/ToBlock range)
	// MUST also carry the range in FromStep/ToStep — the orchestrator's
	// requestGapsFor coverage check uses FromStep/ToStep (see
	// flow/orchestrator.go:coverageForRoleLocked + IsComplete). Without
	// this copy, the entry's range is (0, 0), IsComplete(0, 0) returns
	// true (vacuously covered), and the file is silently dropped before
	// any DownloadRequested fires. This is why the bootstrap path used
	// to download state files but not block .seg files — block entries
	// were silently coverage-skipped, the publisher's chain.v2.toml
	// emerged with zero block entries, and Caplin never saw a tip
	// header on disk. The manifest_exchange/convert.go V2-to-peer
	// translator handles this for peer-fed manifests via
	// parseBlockFileRange; the bootstrap-side path needs the same
	// shape.
	if entry.Domain == "" && entry.FromStep == 0 && entry.ToStep == 0 && entry.ToBlock > 0 {
		entry.FromStep = entry.FromBlock
		entry.ToStep = entry.ToBlock
	}
	return entry
}

// execPoolAdapter wraps a *workerpool.WorkerPool so it satisfies
// app/util.ExecPool (the interface event.NewEventBus expects). The
// framework's componentDomain and the integration harness both wrap a
// workerpool the same way; this is the storage component's local copy
// since it constructs its own bus rather than going through a
// componentDomain.
type execPoolAdapter struct{ wp *workerpool.WorkerPool }

func (a execPoolAdapter) Exec(task func()) { a.wp.Submit(task) }
func (a execPoolAdapter) PoolSize() int    { return a.wp.Size() }
func (a execPoolAdapter) QueueSize() int   { return a.wp.WaitingQueueSize() }

// Bus returns the storage component's event bus, or nil when storage
// isn't running its own orchestrator (LifecycleDrivenByStorage=false
// or pre-Initialize). Production wiring (backend.go) hands this to
// sentry.Provider.BindBus and downloader.Provider.BindBus so all
// three components share one bus and the orchestrator's
// InitialStateReady can fire on real peer events.
func (p *Provider) Bus() event.EventBus { return p.eventBus }

// Stop releases the storage component's runtime resources. The
// lifecycle driver and the flow orchestrator both need explicit
// shutdown; other resources (DB, BlockRetire, etc.) follow the
// framework's existing lifecycle. Multi-call safe.
func (p *Provider) Stop() {
	if p.LifecycleDriver != nil {
		p.LifecycleDriver.Stop()
	}
	if p.Orchestrator != nil {
		_ = p.Orchestrator.Close()
	}
	if p.eventPool != nil {
		p.eventPool.Stop()
	}
}
