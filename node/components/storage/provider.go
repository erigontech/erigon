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
	"fmt"
	"path/filepath"

	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
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
	Aggregator *state.Aggregator

	// IndexWorkers is the parallelism for accessor builds. Sourced
	// from estimate.IndexSnapshot.Workers() in production. Zero →
	// productionIndexBuilder skips the E3 build (Aggregator's
	// BuildMissedAccessors interprets workers strictly).
	IndexWorkers int

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
			for {
				select {
				case <-ctx.Done():
					return
				case cs, ok := <-sub:
					if !ok {
						return
					}
					var toSeed []string
					for _, name := range cs.Files {
						state, exists := inv.LifecycleState(name)
						if !exists {
							continue
						}
						if state == snapshot.LifecycleAdvertisable {
							toSeed = append(toSeed, name)
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
						continue
					}
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
	// Files already on disk at startup are entered as Advertisable
	// directly during bootstrap (see below) — they bypass the batch
	// path because previous runs validated them.
	// Bootstrap Inventory with files already on disk so the lifecycle
	// driver doesn't start blind. Both AllSnapshots (block) and the
	// Aggregator (state) expose visible-file enumeration; we mirror
	// each as LifecycleAdvertisable since visibility implies the file
	// is local AND indexed AND validated by recalcVisibleFiles's gate.
	// Runs whenever Inventory is set, regardless of LifecycleDrivenByStorage,
	// because non-driver consumers (manifest exchange, snapshot-flow) also
	// benefit from a populated registry.
	if deps.Inventory != nil {
		addBootstrapFile := func(name string) {
			entry := &snapshot.FileEntry{
				Name:         filepath.Base(name),
				Local:        true,
				Advertisable: true,
			}
			// Populate step + domain + kind so cross-component
			// queries (per-step grouping, IsMinimum sort) work.
			// Bootstrap files are already Advertisable so they
			// don't go through the lifecycle, but their metadata
			// is read by other consumers.
			snapshot.PopulateFromName(entry)
			_ = deps.Inventory.AddFile(entry)
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

		// Populate the (step, block) binding from the latest bootstrap
		// commitment file. Bootstrap files start at LifecycleAdvertisable
		// directly (back-compat: visible-in-aggregator implies fully
		// validated by previous runs), so they bypass the lifecycle's
		// per-step batch validation — meaning CommitmentDomainValidator
		// never fires on them and no bindings get registered. Without a
		// binding, the block-step wait gate in BuildOnBatchValidation
		// blocks ALL block files indefinitely (they wait for a binding
		// that's never going to materialise via the lifecycle path).
		//
		// We register a binding for the latest commitment file
		// directly. BlockToStep returns the smallest covering boundary,
		// so a single binding for the latest commitment step covers
		// every block range below it. Block files arriving via
		// discoverNewFiles can then validate immediately once their
		// step-siblings are present.
		//
		// Subsequent commitment files produced by post-tip retire go
		// through the lifecycle normally (not via bootstrap) and
		// CommitmentDomainValidator registers their bindings as part of
		// the per-step batch validation — incrementally raising the
		// high-water mark.
		if p.ChainDB != nil && p.BlockReader != nil {
			seedLatestCommitmentBinding(ctx, deps.Inventory,
				CommitmentDomainValidator{
					DB:          p.ChainDB,
					BlockReader: p.BlockReader,
					Inventory:   deps.Inventory,
				}, logger)
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
		snapDir := config.Dirs.Snap
		batchChain := validation.DefaultStepChain(snapDir)
		// Stage-2 commitment-domain validator: opens commitment.kv on
		// commitment-step batches, asserts state is at end of block,
		// registers (step, block) binding. No-op for non-commitment
		// steps. Runs only when the underlying ChainDB + BlockReader
		// are available (tools / tests may construct a Provider
		// without them).
		if p.ChainDB != nil && p.BlockReader != nil {
			batchChain = append(batchChain, CommitmentDomainValidator{
				DB:          p.ChainDB,
				BlockReader: p.BlockReader,
				Inventory:   deps.Inventory,
			})
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
	}

	return nil
}

// Stop releases the storage component's runtime resources. Currently
// only the lifecycle driver needs explicit shutdown; other resources
// (DB, BlockRetire, etc.) follow the framework's existing lifecycle.
// Multi-call safe — Driver.Stop is idempotent.
func (p *Provider) Stop() {
	if p.LifecycleDriver != nil {
		p.LifecycleDriver.Stop()
	}
}
