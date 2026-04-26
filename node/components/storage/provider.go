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

	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
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

	logger log.Logger

	// bg owns the background workers (collation+prune today, retire later).
	// nil until StartBackgroundLoop is called from backend.go after the Sync
	// object is constructed (Sync supplies the prune callback).
	bg *backgroundLoop
}

// StartBackgroundLoop spawns the storage-owned background workers. Must be
// called from backend.go AFTER the Sync object exists (Sync.RunPrune is the
// pruneFn the bg loop invokes via Aggregator.CollateAndPruneIfNeeded).
//
// Today's loop: a single collator goroutine ticking every 5s. If the chainDB
// has no Aggregator (integration tools) or pruneFn is nil, this is a no-op.
//
// Idempotent: repeated calls overwrite the existing loop; the prior loop is
// stopped first. Provider.Close cancels and waits.
func (p *Provider) StartBackgroundLoop(ctx context.Context, pruneFn PruneFn) {
	if p.ChainDB == nil || pruneFn == nil {
		return
	}
	hasAgg, ok := p.ChainDB.(dbstate.HasAgg)
	if !ok {
		return
	}
	agg, ok := hasAgg.Agg().(*dbstate.Aggregator)
	if !ok || agg == nil {
		return
	}
	if p.bg != nil {
		p.bg.Stop()
	}
	p.bg = newBackgroundLoop(ctx, p.ChainDB, agg, pruneFn, p.logger)
	p.bg.Start()
}

// Close stops any running background loop. Safe to call when the loop was
// never started (no-op).
func (p *Provider) Close() {
	if p.bg != nil {
		p.bg.Stop()
		p.bg = nil
	}
}

// WithForegroundPriority runs fn while signalling the background loop to
// yield. Background workers check the counter at tick boundaries and skip
// the tick when it is non-zero, surrendering CPU/IO/lock budget to the
// foreground caller. The counter stacks: multiple concurrent calls to
// WithForegroundPriority all hold the priority simultaneously, and the
// gate releases only when the last one returns.
//
// Useful for FCU heavy operations (unwind, integrity check, debug snapshot)
// that need predictable resource availability without coordinating with
// the background loop's exact phase.
//
// fn is invoked synchronously; this method returns when fn returns.
// Returns fn's error verbatim. Safe to call before StartBackgroundLoop —
// the priority counter is just an atomic; no bg loop means nothing to yield.
func (p *Provider) WithForegroundPriority(fn func() error) error {
	if p.bg != nil {
		p.bg.fgPriority.Add(1)
		defer p.bg.fgPriority.Add(-1)
	}
	return fn()
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
	p.ChainDB.OnFilesChange(
		func(frozenFileNames []string) {
			p.logger.Debug("files changed...sending notification")
			notifications.OnNewSnapshot()
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

	return nil
}
