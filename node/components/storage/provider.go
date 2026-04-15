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
