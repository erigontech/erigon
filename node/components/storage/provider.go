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

// Package storage provides the StorageProvider — the component extracted from
// backend.go responsible for database open, snapshot setup, blockReader/Writer,
// KV RPC server, notifications, and block retirement.
//
// Sequencing: Initialize must be called early in backend.New(), before any
// component that needs ChainDB, BlockReader, or Notifications.
package storage

import (
	"context"
	"fmt"

	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/kvcfg"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/shards"
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
	KvRPC                *remotedbserver.KvServer
	Notifications        *shards.Notifications
	ChainConfig          *chain.Config
	Genesis              *types.Block
	CurrentBlockNumber   uint64
	SegmentsBuildLimiter *semaphore.Weighted
	BlockRetire          services.BlockRetire

	logger log.Logger
}

// Deps holds all external dependencies needed by Initialize.
type Deps struct {
	Ctx              context.Context
	Stack            *node.Node        // for OpenDatabase, Stack.Config()
	Config           *ethconfig.Config // mutated: Prune, PersistReceipts written back
	Tracer           *tracers.Tracer
	Logger           log.Logger
	DownloaderClient downloader.Client // may be nil when NoDownloader is set
}

// Initialize opens the database, sets up snapshots, block reader/writer, KV RPC,
// notifications, reads genesis / chain config, and creates the BlockRetire.
func (p *Provider) Initialize(deps Deps) error {
	ctx := deps.Ctx
	stack := deps.Stack
	config := deps.Config
	logger := deps.Logger
	p.logger = logger

	dirs := stack.Config().Dirs

	// Open the raw chain DB.
	rawChainDB, err := node.OpenDatabase(ctx, stack.Config(), dbcfg.ChainDB, "", false, logger)
	if err != nil {
		return err
	}

	// Persist-flags consistency check: Prune, PersistReceipts, commitment flags.
	if err := rawChainDB.Update(context.Background(), func(tx kv.RwTx) error {
		var notChanged bool

		inConfig := config.PersistReceiptsCacheV2
		notChanged, config.PersistReceiptsCacheV2, err = kvcfg.PersistReceipts.EnsureNotChanged(tx, inConfig)
		if err != nil {
			return err
		}
		if !notChanged {
			logger.Warn("--persist.receipt changed since the last run, enabling historical receipts cache. full resync will be required to use the new configuration. if you do not need this feature, ignore this warning.", "inDB", config.PersistReceiptsCacheV2, "inConfig", inConfig)
		}
		if config.PersistReceiptsCacheV2 {
			statecfg.EnableHistoricalRCache()
		}

		if err := checkAndSetCommitmentHistoryFlag(tx, logger, dirs, config); err != nil {
			return err
		}

		if config.KeepExecutionProofs {
			statecfg.EnableHistoricalCommitment()
		}
		if config.ExperimentalConcurrentCommitment {
			statecfg.ExperimentalConcurrentCommitment = true
		}

		if err = stages.UpdateMetrics(tx); err != nil {
			return err
		}

		config.Prune, err = prune.EnsureNotChanged(tx, config.Prune)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	// Genesis + ChainConfig.
	var chainConfig *chain.Config
	var genesis *types.Block
	if err := rawChainDB.Update(context.Background(), func(tx kv.RwTx) error {
		genesisConfig, err := rawdb.ReadGenesis(tx)
		if err != nil {
			return err
		}
		if genesisConfig != nil {
			config.Genesis = genesisConfig
		}

		if deps.Tracer != nil && deps.Tracer.Hooks != nil && deps.Tracer.Hooks.OnBlockchainInit != nil {
			deps.Tracer.Hooks.OnBlockchainInit(config.Genesis.Config)
		}

		h, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		genesisSpec := config.Genesis
		if h != (common.Hash{}) {
			genesisSpec = nil
		}
		var genesisErr error
		chainConfig, genesis, genesisErr = genesiswrite.WriteGenesisBlock(tx, genesisSpec, config.Snapshot.ChainName, config.OverrideOsakaTime, config.OverrideAmsterdamTime, config.KeepStoredChainConfig, dirs, logger)
		if _, ok := genesisErr.(*chain.ConfigCompatError); genesisErr != nil && !ok {
			return genesisErr
		}
		return nil
	}); err != nil {
		return err
	}
	chainConfig.AllowAA = config.AllowAA

	// Snapshot / BlockReader setup.
	segmentsBuildLimiter := semaphore.NewWeighted(int64(dbg.BuildSnapshotAllowance))

	blockReader, blockWriter, allSnapshots, allBorSnapshots, bridgeStore, heimdallStore, temporalDb, err :=
		setUpBlockReader(ctx, rawChainDB, config, chainConfig, stack.Config().Http.DBReadConcurrency, logger, segmentsBuildLimiter)
	if err != nil {
		return err
	}

	// KV RPC + Notifications.
	kvRPC := remotedbserver.NewKvServer(ctx, temporalDb, allSnapshots, allBorSnapshots, temporalDb.Debug(), logger)
	notifications := shards.NewNotifications(kvRPC)

	// Read current block number.
	var currentBlock *types.Block
	if err := temporalDb.View(context.Background(), func(tx kv.Tx) error {
		currentBlock, err = blockReader.CurrentBlock(tx)
		return err
	}); err != nil {
		panic(fmt.Sprintf("storage: read current block: %v", err))
	}
	currentBlockNumber := uint64(0)
	if currentBlock != nil {
		currentBlockNumber = currentBlock.NumberU64()
	}

	// BlockRetire — heimdallStore and bridgeStore may be nil for non-Bor chains;
	// freezeblocks.NewBlockRetire handles nil stores gracefully.
	blockRetire := freezeblocks.NewBlockRetire(1, dirs, blockReader, blockWriter, temporalDb, heimdallStore, bridgeStore, chainConfig, config, notifications.Events, segmentsBuildLimiter, logger)

	// Populate public outputs.
	p.ChainDB = temporalDb
	p.BlockReader = blockReader
	p.BlockWriter = blockWriter
	p.AllSnapshots = allSnapshots
	p.AllBorSnapshots = allBorSnapshots
	p.BridgeStore = bridgeStore
	p.HeimdallStore = heimdallStore
	p.KvRPC = kvRPC
	p.Notifications = notifications
	p.ChainConfig = chainConfig
	p.Genesis = genesis
	p.CurrentBlockNumber = currentBlockNumber
	p.SegmentsBuildLimiter = segmentsBuildLimiter
	p.BlockRetire = blockRetire

	// Wire file-change callbacks so completed snapshots are seeded and
	// deleted snapshots are removed from the swarm.
	downloaderClient := deps.DownloaderClient
	p.ChainDB.OnFilesChange(
		func(frozenFileNames []string) {
			p.logger.Debug("files changed...sending notification")
			p.Notifications.Events.OnNewSnapshot()
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

// setUpBlockReader is the inlined body of the former eth.SetUpBlockReader,
// reproduced here to avoid a circular import between node/components/storage
// and node/eth.
func setUpBlockReader(ctx context.Context, db kv.RwDB, snConfig *ethconfig.Config, chainConfig *chain.Config, dbReadConcurrency int, logger log.Logger, blockSnapBuildSema *semaphore.Weighted) (*freezeblocks.BlockReader, *blockio.BlockWriter, *freezeblocks.RoSnapshots, *heimdall.RoSnapshots, bridge.Store, heimdall.Store, kv.TemporalRwDB, error) {
	snConfig.Snapshot.ChainName = chainConfig.ChainName
	allSnapshots := freezeblocks.NewRoSnapshots(snConfig.Snapshot, snConfig.Dirs.Snap, logger)

	var allBorSnapshots *heimdall.RoSnapshots
	var bridgeStore bridge.Store
	var heimdallStore heimdall.Store

	if chainConfig.Bor != nil {
		allBorSnapshots = heimdall.NewRoSnapshots(snConfig.Snapshot, snConfig.Dirs.Snap, logger)
		bridgeStore = bridge.NewSnapshotStore(bridge.NewMdbxStore(snConfig.Dirs.DataDir, logger, false, int64(dbReadConcurrency)), allBorSnapshots, chainConfig.Bor)
		heimdallStore = heimdall.NewSnapshotStore(heimdall.NewMdbxStore(logger, snConfig.Dirs.DataDir, false, int64(dbReadConcurrency)), allBorSnapshots)
	}
	blockReader := freezeblocks.NewBlockReader(allSnapshots, allBorSnapshots)

	_, knownSnapCfg := snapcfg.KnownCfg(chainConfig.ChainName)
	createNewSaltFileIfNeeded := snConfig.Snapshot.NoDownloader || snConfig.Snapshot.DisableDownloadE3 || !knownSnapCfg
	if _, err := snaptype.LoadSalt(snConfig.Dirs.Snap, createNewSaltFileIfNeeded, logger); err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}

	erigonDBSettings, err := state.ResolveErigonDBSettings(snConfig.Dirs, logger, snConfig.Snapshot.NoDownloader)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	agg, err := state.New(snConfig.Dirs).Logger(logger).SanityOldNaming().GenSaltIfNeed(createNewSaltFileIfNeeded).WithErigonDBSettings(erigonDBSettings).Open(ctx, db)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	agg.SetSnapshotBuildSema(blockSnapBuildSema)
	agg.SetProduceMod(snConfig.Snapshot.ProduceE3)

	allSegmentsDownloadComplete, err := rawdb.AllSegmentsDownloadCompleteFromDB(db)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	if allSegmentsDownloadComplete {
		allSnapshots.OptimisticalyOpenFolder()
		if chainConfig.Bor != nil {
			allBorSnapshots.OptimisticalyOpenFolder()
		}
		_ = agg.OpenFolder()
	} else {
		logger.Debug("[rpc] download of segments not complete yet. please wait StageSnapshots to finish")
	}

	temporalDb, err := temporal.New(db, agg)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}

	blockWriter := blockio.NewBlockWriter()

	return blockReader, blockWriter, allSnapshots, allBorSnapshots, bridgeStore, heimdallStore, temporalDb, nil
}

// checkAndSetCommitmentHistoryFlag is reproduced from node/eth to avoid cycles.
func checkAndSetCommitmentHistoryFlag(tx kv.RwTx, logger log.Logger, dirs datadir.Dirs, cfg *ethconfig.Config) error {
	isCommitmentHistoryEnabled, ok, err := rawdb.ReadDBCommitmentHistoryEnabled(tx)
	if err != nil {
		return err
	}
	if !ok {
		if !cfg.KeepExecutionProofs {
			if err := rawdb.WriteDBCommitmentHistoryEnabled(tx, cfg.KeepExecutionProofs); err != nil {
				return err
			}
			return nil
		}
		// we need to make sure we do not run from an old version so check amount of keys in kv.AccountDomain
		c, err := tx.Count(kv.TblAccountVals)
		if err != nil {
			return fmt.Errorf("failed to count keys in kv.AccountDomain: %w", err)
		}
		if c > 0 {
			return fmt.Errorf("commitment history is not enabled in the database. restart erigon after deleting the chaindata folder: %s", dirs.Chaindata)
		}

		if err := rawdb.WriteDBCommitmentHistoryEnabled(tx, cfg.KeepExecutionProofs); err != nil {
			return err
		}
		return nil
	}
	if cfg.KeepExecutionProofs != isCommitmentHistoryEnabled {
		return fmt.Errorf(
			"flag '--prune.experimental.include-commitment-history' mismatch: db: %v; config: %v. please restart Erigon '--prune.experimental.include-commitment-history=%v' or delete the chaindata folder: %s",
			isCommitmentHistoryEnabled, cfg.KeepExecutionProofs, cfg.KeepExecutionProofs, dirs.Chaindata)
	}
	if err := rawdb.WriteDBCommitmentHistoryEnabled(tx, cfg.KeepExecutionProofs); err != nil {
		return err
	}
	return nil
}
