package stages

import (
	"context"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/p2p/sentry/sentry_multi_client"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_helpers"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	zkStages "github.com/ledgerwatch/erigon/zk/stages"
	"github.com/ledgerwatch/erigon/zk/syncer"
	"github.com/ledgerwatch/erigon/zk/txpool"
)

// NewDefaultZkStages creates stages for zk syncer (RPC mode)
func NewDefaultZkStages(ctx context.Context,
	db kv.RwDB,
	cfg *ethconfig.Config,
	controlServer *sentry_multi_client.MultiClient,
	notifications *shards.Notifications,
	snapDownloader proto_downloader.DownloaderClient,
	snapshots *freezeblocks.RoSnapshots,
	agg *state.Aggregator,
	forkValidator *engine_helpers.ForkValidator,
	engine consensus.Engine,
	l1Syncer *syncer.L1Syncer,
	l1InfoTreeSyncer *syncer.L1Syncer,
	datastreamClient zkStages.DatastreamClient,
	datastreamServer *datastreamer.StreamServer,
) []*stagedsync.Stage {
	dirs := cfg.Dirs
	blockWriter := blockio.NewBlockWriter(cfg.HistoryV3)
	blockReader := freezeblocks.NewBlockReader(snapshots, nil)

	// todo: upstream merge
	// blockRetire := freezeblocks.NewBlockRetire(1, dirs, blockReader, blockWriter, db, cfg.Genesis.Config, notifications.Events, logger)

	// During Import we don't want other services like header requests, body requests etc. to be running.
	// Hence we run it in the test mode.
	runInTestMode := cfg.ImportMode

	return zkStages.DefaultZkStages(ctx,
		zkStages.StageL1SyncerCfg(db, l1Syncer, cfg.Zk),
		zkStages.StageL1InfoTreeCfg(db, cfg.Zk, l1InfoTreeSyncer),
		zkStages.StageBatchesCfg(db, datastreamClient, cfg.Zk, controlServer.ChainConfig, &cfg.Miner),
		zkStages.StageDataStreamCatchupCfg(datastreamServer, db, cfg.Genesis.Config.ChainID.Uint64(), cfg.DatastreamVersion, cfg.HasExecutors()),
		stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig, blockWriter),
		stagedsync.StageSendersCfg(db, controlServer.ChainConfig, false, dirs.Tmp, cfg.Prune, blockReader, controlServer.Hd, nil),
		stagedsync.StageExecuteBlocksCfg(
			db,
			cfg.Prune,
			cfg.BatchSize,
			nil,
			controlServer.ChainConfig,
			controlServer.Engine,
			&vm.Config{},
			notifications.Accumulator,
			cfg.StateStream,
			/*stateStream=*/ false,
			cfg.HistoryV3,
			dirs,
			blockReader,
			controlServer.Hd,
			cfg.Genesis,
			cfg.Sync,
			agg,
			cfg.Zk,
			nil,
		),
		stagedsync.StageHashStateCfg(db, dirs, cfg.HistoryV3, agg),
		zkStages.StageZkInterHashesCfg(db, true, true, false, dirs.Tmp, blockReader, controlServer.Hd, cfg.HistoryV3, agg, cfg.Zk),
		stagedsync.StageHistoryCfg(db, cfg.Prune, dirs.Tmp),
		stagedsync.StageLogIndexCfg(db, cfg.Prune, dirs.Tmp, cfg.Genesis.Config.NoPruneContracts),
		stagedsync.StageCallTracesCfg(db, cfg.Prune, 0, dirs.Tmp),
		stagedsync.StageTxLookupCfg(db, cfg.Prune, dirs.Tmp, controlServer.ChainConfig.Bor, blockReader),
		stagedsync.StageFinishCfg(db, dirs.Tmp, forkValidator),
		runInTestMode)
}

// NewSequencerZkStages creates stages for a zk sequencer
func NewSequencerZkStages(ctx context.Context,
	db kv.RwDB,
	cfg *ethconfig.Config,
	controlServer *sentry_multi_client.MultiClient,
	notifications *shards.Notifications,
	snapDownloader proto_downloader.DownloaderClient,
	snapshots *freezeblocks.RoSnapshots,
	agg *state.Aggregator,
	forkValidator *engine_helpers.ForkValidator,
	engine consensus.Engine,
	datastreamServer *datastreamer.StreamServer,
	sequencerStageSyncer *syncer.L1Syncer,
	l1Syncer *syncer.L1Syncer,
	l1InfoTreeSyncer *syncer.L1Syncer,
	l1BlockSyncer *syncer.L1Syncer,
	txPool *txpool.TxPool,
	txPoolDb kv.RwDB,
	verifier *legacy_executor_verifier.LegacyExecutorVerifier,
) []*stagedsync.Stage {
	dirs := cfg.Dirs
	blockReader := freezeblocks.NewBlockReader(snapshots, nil)

	// During Import we don't want other services like header requests, body requests etc. to be running.
	// Hence we run it in the test mode.
	runInTestMode := cfg.ImportMode

	return zkStages.SequencerZkStages(ctx,
		zkStages.StageL1SyncerCfg(db, l1Syncer, cfg.Zk),
		zkStages.StageL1SequencerSyncCfg(db, cfg.Zk, sequencerStageSyncer),
		zkStages.StageL1InfoTreeCfg(db, cfg.Zk, l1InfoTreeSyncer),
		zkStages.StageSequencerL1BlockSyncCfg(db, cfg.Zk, l1BlockSyncer),
		zkStages.StageDataStreamCatchupCfg(datastreamServer, db, cfg.Genesis.Config.ChainID.Uint64(), cfg.DatastreamVersion, cfg.HasExecutors()),
		zkStages.StageSequenceBlocksCfg(
			db,
			cfg.Prune,
			cfg.BatchSize,
			nil,
			controlServer.ChainConfig,
			controlServer.Engine,
			&vm.ZkConfig{},
			notifications.Accumulator,
			cfg.StateStream,
			/*stateStream=*/ false,
			cfg.HistoryV3,
			dirs,
			blockReader,
			cfg.Genesis,
			cfg.Sync,
			agg,
			datastreamServer,
			cfg.Zk,
			&cfg.Miner,
			txPool,
			txPoolDb,
			verifier,
			uint16(cfg.YieldSize),
		),
		stagedsync.StageHashStateCfg(db, dirs, cfg.HistoryV3, agg),
		zkStages.StageZkInterHashesCfg(db, true, true, false, dirs.Tmp, blockReader, controlServer.Hd, cfg.HistoryV3, agg, cfg.Zk),
		stagedsync.StageHistoryCfg(db, cfg.Prune, dirs.Tmp),
		stagedsync.StageLogIndexCfg(db, cfg.Prune, dirs.Tmp, cfg.Genesis.Config.NoPruneContracts),
		stagedsync.StageCallTracesCfg(db, cfg.Prune, 0, dirs.Tmp),
		stagedsync.StageTxLookupCfg(db, cfg.Prune, dirs.Tmp, controlServer.ChainConfig.Bor, blockReader),
		stagedsync.StageFinishCfg(db, dirs.Tmp, forkValidator),
		runInTestMode)
}
