package stages

import (
	"context"

	proto_downloader "github.com/erigontech/erigon-lib/gointerfaces/downloader"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/rawdb/blockio"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/p2p/sentry/sentry_multi_client"
	"github.com/erigontech/erigon/turbo/engineapi/engine_helpers"
	"github.com/erigontech/erigon/turbo/shards"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/zk/datastream/server"
	"github.com/erigontech/erigon/zk/l1infotree"
	"github.com/erigontech/erigon/zk/legacy_executor_verifier"
	zkStages "github.com/erigontech/erigon/zk/stages"
	"github.com/erigontech/erigon/zk/syncer"
	"github.com/erigontech/erigon/zk/txpool"
	"github.com/erigontech/erigon/zk/sequencer"
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
	datastreamClient zkStages.DatastreamClient,
	dataStreamServer server.DataStreamServer,
	infoTreeUpdater *l1infotree.Updater,
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
		zkStages.StageL1InfoTreeCfg(db, cfg.Zk, infoTreeUpdater),
		zkStages.StageBatchesCfg(db, datastreamClient, cfg.Zk, controlServer.ChainConfig, &cfg.Miner),
		zkStages.StageDataStreamCatchupCfg(dataStreamServer, db, cfg.Genesis.Config.ChainID.Uint64()),
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
		zkStages.StageZkInterHashesCfg(db, !cfg.DebugDisableStateRootCheck, true, false, dirs.Tmp, blockReader, controlServer.Hd, cfg.HistoryV3, agg, cfg.Zk),
		zkStages.StageWitnessCfg(db, cfg.Zk, controlServer.ChainConfig, engine, blockReader, agg, cfg.HistoryV3, dirs, cfg.WitnessContractInclusion, cfg.WitnessUnwindLimit),
		stagedsync.StageHistoryCfg(db, cfg.Prune, dirs.Tmp),
		stagedsync.StageLogIndexCfg(db, cfg.Prune, dirs.Tmp, &cfg.Genesis.Config.DepositContract),
		stagedsync.StageCallTracesCfg(db, cfg.Prune, 0, dirs.Tmp),
		stagedsync.StageTxLookupCfg(db, cfg.Prune, cfg.Sync, dirs.Tmp, controlServer.ChainConfig.Bor, blockReader),
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
	dataStreamServer server.DataStreamServer,
	sequencerStageSyncer *syncer.L1Syncer,
	l1Syncer *syncer.L1Syncer,
	l1BlockSyncer *syncer.L1Syncer,
	txPool *txpool.TxPool,
	txPoolDb kv.RwDB,
	verifier *legacy_executor_verifier.LegacyExecutorVerifier,
	infoTreeUpdater *l1infotree.Updater,
	hook *Hook,
	txYielder *sequencer.PoolTransactionYielder,
) []*stagedsync.Stage {
	dirs := cfg.Dirs
	blockReader := freezeblocks.NewBlockReader(snapshots, nil)

	// During Import we don't want other services like header requests, body requests etc. to be running.
	// Hence we run it in the test mode.
	runInTestMode := cfg.ImportMode

	return zkStages.SequencerZkStages(ctx,
		zkStages.StageL1SyncerCfg(db, l1Syncer, cfg.Zk),
		zkStages.StageL1SequencerSyncCfg(db, cfg.Zk, sequencerStageSyncer),
		zkStages.StageL1InfoTreeCfg(db, cfg.Zk, infoTreeUpdater),
		zkStages.StageSequencerL1BlockSyncCfg(db, cfg.Zk, l1BlockSyncer),
		zkStages.StageDataStreamCatchupCfg(dataStreamServer, db, cfg.Genesis.Config.ChainID.Uint64()),
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
			dataStreamServer,
			cfg.Zk,
			&cfg.Miner,
			txPool,
			txPoolDb,
			verifier,
			uint16(cfg.YieldSize),
			infoTreeUpdater,
			hook,
			txYielder,
		),
		stagedsync.StageHashStateCfg(db, dirs, cfg.HistoryV3, agg),
		zkStages.StageZkInterHashesCfg(db, !cfg.DebugDisableStateRootCheck, true, false, dirs.Tmp, blockReader, controlServer.Hd, cfg.HistoryV3, agg, cfg.Zk),
		stagedsync.StageHistoryCfg(db, cfg.Prune, dirs.Tmp),
		stagedsync.StageLogIndexCfg(db, cfg.Prune, dirs.Tmp, &cfg.Genesis.Config.DepositContract),
		stagedsync.StageCallTracesCfg(db, cfg.Prune, 0, dirs.Tmp),
		stagedsync.StageTxLookupCfg(db, cfg.Prune, cfg.Sync, dirs.Tmp, controlServer.ChainConfig.Bor, blockReader),
		stagedsync.StageFinishCfg(db, dirs.Tmp, forkValidator),
		runInTestMode)
}
