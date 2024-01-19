package stages

import (
	"context"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/turbo/engineapi"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/zk/datastream/client"
	zkStages "github.com/ledgerwatch/erigon/zk/stages"
	"github.com/ledgerwatch/erigon/zk/syncer"
	"github.com/ledgerwatch/erigon/zk/txpool"
)

// NewDefaultZkStages creates stages for zk syncer (RPC mode)
func NewDefaultZkStages(ctx context.Context,
	db kv.RwDB,
	cfg *ethconfig.Config,
	controlServer *sentry.MultiClient,
	notifications *shards.Notifications,
	snapDownloader proto_downloader.DownloaderClient,
	snapshots *snapshotsync.RoSnapshots,
	agg *state.AggregatorV3,
	forkValidator *engineapi.ForkValidator,
	engine consensus.Engine,
	l1Syncer *syncer.L1Syncer,
	datastreamClient *client.StreamClient,
	datastreamServer *datastreamer.StreamServer,
) []*stagedsync.Stage {
	dirs := cfg.Dirs
	blockReader := snapshotsync.NewBlockReaderWithSnapshots(snapshots, cfg.TransactionsV3)
	blockRetire := snapshotsync.NewBlockRetire(1, dirs.Tmp, snapshots, db, snapDownloader, notifications.Events)

	// During Import we don't want other services like header requests, body requests etc. to be running.
	// Hence we run it in the test mode.
	runInTestMode := cfg.ImportMode

	return zkStages.DefaultZkStages(ctx,
		zkStages.StageL1SyncerCfg(db, l1Syncer, cfg.Zk),
		zkStages.StageBatchesCfg(db, datastreamClient),
		zkStages.StageDataStreamCatchupCfg(datastreamServer, db),
		stagedsync.StageCumulativeIndexCfg(db),
		stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig),
		stagedsync.StageSendersCfg(db, controlServer.ChainConfig, false, dirs.Tmp, cfg.Prune, blockRetire, controlServer.Hd),
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
		),
		stagedsync.StageHashStateCfg(db, dirs, cfg.HistoryV3, agg),
		zkStages.StageZkInterHashesCfg(db, true, true, false, dirs.Tmp, blockReader, controlServer.Hd, cfg.HistoryV3, agg, cfg.Zk),
		stagedsync.StageHistoryCfg(db, cfg.Prune, dirs.Tmp),
		stagedsync.StageLogIndexCfg(db, cfg.Prune, dirs.Tmp),
		stagedsync.StageCallTracesCfg(db, cfg.Prune, 0, dirs.Tmp),
		stagedsync.StageTxLookupCfg(db, cfg.Prune, dirs.Tmp, snapshots, controlServer.ChainConfig.Bor),
		stagedsync.StageFinishCfg(db, dirs.Tmp, forkValidator),
		runInTestMode)
}

// NewSequencerZkStages creates stages for a zk sequencer
func NewSequencerZkStages(ctx context.Context,
	db kv.RwDB,
	cfg *ethconfig.Config,
	controlServer *sentry.MultiClient,
	notifications *shards.Notifications,
	snapDownloader proto_downloader.DownloaderClient,
	snapshots *snapshotsync.RoSnapshots,
	agg *state.AggregatorV3,
	forkValidator *engineapi.ForkValidator,
	engine consensus.Engine,
	datastreamServer *datastreamer.StreamServer,
	txPool *txpool.TxPool,
	txPoolDb kv.RwDB,
) []*stagedsync.Stage {
	dirs := cfg.Dirs
	blockReader := snapshotsync.NewBlockReaderWithSnapshots(snapshots, cfg.TransactionsV3)

	// During Import we don't want other services like header requests, body requests etc. to be running.
	// Hence we run it in the test mode.
	runInTestMode := cfg.ImportMode

	return zkStages.SequencerZkStages(ctx,
		stagedsync.StageCumulativeIndexCfg(db),
		zkStages.StageDataStreamCatchupCfg(datastreamServer, db),
		zkStages.StageSequencerInterhashesCfg(db, notifications.Accumulator),
		zkStages.StageSequenceBlocksCfg(
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
			cfg.Genesis,
			cfg.Sync,
			agg,
			cfg.Zk,
			txPool,
			txPoolDb,
		),
		stagedsync.StageHashStateCfg(db, dirs, cfg.HistoryV3, agg),
		zkStages.StageZkInterHashesCfg(db, true, true, false, dirs.Tmp, blockReader, controlServer.Hd, cfg.HistoryV3, agg, cfg.Zk),
		stagedsync.StageHistoryCfg(db, cfg.Prune, dirs.Tmp),
		stagedsync.StageLogIndexCfg(db, cfg.Prune, dirs.Tmp),
		stagedsync.StageCallTracesCfg(db, cfg.Prune, 0, dirs.Tmp),
		stagedsync.StageTxLookupCfg(db, cfg.Prune, dirs.Tmp, snapshots, controlServer.ChainConfig.Bor),
		stagedsync.StageFinishCfg(db, dirs.Tmp, forkValidator),
		runInTestMode)
}
