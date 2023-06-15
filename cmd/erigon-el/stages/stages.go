package stagedsync

import (
	"context"

	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/turbo/engineapi"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
)

func nullStage(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
	return nil
}
func ExecutionStages(ctx context.Context, sm prune.Mode, snapshots stagedsync.SnapshotsCfg, headers stagedsync.HeadersCfg, cumulativeIndex stagedsync.CumulativeIndexCfg, blockHashCfg stagedsync.BlockHashesCfg, bodies stagedsync.BodiesCfg, senders stagedsync.SendersCfg, exec stagedsync.ExecuteBlockCfg, hashState stagedsync.HashStateCfg, trieCfg stagedsync.TrieCfg, history stagedsync.HistoryCfg, logIndex stagedsync.LogIndexCfg, callTraces stagedsync.CallTracesCfg, txLookup stagedsync.TxLookupCfg, finish stagedsync.FinishCfg, test bool) []*stagedsync.Stage {
	defaultStages := stagedsync.DefaultStages(ctx, snapshots, headers, cumulativeIndex, blockHashCfg, bodies, senders, exec, hashState, trieCfg, history, logIndex, callTraces, txLookup, finish, test)
	// Remove body/headers stages
	defaultStages[1].Forward = nullStage
	defaultStages[4].Forward = nullStage
	return defaultStages
}

func NewStagedSync(
	ctx context.Context,
	db kv.RwDB,
	p2pCfg p2p.Config,
	cfg *ethconfig.Config,
	controlServer *sentry.MultiClient,
	notifications *shards.Notifications,
	snapDownloader proto_downloader.DownloaderClient,
	agg *state.AggregatorV3,
	forkValidator *engineapi.ForkValidator,
	logger log.Logger,
	blockReader services.FullBlockReader,
	blockWriter *blockio.BlockWriter,
	blockRetire services.BlockRetire,
) (*stagedsync.Sync, error) {
	dirs := cfg.Dirs

	// During Import we don't want other services like header requests, body requests etc. to be running.
	// Hence we run it in the test mode.
	runInTestMode := cfg.ImportMode

	return stagedsync.New(
		ExecutionStages(ctx, cfg.Prune,
			stagedsync.StageSnapshotsCfg(db, *controlServer.ChainConfig, dirs, blockRetire, snapDownloader, blockReader, notifications.Events, cfg.HistoryV3, agg),
			stagedsync.StageHeadersCfg(db, controlServer.Hd, controlServer.Bd, *controlServer.ChainConfig, controlServer.SendHeaderRequest, controlServer.PropagateNewBlockHashes, controlServer.Penalize, cfg.BatchSize, p2pCfg.NoDiscovery, blockReader, blockWriter, dirs.Tmp, notifications, forkValidator),
			stagedsync.StageCumulativeIndexCfg(db, blockReader),
			stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig, blockWriter),
			stagedsync.StageBodiesCfg(db, controlServer.Bd, controlServer.SendBodyRequest, controlServer.Penalize, controlServer.BroadcastNewBlock, cfg.Sync.BodyDownloadTimeoutSeconds, *controlServer.ChainConfig, blockReader, cfg.HistoryV3, blockWriter),
			stagedsync.StageSendersCfg(db, controlServer.ChainConfig, false, dirs.Tmp, cfg.Prune, blockReader, controlServer.Hd),
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
			),
			stagedsync.StageHashStateCfg(db, dirs, cfg.HistoryV3),
			stagedsync.StageTrieCfg(db, true, true, false, dirs.Tmp, blockReader, controlServer.Hd, cfg.HistoryV3, agg),
			stagedsync.StageHistoryCfg(db, cfg.Prune, dirs.Tmp),
			stagedsync.StageLogIndexCfg(db, cfg.Prune, dirs.Tmp),
			stagedsync.StageCallTracesCfg(db, cfg.Prune, 0, dirs.Tmp),
			stagedsync.StageTxLookupCfg(db, cfg.Prune, dirs.Tmp, controlServer.ChainConfig.Bor, blockReader),
			stagedsync.StageFinishCfg(db, dirs.Tmp, forkValidator),
			runInTestMode),
		stagedsync.DefaultUnwindOrder,
		stagedsync.DefaultPruneOrder,
		logger,
	), nil
}
