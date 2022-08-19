package commands

import (
	"context"
	"fmt"
	"path"
	"runtime"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/cmd/hack/tool"
	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/cmd/state/exec22"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb/rawdbreset"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	datadir2 "github.com/ledgerwatch/erigon/node/nodecfg/datadir"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	stages2 "github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"golang.org/x/sync/semaphore"
)

var (
	reset   bool
	workers int
)

func init() {
	erigon22Cmd.Flags().BoolVar(&reset, "reset", false, "Resets the state database and static files")
	erigon22Cmd.Flags().IntVar(&workers, "workers", 1, "Number of workers")
	withDataDir(erigon22Cmd)
	rootCmd.AddCommand(erigon22Cmd)
	withChain(erigon22Cmd)
}

var erigon22Cmd = &cobra.Command{
	Use:   "erigon22",
	Short: "Exerimental command to re-execute blocks from beginning using erigon2 histoty (ugrade 2)",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		return Erigon22(cmd.Context(), genesis, logger)
	},
}

func Erigon22(execCtx context.Context, genesis *core.Genesis, logger log.Logger) error {
	ctx := context.Background()
	var err error
	dirs := datadir2.New(datadir)

	limiter := semaphore.NewWeighted(int64(runtime.NumCPU() + 1))
	db, err := kv2.NewMDBX(logger).Path(dirs.Chaindata).RoTxsLimiter(limiter).Open()
	if err != nil {
		return err
	}
	if reset {
		if err := db.Update(ctx, func(tx kv.RwTx) error { return rawdbreset.ResetExec(tx, chainConfig.ChainName) }); err != nil {
			return err
		}
	}

	startTime := time.Now()
	var blockReader services.FullBlockReader
	var allSnapshots = snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, false, true), dirs.Snap)
	defer allSnapshots.Close()
	if err := allSnapshots.ReopenFolder(); err != nil {
		return fmt.Errorf("reopen snapshot segments: %w", err)
	}
	blockReader = snapshotsync.NewBlockReaderWithSnapshots(allSnapshots)
	txNums := exec22.TxNumsFromDB(allSnapshots, db)

	engine := initConsensusEngine(chainConfig, logger, allSnapshots)
	sentryControlServer, err := sentry.NewMultiClient(
		db,
		"",
		chainConfig,
		common.Hash{},
		engine,
		1,
		nil,
		ethconfig.Defaults.Sync,
		blockReader,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	cfg := ethconfig.Defaults
	cfg.HistoryV2 = tool.HistoryV2FromDB(db)
	cfg.DeprecatedTxPool.Disable = true
	cfg.Dirs = datadir2.New(datadir)
	cfg.Snapshot = allSnapshots.Cfg()

	aggDir := path.Join(dirs.DataDir, "agg22")
	if reset {
		dir.Recreate(aggDir)
	}
	dir.MustExist(aggDir)
	agg, err := libstate.NewAggregator22(aggDir, ethconfig.HistoryV2AggregationStep)
	if err != nil {
		return err
	}
	defer agg.Close()

	stagedSync, err := stages2.NewStagedSync(context.Background(), db, p2p.Config{}, &cfg, sentryControlServer, &stagedsync.Notifications{}, nil, allSnapshots, nil, txNums, agg, nil)
	if err != nil {
		return err
	}
	var execStage *stagedsync.StageState
	if err := db.View(ctx, func(tx kv.Tx) error {
		execStage, err = stagedSync.StageState(stages.Execution, tx, db)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if !reset {
		block = execStage.BlockNumber + 1
	}

	workerCount := workers
	execCfg := stagedsync.StageExecuteBlocksCfg(db, cfg.Prune, cfg.BatchSize, nil, chainConfig, engine, &vm.Config{}, nil,
		/*stateStream=*/ false,
		/*badBlockHalt=*/ false, cfg.HistoryV2, dirs, blockReader, nil, genesis, workerCount, agg)
	maxBlockNum := allSnapshots.BlocksAvailable() + 1
	if err := stagedsync.SpawnExecuteBlocksStage(execStage, stagedSync, nil, maxBlockNum, ctx, execCfg, true); err != nil {
		return err
	}

	if err = db.Update(ctx, func(tx kv.RwTx) error {
		log.Info("Transaction replay complete", "duration", time.Since(startTime))
		log.Info("Computing hashed state")
		if err = tx.ClearBucket(kv.HashedAccounts); err != nil {
			return err
		}
		if err = tx.ClearBucket(kv.HashedStorage); err != nil {
			return err
		}
		if err = tx.ClearBucket(kv.ContractCode); err != nil {
			return err
		}
		if err = stagedsync.PromoteHashedStateCleanly("recon", tx, stagedsync.StageHashStateCfg(db, dirs, cfg.HistoryV2, txNums, agg), ctx); err != nil {
			return err
		}
		var rootHash common.Hash
		if rootHash, err = stagedsync.RegenerateIntermediateHashes("recon", tx, stagedsync.StageTrieCfg(db, false /* checkRoot */, false /* saveHashesToDB */, false /* badBlockHalt */, dirs.Tmp, blockReader, nil /* HeaderDownload */, cfg.HistoryV2, txNums, agg), common.Hash{}, make(chan struct{}, 1)); err != nil {
			return err
		}
		execStage, err = stagedSync.StageState(stages.Execution, tx, db)
		if err != nil {
			return err
		}
		header, err := blockReader.HeaderByNumber(ctx, tx, execStage.BlockNumber)
		if err != nil {
			return err
		}
		if rootHash != header.Root {
			err := fmt.Errorf("incorrect root hash: expecteed %x", header.Root)
			log.Error(err.Error())
			return err
		}

		return nil
	}); err != nil {
		return err
	}
	return nil
}
