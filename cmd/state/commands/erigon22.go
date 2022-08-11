package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"runtime"
	"syscall"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/cmd/state/exec22"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
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

func Erigon22(ctx context.Context, genesis *core.Genesis, logger log.Logger) error {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	execCtx, cancel := context.WithCancel(ctx)
	go func() {
		<-sigs
		cancel()
	}()
	ctx = context.Background()
	var err error
	dirs := datadir2.New(datadir, snapdir)
	reconDbPath := path.Join(datadir, "db22")
	if reset && dir.Exist(reconDbPath) {
		if err = os.RemoveAll(reconDbPath); err != nil {
			return err
		}
	}
	dir.MustExist(reconDbPath)
	limiter := semaphore.NewWeighted(int64(runtime.NumCPU() + 1))
	db, err := kv2.NewMDBX(logger).Path(reconDbPath).RoTxsLimiter(limiter).Open()
	if err != nil {
		return err
	}
	chainDbPath := path.Join(datadir, "chaindata")
	chainDb, err := kv2.NewMDBX(logger).Path(chainDbPath).RoTxsLimiter(limiter).Readonly().Open()
	if err != nil {
		return err
	}
	startTime := time.Now()
	var blockReader services.FullBlockReader
	var allSnapshots *snapshotsync.RoSnapshots
	allSnapshots = snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, false, true), dirs.Snap)
	defer allSnapshots.Close()
	if err := allSnapshots.ReopenFolder(); err != nil {
		return fmt.Errorf("reopen snapshot segments: %w", err)
	}
	blockReader = snapshotsync.NewBlockReaderWithSnapshots(allSnapshots)
	// Compute mapping blockNum -> last TxNum in that block
	maxBlockNum := allSnapshots.BlocksAvailable() + 1
	txNums := make([]uint64, maxBlockNum)
	if err = allSnapshots.Bodies.View(func(bs []*snapshotsync.BodySegment) error {
		for _, b := range bs {
			if err = b.Iterate(func(blockNum, baseTxNum, txAmount uint64) {
				txNums[blockNum] = baseTxNum + txAmount
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("build txNum => blockNum mapping: %w", err)
	}

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
	)
	if err != nil {
		return err
	}
	cfg := ethconfig.Defaults
	cfg.DeprecatedTxPool.Disable = true
	cfg.Dirs = datadir2.New(datadir, snapdir)
	cfg.Snapshot = allSnapshots.Cfg()
	stagedSync, err := stages2.NewStagedSync(context.Background(), logger, db, p2p.Config{}, &cfg, sentryControlServer, &stagedsync.Notifications{}, nil, allSnapshots, nil, nil)
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

	rs := state.NewState22()
	aggDir := path.Join(dirs.DataDir, "agg22")
	if reset && dir.Exist(aggDir) {
		if err = os.RemoveAll(aggDir); err != nil {
			return err
		}
	}
	dir.MustExist(aggDir)
	agg, err := libstate.NewAggregator22(aggDir, AggregationStep)
	if err != nil {
		return err
	}
	defer agg.Close()

	workerCount := workers
	if err := exec22.Exec22(execCtx, execStage, block, workerCount, db, chainDb, nil, rs, blockReader, allSnapshots, txNums, logger, agg, engine, maxBlockNum, chainConfig, genesis, true); err != nil {
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
		if err = stagedsync.PromoteHashedStateCleanly("recon", tx, stagedsync.StageHashStateCfg(db, dirs.Tmp), ctx); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	var rootHash common.Hash
	if err = db.Update(ctx, func(tx kv.RwTx) error {
		if rootHash, err = stagedsync.RegenerateIntermediateHashes("recon", tx, stagedsync.StageTrieCfg(db, false /* checkRoot */, false /* saveHashesToDB */, false /* badBlockHalt */, dirs.Tmp, blockReader, nil /* HeaderDownload */), common.Hash{}, make(chan struct{}, 1)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	var header *types.Header
	if err := db.View(ctx, func(tx kv.Tx) error {
		execStage, err = stagedSync.StageState(stages.Execution, tx, db)
		if err != nil {
			return err
		}
		header, err = blockReader.HeaderByNumber(ctx, tx, execStage.BlockNumber)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if rootHash != header.Root {
		log.Error("Incorrect root hash", "expected", fmt.Sprintf("%x", header.Root))
	}
	return nil
}
