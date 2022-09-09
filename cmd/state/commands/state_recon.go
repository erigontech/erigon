package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
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

func init() {
	withBlock(reconCmd)
	withChain(reconCmd)
	withDataDir(reconCmd)
	rootCmd.AddCommand(reconCmd)
}

var reconCmd = &cobra.Command{
	Use:   "recon",
	Short: "Exerimental command to reconstitute the state from state history at given block",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		return Recon(genesis, logger)
	},
}

func Recon(genesis *core.Genesis, logger log.Logger) error {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()
	ctx := context.Background()
	aggPath := filepath.Join(datadir, "agg22")
	agg, err := libstate.NewAggregator22(aggPath, ethconfig.HistoryV2AggregationStep)
	if err != nil {
		return fmt.Errorf("create history: %w", err)
	}
	defer agg.Close()
	reconDbPath := path.Join(datadir, "recondb")
	os.RemoveAll(reconDbPath)
	dir.MustExist(reconDbPath)
	startTime := time.Now()
	workerCount := runtime.NumCPU()
	limiterB := semaphore.NewWeighted(int64(workerCount + 1))
	db, err := kv2.NewMDBX(logger).Path(reconDbPath).RoTxsLimiter(limiterB).WriteMap().WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.ReconTablesCfg }).Open()
	if err != nil {
		return err
	}
	limiter := semaphore.NewWeighted(int64(workerCount + 1))
	chainDbPath := path.Join(datadir, "chaindata")
	chainDb, err := kv2.NewMDBX(logger).Path(chainDbPath).RoTxsLimiter(limiter).Open()
	if err != nil {
		return err
	}
	var blockReader services.FullBlockReader
	dirs := datadir2.New(datadir)
	allSnapshots := snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, false, true), dirs.Snap)
	defer allSnapshots.Close()
	if err := allSnapshots.ReopenFolder(); err != nil {
		return fmt.Errorf("reopen snapshot segments: %w", err)
	}
	blockReader = snapshotsync.NewBlockReaderWithSnapshots(allSnapshots)
	// Compute mapping blockNum -> last TxNum in that block
	txNums := exec22.TxNumsFromDB(allSnapshots, chainDb)
	engine := initConsensusEngine(chainConfig, logger, allSnapshots)
	sentryControlServer, err := sentry.NewMultiClient(
		chainDb,
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
	cfg.HistoryV2 = true
	cfg.DeprecatedTxPool.Disable = true
	cfg.Dirs = dirs
	cfg.Snapshot = allSnapshots.Cfg()
	stagedSync, err := stages2.NewStagedSync(context.Background(), chainDb, p2p.Config{}, &cfg, sentryControlServer, &stagedsync.Notifications{}, nil, allSnapshots, nil, txNums, agg, nil)
	if err != nil {
		return err
	}
	var execStage *stagedsync.StageState
	chainDb.View(ctx, func(tx kv.Tx) error {
		execStage, err = stagedSync.StageState(stages.Execution, tx, chainDb)
		if err != nil {
			return err
		}
		return nil
	})

	if err := stagedsync.Recon22(
		ctx,
		execStage,
		dirs,
		workers,
		chainDb,
		db,
		blockReader, allSnapshots, txNums, log.New(),
		agg, engine, chainConfig, genesis,
	); err != nil {
		return err
	}

	if err := chainDb.Update(ctx, func(tx kv.RwTx) error {
		log.Info("Reconstitution complete", "duration", time.Since(startTime))
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
		if err = stagedsync.PromoteHashedStateCleanly("recon", tx, stagedsync.StageHashStateCfg(chainDb, cfg.Dirs, true, txNums, agg), ctx); err != nil {
			return err
		}
		hashStage, err := stagedSync.StageState(stages.HashState, tx, chainDb)
		if err != nil {
			return err
		}
		if err = hashStage.Update(tx, block); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if err = chainDb.Update(ctx, func(tx kv.RwTx) error {
		var rootHash common.Hash
		if rootHash, err = stagedsync.RegenerateIntermediateHashes("recon", tx, stagedsync.StageTrieCfg(chainDb, false /* checkRoot */, true /* saveHashesToDB */, false /* badBlockHalt */, dirs.Tmp, blockReader, nil /* HeaderDownload */, cfg.HistoryV2, txNums, agg), common.Hash{}, make(chan struct{}, 1)); err != nil {
			return err
		}
		trieStage, err := stagedSync.StageState(stages.IntermediateHashes, tx, chainDb)
		if err != nil {
			return err
		}
		if err = trieStage.Update(tx, block); err != nil {
			return err
		}

		header, err := blockReader.HeaderByNumber(ctx, tx, execStage.BlockNumber)
		if err != nil {
			panic(err)
		}

		if rootHash != header.Root {
			log.Error("Incorrect root hash", "expected", fmt.Sprintf("%x", header.Root))
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}
