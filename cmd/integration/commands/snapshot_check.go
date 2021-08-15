package commands

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/ethdb/snapshotdb"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func init() {
	withDatadir(cmdSnapshotCheck)
	withBlock(cmdSnapshotCheck)
	withBatchSize(cmdSnapshotCheck)
	withChain(cmdSnapshotCheck)
	cmdSnapshotCheck.Flags().StringVar(&tmpDBPath, "tmp_db", "", "path to temporary db(for debug)")
}

var tmpDBPath string

var cmdSnapshotCheck = &cobra.Command{
	Use:     "snapshot_check",
	Short:   "check execution over state snapshot by block",
	Example: "go run cmd/integration/main.go snapshot_check --block 11400000 --datadir /media/b00ris/nvme/backup/snapshotsync/ --snapshotDir /media/b00ris/nvme/snapshots/ --snapshotMode s --tmp_db /media/b00ris/nvme/tmp/debug",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		logger := log.New()
		//db to provide headers, blocks, senders ...
		mainDB, err := kv2.Open(chaindata, logger, true)
		if err != nil {
			return err
		}
		mode, err := snapshotsync.SnapshotModeFromString(snapshotMode)
		if err != nil {
			panic(err)
		}

		if !mode.State || len(snapshotDir) == 0 {
			return fmt.Errorf("you need state snapshot for it")
		}

		stateSnapshotPath := filepath.Join(snapshotDir, "state")
		stateSnapshot := kv2.NewMDBX(logger).Path(stateSnapshotPath).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
			return kv.TableCfg{
				kv.PlainState:        kv.ChaindataTablesCfg[kv.PlainState],
				kv.PlainContractCode: kv.ChaindataTablesCfg[kv.PlainContractCode],
				kv.Code:              kv.ChaindataTablesCfg[kv.Code],
			}
		}).Readonly().MustOpen()
		isNew := true
		var path string
		if len(tmpDBPath) > 0 {
			isNew = false
			path = tmpDBPath
		} else {
			path, err = ioutil.TempDir(os.TempDir(), "sndbg")
			if err != nil {
				return err
			}
		}

		defer func() {
			if err == nil {
				os.RemoveAll(path)
			} else {
				log.Info("Temp database", "path", path)
			}
		}()
		tmpDb := kv2.NewMDBX(logger).Path(path).MustOpen()
		db := snapshotdb.NewSnapshotKV().
			DB(tmpDb).
			//broken
			//SnapshotDB([]string{dbutils.Headers, dbutils.HeaderCanonical, dbutils.HeaderTD, dbutils.BlockBody, dbutils.Senders, dbutils.HeadBlockKey, dbutils.HeaderNumber}, mainDB.RwDB()).
			//SnapshotDB([]string{dbutils.PlainState, dbutils.Code, dbutils.PlainContractCode}, stateSnapshot).
			Open()
		_ = mainDB
		_ = stateSnapshot

		if isNew {
			if err := db.Update(ctx, func(tx kv.RwTx) error {
				return prune.SetIfNotExist(tx, prune.DefaultMode)
			}); err != nil {
				return err
			}
		}
		if err := snapshotCheck(ctx, db, isNew, os.TempDir()); err != nil {
			log.Error("snapshotCheck error", "err", err)
			return err
		}
		return nil
	},
}

func snapshotCheck(ctx context.Context, db kv.RwDB, isNew bool, tmpDir string) (err error) {
	pm, engine, chainConfig, vmConfig, _, sync, _, _ := newSync(ctx, db, nil)

	var snapshotBlock uint64 = 11_000_000
	var lastBlockHeaderNumber, blockNum uint64
	if err := db.View(ctx, func(tx kv.Tx) error {
		blockNum, err = stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return err
		}

		//snapshot or last executed block
		if blockNum > snapshotBlock {
			snapshotBlock = blockNum
		}

		//get end of check
		if block == 0 {
			lastBlockHash := rawdb.ReadHeadBlockHash(tx)
			lastBlockHeader, innerErr := rawdb.ReadHeaderByHash(tx, lastBlockHash)
			if innerErr != nil {
				return innerErr
			}
			lastBlockHeaderNumber = lastBlockHeader.Number.Uint64()
		} else {
			lastBlockHeaderNumber = block
		}

		if lastBlockHeaderNumber <= snapshotBlock {
			return fmt.Errorf("incorrect header number last block:%v, snapshotBlock: %v", lastBlockHeaderNumber, snapshotBlock)
		}
		return nil
	}); err != nil {
		return err
	}

	if isNew {
		log.Info("New tmp db. We need to promote hash state.")
		if err := db.Update(ctx, func(tx kv.RwTx) error {

			tt := time.Now()
			err = stagedsync.PromoteHashedStateCleanly("", tx, stagedsync.StageHashStateCfg(db, tmpDir), ctx.Done())
			log.Info("Promote took", "t", time.Since(tt))
			if err != nil {
				return fmt.Errorf("promote state err: %w", err)
			}
			tt = time.Now()
			err = tx.Commit()
			if err != nil {
				return fmt.Errorf("commit promote state err: %w", err)
			}
			log.Info("promote committed", "t", time.Since(tt))
			return nil
		}); err != nil {
			return err
		}
	}

	if isNew {
		log.Info("Regenerate IH")
		if err := db.Update(ctx, func(tx kv.RwTx) error {
			hash, innerErr := rawdb.ReadCanonicalHash(tx, snapshotBlock)
			if innerErr != nil {
				return innerErr
			}

			syncHeadHeader := rawdb.ReadHeader(tx, hash, snapshotBlock)
			if syncHeadHeader == nil {
				return fmt.Errorf("empty header for %v", snapshotBlock)
			}
			expectedRootHash := syncHeadHeader.Root

			tt := time.Now()
			_, err = stagedsync.RegenerateIntermediateHashes("", tx, stagedsync.StageTrieCfg(db, true, true, tmpDir), expectedRootHash, ctx.Done())
			if err != nil {
				return fmt.Errorf("regenerateIntermediateHashes err: %w", err)
			}
			log.Info("RegenerateIntermediateHashes took", "t", time.Since(tt))
			return nil
		}); err != nil {
			return err
		}
	}

	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	sync.DisableStages(stages.Headers,
		stages.BlockHashes,
		stages.Bodies,
		stages.Senders,
		stages.Translation,
		stages.AccountHistoryIndex,
		stages.StorageHistoryIndex,
		stages.LogIndex,
		stages.CallTraces,
		stages.TxLookup,
		stages.TxPool,
		stages.Finish,
	)

	if isNew {
		stage3 := stage(sync, tx, nil, stages.Senders)
		err = stage3.Update(tx, lastBlockHeaderNumber)
		if err != nil {
			return err
		}

		stage4 := stage(sync, tx, nil, stages.Execution)
		err = stage4.Update(tx, snapshotBlock)
		if err != nil {
			return err
		}
		stage5 := stage(sync, tx, nil, stages.HashState)
		err = stage5.Update(tx, snapshotBlock)
		if err != nil {
			return err
		}

		stage6 := stage(sync, tx, nil, stages.IntermediateHashes)
		err = stage6.Update(tx, snapshotBlock)
		if err != nil {
			return err
		}
	}

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	for blockNumber := snapshotBlock + 1; blockNumber <= lastBlockHeaderNumber; blockNumber++ {
		err = sync.SetCurrentStage(stages.Execution)
		if err != nil {
			return err
		}
		stage4 := stage(sync, tx, nil, stages.Execution)
		stage4.BlockNumber = blockNumber - 1
		log.Info("Stage4", "progress", stage4.BlockNumber)

		err = stagedsync.SpawnExecuteBlocksStage(stage4, sync, tx, blockNumber, ctx,
			stagedsync.StageExecuteBlocksCfg(db, pm, batchSize, nil, chainConfig, engine, vmConfig, nil, false, tmpDir),
			false)
		if err != nil {
			return fmt.Errorf("execution err %w", err)
		}

		stage5 := stage(sync, tx, nil, stages.HashState)
		stage5.BlockNumber = blockNumber - 1
		log.Info("Stage5", "progress", stage5.BlockNumber)
		err = stagedsync.SpawnHashStateStage(stage5, tx, stagedsync.StageHashStateCfg(db, tmpDir), ctx)
		if err != nil {
			return fmt.Errorf("spawnHashStateStage err %w", err)
		}

		stage6 := stage(sync, tx, nil, stages.IntermediateHashes)
		stage6.BlockNumber = blockNumber - 1
		log.Info("Stage6", "progress", stage6.BlockNumber)
		if _, err = stagedsync.SpawnIntermediateHashesStage(stage5, sync /* Unwinder */, tx, stagedsync.StageTrieCfg(db, true, true, tmpDir), ctx); err != nil {
			log.Error("Error on ih", "err", err, "block", blockNumber)
			return fmt.Errorf("spawnIntermediateHashesStage %w", err)
		}

		log.Info("Done", "progress", blockNumber)
		err = tx.Commit()
		if err != nil {
			log.Error("Error on commit", "err", err, "block", blockNumber)
			return err
		}
		tx, err = db.BeginRw(ctx)
		if err != nil {
			log.Error("Error on begin", "err", err, "block", blockNumber)
			return err
		}
		defer tx.Rollback()
	}

	return nil
}
