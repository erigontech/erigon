package commands

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/spf13/cobra"
)

func init() {
	withDatadir(cmdSnapshotCheck)
	withBlock(cmdSnapshotCheck)
	withBatchSize(cmdSnapshotCheck)
	cmdSnapshotCheck.Flags().StringVar(&tmpDBPath, "tmp_db", "", "path to temporary db(for debug)")
}

var tmpDBPath string

var cmdSnapshotCheck = &cobra.Command{
	Use:     "snapshot_check",
	Short:   "check execution over state snapshot by block",
	Example: "go run cmd/integration/main.go snapshot_check --block 11400000 --datadir /media/b00ris/nvme/backup/snapshotsync/ --snapshotDir /media/b00ris/nvme/snapshots/ --snapshotMode s --tmp_db /media/b00ris/nvme/tmp/debug",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		//db to provide headers, blocks, senders ...
		mainDB, err := ethdb.Open(chaindata, true)
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
		var stateSnapshot ethdb.RwKV
		if database == "lmdb" {
			stateSnapshot = ethdb.NewLMDB().Path(stateSnapshotPath).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
				return dbutils.BucketsCfg{
					dbutils.PlainStateBucket:        dbutils.BucketsConfigs[dbutils.PlainStateBucket],
					dbutils.PlainContractCodeBucket: dbutils.BucketsConfigs[dbutils.PlainContractCodeBucket],
					dbutils.CodeBucket:              dbutils.BucketsConfigs[dbutils.CodeBucket],
				}
			}).Readonly().MustOpen()
		} else {
			stateSnapshot = ethdb.NewMDBX().Path(stateSnapshotPath).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
				return dbutils.BucketsCfg{
					dbutils.PlainStateBucket:        dbutils.BucketsConfigs[dbutils.PlainStateBucket],
					dbutils.PlainContractCodeBucket: dbutils.BucketsConfigs[dbutils.PlainContractCodeBucket],
					dbutils.CodeBucket:              dbutils.BucketsConfigs[dbutils.CodeBucket],
				}
			}).Readonly().MustOpen()
		}
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
		var tmpDb ethdb.RwKV
		if database == "lmdb" {
			tmpDb = ethdb.NewLMDB().Path(path).MustOpen()
		} else {
			tmpDb = ethdb.NewMDBX().Path(path).MustOpen()
		}
		kv := ethdb.NewSnapshotKV().
			DB(tmpDb).
			SnapshotDB([]string{dbutils.HeadersBucket, dbutils.HeaderCanonicalBucket, dbutils.HeaderTDBucket, dbutils.BlockBodyPrefix, dbutils.Senders, dbutils.HeadBlockKey, dbutils.HeaderNumberBucket}, mainDB.RwKV()).
			SnapshotDB([]string{dbutils.PlainStateBucket, dbutils.CodeBucket, dbutils.PlainContractCodeBucket}, stateSnapshot).
			Open()

		db := ethdb.NewObjectDatabase(kv)
		defer db.Close()
		if isNew {
			err = ethdb.SetStorageModeIfNotExist(db, ethdb.StorageMode{})
			if err != nil {
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

func snapshotCheck(ctx context.Context, db ethdb.Database, isNew bool, tmpDir string) (err error) {
	kv := db.RwKV()
	sm, engine, chainConfig, vmConfig, _, st, _ := newSync3(db)

	var snapshotBlock uint64 = 11_000_000
	blockNum, err := stages.GetStageProgress(db, stages.Execution)
	if err != nil {
		return err
	}

	//snapshot or last executed block
	if blockNum > snapshotBlock {
		snapshotBlock = blockNum
	}

	//get end of check
	var lastBlockHeaderNumber uint64
	if block == 0 {
		lastBlockHash := rawdb.ReadHeadBlockHash(db)
		lastBlockHeader, innerErr := rawdb.ReadHeaderByHash(db, lastBlockHash)
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

	if isNew {
		log.Info("New tmp db. We need to promote hash state.")
		tx, innerErr := kv.BeginRw(ctx)
		if innerErr != nil {
			return innerErr
		}
		defer tx.Rollback()

		tt := time.Now()
		err = stagedsync.PromoteHashedStateCleanly("", tx, stagedsync.StageHashStateCfg(kv, tmpDir), ctx.Done())
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
	}

	if isNew {
		log.Info("Regenerate IH")
		tx, innerErr := kv.BeginRw(context.Background())
		if innerErr != nil {
			return innerErr
		}
		defer tx.Rollback()

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
		_, err = stagedsync.RegenerateIntermediateHashes("", tx, stagedsync.StageTrieCfg(kv, true, true, tmpDir), expectedRootHash, ctx.Done())
		if err != nil {
			return fmt.Errorf("regenerateIntermediateHashes err: %w", err)
		}
		log.Info("RegenerateIntermediateHashes took", "t", time.Since(tt))
		tt = time.Now()
		err = tx.Commit()
		if err != nil {
			return err
		}
		log.Info("Commit", "t", time.Since(tt))
	}

	tx, err := kv.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	tmpdir := path.Join(datadir, etl.TmpDirName)
	sync, err := st.Prepare(nil, chainConfig, engine, vmConfig, db, tx, "integration_test", sm, tmpdir, 0, ctx.Done(), nil, nil, false, nil)
	if err != nil {
		return nil
	}

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
		stage3 := stage(sync, tx, stages.Senders)
		err = stage3.DoneAndUpdate(db, lastBlockHeaderNumber)
		if err != nil {
			return err
		}

		stage4 := stage(sync, tx, stages.Execution)
		err = stage4.DoneAndUpdate(db, snapshotBlock)
		if err != nil {
			return err
		}
		stage5 := stage(sync, tx, stages.HashState)
		err = stage5.DoneAndUpdate(db, snapshotBlock)
		if err != nil {
			return err
		}

		stage6 := stage(sync, tx, stages.IntermediateHashes)
		err = stage6.DoneAndUpdate(db, snapshotBlock)
		if err != nil {
			return err
		}
	}

	ch := ctx.Done()

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	for blockNumber := snapshotBlock + 1; blockNumber <= lastBlockHeaderNumber; blockNumber++ {
		err = sync.SetCurrentStage(stages.Execution)
		if err != nil {
			return err
		}
		stage4 := stage(sync, tx, stages.Execution)
		stage4.BlockNumber = blockNumber - 1
		log.Info("Stage4", "progress", stage4.BlockNumber)

		err = stagedsync.SpawnExecuteBlocksStage(stage4, tx, blockNumber, ch,
			stagedsync.StageExecuteBlocksCfg(kv, false, false, batchSize, nil, nil, nil, nil, chainConfig, engine, vmConfig, tmpDir),
		)
		if err != nil {
			return fmt.Errorf("execution err %w", err)
		}

		stage5 := stage(sync, tx, stages.HashState)
		stage5.BlockNumber = blockNumber - 1
		log.Info("Stage5", "progress", stage5.BlockNumber)
		err = stagedsync.SpawnHashStateStage(stage5, tx, stagedsync.StageHashStateCfg(kv, tmpDir), ch)
		if err != nil {
			return fmt.Errorf("spawnHashStateStage err %w", err)
		}

		stage6 := stage(sync, tx, stages.IntermediateHashes)
		stage6.BlockNumber = blockNumber - 1
		log.Info("Stage6", "progress", stage6.BlockNumber)
		if _, err = stagedsync.SpawnIntermediateHashesStage(stage5, nil /* Unwinder */, tx, stagedsync.StageTrieCfg(kv, true, true, tmpDir), ch); err != nil {
			log.Error("Error on ih", "err", err, "block", blockNumber)
			return fmt.Errorf("spawnIntermediateHashesStage %w", err)
		}

		log.Info("Done", "progress", blockNumber)
		err = tx.Commit()
		if err != nil {
			log.Error("Error on commit", "err", err, "block", blockNumber)
			return err
		}
		tx, err = kv.BeginRw(ctx)
		if err != nil {
			log.Error("Error on begin", "err", err, "block", blockNumber)
			return err
		}
	}

	return nil
}
