package debug

import (
	"context"
	"fmt"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"time"
)

//func init() {
//	withChaindata(cmdSnapshotCheck)
//	withBlock(cmdSnapshotCheck)
//	cmdSnapshotCheck.Flags().StringVar(&tmpDBPath, "tmp_db", "", "path to temporary db(for debug)")
//	cmdSnapshotCheck.Flags().BoolVar(&fastcheck, "fastcheck", false, "check only final root")
//	withChaindata(dbCopyCmd)
//	rootCmd.AddCommand(dbCopyCmd)
//	rootCmd.AddCommand(cmdSnapshotCheck)
//}

var (
	tmpDBPath string
	fastcheck bool
	chaindata string
	block uint64
	snapshotMode string
	snapshotDir string

	tmpDir string
	datadir string

	cacheSizeStr       string
	batchSizeStr       string

)


var cmdSnapshotCheck = &cobra.Command{
	Use:     "snapshot_check",
	Short:   "check execution over state snapshot by block",
	Example: "go run cmd/integration/main.go snapshot_check --block 11400000 --chaindata /media/b00ris/nvme/backup/snapshotsync/tg/chaindata/ --snapshotDir /media/b00ris/nvme/snapshots/ --snapshotMode s --tmp_db /media/b00ris/nvme/tmp/debug",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
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
		stateSnapshot := ethdb.NewLMDB().Path(stateSnapshotPath).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return dbutils.BucketsCfg{
				dbutils.PlainStateBucket:        dbutils.BucketsConfigs[dbutils.PlainStateBucket],
				dbutils.PlainContractCodeBucket: dbutils.BucketsConfigs[dbutils.PlainContractCodeBucket],
				dbutils.CodeBucket:              dbutils.BucketsConfigs[dbutils.CodeBucket],
			}
		}).Flags(func(flags uint) uint { return flags | lmdb.Readonly }).MustOpen()

		isNew := true
		var path string
		if len(tmpDBPath) > 0 {
			isNew = false
			path = tmpDBPath
		} else {
			if tmpDir == "" {
				tmpDir=os.TempDir()
			}
			path, err = ioutil.TempDir(os.TempDir(), "sndbg")
			if err != nil {
				return err
			}
		}

		defer func() {
			if err == nil {
				//os.RemoveAll(path)
			} else {
				log.Info("Temp database", "path", path)
			}
		}()
		tmpDb := ethdb.NewLMDB().Path(path).MustOpen()

		kv := ethdb.NewSnapshotKV().
			DB(tmpDb).
			SnapshotDB([]string{dbutils.HeadersBucket, dbutils.HeaderCanonicalBucket, dbutils.HeaderTDBucket, dbutils.HeaderNumberBucket, dbutils.BlockBodyPrefix, dbutils.Senders, dbutils.HeadBlockKey,  dbutils.HeadHeaderKey, dbutils.EthTx, dbutils.Sequence}, mainDB.KV()).
			SnapshotDB([]string{dbutils.PlainStateBucket, dbutils.CodeBucket, dbutils.PlainContractCodeBucket}, stateSnapshot).
			Open()

		db := ethdb.NewObjectDatabase(kv)
		if isNew {
			err = ethdb.SetStorageModeIfNotExist(db, ethdb.StorageMode{})
			if err != nil {
				return err
			}
		}

		if err := stateRootCheck(ctx, db, isNew, os.TempDir()); err != nil {
			log.Error("stateRootCheck error", "err", err)
			return err
		}
		return nil
	},
}

func stateRootCheck(ctx context.Context, db ethdb.Database, isNew bool, tmpDir string) (err error) {
	var snapshotBlock uint64 = snapshotsync.SnapshotBlock
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

	if isNew && !fastcheck {
		log.Info("New tmp db. We need to promote hash state.")
		tx, innerErr := db.Begin(context.Background(), ethdb.RW)
		if innerErr != nil {
			return innerErr
		}

		tt := time.Now()
		err = stagedsync.PromoteHashedStateCleanly("", tx, tmpDir, ctx.Done())
		log.Info("Promote took", "t", time.Since(tt))
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("promote state err: %w", err)
		}
		tt = time.Now()
		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("commit promote state err: %w", err)
		}
		log.Info("promote committed", "t", time.Since(tt))
	}

	if isNew && !fastcheck{
		log.Info("Regenerate IH")
		tx, innerErr := db.Begin(context.Background(), ethdb.RW)
		if innerErr != nil {
			return innerErr
		}

		hash, innerErr := rawdb.ReadCanonicalHash(tx, snapshotBlock)
		if innerErr != nil {
			tx.Rollback()
			return innerErr
		}

		syncHeadHeader := rawdb.ReadHeader(tx, hash, snapshotBlock)
		if syncHeadHeader == nil {
			tx.Rollback()
			return fmt.Errorf("empty header for %v", snapshotBlock)
		}
		expectedRootHash := syncHeadHeader.Root

		tt := time.Now()
		_, err = stagedsync.RegenerateIntermediateHashes("", tx, true,nil, tmpDir, expectedRootHash, ctx.Done())
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("regenerateIntermediateHashes err: %w", err)
		}
		log.Info("RegenerateIntermediateHashes took", "t", time.Since(tt))
		tt = time.Now()
		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			return err
		}
		log.Info("Commit", "t", time.Since(tt))
	}

	cc, bc, st, progress,err := newSync(ctx.Done(), db, db, nil)
	if err!=nil {
		return err
	}
	defer bc.Stop()
	st.DisableStages(stages.Headers,
		stages.BlockHashes,
		stages.Bodies,
		stages.Senders,
		stages.AccountHistoryIndex,
		stages.StorageHistoryIndex,
		stages.LogIndex,
		stages.CallTraces,
		stages.TxLookup,
		stages.TxPool,
		stages.Finish,
	)
	fmt.Println("lastBlockHeaderNumber",lastBlockHeaderNumber)
	if isNew {
		stage3 := progress(stages.Senders)
		err = stage3.DoneAndUpdate(db, lastBlockHeaderNumber)
		if err != nil {
			return err
		}

		stage4 := progress(stages.Execution)
		err = stage4.DoneAndUpdate(db, snapshotBlock)
		if err != nil {
			return err
		}
		if !fastcheck {
			stage5 := progress(stages.HashState)
			err = stage5.DoneAndUpdate(db, snapshotBlock)
			if err != nil {
				return err
			}

			stage6 := progress(stages.IntermediateHashes)
			err = stage6.DoneAndUpdate(db, snapshotBlock)
			if err != nil {
				return err
			}
		}
	}

	ch := ctx.Done()
	var batchSize datasize.ByteSize
	err = batchSize.UnmarshalText([]byte(batchSizeStr))
	if err!=nil {
		return err
	}

	if fastcheck {
		err = st.SetCurrentStage(stages.Execution)
		if err != nil {
			return err
		}
		stage4 := progress(stages.Execution)
		stage4.BlockNumber = snapshotBlock
		log.Info("Stage4", "progress", stage4.BlockNumber)

		err = stagedsync.SpawnExecuteBlocksStage(stage4, db,
			bc.Config(), cc, bc.GetVMConfig(),
			ch,
			stagedsync.ExecuteBlockStageParams{
				ToBlock:       lastBlockHeaderNumber, // limit execution to the specified block
				WriteReceipts: false,
				BatchSize:     batchSize,
			})
		if err != nil {
			return fmt.Errorf("execution err %w", err)
		}
	}
	if fastcheck==false {
		tx, err := db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		batch:=10000
		i:=0

		for blockNumber := snapshotBlock + 1; blockNumber <= lastBlockHeaderNumber; blockNumber++ {
			err = st.SetCurrentStage(stages.Execution)
			if err != nil {
				return err
			}
			stage4 := progress(stages.Execution)
			stage4.BlockNumber = blockNumber - 1
			log.Info("Stage4", "progress", stage4.BlockNumber)

			err = stagedsync.SpawnExecuteBlocksStage(stage4, tx,
				bc.Config(), cc, bc.GetVMConfig(),
				ch,
				stagedsync.ExecuteBlockStageParams{
					ToBlock:       blockNumber, // limit execution to the specified block
					WriteReceipts: false,
					BatchSize:     batchSize,
				})
			if err != nil {
				return fmt.Errorf("execution err %w", err)
			}

			if !fastcheck {
				stage5 := progress(stages.HashState)
				stage5.BlockNumber = blockNumber - 1
				log.Info("Stage5", "progress", stage5.BlockNumber)
				err = stagedsync.SpawnHashStateStage(stage5, tx, nil, tmpDir, ch)
				if err != nil {
					return fmt.Errorf("spawnHashStateStage err %w", err)
				}

				stage6 := progress(stages.IntermediateHashes)
				stage6.BlockNumber = blockNumber - 1
				log.Info("Stage6", "progress", stage6.BlockNumber)
				if _, err = stagedsync.SpawnIntermediateHashesStage(stage5, tx, true,nil, tmpDir, ch); err != nil {
					log.Error("Error on ih", "err", err, "block", blockNumber)
					return fmt.Errorf("spawnIntermediateHashesStage %w", err)
				}
			}
			if !fastcheck {
				log.Info("Done", "progress", blockNumber)
				err = tx.CommitAndBegin(context.TODO())
				if err != nil {
					log.Error("Error on commit", "err", err, "block", blockNumber)
					return err
				}
			} else if i>batch {
				log.Info("Commit!!!!", "progress", blockNumber)
				err = tx.CommitAndBegin(context.TODO())
				if err != nil {
					log.Error("Error on commit", "err", err, "block", blockNumber)
					return err
				}
				i=0
			} else {
				i++
			}

		}
		if fastcheck {
			err = tx.Commit()
			if err != nil {
				log.Error("Error on last commit", "err", err,)
				return err
			}

		}
		tx.Rollback()
	}

	if fastcheck {
		log.Info("New tmp db. We need to promote hash state.")
		tx, innerErr := db.Begin(context.Background(), ethdb.RW)
		if innerErr != nil {
			return innerErr
		}

		tt := time.Now()
		err = stagedsync.PromoteHashedStateCleanly("", tx, tmpDir, ctx.Done())
		log.Info("Promote took", "t", time.Since(tt))
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("promote state err: %w", err)
		}
		tt = time.Now()
		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("commit promote state err: %w", err)
		}
		log.Info("promote committed", "t", time.Since(tt))

		log.Info("Regenerate IH")
		tx, innerErr = db.Begin(context.Background(), ethdb.RW)
		if innerErr != nil {
			return innerErr
		}

		hash, innerErr := rawdb.ReadCanonicalHash(tx, lastBlockHeaderNumber)
		if innerErr != nil {
			tx.Rollback()
			return innerErr
		}

		syncHeadHeader := rawdb.ReadHeader(tx, hash, lastBlockHeaderNumber)
		if syncHeadHeader == nil {
			tx.Rollback()
			return fmt.Errorf("empty header for %v", snapshotBlock)
		}
		expectedRootHash := syncHeadHeader.Root

		tt = time.Now()
		_, err = stagedsync.RegenerateIntermediateHashes("", tx, true,nil, tmpDir, expectedRootHash, ctx.Done())
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("regenerateIntermediateHashes err: %w", err)
		}
		log.Info("RegenerateIntermediateHashes took", "t", time.Since(tt))
		tt = time.Now()
		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			return err
		}
		log.Info("Commit", "t", time.Since(tt))
	}

	return nil
}




func newSync(quitCh <-chan struct{}, db ethdb.Database, tx ethdb.Database, hook stagedsync.ChangeSetHook) (*core.TinyChainContext, *core.BlockChain, *stagedsync.State, func(stage stages.SyncStage) *stagedsync.StageState, error) {
	sm, err := ethdb.GetStorageModeFromDB(db)
	if err != nil {
		panic(err)
	}

	chainConfig, bc, err := newBlockChain(db, sm)
	if err != nil {
		panic(err)
	}

	cc := &core.TinyChainContext{}
	cc.SetDB(tx)
	cc.SetEngine(ethash.NewFaker())
	var cacheSize datasize.ByteSize
	err = cacheSize.UnmarshalText([]byte(cacheSizeStr))
	if err!=nil {
		return nil, nil,nil, nil, err
	}
	var batchSize datasize.ByteSize
	err = batchSize.UnmarshalText([]byte(batchSizeStr))
	if err!=nil {
		return nil, nil,nil, nil, err
	}

	st, err := stagedsync.New(
		stagedsync.DefaultStages(),
		stagedsync.DefaultUnwindOrder(),
		stagedsync.OptionalParameters{},
	).Prepare(nil, chainConfig, cc, bc.GetVMConfig(), db, tx, "integration_test", sm, path.Join(datadir, etl.TmpDirName),nil,  batchSize, quitCh, nil, nil, func() error { return nil }, false, nil)
	if err != nil {
		panic(err)
	}

	progress := func(stage stages.SyncStage) *stagedsync.StageState {
		if hasTx, ok := tx.(ethdb.HasTx); ok && hasTx.Tx() != nil {
			s, err := st.StageState(stage, tx)
			if err != nil {
				panic(err)
			}
			return s
		}
		s, err := st.StageState(stage, tx)
		if err != nil {
			panic(err)
		}
		return s
	}

	return cc, bc, st, progress, nil
}

func newBlockChain(db ethdb.Database, sm ethdb.StorageMode) (*params.ChainConfig, *core.BlockChain, error) {
	blockchain, err1 := core.NewBlockChain(db, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{
		NoReceipts: !sm.Receipts,
	}, nil, nil)
	if err1 != nil {
		return nil, nil, err1
	}
	return params.MainnetChainConfig, blockchain, nil
}

func SetSnapshotKV(db ethdb.Database, snapshotDir, snapshotMode string) error {
	if len(snapshotMode) > 0 && len(snapshotDir) > 0 {
		mode, err := snapshotsync.SnapshotModeFromString(snapshotMode)
		if err != nil {
			panic(err)
		}

		snapshotKV := db.(ethdb.HasKV).KV()
		snapshotKV, err = snapshotsync.WrapBySnapshotsFromDir(snapshotKV, snapshotDir, mode)
		if err != nil {
			return err
		}
		db.(ethdb.HasKV).SetKV(snapshotKV)
	}
	return nil
}
