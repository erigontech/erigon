package commands

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"github.com/spf13/cobra"
	"io/ioutil"
	"path/filepath"
	"time"
)

func init() {
	withChaindata(cmdSnapshotCheck)
	withBlock(cmdSnapshotCheck)
	cmdSnapshotCheck.Flags().StringVar(&tmpDBPath, "tmd_db", "", "path to temporary db(for debug)")
	rootCmd.AddCommand(cmdSnapshotCheck)
}

var tmpDBPath string
///media/b00ris/nvme/tmp/sndbg351954303
var cmdSnapshotCheck = &cobra.Command{
	Use:   "snapshot_check",
	Short: "check execution over state snapshot by block",
	RunE: func(cmd *cobra.Command, args []string) error {
		borisTmpDir:="/media/b00ris/nvme/tmp"
		osTmpDir:= func() string{
			//return os.TempDir()
			return borisTmpDir
		}
		ctx := utils.RootContext()
		mainDB, err:=ethdb.Open(chaindata, true)
		if err!= nil {
			return err
		}
		mode, err := snapshotsync.SnapshotModeFromString(snapshotMode)
		if err != nil {
			panic(err)
		}

		if !mode.State || len(snapshotDir)==0 {
			return fmt.Errorf("you need state snapshot for it")
		}

		stateSnapshotPath:=filepath.Join(snapshotDir, "state")
		stateSnapshot:=ethdb.NewLMDB().Path(stateSnapshotPath).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return dbutils.BucketsCfg{
				dbutils.PlainStateBucket:   dbutils.BucketsConfigs[dbutils.PlainStateBucket],
				dbutils.PlainContractCodeBucket:   dbutils.BucketsConfigs[dbutils.PlainContractCodeBucket],
				dbutils.CodeBucket:   dbutils.BucketsConfigs[dbutils.CodeBucket],
			}
		}).ReadOnly().MustOpen()

		isNew:=true
		var path string
		if len(tmpDBPath) > 0 {
			isNew=false
			path = tmpDBPath
		} else {
			path,err=ioutil.TempDir(osTmpDir(), "sndbg")
			if err!= nil {
				return err
			}
		}
		log.Info("TmpDbPath", "path", path)
		defer func() {
			fmt.Println("tmp db path", path)
		}()
		tmpDb :=ethdb.NewLMDB().Path(path).MustOpen()

		kv:=ethdb.NewSnapshot2KV().
			DB(tmpDb).
			SnapshotDB([]string{dbutils.HeaderPrefix, dbutils.BlockBodyPrefix, dbutils.Senders}, mainDB.KV()).
			SnapshotDB([]string{dbutils.PlainStateBucket, dbutils.CodeBucket, dbutils.PlainContractCodeBucket}, stateSnapshot).
			MustOpen()


		db:=ethdb.NewObjectDatabase(kv)
		if isNew {
			err = ethdb.SetStorageModeIfNotExist(db, ethdb.StorageMode{})
			if err!=nil {
				return err
			}
		}

		if err := snapshotCheck(ctx, db, isNew, osTmpDir()); err != nil {
			log.Error("snapshotCheck error", "err", err)
			return err
		}
		return nil
	},
}

func snapshotCheck(ctx context.Context, db ethdb.Database, isNew bool, tmpDir string) (err error) {
	var snapshotBlock uint64 = 11_000_000
	blockNum, _,err:= stages.GetStageProgress(db, stages.Execution)
	if err!=nil {
		return err
	}
	if blockNum > snapshotBlock {
		snapshotBlock=blockNum
	}

	var lastBlockHeaderNumber uint64
	if block == 0 {
		lastBlockHash := rawdb.ReadHeadBlockHash(db)
		lastBlockHeader, err:=rawdb.ReadHeaderByHash(db, lastBlockHash)
		if err!=nil {
			return err
		}
		lastBlockHeaderNumber=lastBlockHeader.Number.Uint64()
	} else {
		lastBlockHeaderNumber = block
	}

	if lastBlockHeaderNumber <= snapshotBlock {
		return fmt.Errorf("incorrect header number last block:%v, snapshotBlock: %v", lastBlockHeaderNumber, snapshotBlock)
	}

	if isNew {
		log.Info("New tmp db. We need to promote hash state.")
		tx,err:=db.Begin(context.Background(), ethdb.RW)
		if err!=nil {
			return err
		}
		defer tx.Rollback()

		hash, err := rawdb.ReadCanonicalHash(db, snapshotBlock)
		if err != nil {
			return err
		}

		syncHeadHeader := rawdb.ReadHeader(db, hash, snapshotBlock)
		if syncHeadHeader==nil {
			return fmt.Errorf("empty header for %v", snapshotBlock)
		}
		expectedRootHash := syncHeadHeader.Root
		tt:=time.Now()
		err = stagedsync.PromoteHashedStateCleanly("",db, tmpDir, ctx.Done())
		log.Info("Promote took", "t", time.Since(tt))
		if err!=nil {
			return fmt.Errorf("Promote state err: %w",err)
		}
		tt= time.Now()
		err = stagedsync.RegenerateIntermediateHashes("", db, tmpDir,expectedRootHash, ctx.Done())
		if err!=nil {
			return fmt.Errorf("RegenerateIntermediateHashes err: %w", err)
		}
		log.Info("RegenerateIntermediateHashes took", "t", time.Since(tt))
		_, err = tx.Commit()
		if err!=nil {
			return err
		}
	}

	for blockNumber :=snapshotBlock; blockNumber <= lastBlockHeaderNumber; blockNumber++ {
		tx,err:=db.Begin(context.Background(), ethdb.RW)
		if err!=nil {
			return err
		}
		defer tx.Rollback()

		if err := stageExec(db, ctx); err != nil {
			log.Error("Error on execution", "err", err, "block", blockNumber)
			return err
		}
		if err := stageIHash(db, ctx); err != nil {
			log.Error("Error on ih", "err", err, "block", blockNumber)
			return err
		}
		_,err = tx.Commit()
		if err!=nil {
			log.Error("Error on commit", "err", err, "block", blockNumber)
			return err
		}
	}

	return nil
}