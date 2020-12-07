package commands

import (
	"context"
	"fmt"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
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
	cmdSnapshotCheck.Flags().StringVar(&tmpDBPath, "tmp_db", "", "path to temporary db(for debug)")
	rootCmd.AddCommand(cmdSnapshotCheck)
}

var tmpDBPath string
//go run cmd/integration/main.go snapshot_check --block 11000002 --chaindata /media/b00ris/nvme/backup/snapshotsync/tg/chaindata/ --snapshotDir /media/b00ris/nvme/snapshots/ --snapshotMode s


//tmp db path /media/b00ris/nvme/tmp/sndbg361939096
/**
k = [91,181,142,163,243,235,238,244,207,200,157,89,244,152,99,31,229,13,63,145]
v = [105,116,32,105,115,32,100,101,108,101,,116,101,100,32,118,97,108,117,101]


media/b00ris/nvme/snapshots/ --snapshotMode s --pprof
INFO [12-04|17:51:11.549] Starting pprof server                    cpu="go tool pprof -lines -http=: http://127.0.0.1:6060/debug/pprof/profile?seconds=20" heap="go tool pprof -lines -http=: http://127.0.0.1:6060/debug/pprof/heap"

 */
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
			SnapshotDB([]string{dbutils.HeaderPrefix, dbutils.BlockBodyPrefix, dbutils.Senders, dbutils.HeadBlockKey, dbutils.HeaderNumberPrefix}, mainDB.KV()).
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
		tx, err := db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}

		tt := time.Now()
		err = stagedsync.PromoteHashedStateCleanly("", tx, tmpDir, ctx.Done())
		log.Info("Promote took", "t", time.Since(tt))
		if err != nil {
			return fmt.Errorf("Promote state err: %w", err)
		}
		tt = time.Now()
		_, err = tx.Commit()
		if err != nil {
			return fmt.Errorf("Commit promote state err: %w", err)
		}
		log.Info("Promote commited", "t", time.Since(tt))
	}

	if isNew {
		log.Info("Regenerate IH")
		tx,err:=db.Begin(context.Background(), ethdb.RW)
		if err!=nil {
			return err
		}
		defer tx.Rollback()

		hash, err := rawdb.ReadCanonicalHash(tx, snapshotBlock)
		if err != nil {
			return err
		}

		syncHeadHeader := rawdb.ReadHeader(tx, hash, snapshotBlock)
		if syncHeadHeader==nil {
			return fmt.Errorf("empty header for %v", snapshotBlock)
		}
		expectedRootHash := syncHeadHeader.Root

		tt:= time.Now()
		core.UsePlainStateExecution = true
		err = stagedsync.RegenerateIntermediateHashes("", tx, tmpDir,expectedRootHash, ctx.Done())
		if err!=nil {
			return fmt.Errorf("RegenerateIntermediateHashes err: %w", err)
		}
		log.Info("RegenerateIntermediateHashes took", "t", time.Since(tt))
		tt = time.Now()
		_, err = tx.Commit()
		if err!=nil {
			return err
		}
		log.Info("Commit", "t", time.Since(tt))
	}

	cc, bc, st, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()
	st.DisableStages(stages.Headers,
		stages.BlockHashes,
		stages.Bodies,
		stages.Senders,
		//stages.Execution,
		//stages.IntermediateHashes,
		//stages.HashState,
		stages.AccountHistoryIndex,
		stages.StorageHistoryIndex,
		stages.LogIndex,
		stages.CallTraces,
		stages.TxLookup,
		stages.TxPool,
		stages.Finish,
	)

	if true {

		stage3 := progress(stages.Senders)
		err = stage3.DoneAndUpdate(db, lastBlockHeaderNumber)
		if err!=nil {
			return err
		}

		stage4 := progress(stages.Execution)
		err = stage4.DoneAndUpdate(db, snapshotBlock)
		if err!=nil {
			return err
		}
		stage5 := progress(stages.HashState)
		err = stage5.DoneAndUpdate(db, snapshotBlock)
		if err!=nil {
			return err
		}

		stage6 := progress(stages.IntermediateHashes)
		err = stage6.DoneAndUpdate(db, snapshotBlock)
		if err!=nil {
			return err
		}
	}

	log.Info("Stage3", "progress", progress(stages.Senders).BlockNumber)
	log.Info("Stage4", "progress", progress(stages.Execution).BlockNumber)
	log.Info("Stage5", "progress", progress(stages.IntermediateHashes).BlockNumber)


	ch := ctx.Done()

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))
	core.UsePlainStateExecution = true

	tx,err:=db.Begin(context.Background(), ethdb.RW)
	if err!=nil {
		return err
	}
	defer tx.Rollback()

	for blockNumber :=snapshotBlock+1; blockNumber <= lastBlockHeaderNumber; blockNumber++ {
		err = st.SetCurrentStage(stages.Execution)
		if err!=nil {
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
				BatchSize:     int(batchSize),
			})
		if err!=nil {
			return fmt.Errorf("Execution err %w", err)
		}

		stage5 := progress(stages.HashState)
		stage5.BlockNumber =  blockNumber - 1
		log.Info("Stage5", "progress", stage5.BlockNumber)
		err = stagedsync.SpawnHashStateStage(stage5, tx, tmpDir, ch)
		if err!=nil {
			return fmt.Errorf("SpawnHashStateStage err %w", err)
		}

		stage6 := progress(stages.IntermediateHashes)
		stage6.BlockNumber =  blockNumber - 1
		log.Info("Stage6", "progress", stage6.BlockNumber)
		if err := stagedsync.SpawnIntermediateHashesStage(stage5, tx, tmpDir, ch); err != nil {
			log.Error("Error on ih", "err", err, "block", blockNumber)
			return fmt.Errorf("SpawnIntermediateHashesStage %w", err)
		}

		log.Info("Done", "progress", blockNumber)
		err = tx.CommitAndBegin(context.TODO())
		if err!=nil {
			log.Error("Error on commit", "err", err, "block", blockNumber)
			return err
		}


	}
	time.Sleep(time.Second*30)

	return nil
}