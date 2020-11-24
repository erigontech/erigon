package commands

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"
	"time"
)

func init() {
	withChaindata(verifyStateSnapshotCmd)
	withSnapshotFile(verifyStateSnapshotCmd)
	withBlock(verifyStateSnapshotCmd)

	rootCmd.AddCommand(verifyStateSnapshotCmd)
}

//go run cmd/snapshots/generator/main.go verify_state --block 11000000 --snapshot /media/b00ris/nvme/snapshotsync/tg/chaindata/ --chaindata /media/b00ris/nvme/backup/snapshotsync/tg/chaindata/ &> /media/b00ris/nvme/verify.log
var verifyStateSnapshotCmd = &cobra.Command{
	Use:     "verify_state",
	Short:   "Verify state snapshot",
	Example: "tbd",
	RunE: func(cmd *cobra.Command, args []string) error {
		return VerifyStateSnapshot(cmd.Context(), chaindata, snapshotFile, block)
	},
}

func VerifyStateSnapshot(ctx context.Context, dbPath, snapshotPath string, block uint64) error {
	db, err:= ethdb.Open(dbPath, true)
	if err!=nil {
		return fmt.Errorf("Open err: %w", err)
	}
	snapshotDir:= "/media/b00ris/nvme/snapshotsync/tg/snapshots"
	snapshotMode:="hb"
	kv:=db.KV()
	if snapshotDir != "" {
		var mode snapshotsync.SnapshotMode
		mode, err = snapshotsync.SnapshotModeFromString(snapshotMode)
		if err != nil {
			return err
		}
		kv, err = snapshotsync.WrapBySnapshots(kv, snapshotDir, mode)
		if err != nil {
			return err
		}
	}
	db.SetKV(kv)


	snkv := ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.PlainStateBucket:   dbutils.BucketsConfigs[dbutils.PlainStateBucket],//    dbutils.BucketConfigItem{},
			dbutils.PlainContractCodeBucket:   dbutils.BucketsConfigs[dbutils.PlainContractCodeBucket],//    dbutils.BucketConfigItem{},
			dbutils.CodeBucket:   dbutils.BucketsConfigs[dbutils.CodeBucket],//    dbutils.BucketConfigItem{},
			//dbutils.C:   dbutils.BucketsConfigs[dbutils.CodeBucket],//    dbutils.BucketConfigItem{},
			//dbutils.SnapshotInfoBucket: dbutils.BucketConfigItem{},
		}
	}).Path(snapshotPath).ReadOnly().MustOpen()
	tmpPath,err:=ioutil.TempDir(os.TempDir(),"vrf*")
	if err!=nil {
		return err
	}
	tmpDB:=ethdb.NewLMDB().Path(tmpPath).MustOpen()
	defer os.RemoveAll(tmpPath)
	defer tmpDB.Close()
	snkv=ethdb.NewSnapshotKV().SnapshotDB(snkv).DB(tmpDB).For(dbutils.PlainStateBucket).For(dbutils.PlainContractCodeBucket).For(dbutils.CodeBucket).MustOpen()
	sndb:=ethdb.NewObjectDatabase(snkv)
	tx,err:=sndb.Begin(context.Background(), ethdb.RW)
	if err!=nil {
		return err
	}
	defer tx.Rollback()
	hash, err := rawdb.ReadCanonicalHash(db, block)
	if err != nil {
		return err
	}

	syncHeadHeader := rawdb.ReadHeader(db, hash, block)
	if syncHeadHeader==nil {
		return fmt.Errorf("empty header")
	}
	expectedRootHash := syncHeadHeader.Root
	tt:=time.Now()
	err = stagedsync.PromoteHashedStateCleanly("",tx, os.TempDir(), ctx.Done())
	fmt.Println("Promote took", time.Since(tt))
	if err!=nil {
		return fmt.Errorf("Promote state err: %w",err)
	}

	err = stagedsync.RegenerateIntermediateHashes("", tx, os.TempDir(),expectedRootHash, ctx.Done())
	if err!=nil {
		return fmt.Errorf("RegenerateIntermediateHashes err: %w", err)
	}
	return nil
}