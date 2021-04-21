package commands

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"github.com/spf13/cobra"
)

func init() {
	withDatadir(verifyStateSnapshotCmd)
	withSnapshotFile(verifyStateSnapshotCmd)
	withBlock(verifyStateSnapshotCmd)

	rootCmd.AddCommand(verifyStateSnapshotCmd)
}

//
var verifyStateSnapshotCmd = &cobra.Command{
	Use:     "verify_state",
	Short:   "Verify state snapshot",
	Example: "go run cmd/snapshots/generator/main.go verify_state --block 11000000 --snapshot /media/b00ris/nvme/snapshots/state/ --datadir /media/b00ris/nvme/backup/snapshotsync/",
	RunE: func(cmd *cobra.Command, args []string) error {
		return VerifyStateSnapshot(cmd.Context(), chaindata, snapshotFile, block)
	},
}

func VerifyStateSnapshot(ctx context.Context, dbPath, snapshotPath string, block uint64) error {
	db, err := ethdb.Open(dbPath, true)
	if err != nil {
		return fmt.Errorf("open err: %w", err)
	}

	kv := db.RwKV()
	if snapshotDir != "" {
		var mode snapshotsync.SnapshotMode
		mode, err = snapshotsync.SnapshotModeFromString(snapshotMode)
		if err != nil {
			return err
		}
		kv, err = snapshotsync.WrapBySnapshotsFromDir(kv, snapshotDir, mode)
		if err != nil {
			return err
		}
	}
	db.SetRwKV(kv)

	snkv := ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.PlainStateBucket:        dbutils.BucketsConfigs[dbutils.PlainStateBucket],
			dbutils.PlainContractCodeBucket: dbutils.BucketsConfigs[dbutils.PlainContractCodeBucket],
			dbutils.CodeBucket:              dbutils.BucketsConfigs[dbutils.CodeBucket],
		}
	}).Path(snapshotPath).Flags(func(flags uint) uint { return flags | lmdb.Readonly }).MustOpen()

	tmpPath, err := ioutil.TempDir(os.TempDir(), "vrf*")
	if err != nil {
		return err
	}
	tmpDB := ethdb.NewLMDB().Path(tmpPath).MustOpen()
	defer os.RemoveAll(tmpPath)
	defer tmpDB.Close()
	snkv = ethdb.NewSnapshot2KV().SnapshotDB([]string{dbutils.PlainStateBucket, dbutils.PlainContractCodeBucket, dbutils.CodeBucket}, snkv).DB(tmpDB).MustOpen()
	sndb := ethdb.NewObjectDatabase(snkv)
	tx, err := sndb.Begin(context.Background(), ethdb.RW)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	hash, err := rawdb.ReadCanonicalHash(tx, block)
	if err != nil {
		return err
	}

	syncHeadHeader := rawdb.ReadHeader(tx, hash, block)
	if syncHeadHeader == nil {
		return fmt.Errorf("empty header")
	}
	expectedRootHash := syncHeadHeader.Root
	tt := time.Now()
	err = stagedsync.PromoteHashedStateCleanly("", tx.(ethdb.HasTx).Tx().(ethdb.RwTx), os.TempDir(), ctx.Done())
	fmt.Println("Promote took", time.Since(tt))
	if err != nil {
		return fmt.Errorf("promote state err: %w", err)
	}

	_, err = stagedsync.RegenerateIntermediateHashes("", tx.(ethdb.HasTx).Tx().(ethdb.RwTx), true, os.TempDir(), expectedRootHash, ctx.Done())
	if err != nil {
		return fmt.Errorf("regenerateIntermediateHashes err: %w", err)
	}
	return nil
}
