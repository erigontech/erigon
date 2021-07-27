package commands

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/ethdb/mdbxdb"
	"github.com/ledgerwatch/erigon/ethdb/snapshotdb"
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
	var snkv, tmpDB kv.RwDB
	tmpPath, err := ioutil.TempDir(os.TempDir(), "vrf*")
	if err != nil {
		return err
	}

	snkv = mdbx.NewMDBX().WithBucketsConfig(func(defaultBuckets kv.BucketsCfg) kv.BucketsCfg {
		return kv.BucketsCfg{
			kv.PlainStateBucket:        kv.BucketsConfigs[kv.PlainStateBucket],
			kv.PlainContractCodeBucket: kv.BucketsConfigs[kv.PlainContractCodeBucket],
			kv.CodeBucket:              kv.BucketsConfigs[kv.CodeBucket],
		}
	}).Path(snapshotPath).Readonly().MustOpen()
	tmpDB = mdbx.NewMDBX().Path(tmpPath).MustOpen()

	defer os.RemoveAll(tmpPath)
	defer tmpDB.Close()
	snkv = snapshotdb.NewSnapshotKV().StateSnapshot(snkv).DB(tmpDB).Open()
	tx, err := snkv.BeginRw(context.Background())
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
	err = stagedsync.PromoteHashedStateCleanly("", tx, stagedsync.StageHashStateCfg(snkv, os.TempDir()), ctx.Done())
	fmt.Println("Promote took", time.Since(tt))
	if err != nil {
		return fmt.Errorf("promote state err: %w", err)
	}

	_, err = stagedsync.RegenerateIntermediateHashes("", tx, stagedsync.StageTrieCfg(snkv, true, true, os.TempDir()), expectedRootHash, ctx.Done())
	if err != nil {
		return fmt.Errorf("regenerateIntermediateHashes err: %w", err)
	}
	return nil
}
