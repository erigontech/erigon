package commands

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/ethdb/mdbx"
	"github.com/ledgerwatch/erigon/ethdb/olddb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/spf13/cobra"
)

func init() {
	withDatadir(copyFromStateSnapshotCmd)
	withSnapshotFile(copyFromStateSnapshotCmd)
	withBlock(copyFromStateSnapshotCmd)
	rootCmd.AddCommand(copyFromStateSnapshotCmd)

}

//go run cmd/snapshots/generator/main.go state_copy --block 11000000 --snapshot /media/b00ris/nvme/snapshots/state --datadir /media/b00ris/nvme/backup/snapshotsync/ &> /media/b00ris/nvme/copy.log
var copyFromStateSnapshotCmd = &cobra.Command{
	Use:     "state_copy",
	Short:   "Copy from state snapshot",
	Example: "go run cmd/snapshots/generator/main.go state_copy --block 11000000 --snapshot /media/b00ris/nvme/snapshots/state --datadir /media/b00ris/nvme/backup/snapshotsync",
	RunE: func(cmd *cobra.Command, args []string) error {
		return CopyFromState(cmd.Context(), chaindata, snapshotFile, block, snapshotDir, snapshotMode)
	},
}

func CopyFromState(ctx context.Context, dbpath string, snapshotPath string, block uint64, snapshotDir, snapshotMode string) error {
	db, err := olddb.Open(dbpath, true)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = os.RemoveAll(snapshotPath)
	if err != nil {
		return err
	}
	snkv := mdbx.NewMDBX().WithBucketsConfig(func(defaultBuckets kv.BucketsCfg) kv.BucketsCfg {
		return kv.BucketsCfg{
			kv.PlainStateBucket:  kv.BucketsConfigs[kv.PlainStateBucket],
			kv.PlainContractCode: kv.BucketsConfigs[kv.PlainContractCode],
			kv.CodeBucket:        kv.BucketsConfigs[kv.CodeBucket],
		}
	}).Path(snapshotPath).MustOpen()
	log.Info("Create snapshot db", "path", snapshotPath)

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	tt := time.Now()
	if err = snkv.Update(ctx, func(snTx kv.RwTx) error {
		return tx.ForEach(kv.PlainStateBucket, []byte{}, func(k, v []byte) error {
			innerErr := snTx.Put(kv.PlainStateBucket, k, v)
			if innerErr != nil {
				return fmt.Errorf("put state err: %w", innerErr)
			}
			select {
			case <-logEvery.C:
				log.Info("progress", "bucket", kv.PlainStateBucket, "key", fmt.Sprintf("%x", k))
			default:
			}

			return nil
		})
	}); err != nil {
		return err
	}
	log.Info("Copy state", "batch", "t", time.Since(tt))

	log.Info("Copy plain state end", "t", time.Since(tt))
	tt = time.Now()
	if err = snkv.Update(ctx, func(sntx kv.RwTx) error {
		return tx.ForEach(kv.PlainContractCode, []byte{}, func(k, v []byte) error {
			innerErr := sntx.Put(kv.PlainContractCode, k, v)
			if innerErr != nil {
				return fmt.Errorf("put contract code err: %w", innerErr)
			}
			select {
			case <-logEvery.C:
				log.Info("progress", "bucket", kv.PlainContractCode, "key", fmt.Sprintf("%x", k))
			default:
			}
			return nil
		})
	}); err != nil {
		return err
	}
	log.Info("Copy contract code end", "t", time.Since(tt))

	tt = time.Now()
	if err = snkv.Update(ctx, func(sntx kv.RwTx) error {
		return tx.ForEach(kv.CodeBucket, []byte{}, func(k, v []byte) error {
			innerErr := sntx.Put(kv.CodeBucket, k, v)
			if innerErr != nil {
				return fmt.Errorf("put code err: %w", innerErr)
			}
			select {
			case <-logEvery.C:
				log.Info("progress", "bucket", kv.CodeBucket, "key", fmt.Sprintf("%x", k))
			default:
			}
			return nil
		})
	}); err != nil {
		return err
	}
	log.Info("Copy code", "t", time.Since(tt))

	db.Close()
	snkv.Close()
	tt = time.Now()
	defer func() {
		log.Info("Verify end", "t", time.Since(tt))
	}()
	return VerifyStateSnapshot(ctx, dbpath, snapshotPath, block)
}
