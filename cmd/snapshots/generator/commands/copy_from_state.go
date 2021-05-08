package commands

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
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
	db, err := ethdb.Open(dbpath, true)
	if err != nil {
		return err
	}

	err = os.RemoveAll(snapshotPath)
	if err != nil {
		return err
	}
	snkv := ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.PlainStateBucket:        dbutils.BucketsConfigs[dbutils.PlainStateBucket],
			dbutils.PlainContractCodeBucket: dbutils.BucketsConfigs[dbutils.PlainContractCodeBucket],
			dbutils.CodeBucket:              dbutils.BucketsConfigs[dbutils.CodeBucket],
		}
	}).Path(snapshotPath).MustOpen()
	log.Info("Create snapshot db", "path", snapshotPath)

	sndb := ethdb.NewObjectDatabase(snkv).NewBatch()

	tt := time.Now()
	tt2 := time.Now()
	max := 10000000
	i := 0
	err = db.Walk(dbutils.PlainStateBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		innerErr := sndb.Put(dbutils.PlainStateBucket, k, v)
		if innerErr != nil {
			return false, fmt.Errorf("put state err: %w", innerErr)
		}
		i++
		if i > max {
			i = 0
			innerErr = sndb.CommitAndBegin(ctx)
			if innerErr != nil {
				return false, fmt.Errorf("commit state err: %w", innerErr)
			}
			log.Info("Commit state", "batch", time.Since(tt2), "all", time.Since(tt))
			tt2 = time.Now()
		}

		return true, nil
	})
	if err != nil {
		return err
	}
	err = sndb.CommitAndBegin(ctx)
	if err != nil {
		return err
	}

	log.Info("Copy plain state end", "t", time.Since(tt))
	tt = time.Now()
	tt2 = time.Now()
	err = db.Walk(dbutils.PlainContractCodeBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		innerErr := sndb.Put(dbutils.PlainContractCodeBucket, k, v)
		if innerErr != nil {
			return false, fmt.Errorf("put contract code err: %w", innerErr)
		}
		i++
		if i > max {
			i = 0
			innerErr = sndb.CommitAndBegin(ctx)
			if innerErr != nil {
				return false, fmt.Errorf("commit contract code err: %w", innerErr)
			}
			log.Info("Commit contract code", "batch", time.Since(tt2), "all", time.Since(tt))
			tt2 = time.Now()
		}

		return true, nil
	})
	if err != nil {
		return err
	}
	log.Info("Copy contract code end", "t", time.Since(tt))
	err = sndb.Commit()
	if err != nil {
		return err
	}

	tt = time.Now()
	tt2 = time.Now()
	err = db.Walk(dbutils.CodeBucket, []byte{}, 0, func(k, v []byte) (bool, error) {
		innerErr := sndb.Put(dbutils.CodeBucket, k, v)
		if innerErr != nil {
			return false, fmt.Errorf("put code err: %w", innerErr)
		}
		i++
		if i > max {
			i = 0
			innerErr = sndb.CommitAndBegin(ctx)
			if innerErr != nil {
				return false, fmt.Errorf("commit code err: %w", innerErr)
			}
			log.Info("Commit code", "batch", time.Since(tt2), "all", time.Since(tt))
			tt2 = time.Now()
		}

		return true, nil
	})
	if err != nil {
		return err
	}
	log.Info("Copy code end", "t", time.Since(tt))
	err = sndb.Commit()
	if err != nil {
		return err
	}
	sndb.Close()
	db.Close()
	tt = time.Now()
	defer func() {
		log.Info("Verify end", "t", time.Since(tt))
	}()
	return VerifyStateSnapshot(ctx, dbpath, snapshotPath, block)
}
