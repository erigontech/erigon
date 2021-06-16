package commands

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
)

func init() {
	withDatadir(generateBodiesSnapshotCmd)
	withSnapshotFile(generateBodiesSnapshotCmd)
	withBlock(generateBodiesSnapshotCmd)
	rootCmd.AddCommand(generateBodiesSnapshotCmd)

}

var generateBodiesSnapshotCmd = &cobra.Command{
	Use:     "bodies",
	Short:   "Generate bodies snapshot",
	Example: "go run cmd/snapshots/generator/main.go bodies --block 11000000 --datadir /media/b00ris/nvme/snapshotsync/ --snapshotDir /media/b00ris/nvme/snapshotsync/tg/snapshots/ --snapshotMode \"hb\" --snapshot /media/b00ris/nvme/snapshots/bodies_test",
	RunE: func(cmd *cobra.Command, args []string) error {
		return BodySnapshot(cmd.Context(), chaindata, snapshotFile, block, snapshotDir, snapshotMode)
	},
}

func BodySnapshot(ctx context.Context, dbPath, snapshotPath string, toBlock uint64, snapshotDir string, snapshotMode string) error {
	kv := ethdb.NewMDBX().Path(dbPath).MustOpen()
	snKV := ethdb.NewMDBX().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.BlockBodyPrefix:          dbutils.BucketConfigItem{},
			dbutils.BodiesSnapshotInfoBucket: dbutils.BucketConfigItem{},
		}
	}).Path(snapshotPath).MustOpen()

	tx, err := kv.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	t := time.Now()
	var hash common.Hash
	if err := snKV.Update(ctx, func(sntx ethdb.RwTx) error {
		for i := uint64(1); i <= toBlock; i++ {
			if common.IsCanceled(ctx) {
				return common.ErrStopped
			}

			hash, err = rawdb.ReadCanonicalHash(tx, i)
			if err != nil {
				return fmt.Errorf("getting canonical hash for block %d: %v", i, err)
			}
			body := rawdb.ReadBodyRLP(tx, hash, i)
			if err = sntx.Put(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(i, hash), body); err != nil {
				return err
			}
			select {
			case <-logEvery.C:
				log.Info("progress", "bucket", dbutils.BlockBodyPrefix, "block num", i)
			default:
			}
		}

		err = sntx.Put(dbutils.BodiesSnapshotInfoBucket, []byte(dbutils.SnapshotBodyHeadNumber), big.NewInt(0).SetUint64(toBlock).Bytes())
		if err != nil {
			log.Crit("SnapshotBodyHeadNumber error", "err", err)
			return err
		}
		err = sntx.Put(dbutils.BodiesSnapshotInfoBucket, []byte(dbutils.SnapshotBodyHeadHash), hash.Bytes())
		if err != nil {
			log.Crit("SnapshotBodyHeadHash error", "err", err)
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	snKV.Close()
	err = os.Remove(snapshotPath + "/mdbx.lck")
	if err != nil {
		log.Warn("Remove lock", "err", err)
		return err
	}

	log.Info("Finished", "duration", time.Since(t))
	return nil
}
