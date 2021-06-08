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
		return BodySnapshot(cmd.Context(), chaindata, snapshotFile, block, snapshotDir, snapshotMode, database)
	},
}

func BodySnapshot(ctx context.Context, dbPath, snapshotPath string, toBlock uint64, snapshotDir string, snapshotMode string, database string) error {
	var kv, snKV ethdb.RwKV
	var err error
	if database == "lmdb" {
		kv = ethdb.NewLMDB().Path(dbPath).MustOpen()
		snKV = ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return dbutils.BucketsCfg{
				dbutils.BlockBodyPrefix:          dbutils.BucketConfigItem{},
				dbutils.BodiesSnapshotInfoBucket: dbutils.BucketConfigItem{},
			}
		}).Path(snapshotPath).MustOpen()
	} else {
		kv = ethdb.NewMDBX().Path(dbPath).MustOpen()
		snKV = ethdb.NewMDBX().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return dbutils.BucketsCfg{
				dbutils.BlockBodyPrefix:          dbutils.BucketConfigItem{},
				dbutils.BodiesSnapshotInfoBucket: dbutils.BucketConfigItem{},
			}
		}).Path(snapshotPath).MustOpen()
	}

	snDB := ethdb.NewObjectDatabase(snKV)
	tx, err := kv.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	t := time.Now()
	chunkFile := 30000
	tuples := make(ethdb.MultiPutTuples, 0, chunkFile*3+100)
	var hash common.Hash

	for i := uint64(1); i <= toBlock; i++ {
		if common.IsCanceled(ctx) {
			return common.ErrStopped
		}

		hash, err = rawdb.ReadCanonicalHash(tx, i)
		if err != nil {
			return fmt.Errorf("getting canonical hash for block %d: %v", i, err)
		}
		body := rawdb.ReadBodyRLP(tx, hash, i)
		tuples = append(tuples, []byte(dbutils.BlockBodyPrefix), dbutils.BlockBodyKey(i, hash), body)
		if len(tuples) >= chunkFile {
			log.Info("Committed", "block", i)
			if _, err = snDB.MultiPut(tuples...); err != nil {
				log.Crit("Multiput error", "err", err)
				return err
			}
			tuples = tuples[:0]
		}
	}

	if len(tuples) > 0 {
		if _, err = snDB.MultiPut(tuples...); err != nil {
			log.Crit("Multiput error", "err", err)
			return err
		}
	}

	err = snDB.Put(dbutils.BodiesSnapshotInfoBucket, []byte(dbutils.SnapshotBodyHeadNumber), big.NewInt(0).SetUint64(toBlock).Bytes())
	if err != nil {
		log.Crit("SnapshotBodyHeadNumber error", "err", err)
		return err
	}
	err = snDB.Put(dbutils.BodiesSnapshotInfoBucket, []byte(dbutils.SnapshotBodyHeadHash), hash.Bytes())
	if err != nil {
		log.Crit("SnapshotBodyHeadHash error", "err", err)
		return err
	}
	snDB.Close()
	if database == "lmdb" {
		err = os.Remove(snapshotPath + "/lock.mdb")
	} else {
		err = os.Remove(snapshotPath + "/mdbx.lck")
	}
	if err != nil {
		log.Warn("Remove lock", "err", err)
		return err
	}

	log.Info("Finished", "duration", time.Since(t))
	return nil
}
