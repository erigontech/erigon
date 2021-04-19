package commands

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
)

func init() {
	withDatadir(generateBodiesSnapshotCmd)
	withSnapshotFile(generateBodiesSnapshotCmd)
	withSnapshotData(generateBodiesSnapshotCmd)
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
	kv := ethdb.NewLMDB().Path(dbPath).MustOpen()
	var err error
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

	snKV := ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.BlockBodyPrefix:          dbutils.BucketConfigItem{},
			dbutils.BodiesSnapshotInfoBucket: dbutils.BucketConfigItem{},
		}
	}).Path(snapshotPath).MustOpen()

	db := ethdb.NewObjectDatabase(kv)
	snDB := ethdb.NewObjectDatabase(snKV)

	t := time.Now()
	chunkFile := 30000
	tuples := make(ethdb.MultiPutTuples, 0, chunkFile*3+100)
	var hash common.Hash

	for i := uint64(1); i <= toBlock; i++ {
		if common.IsCanceled(ctx) {
			return common.ErrStopped
		}

		hash, err = rawdb.ReadCanonicalHash(db, i)
		if err != nil {
			return fmt.Errorf("getting canonical hash for block %d: %v", i, err)
		}
		body := rawdb.ReadBodyRLP(db, hash, i)
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
	err = os.Remove(snapshotPath + "/lock.mdb")
	if err != nil {
		log.Warn("Remove lock", "err", err)
		return err
	}

	log.Info("Finished", "duration", time.Since(t))
	return nil
}
