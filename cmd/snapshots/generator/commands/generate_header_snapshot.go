package commands

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"math/big"
	"os"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
)

func init() {
	withChaindata(generateHeadersSnapshotCmd)
	withSnapshotFile(generateHeadersSnapshotCmd)
	withSnapshotData(generateHeadersSnapshotCmd)
	withBlock(generateHeadersSnapshotCmd)

	rootCmd.AddCommand(generateHeadersSnapshotCmd)
}

var generateHeadersSnapshotCmd = &cobra.Command{
	Use:       "headers",
	Short:     "Generate headers snapshot",
	Example:   "go run cmd/snapshots/generator/main.go headers --block 11000000 --chaindata /media/b00ris/nvme/snapshotsync/tg/chaindata/ --snapshotDir /media/b00ris/nvme/snapshotsync/tg/snapshots/ --snapshotMode \"hb\" --snapshot /media/b00ris/nvme/snapshots/headers_test",
	ValidArgs: []string{"chaindata", "snapshotdir", "block"},
	//ArgAliases : []string{"chaindata", "snapshotdir", "block"},
	Args: cobra.OnlyValidArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return HeaderSnapshot(cmd.Context(), chaindata, snapshotFile, block, snapshotDir, snapshotMode)
	},
}

func HeaderSnapshot(ctx context.Context, dbPath, snapshotPath string, toBlock uint64, snapshotDir string, snapshotMode string) error {
	if snapshotPath == "" {
		return errors.New("empty snapshot path")
	}
	err := os.RemoveAll(snapshotPath)
	if err != nil {
		return err
	}
	kv := ethdb.NewLMDB().Path(dbPath).MustOpen()

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
	snkv := ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix:       dbutils.BucketConfigItem{},
			dbutils.SnapshotInfoBucket: dbutils.BucketConfigItem{},
		}
	}).Path(snapshotPath).MustOpen()

	db := ethdb.NewObjectDatabase(kv)
	sndb := ethdb.NewObjectDatabase(snkv)

	t := time.Now()
	chunkFile := 30000
	tuples := make(ethdb.MultiPutTuples, 0, chunkFile*3)
	var hash common.Hash
	var header []byte
	for i := uint64(1); i <= toBlock; i++ {
		select {
		case <-ctx.Done():
			return errors.New("interrupted")
		default:

		}
		hash, err = rawdb.ReadCanonicalHash(db, i)
		if err != nil {
			return fmt.Errorf("getting canonical hash for block %d: %v", i, err)
		}
		header = rawdb.ReadHeaderRLP(db, hash, i)
		if len(header) == 0 {
			return fmt.Errorf("empty header: %v", i)
		}
		tuples = append(tuples, []byte(dbutils.HeaderPrefix), dbutils.HeaderKey(i, hash), header)
		if len(tuples) >= chunkFile {
			log.Info("Committed", "block", i)
			_, err = sndb.MultiPut(tuples...)
			if err != nil {
				log.Crit("Multiput error", "err", err)
				return err
			}
			tuples = tuples[:0]
		}
	}

	if len(tuples) > 0 {
		_, err = sndb.MultiPut(tuples...)
		if err != nil {
			log.Crit("Multiput error", "err", err)
			return err
		}
	}

	err = sndb.Put(dbutils.SnapshotInfoBucket, []byte(dbutils.SnapshotHeadersHeadNumber), big.NewInt(0).SetUint64(toBlock).Bytes())
	if err != nil {
		log.Crit("SnapshotHeadersHeadNumber error", "err", err)
		return err
	}
	err = sndb.Put(dbutils.SnapshotInfoBucket, []byte(dbutils.SnapshotHeadersHeadHash), hash.Bytes())
	if err != nil {
		log.Crit("SnapshotHeadersHeadHash error", "err", err)
		return err
	}

	log.Info("Finished", "duration", time.Since(t))
	sndb.Close()
	err = os.Remove(snapshotPath + "/lock.mdb")
	if err != nil {
		log.Warn("Remove lock", "err", err)
		return err
	}

	return nil
}
