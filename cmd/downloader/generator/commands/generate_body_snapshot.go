package commands

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/spf13/cobra"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/log/v3"
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
		return BodySnapshot(cmd.Context(), log.New(), chaindata, snapshotFile, block, snapshotDir, snapshotMode)
	},
}

func BodySnapshot(ctx context.Context, logger log.Logger, dbPath, snapshotPath string, toBlock uint64, snapshotDir string, snapshotMode string) error {
	db := kv2.NewMDBX(logger).Path(dbPath).MustOpen()
	snKV := kv2.NewMDBX(logger).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			kv.BlockBody: kv.TableCfgItem{},
		}
	}).Path(snapshotPath).MustOpen()

	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	t := time.Now()
	var hash common.Hash
	if err := snKV.Update(ctx, func(sntx kv.RwTx) error {
		for i := uint64(1); i <= toBlock; i++ {
			if common.IsCanceled(ctx) {
				return libcommon.ErrStopped
			}

			hash, err = rawdb.ReadCanonicalHash(tx, i)
			if err != nil {
				return fmt.Errorf("getting canonical hash for block %d: %w", i, err)
			}
			body := rawdb.ReadBodyRLP(tx, hash, i)
			if err = sntx.Put(kv.BlockBody, dbutils.BlockBodyKey(i, hash), body); err != nil {
				return err
			}
			select {
			case <-logEvery.C:
				log.Info("progress", "bucket", kv.BlockBody, "block num", i)
			default:
			}
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
