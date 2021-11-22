package commands

import (
	"context"
	"errors"
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
	withDatadir(generateHeadersSnapshotCmd)
	withSnapshotFile(generateHeadersSnapshotCmd)
	withBlock(generateHeadersSnapshotCmd)

	rootCmd.AddCommand(generateHeadersSnapshotCmd)
}

var generateHeadersSnapshotCmd = &cobra.Command{
	Use:     "headers",
	Short:   "Generate headers snapshot",
	Example: "go run cmd/snapshots/generator/main.go headers --block 11000000 --datadir /media/b00ris/nvme/snapshotsync/ --snapshotDir /media/b00ris/nvme/snapshotsync/tg/snapshots/ --snapshotMode \"hb\" --snapshot /media/b00ris/nvme/snapshots/headers_test",
	RunE: func(cmd *cobra.Command, args []string) error {
		return HeaderSnapshot(cmd.Context(), log.New(), chaindata, snapshotFile, block, snapshotDir, snapshotMode)
	},
}

func HeaderSnapshot(ctx context.Context, logger log.Logger, dbPath, snapshotPath string, toBlock uint64, snapshotDir string, snapshotMode string) error {
	if snapshotPath == "" {
		return errors.New("empty snapshot path")
	}
	err := os.RemoveAll(snapshotPath)
	if err != nil {
		return err
	}
	db := kv2.NewMDBX(logger).Path(dbPath).MustOpen()

	snKV := kv2.NewMDBX(logger).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			kv.Headers: kv.TableCfgItem{},
		}
	}).Path(snapshotPath).MustOpen()

	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	snTx, err := snKV.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer snTx.Rollback()

	t := time.Now()
	var hash common.Hash
	var header []byte
	c, err := snTx.RwCursor(kv.Headers)
	if err != nil {
		return err
	}
	defer c.Close()
	for i := uint64(1); i <= toBlock; i++ {
		if common.IsCanceled(ctx) {
			return libcommon.ErrStopped
		}

		hash, err = rawdb.ReadCanonicalHash(tx, i)
		if err != nil {
			return fmt.Errorf("getting canonical hash for block %d: %w", i, err)
		}
		header = rawdb.ReadHeaderRLP(tx, hash, i)
		if len(header) == 0 {
			return fmt.Errorf("empty header: %v", i)
		}
		if err = c.Append(dbutils.HeaderKey(i, hash), header); err != nil {
			return err
		}
		if i%1000 == 0 {
			log.Info("Committed", "block", i)
		}
	}

	if err = snTx.Commit(); err != nil {
		return err
	}

	snKV.Close()

	err = os.Remove(snapshotPath + "/lock.mdb")
	if err != nil {
		log.Warn("Remove lock", "err", err)
		return err
	}
	log.Info("Finished", "duration", time.Since(t))

	return nil
}
