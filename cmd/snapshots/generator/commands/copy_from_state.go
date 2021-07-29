package commands

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
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
		return CopyFromState(cmd.Context(), log.New(), chaindata, snapshotFile, block, snapshotDir, snapshotMode)
	},
}

func CopyFromState(ctx context.Context, logger log.Logger, dbpath string, snapshotPath string, block uint64, snapshotDir, snapshotMode string) error {
	db, err := mdbx.Open(dbpath, logger, true)
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
	snkv := mdbx.NewMDBX(logger).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			kv.PlainState:        kv.ChaindataTablesCfg[kv.PlainState],
			kv.PlainContractCode: kv.ChaindataTablesCfg[kv.PlainContractCode],
			kv.Code:              kv.ChaindataTablesCfg[kv.Code],
		}
	}).Path(snapshotPath).MustOpen()
	log.Info("Create snapshot db", "path", snapshotPath)

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	tt := time.Now()
	if err = snkv.Update(ctx, func(snTx kv.RwTx) error {
		return tx.ForEach(kv.PlainState, []byte{}, func(k, v []byte) error {
			innerErr := snTx.Put(kv.PlainState, k, v)
			if innerErr != nil {
				return fmt.Errorf("put state err: %w", innerErr)
			}
			select {
			case <-logEvery.C:
				log.Info("progress", "bucket", kv.PlainState, "key", fmt.Sprintf("%x", k))
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
		return tx.ForEach(kv.Code, []byte{}, func(k, v []byte) error {
			innerErr := sntx.Put(kv.Code, k, v)
			if innerErr != nil {
				return fmt.Errorf("put code err: %w", innerErr)
			}
			select {
			case <-logEvery.C:
				log.Info("progress", "bucket", kv.Code, "key", fmt.Sprintf("%x", k))
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
	return VerifyStateSnapshot(ctx, logger, dbpath, snapshotPath, block)
}
