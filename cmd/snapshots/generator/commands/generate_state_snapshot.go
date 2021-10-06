package commands

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func init() {
	withDatadir(generateStateSnapshotCmd)
	withSnapshotFile(generateStateSnapshotCmd)
	withBlock(generateStateSnapshotCmd)
	rootCmd.AddCommand(generateStateSnapshotCmd)

}

//go run cmd/snapshots/generator/main.go state_copy --block 11000000 --snapshot /media/b00ris/nvme/snapshots/state --datadir /media/b00ris/nvme/backup/snapshotsync &> /media/b00ris/nvme/copy.log
var generateStateSnapshotCmd = &cobra.Command{
	Use:     "state",
	Short:   "Generate state snapshot",
	Example: "go run ./cmd/state/main.go stateSnapshot --block 11000000 --datadir /media/b00ris/nvme/tgstaged/ --snapshot /media/b00ris/nvme/snapshots/state",
	RunE: func(cmd *cobra.Command, args []string) error {
		return GenerateStateSnapshot(cmd.Context(), log.New(), chaindata, snapshotFile, block, snapshotDir, snapshotMode)
	},
}

func GenerateStateSnapshot(ctx context.Context, logger log.Logger, dbPath, snapshotPath string, toBlock uint64, snapshotDir string, snapshotMode string) error {
	if snapshotPath == "" {
		return errors.New("empty snapshot path")
	}

	err := os.RemoveAll(snapshotPath)
	if err != nil {
		return err
	}
	var db, snkv kv.RwDB

	db = kv2.NewMDBX(logger).Path(dbPath).MustOpen()
	snkv = kv2.NewMDBX(logger).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			kv.PlainState:        kv.TableCfgItem{},
			kv.PlainContractCode: kv.TableCfgItem{},
			kv.Code:              kv.TableCfgItem{},
		}
	}).Path(snapshotPath).MustOpen()

	writeTx, err := snkv.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer writeTx.Rollback()

	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	tx2, err := db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	i := 0
	t := time.Now()
	tt := time.Now()
	commitEvery := time.NewTicker(30 * time.Second)
	defer commitEvery.Stop()

	err = state.WalkAsOfAccounts(tx, common.Address{}, toBlock+1, func(k []byte, v []byte) (bool, error) {
		i++
		if i%100000 == 0 {
			fmt.Println(i, common.Bytes2Hex(k), "batch", time.Since(tt))
			tt = time.Now()
			select {
			case <-ctx.Done():
				return false, errors.New("interrupted")
			default:
			}
		}
		if len(k) != 20 {
			return true, nil
		}

		var acc accounts.Account
		if err = acc.DecodeForStorage(v); err != nil {
			return false, fmt.Errorf("decoding %x for %x: %w", v, k, err)
		}

		if acc.Incarnation > 0 {
			storagePrefix := dbutils.PlainGenerateStoragePrefix(k, acc.Incarnation)
			if acc.IsEmptyRoot() {
				t := trie.New(common.Hash{})
				j := 0
				innerErr := state.WalkAsOfStorage(tx2, common.BytesToAddress(k), acc.Incarnation, common.Hash{}, toBlock+1, func(k1, k2 []byte, vv []byte) (bool, error) {
					j++
					innerErr1 := writeTx.Put(kv.PlainState, dbutils.PlainGenerateCompositeStorageKey(k1, acc.Incarnation, k2), common.CopyBytes(vv))
					if innerErr1 != nil {
						return false, innerErr1
					}

					h, _ := common.HashData(k1)
					t.Update(h.Bytes(), common.CopyBytes(vv))

					return true, nil
				})
				if innerErr != nil {
					return false, innerErr
				}
				acc.Root = t.Hash()
			}

			if acc.IsEmptyCodeHash() {
				codeHash, err1 := tx2.GetOne(kv.PlainContractCode, storagePrefix)
				if err1 != nil && errors.Is(err1, ethdb.ErrKeyNotFound) {
					return false, fmt.Errorf("getting code hash for %x: %w", k, err1)
				}
				if len(codeHash) > 0 {
					code, err1 := tx2.GetOne(kv.Code, codeHash)
					if err1 != nil {
						return false, err1
					}
					if err1 = writeTx.Put(kv.Code, codeHash, code); err1 != nil {
						return false, err1
					}
					if err1 = writeTx.Put(kv.PlainContractCode, storagePrefix, codeHash); err1 != nil {
						return false, err1
					}
				}
			}
		}
		newAcc := make([]byte, acc.EncodingLengthForStorage())
		acc.EncodeForStorage(newAcc)
		innerErr := writeTx.Put(kv.PlainState, common.CopyBytes(k), newAcc)
		if innerErr != nil {
			return false, innerErr
		}

		return true, nil
	})
	if err != nil {
		return err
	}
	err = writeTx.Commit()
	if err != nil {
		return err
	}
	fmt.Println("took", time.Since(t))

	return VerifyStateSnapshot(ctx, logger, dbPath, snapshotFile, block)
}
