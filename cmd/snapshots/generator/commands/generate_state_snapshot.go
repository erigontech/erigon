package commands

import (
	"context"
	"errors"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
	"github.com/spf13/cobra"
	"os"
	"time"
)

func init() {
	withChaindata(generateStateSnapshotCmd)
	withSnapshotFile(generateStateSnapshotCmd)
	withSnapshotData(generateStateSnapshotCmd)
	withBlock(generateStateSnapshotCmd)
	rootCmd.AddCommand(generateStateSnapshotCmd)

}

//go run cmd/snapshots/generator/main.go state_copy --block 11000000 --snapshot /media/b00ris/nvme/snapshots/state --chaindata /media/b00ris/nvme/backup/snapshotsync/tg/chaindata/ &> /media/b00ris/nvme/copy.log
var generateStateSnapshotCmd = &cobra.Command{
	Use:     "state",
	Short:   "Generate state snapshot",
	Example: "go run ./cmd/state/main.go stateSnapshot --block 11000000 --chaindata /media/b00ris/nvme/tgstaged/tg/chaindata/ --snapshot /media/b00ris/nvme/snapshots/state",
	RunE: func(cmd *cobra.Command, args []string) error {
		return GenerateStateSnapshot(cmd.Context(), chaindata, snapshotFile, block, snapshotDir, snapshotMode)
	},
}

func GenerateStateSnapshot(ctx context.Context, dbPath, snapshotPath string, toBlock uint64, snapshotDir string, snapshotMode string) error {
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
			dbutils.PlainStateBucket:        dbutils.BucketConfigItem{},
			dbutils.PlainContractCodeBucket: dbutils.BucketConfigItem{},
			dbutils.CodeBucket:              dbutils.BucketConfigItem{},
			dbutils.StateSnapshotInfoBucket:      dbutils.BucketConfigItem{},
		}
	}).Path(snapshotPath).MustOpen()

	sndb := ethdb.NewObjectDatabase(snkv)
	mt := sndb.NewBatch()

	tx, err := kv.Begin(context.Background(), nil, ethdb.RO)
	if err != nil {
		return err
	}
	tx2, err := kv.Begin(context.Background(), nil, ethdb.RO)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	i := 0
	t := time.Now()
	tt := time.Now()
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
			return false, fmt.Errorf("decoding %x for %x: %v", v, k, err)
		}

		if acc.Incarnation > 0 {
			storagePrefix := dbutils.PlainGenerateStoragePrefix(k, acc.Incarnation)
			if acc.IsEmptyRoot() {
				t := trie.New(common.Hash{})
				j := 0
				innerErr := state.WalkAsOfStorage(tx2, common.BytesToAddress(k), acc.Incarnation, common.Hash{}, toBlock+1, func(k1, k2 []byte, vv []byte) (bool, error) {
					j++
					innerErr1 := mt.Put(dbutils.PlainStateBucket, dbutils.PlainGenerateCompositeStorageKey(k1, acc.Incarnation, k2), common.CopyBytes(vv))
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
				codeHash, err1 := tx2.GetOne(dbutils.PlainContractCodeBucket, storagePrefix)
				if err1 != nil && errors.Is(err1, ethdb.ErrKeyNotFound) {
					return false, fmt.Errorf("getting code hash for %x: %v", k, err1)
				}
				if len(codeHash) > 0 {
					code, err1 := tx2.GetOne(dbutils.CodeBucket, codeHash)
					if err1 != nil {
						return false, err1
					}
					if err1 = mt.Put(dbutils.CodeBucket, codeHash, code); err1 != nil {
						return false, err1
					}
					if err1 = mt.Put(dbutils.PlainContractCodeBucket, storagePrefix, codeHash); err1 != nil {
						return false, err1
					}
				}
			}
		}
		newAcc := make([]byte, acc.EncodingLengthForStorage())
		acc.EncodeForStorage(newAcc)
		innerErr := mt.Put(dbutils.PlainStateBucket, common.CopyBytes(k), newAcc)
		if innerErr != nil {
			return false, innerErr
		}

		if mt.BatchSize() >= mt.IdealBatchSize() {
			ttt := time.Now()
			innerErr = mt.CommitAndBegin(context.Background())
			if innerErr != nil {
				return false, innerErr
			}
			fmt.Println("Committed", time.Since(ttt))
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	_, err = mt.Commit()
	if err != nil {
		return err
	}
	fmt.Println("took", time.Since(t))

	return VerifyStateSnapshot(ctx, dbPath, snapshotFile, block)
}
