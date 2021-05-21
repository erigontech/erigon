package migrations

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
)

var splitHashStateBucket = Migration{
	Name: "split_hash_state_bucket",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) (err error) {
		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
		logPrefix := "split_hash_state_bucket"

		const loadStep = "load"

		collectorS, err1 := etl.NewCollectorFromFiles(tmpdir + "1") // B - stands for blocks
		if err1 != nil {
			return err1
		}
		collectorA, err1 := etl.NewCollectorFromFiles(tmpdir + "2") // T - stands for transactions
		if err1 != nil {
			return err1
		}
		switch string(progress) {
		case "":
			// can't use files if progress field not set, clear them
			if collectorS != nil {
				collectorS.Close(logPrefix)
				collectorS = nil
			}

			if collectorA != nil {
				collectorA.Close(logPrefix)
				collectorA = nil
			}
		case loadStep:
			if collectorS == nil || collectorA == nil {
				return ErrMigrationETLFilesDeleted
			}
			defer func() {
				// don't clean if error or panic happened
				if err != nil {
					return
				}
				if rec := recover(); rec != nil {
					panic(rec)
				}
				collectorS.Close(logPrefix)
				collectorA.Close(logPrefix)
			}()
			goto LoadStep
		}

		collectorS = etl.NewCriticalCollector(tmpdir+"1", etl.NewSortableBuffer(etl.BufferOptimalSize*4))
		collectorA = etl.NewCriticalCollector(tmpdir+"2", etl.NewSortableBuffer(etl.BufferOptimalSize*4))
		defer func() {
			// don't clean if error or panic happened
			if err != nil {
				return
			}
			if rec := recover(); rec != nil {
				panic(rec)
			}
			collectorS.Close(logPrefix)
			collectorA.Close(logPrefix)
		}()

		if err = db.Walk(dbutils.CurrentStateBucketOld2, nil, 0, func(k, v []byte) (bool, error) {
			select {
			default:
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[%s] Progress2", logPrefix), "current key", fmt.Sprintf("%x", k))
			}

			if len(k) == common.HashLength {
				if err = collectorA.Collect(k, v); err != nil {
					return false, err
				}
			} else {
				if err = collectorS.Collect(k, v); err != nil {
					return false, err
				}
			}
			return true, nil
		}); err != nil {
			return err
		}

		if err = db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.CurrentStateBucketOld2); err != nil {
			return fmt.Errorf("clearing the target bucket: %w", err)
		}

		// Commit clearing of the bucket - freelist should now be written to the database
		if err = CommitProgress(db, []byte(loadStep), false); err != nil {
			return fmt.Errorf("committing the removal of table: %w", err)
		}

	LoadStep:
		// Now transaction would have been re-opened, and we should be re-using the space
		if err = collectorS.Load(logPrefix, db.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.HashedStorageBucket, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
			return fmt.Errorf("loading the transformed data back into the storage table: %w", err)
		}
		if err = collectorA.Load(logPrefix, db.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.HashedAccountsBucket, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
			return fmt.Errorf("loading the transformed data back into the acc table: %w", err)
		}
		return CommitProgress(db, nil, true)
	},
}

var splitIHBucket = Migration{
	Name: "split_ih_bucket",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) error {
		logPrefix := "db_migration: split_ih_bucket"

		const loadStep = "load"
		if err := stagedsync.ResetIH(db.(ethdb.HasTx).Tx().(ethdb.RwTx)); err != nil {
			return err
		}
		if err := CommitProgress(db, []byte(loadStep), false); err != nil {
			return fmt.Errorf("committing the removal of table: %w", err)
		}

		to, err := stages.GetStageProgress(db, stages.Execution)
		if err != nil {
			return err
		}
		hash, err := rawdb.ReadCanonicalHash(db, to)
		if err != nil {
			return err
		}
		syncHeadHeader := rawdb.ReadHeader(db, hash, to)
		if syncHeadHeader == nil {
			if err := CommitProgress(db, nil, true); err != nil {
				return fmt.Errorf("committing the removal of table: %w", err)
			}
			return nil
		}
		expectedRootHash := syncHeadHeader.Root

		if _, err := stagedsync.RegenerateIntermediateHashes(logPrefix, db.(ethdb.HasTx).Tx().(ethdb.RwTx), stagedsync.StageTrieCfg(db.RwKV(), true, true, tmpdir), expectedRootHash, nil); err != nil {
			return err
		}
		if err := CommitProgress(db, nil, true); err != nil {
			return fmt.Errorf("committing the removal of table: %w", err)
		}

		return nil
	},
}

// see https://github.com/ledgerwatch/turbo-geth/pull/1535
var deleteExtensionHashesFromTrieBucket = Migration{
	Name: "delete_extension_hashes_from_trie",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) error {
		logPrefix := "db_migration: delete_extension_hashes_from_trie"

		const loadStep = "load"
		if err := stagedsync.ResetIH(db.(ethdb.HasTx).Tx().(ethdb.RwTx)); err != nil {
			return err
		}
		if err := CommitProgress(db, []byte(loadStep), false); err != nil {
			return err
		}

		to, err := stages.GetStageProgress(db, stages.Execution)
		if err != nil {
			return err
		}
		hash, err := rawdb.ReadCanonicalHash(db, to)
		if err != nil {
			return err
		}
		syncHeadHeader := rawdb.ReadHeader(db, hash, to)
		if syncHeadHeader == nil {
			if err := CommitProgress(db, nil, true); err != nil {
				return err
			}
			return nil
		}
		expectedRootHash := syncHeadHeader.Root

		if _, err := stagedsync.RegenerateIntermediateHashes(logPrefix, db.(ethdb.HasTx).Tx().(ethdb.RwTx), stagedsync.StageTrieCfg(db.RwKV(), true, true, tmpdir), expectedRootHash, nil); err != nil {
			return err
		}
		if err := CommitProgress(db, nil, true); err != nil {
			return err
		}

		return nil
	},
}
