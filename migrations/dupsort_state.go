package migrations

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

var dupSortHashState = Migration{
	Name: "dupsort_hash_state",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
		if exists, err := db.(ethdb.BucketsMigrator).BucketExists(dbutils.CurrentStateBucketOld1); err != nil {
			return err
		} else if !exists {
			return OnLoadCommit(db, nil, true)
		}

		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.CurrentStateBucketOld2); err != nil {
			return err
		}
		extractFunc := func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			return next(k, k, v)
		}

		if err := etl.Transform(
			"dupsort_hash_state",
			db,
			dbutils.CurrentStateBucketOld1,
			dbutils.CurrentStateBucketOld2,
			tmpdir,
			extractFunc,
			etl.IdentityLoadFunc,
			etl.TransformArgs{OnLoadCommit: OnLoadCommit},
		); err != nil {
			return err
		}

		if err := db.(ethdb.BucketsMigrator).DropBuckets(dbutils.CurrentStateBucketOld1); err != nil {
			return err
		}
		return nil
	},
}

var dupSortPlainState = Migration{
	Name: "dupsort_plain_state",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
		if exists, err := db.(ethdb.BucketsMigrator).BucketExists(dbutils.PlainStateBucketOld1); err != nil {
			return err
		} else if !exists {
			return OnLoadCommit(db, nil, true)
		}

		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.PlainStateBucket); err != nil {
			return err
		}
		extractFunc := func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			return next(k, k, v)
		}

		if err := etl.Transform(
			"dupsort_plain_state",
			db,
			dbutils.PlainStateBucketOld1,
			dbutils.PlainStateBucket,
			tmpdir,
			extractFunc,
			etl.IdentityLoadFunc,
			etl.TransformArgs{OnLoadCommit: OnLoadCommit},
		); err != nil {
			return err
		}

		if err := db.(ethdb.BucketsMigrator).DropBuckets(dbutils.PlainStateBucketOld1); err != nil {
			return err
		}
		return nil
	},
}

var dupSortIH = Migration{
	Name: "dupsort_intermediate_trie_hashes",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
		if err := db.(ethdb.BucketsMigrator).ClearBuckets(
			dbutils.IntermediateTrieHashBucketOld2,
			dbutils.IntermediateTrieHashBucketOld1,
			dbutils.IntermediateHashOfStorageBucket,
			dbutils.IntermediateHashOfAccountBucket); err != nil {
			return err
		}
		if err := stages.SaveStageProgress(db, stages.IntermediateHashes, 0); err != nil {
			return err
		}
		if err := stages.SaveStageUnwind(db, stages.IntermediateHashes, 0); err != nil {
			return err
		}
		return OnLoadCommit(db, nil, true)
	},
}

var clearIndices = Migration{
	Name: "clear_log_indices7",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.LogAddressIndex, dbutils.LogTopicIndex); err != nil {
			return err
		}

		if err := stages.SaveStageProgress(db, stages.LogIndex, 0); err != nil {
			return err
		}
		if err := stages.SaveStageUnwind(db, stages.LogIndex, 0); err != nil {
			return err
		}

		return OnLoadCommit(db, nil, true)
	},
}

var resetIHBucketToRecoverDB = Migration{
	Name: "reset_in_bucket_to_recover_db",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.IntermediateTrieHashBucketOld2); err != nil {
			return err
		}
		if err := stages.SaveStageProgress(db, stages.IntermediateHashes, 0); err != nil {
			return err
		}
		if err := stages.SaveStageUnwind(db, stages.IntermediateHashes, 0); err != nil {
			return err
		}
		return OnLoadCommit(db, nil, true)
	},
}

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
		if err = collectorS.Load(logPrefix, db, dbutils.HashedStorageBucket, etl.IdentityLoadFunc, etl.TransformArgs{
			OnLoadCommit: CommitProgress,
		}); err != nil {
			return fmt.Errorf("loading the transformed data back into the storage table: %w", err)
		}
		if err = collectorA.Load(logPrefix, db, dbutils.HashedAccountsBucket, etl.IdentityLoadFunc, etl.TransformArgs{
			OnLoadCommit: CommitProgress,
		}); err != nil {
			return fmt.Errorf("loading the transformed data back into the acc table: %w", err)
		}
		return nil
	},
}

var splitIHBucket = Migration{
	Name: "split_ih_bucket",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) (err error) {
		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
		logPrefix := "split_ih_bucket"

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

		if err = db.Walk(dbutils.IntermediateTrieHashBucketOld2, nil, 0, func(k, v []byte) (bool, error) {
			select {
			default:
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[%s] Progress2", logPrefix), "current key", fmt.Sprintf("%x", k))
			}

			if len(k) > common.HashLength*2 {
				keyPart := len(v) - common.HashLength
				k = append(k, v[:keyPart]...)
				v = v[keyPart:]
				if err = collectorS.Collect(k, v); err != nil {
					return false, err
				}
			} else {
				if err = collectorA.Collect(k, v); err != nil {
					return false, err
				}
			}

			return true, nil
		}); err != nil {
			return err
		}

		if err = db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.IntermediateTrieHashBucketOld2); err != nil {
			return fmt.Errorf("clearing the target bucket: %w", err)
		}

		// Commit clearing of the bucket - freelist should now be written to the database
		if err = CommitProgress(db, []byte(loadStep), false); err != nil {
			return fmt.Errorf("committing the removal of table: %w", err)
		}

	LoadStep:
		// Now transaction would have been re-opened, and we should be re-using the space
		if err = collectorS.Load(logPrefix, db, dbutils.IntermediateHashOfStorageBucket, etl.IdentityLoadFunc, etl.TransformArgs{
			OnLoadCommit: CommitProgress,
		}); err != nil {
			return fmt.Errorf("loading the transformed data back into the storage table: %w", err)
		}
		if err = collectorA.Load(logPrefix, db, dbutils.IntermediateHashOfAccountBucket, etl.IdentityLoadFunc, etl.TransformArgs{
			OnLoadCommit: CommitProgress,
		}); err != nil {
			return fmt.Errorf("loading the transformed data back into the acc table: %w", err)
		}
		return nil
	},
}
var _ = splitHashStateBucket
var _ = splitIHBucket
