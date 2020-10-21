package migrations

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
)

var dupSortHashState = Migration{
	Name: "dupsort_hash_state",
	Up: func(db ethdb.Database, datadir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
		if exists, err := db.(ethdb.BucketsMigrator).BucketExists(dbutils.CurrentStateBucketOld1); err != nil {
			return err
		} else if !exists {
			return OnLoadCommit(db, nil, true)
		}

		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.CurrentStateBucket); err != nil {
			return err
		}
		extractFunc := func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			return next(k, k, v)
		}

		if err := etl.Transform(
			"dupsort_hash_state",
			db,
			dbutils.CurrentStateBucketOld1,
			dbutils.CurrentStateBucket,
			datadir,
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
	Up: func(db ethdb.Database, datadir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
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
			datadir,
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
	Up: func(db ethdb.Database, datadir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.IntermediateTrieHashBucket); err != nil {
			return err
		}
		buf := etl.NewSortableBuffer(etl.BufferOptimalSize)
		comparator := db.(ethdb.HasTx).Tx().Comparator(dbutils.IntermediateTrieHashBucket)
		buf.SetComparator(comparator)
		collector := etl.NewCollector(datadir, buf)
		hashCollector := func(keyHex []byte, hash []byte) error {
			if len(keyHex) == 0 {
				return nil
			}
			if len(keyHex) > trie.IHDupKeyLen {
				return collector.Collect(keyHex[:trie.IHDupKeyLen], append(keyHex[trie.IHDupKeyLen:], hash...))
			}
			return collector.Collect(keyHex, hash)
		}
		loader := trie.NewFlatDBTrieLoader("dupsort_intermediate_trie_hashes", dbutils.CurrentStateBucket, dbutils.IntermediateTrieHashBucket)
		if err := loader.Reset(trie.NewRetainList(0), hashCollector /* HashCollector */, false); err != nil {
			return err
		}
		if _, err := loader.CalcTrieRoot(db, nil); err != nil {
			return err
		}
		if err := collector.Load("dupsort_intermediate_trie_hashes", db, dbutils.IntermediateTrieHashBucket, etl.IdentityLoadFunc, etl.TransformArgs{
			Comparator: comparator,
		}); err != nil {
			return fmt.Errorf("gen ih stage: fail load data to bucket: %w", err)
		}

		// this Migration is empty, sync will regenerate IH bucket values automatically
		// alternative is - to copy whole stage here
		if err := db.(ethdb.BucketsMigrator).DropBuckets(dbutils.IntermediateTrieHashBucketOld1); err != nil {
			return err
		}
		return OnLoadCommit(db, nil, true)
	},
}

var clearIndices = Migration{
	Name: "clear_log_indices7",
	Up: func(db ethdb.Database, datadir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.LogAddressIndex, dbutils.LogTopicIndex); err != nil {
			return err
		}

		if err := stages.SaveStageProgress(db, stages.LogIndex, 0, nil); err != nil {
			return err
		}
		if err := stages.SaveStageUnwind(db, stages.LogIndex, 0, nil); err != nil {
			return err
		}

		return OnLoadCommit(db, nil, true)
	},
}

var resetIHBucketToRecoverDB = Migration{
	Name: "reset_in_bucket_to_recover_db",
	Up: func(db ethdb.Database, datadir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.IntermediateTrieHashBucket); err != nil {
			return err
		}
		if err := stages.SaveStageProgress(db, stages.IntermediateHashes, 0, nil); err != nil {
			return err
		}
		if err := stages.SaveStageUnwind(db, stages.IntermediateHashes, 0, nil); err != nil {
			return err
		}
		return OnLoadCommit(db, nil, true)
	},
}
