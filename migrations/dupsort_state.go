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
	Up: func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
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
	Up: func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
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
	Up: func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
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
		loader := trie.NewFlatDBTrieLoader(dbutils.CurrentStateBucket, dbutils.IntermediateTrieHashBucket)
		if err := loader.Reset(trie.NewRetainList(0), hashCollector /* HashCollector */, false); err != nil {
			return err
		}
		if _, err := loader.CalcTrieRoot(db, nil); err != nil {
			return err
		}
		if err := collector.Load(db, dbutils.IntermediateTrieHashBucket, etl.IdentityLoadFunc, etl.TransformArgs{
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
	Up: func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
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
	Up: func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
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

var zstd = Migration{
	Name: "zstd_2",
	Up: func(tx ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
		if err := tx.(ethdb.BucketsMigrator).ClearBuckets(dbutils.BlockReceiptsZstd); err != nil {
			return err
		}

		logEvery := time.NewTicker(5 * time.Second)
		defer logEvery.Stop()

		// train
		var samples [][]byte

		total := 0
		c := tx.(ethdb.HasTx).Tx().Cursor(dbutils.BlockReceiptsPrefix)
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			total += len(v)
			blockNum := binary.BigEndian.Uint64(k)
			if blockNum%7 != 0 {
				continue
			}
			samples = append(samples, v)
			if blockNum >= 6_000_000 {
				break
			}

			select {
			default:
			case <-logEvery.C:
				log.Info("Progress", "blockNum", blockNum, "sz", common.StorageSize(total))
			}
		}

		t := time.Now()
		dict4 := gozstd.BuildDict(samples, 4*1024)
		cd4, err := gozstd.NewCDictLevel(dict4, gozstd.DefaultCompressionLevel)
		if err != nil {
			return err
		}
		defer cd4.Release()
		fmt.Printf("dict4: %s\n", time.Since(t))
		t = time.Now()
		dict8 := gozstd.BuildDict(samples, 8*1024)
		cd8, err := gozstd.NewCDictLevel(dict8, gozstd.DefaultCompressionLevel)
		if err != nil {
			return err
		}
		defer cd8.Release()
		fmt.Printf("dict8: %s\n", time.Since(t))
		t = time.Now()

		dict12 := gozstd.BuildDict(samples, 12*1024)
		cd12, err := gozstd.NewCDictLevel(dict12, gozstd.DefaultCompressionLevel)
		if err != nil {
			return err
		}
		defer cd12.Release()
		fmt.Printf("dict12: %s\n", time.Since(t))
		t = time.Now()

		dict16 := gozstd.BuildDict(samples, 16*1024)
		cd16, err := gozstd.NewCDictLevel(dict16, gozstd.DefaultCompressionLevel)
		if err != nil {
			return err
		}
		defer cd16.Release()
		fmt.Printf("dict16: %s\n", time.Since(t))
		t = time.Now()

		dict32 := gozstd.BuildDict(samples, 32*1024)
		cd32, err := gozstd.NewCDictLevel(dict32, gozstd.DefaultCompressionLevel)
		if err != nil {
			return err
		}
		defer cd32.Release()
		fmt.Printf("dict32: %s\n", time.Since(t))
		t = time.Now()

		dict64 := gozstd.BuildDict(samples, 64*1024)
		cd64, err := gozstd.NewCDictLevel(dict64, gozstd.DefaultCompressionLevel)
		if err != nil {
			return err
		}
		defer cd64.Release()
		fmt.Printf("dict64: %s\n", time.Since(t))

		t = time.Now()
		total4 := 0
		total8 := 0
		total12 := 0
		total16 := 0
		total32 := 0
		buf := make([]byte, 0, 1024)
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			blockNum := binary.BigEndian.Uint64(k)
			if blockNum >= 6_000_000 {
				break
			}

			buf := gozstd.CompressDict(buf[:0], v, cd4)
			total4 += len(buf)
			buf = gozstd.CompressDict(buf[:0], v, cd8)
			total8 += len(buf)
			buf = gozstd.CompressDict(buf[:0], v, cd12)
			total12 += len(buf)
			buf = gozstd.CompressDict(buf[:0], v, cd16)
			total16 += len(buf)
			buf = gozstd.CompressDict(buf[:0], v, cd32)
			total32 += len(buf)
			select {
			default:
			case <-logEvery.C:
				log.Info("Progress 8", "blockNum", blockNum,
					"total4", common.StorageSize(total4),
					"total8", common.StorageSize(total8),
					"total12", common.StorageSize(total12),
					"total16", common.StorageSize(total16),
					"total32", common.StorageSize(total32),
				)
			}
		}

		fmt.Printf("s: %s\n", time.Since(t))
		//if err := tx.(ethdb.BucketsMigrator).DropBuckets(dbutils.BlockReceiptsPrefixOld1); err != nil {
		//	return err
		//}
		return OnLoadCommit(tx, nil, true)
	},
}
