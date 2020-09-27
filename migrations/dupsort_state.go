package migrations

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/valyala/gozstd"
	"time"

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
	Name: "zstd_5",
	Up: func(tx ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
		if err := tx.(ethdb.BucketsMigrator).ClearBuckets(dbutils.BlockReceiptsZstd); err != nil {
			return err
		}

		logEvery := time.NewTicker(5 * time.Second)
		defer logEvery.Stop()

		// train
		var samples [][]byte

		total := 0
		c := tx.(ethdb.HasTx).Tx().Cursor(dbutils.BlockBodyPrefix)
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			total += len(v)
			blockNum := binary.BigEndian.Uint64(k)
			if blockNum%160 != 0 {
				continue
			}
			samples = append(samples, v)
			if blockNum >= 8_000_000 {
				break
			}

			select {
			default:
			case <-logEvery.C:
				log.Info("Progress", "blockNum", blockNum, "sz", common.StorageSize(total))
			}
		}

		fmt.Printf("samples: %d, total: %s\n", len(samples), common.StorageSize(total))
		t := time.Now()
		//dict4 := gozstd.BuildDict(samples, 4*1024)
		//cd4, err := gozstd.NewCDictLevel(dict4, -1)
		//if err != nil {
		//	return err
		//}
		//defer cd4.Release()
		//fmt.Printf("dict4: %s\n", time.Since(t))
		//t = time.Now()
		//dict8 := gozstd.BuildDict(samples, 8*1024)
		//cd8, err := gozstd.NewCDictLevel(dict8, -1)
		//if err != nil {
		//	return err
		//}
		//defer cd8.Release()
		//fmt.Printf("dict8: %s\n", time.Since(t))
		//t = time.Now()
		//
		//dict12 := gozstd.BuildDict(samples, 12*1024)
		//cd12, err := gozstd.NewCDictLevel(dict12, -1)
		//if err != nil {
		//	return err
		//}
		//defer cd12.Release()
		//fmt.Printf("dict12: %s\n", time.Since(t))

		t = time.Now()
		dict16 := gozstd.BuildDict(samples, 16*1024)
		cd16, err := gozstd.NewCDictLevel(dict16, -1)
		if err != nil {
			return err
		}
		defer cd16.Release()
		fmt.Printf("dict16: %s\n", time.Since(t))

		t = time.Now()
		dict32 := gozstd.BuildDict(samples, 32*1024)
		cd32, err := gozstd.NewCDictLevel(dict32, -1)
		if err != nil {
			return err
		}
		defer cd32.Release()
		fmt.Printf("dict32: %s\n", time.Since(t))

		t = time.Now()
		dict64 := gozstd.BuildDict(samples, 64*1024)
		cd64, err := gozstd.NewCDictLevel(dict64, -1)
		if err != nil {
			return err
		}
		defer cd64.Release()
		fmt.Printf("dict64: %s\n", time.Since(t))

		dict128 := gozstd.BuildDict(samples, 128*1024)
		cd128, err := gozstd.NewCDictLevel(dict128, -1)
		if err != nil {
			return err
		}
		defer cd128.Release()
		fmt.Printf("dict128: %s\n", time.Since(t))

		dict256 := gozstd.BuildDict(samples, 256*1024)
		cd256, err := gozstd.NewCDictLevel(dict256, -1)
		if err != nil {
			return err
		}
		defer cd256.Release()
		fmt.Printf("dict256: %s\n", time.Since(t))

		cd128_minus3, err := gozstd.NewCDictLevel(dict128, -3)
		if err != nil {
			return err
		}
		defer cd128_minus3.Release()

		cd128_minus20, err := gozstd.NewCDictLevel(dict128, -20)
		if err != nil {
			return err
		}
		defer cd128_minus20.Release()

		cd128_3, err := gozstd.NewCDictLevel(dict128, 3)
		if err != nil {
			return err
		}
		defer cd128_3.Release()

		//cd128_19, err := gozstd.NewCDictLevel(dict128, 19)
		//if err != nil {
		//	return err
		//}
		//defer cd128_19.Release()
		//
		//cd128_22, err := gozstd.NewCDictLevel(dict128, 22)
		//if err != nil {
		//	return err
		//}
		//defer cd128_22.Release()

		t = time.Now()
		//total4 := 0
		//total8 := 0
		//total12 := 0
		total16 := 0
		total32 := 0
		total64 := 0
		total128 := 0
		total256 := 0
		total128_minus20 := 0
		total128_minus3 := 0
		total128_3 := 0
		//total128_19 := 0
		//total128_22 := 0
		buf := make([]byte, 0, 1024)
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			blockNum := binary.BigEndian.Uint64(k)
			if blockNum >= 8_000_000 {
				break
			}

			t := time.Now()
			//buf := gozstd.CompressDict(buf[:0], v, cd4)
			//total4 += len(buf)
			//t4 := time.Since(t)

			//t = time.Now()
			//buf = gozstd.CompressDict(buf[:0], v, cd8)
			//total8 += len(buf)
			//t8 := time.Since(t)

			//t = time.Now()
			//buf = gozstd.CompressDict(buf[:0], v, cd12)
			//total12 += len(buf)
			//t12 := time.Since(t)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd16)
			total16 += len(buf)
			t16 := time.Since(t)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd32)
			total32 += len(buf)
			t32 := time.Since(t)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd64)
			total64 += len(buf)
			t64 := time.Since(t)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd128)
			total128 += len(buf)
			t128 := time.Since(t)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd256)
			total256 += len(buf)
			t256 := time.Since(t)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd128_minus20)
			total128_minus20 += len(buf)
			t128_minus20 := time.Since(t)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd128_minus3)
			total128_minus20 += len(buf)
			t128_minus3 := time.Since(t)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd128_3)
			total128_3 += len(buf)
			t128_3 := time.Since(t)

			//t = time.Now()
			//buf = gozstd.CompressDict(buf[:0], v, cd128_19)
			//total128_19 += len(buf)
			//t128_19 := time.Since(t)
			//
			//t = time.Now()
			//buf = gozstd.CompressDict(buf[:0], v, cd128_22)
			//total128_22 += len(buf)
			//t128_22 := time.Since(t)

			select {
			default:
			case <-logEvery.C:
				log.Info("Progress 8", "blockNum", blockNum,
					//"total4", common.StorageSize(total4), "time4", t4,
					//"total8", common.StorageSize(total8), "time8", t8,
					//"total12", common.StorageSize(total12), "time12", t12,
					"total16", common.StorageSize(total16), "time16", t16,
					"total32", common.StorageSize(total32), "time32", t32,
					"total64", common.StorageSize(total64), "time64", t64,
					"total128", common.StorageSize(total128), "time128", t128,
					"total256", common.StorageSize(total256), "time256", t256,
					"total128_minus20", common.StorageSize(total128_minus20), "time128_minus20", t128_minus20,
					"total128_minus3", common.StorageSize(total128_minus3), "time128_minus3", t128_minus3,
					"total128_3", common.StorageSize(total128_3), "time128_3", t128_3,
					//"total128_19", common.StorageSize(total128_19), "time128_19", t128_19,
					//"total128_22", common.StorageSize(total128_22), "time128_22", t128_22,
				)
			}
		}

		fmt.Printf("s: %s\n", time.Since(t))
		//if err := tx.(ethdb.BucketsMigrator).DropBuckets(dbutils.BlockReceiptsPrefixOld1); err != nil {
		//	return err
		//}
		//return OnLoadCommit(tx, nil, true)
		return nil
	},
}
