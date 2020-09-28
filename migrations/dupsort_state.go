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
		var samples1 [][]byte
		var samples2 [][]byte

		total := 0
		bucket := dbutils.BlockReceiptsPrefix
		fmt.Printf("bucket: %s\n", bucket)
		c := tx.(ethdb.HasTx).Tx().Cursor(bucket)
		count, _ := c.Count()
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			total += len(v)
			blockNum := binary.BigEndian.Uint64(k)
			if blockNum > 8_000_000 && (blockNum%(2_000_000/4000)) == 0 {
				samples2 = append(samples2, v)
			}
			if blockNum%(count/4000) == 0 {
				samples1 = append(samples1, v)
			}

			//if blockNum >= 8_000_000 {
			//	break
			//}

			select {
			default:
			case <-logEvery.C:
				log.Info("Progress", "blockNum", blockNum, "sz", common.StorageSize(total))
			}
		}

		fmt.Printf("samples1: %d, samples2: %d, total: %s\n", len(samples1), len(samples2), common.StorageSize(total))
		t := time.Now()
		dict128 := gozstd.BuildDict(samples1, 64*1024)
		fmt.Printf("dict128: %s\n", time.Since(t))

		t = time.Now()
		dict128_100k := gozstd.BuildDict(samples2, 64*1024)
		fmt.Printf("dict128_100k: %s\n", time.Since(t))

		t = time.Now()
		dict64 := gozstd.BuildDict(samples1, 32*1024)
		fmt.Printf("dict64: %s\n", time.Since(t))

		t = time.Now()
		dict64_100k := gozstd.BuildDict(samples2, 32*1024)
		fmt.Printf("dict64_100k: %s\n", time.Since(t))

		t = time.Now()
		dict32 := gozstd.BuildDict(samples1, 16*1024)
		fmt.Printf("dict32: %s\n", time.Since(t))

		t = time.Now()
		dict32_100k := gozstd.BuildDict(samples2, 16*1024)
		fmt.Printf("dict32_100k: %s\n", time.Since(t))

		cd128_s1_minus2, err := gozstd.NewCDictLevel(dict128, -2)
		if err != nil {
			panic(err)
		}
		defer cd128_s1_minus2.Release()

		cd128_s100k_minus2, err := gozstd.NewCDictLevel(dict128_100k, -2)
		if err != nil {
			panic(err)
		}
		defer cd128_s100k_minus2.Release()

		cd64_minus2, err := gozstd.NewCDictLevel(dict64, -2)
		if err != nil {
			panic(err)
		}
		defer cd64_minus2.Release()

		cd64_s100k_minus2, err := gozstd.NewCDictLevel(dict64_100k, -2)
		if err != nil {
			panic(err)
		}
		defer cd64_s100k_minus2.Release()

		cd32_minus2, err := gozstd.NewCDictLevel(dict32, -2)
		if err != nil {
			panic(err)
		}
		defer cd64_minus2.Release()

		cd32_s100k_minus2, err := gozstd.NewCDictLevel(dict32_100k, -2)
		if err != nil {
			panic(err)
		}
		defer cd32_s100k_minus2.Release()

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
		//total32 := 0
		//total64 := 0
		//total64_minus3 := 0
		total128_s1_minus2 := 0
		total128_s2_minus2 := 0
		total64_s1_minus2 := 0
		total64_s2_minus2 := 0
		total32_s1_minus2 := 0
		total32_s2_minus2 := 0

		total = 0
		var d_s1_minus2 time.Duration
		var d_s2_minus2 time.Duration

		var d64_s1_minus2 time.Duration
		var d64_s2_minus2 time.Duration

		var d32_s1_minus2 time.Duration
		var d32_s2_minus2 time.Duration

		buf := make([]byte, 0, 1024)
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			total += len(v)
			blockNum := binary.BigEndian.Uint64(k)
			//if blockNum >= 8_000_000 {
			//	break
			//}

			t := time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd128_s1_minus2)
			d_s1_minus2 += time.Since(t)
			total128_s1_minus2 += len(buf)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd128_s100k_minus2)
			d_s2_minus2 += time.Since(t)
			total128_s2_minus2 += len(buf)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd64_minus2)
			d64_s1_minus2 += time.Since(t)
			total64_s1_minus2 += len(buf)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd64_s100k_minus2)
			d64_s2_minus2 += time.Since(t)
			total64_s2_minus2 += len(buf)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd32_minus2)
			d32_s1_minus2 += time.Since(t)
			total32_s1_minus2 += len(buf)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd32_s100k_minus2)
			d32_s2_minus2 += time.Since(t)
			total32_s2_minus2 += len(buf)

			select {
			default:
			case <-logEvery.C:
				totalf := float64(total)
				log.Info("Progress 8", "blockNum", blockNum, "before", common.StorageSize(total),
					"128_s1_minus2", fmt.Sprintf("%.2f", totalf/float64(total128_s1_minus2)), "d_s1_minus2", d_s1_minus2,
					"128_s2_minus2", fmt.Sprintf("%.2f", totalf/float64(total128_s2_minus2)), "d_s2_minus2", d_s2_minus2,

					"64_s1_minus2", fmt.Sprintf("%.2f", totalf/float64(total64_s1_minus2)), "d64_s1_minus2", d64_s1_minus2,
					"64_s2_minus2", fmt.Sprintf("%.2f", totalf/float64(total64_s2_minus2)), "d64_s2_minus2", d64_s2_minus2,

					"32_s1_minus2", fmt.Sprintf("%.2f", totalf/float64(total32_s1_minus2)), "d32_s1_minus2", d32_s1_minus2,
					"32_s2_minus2", fmt.Sprintf("%.2f", totalf/float64(total32_s2_minus2)), "d32_s2_minus2", d32_s2_minus2,
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
