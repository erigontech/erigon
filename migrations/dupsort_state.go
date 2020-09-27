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
		var samples1K [][]byte
		var samples10k [][]byte
		var samples100k [][]byte

		total := 0
		bucket := dbutils.BlockBodyPrefix
		fmt.Printf("bucket: %s\n", bucket)
		c := tx.(ethdb.HasTx).Tx().Cursor(bucket)
		count, _ := c.Count()
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			total += len(v)
			blockNum := binary.BigEndian.Uint64(k)
			if blockNum%(count/3000) == 0 {
				samples100k = append(samples100k, v)
			}
			if blockNum%(count/2000) == 0 {
				samples10k = append(samples10k, v)
			}
			if blockNum%(count/1000) == 0 {
				samples1K = append(samples1K, v)
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

		fmt.Printf("samples1K: %d, samples10k: %d, total: %s\n", len(samples1K), len(samples10k), common.StorageSize(total))
		t := time.Now()
		//dict32 := gozstd.BuildDict(samples, 32*1024)
		//cd32, err := gozstd.NewCDictLevel(dict32, -1)
		//if err != nil {
		//	return err
		//}
		//defer cd32.Release()
		//fmt.Printf("dict32: %s\n", time.Since(t))
		//
		//t = time.Now()
		//dict64 := gozstd.BuildDict(samples, 64*1024)
		//cd64, err := gozstd.NewCDictLevel(dict64, -1)
		//if err != nil {
		//	return err
		//}
		//defer cd64.Release()
		//fmt.Printf("dict64: %s\n", time.Since(t))
		//
		//cd64_minus3, err := gozstd.NewCDictLevel(dict64, -3)
		//if err != nil {
		//	return err
		//}
		//defer cd64_minus3.Release()

		t = time.Now()
		dict128 := gozstd.BuildDict(samples1K, 128*1024)
		fmt.Printf("dict128: %s\n", time.Since(t))

		t = time.Now()
		dict128_10k := gozstd.BuildDict(samples10k, 128*1024)
		fmt.Printf("dict128_10k: %s\n", time.Since(t))

		t = time.Now()
		dict128_100k := gozstd.BuildDict(samples100k, 128*1024)
		fmt.Printf("dict128_100k: %s\n", time.Since(t))

		t = time.Now()
		dict64 := gozstd.BuildDict(samples1K, 64*1024)
		fmt.Printf("dict64: %s\n", time.Since(t))

		t = time.Now()
		dict64_10k := gozstd.BuildDict(samples10k, 64*1024)
		fmt.Printf("dict64_10k: %s\n", time.Since(t))

		t = time.Now()
		dict64_100k := gozstd.BuildDict(samples100k, 64*1024)
		fmt.Printf("dict64_100k: %s\n", time.Since(t))

		//t = time.Now()
		//dict256 := gozstd.BuildDict(samples, 256*1024)
		//cd256, err := gozstd.NewCDictLevel(dict256, -1)
		//if err != nil {
		//	panic(err)
		//}
		//defer cd256.Release()
		//fmt.Printf("dict256: %s\n", time.Since(t))

		cd128_minus2, err := gozstd.NewCDictLevel(dict128, -2)
		if err != nil {
			panic(err)
		}
		defer cd128_minus2.Release()

		cd128_s10k_minus2, err := gozstd.NewCDictLevel(dict128_10k, -2)
		if err != nil {
			panic(err)
		}
		defer cd128_s10k_minus2.Release()

		cd128_s100k_minus2, err := gozstd.NewCDictLevel(dict128_100k, -2)
		if err != nil {
			panic(err)
		}
		defer cd128_s100k_minus2.Release()

		cd128_10K_minus1, err := gozstd.NewCDictLevel(dict128_10k, -1)
		if err != nil {
			panic(err)
		}
		defer cd128_10K_minus1.Release()

		cd64_minus2, err := gozstd.NewCDictLevel(dict64, -2)
		if err != nil {
			panic(err)
		}
		defer cd64_minus2.Release()

		cd64_s10k_minus2, err := gozstd.NewCDictLevel(dict64_10k, -2)
		if err != nil {
			panic(err)
		}
		defer cd64_s10k_minus2.Release()

		cd64_s100k_minus2, err := gozstd.NewCDictLevel(dict64_100k, -2)
		if err != nil {
			panic(err)
		}
		defer cd64_s100k_minus2.Release()

		cd64_10K_minus1, err := gozstd.NewCDictLevel(dict64_10k, -1)
		if err != nil {
			panic(err)
		}
		defer cd64_10K_minus1.Release()

		//cd128_1, err := gozstd.NewCDictLevel(dict128, 1)
		//if err != nil {
		//	panic(err)
		//}
		//defer cd128_1.Release()

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
		total128_minus2 := 0
		total128_10k_minus2 := 0
		total128_100k_minus2 := 0
		total128_10K_minus1 := 0
		total64_minus2 := 0
		total64_10k_minus2 := 0
		total64_100k_minus2 := 0
		total64_10K_minus1 := 0
		//total128_0 := 0
		//total128_2 := 0
		//total128_3 := 0
		//total128_4 := 0
		//total256 := 0
		//total256_minus3 := 0
		//total512 := 0
		//total512_minus3 := 0
		//total512_s20_minus3 := 0
		//total128_19 := 0
		//total128_22 := 0

		total = 0
		var d_minus2 time.Duration
		var d_10k_minus2 time.Duration
		var d_100k_minus2 time.Duration
		var d_10K_minus1 time.Duration

		var d64_minus2 time.Duration
		var d64_10k_minus2 time.Duration
		var d64_100k_minus2 time.Duration
		var d64_10K_minus1 time.Duration

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
			//buf = gozstd.CompressDict(buf[:0], v, cd32)
			//total32 += len(buf)
			//t32 := time.Since(t)

			//t = time.Now()
			//buf = gozstd.CompressDict(buf[:0], v, cd64)
			//total64 += len(buf)
			//t64 := time.Since(t)
			//
			//t = time.Now()
			//buf = gozstd.CompressDict(buf[:0], v, cd64_minus3)
			//total64_minus3 += len(buf)
			//t64_minus3 := time.Since(t)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd128_minus2)
			d_minus2 += time.Since(t)
			total128_minus2 += len(buf)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd128_10K_minus1)
			d_10K_minus1 += time.Since(t)
			total128_10K_minus1 += len(buf)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd128_s10k_minus2)
			d_10k_minus2 += time.Since(t)
			total128_10k_minus2 += len(buf)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd128_s100k_minus2)
			d_100k_minus2 += time.Since(t)
			total128_100k_minus2 += len(buf)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd64_minus2)
			d64_minus2 += time.Since(t)
			total64_minus2 += len(buf)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd64_10K_minus1)
			d64_10K_minus1 += time.Since(t)
			total64_10K_minus1 += len(buf)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd64_s10k_minus2)
			d64_10k_minus2 += time.Since(t)
			total64_10k_minus2 += len(buf)

			t = time.Now()
			buf = gozstd.CompressDict(buf[:0], v, cd64_s100k_minus2)
			d64_100k_minus2 += time.Since(t)
			total64_100k_minus2 += len(buf)

			//t = time.Now()
			//buf = gozstd.CompressDict(buf[:0], v, cd128_2)
			//total128_2 += len(buf)
			//t128_2 := time.Since(t)

			//t = time.Now()
			//buf = gozstd.CompressDict(buf[:0], v, cd128_3)
			//total128_3 += len(buf)
			//t128_3 := time.Since(t)
			//
			//t = time.Now()
			//buf = gozstd.CompressDict(buf[:0], v, cd128_4)
			//total128_4 += len(buf)
			//t128_4 := time.Since(t)

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
				totalf := float64(total)
				log.Info("Progress 8", "blockNum", blockNum, "before", common.StorageSize(total),
					//"32", fmt.Sprintf("%.2f", totalf/float64(total32)), "t32", t32,
					//"64", fmt.Sprintf("%.2f", totalf/float64(total64)), "t64", t64,
					//"64_minus3", fmt.Sprintf("%.2f", totalf/float64(total64_minus3)), "t64_minus3", t64_minus3,
					//"128_3", fmt.Sprintf("%.2f", totalf/float64(total128_3)), "t128_3", t128_3,
					//"128_minus3", fmt.Sprintf("%.2f", totalf/float64(total128_minus3)), "d_minus3", d_minus3,
					"128_10K_minus1", fmt.Sprintf("%.2f", totalf/float64(total128_10K_minus1)), "d_10K_minus1", d_10K_minus1,
					"128_minus2", fmt.Sprintf("%.2f", totalf/float64(total128_minus2)), "d_minus2", d_minus2,
					"128_10k_minus2", fmt.Sprintf("%.2f", totalf/float64(total128_10k_minus2)), "d_10k_minus2", d_10k_minus2,
					"128_100k_minus2", fmt.Sprintf("%.2f", totalf/float64(total128_100k_minus2)), "d_100k_minus2", d_100k_minus2,

					"64_10K_minus1", fmt.Sprintf("%.2f", totalf/float64(total64_10K_minus1)), "d64_10K_minus1", d64_10K_minus1,
					"64_minus2", fmt.Sprintf("%.2f", totalf/float64(total64_minus2)), "d64_minus2", d64_minus2,
					"64_10k_minus2", fmt.Sprintf("%.2f", totalf/float64(total64_10k_minus2)), "d64_10k_minus2", d64_10k_minus2,
					"64_100k_minus2", fmt.Sprintf("%.2f", totalf/float64(total64_100k_minus2)), "d64_100k_minus2", d64_100k_minus2,
					//"128_0", fmt.Sprintf("%.2f", totalf/float64(total128_0)), "t128_0", t128_0,
					//"128_1", fmt.Sprintf("%.2f", totalf/float64(total128_1)), "d_1", d_1,
					//"128_2", fmt.Sprintf("%.2f", totalf/float64(total128_2)), "t128_2", t128_2,
					//"128_3", fmt.Sprintf("%.2f", totalf/float64(total128_3)), "t128_3", t128_3,
					//"128_4", fmt.Sprintf("%.2f", totalf/float64(total128_4)), "t128_4", t128_4,
					//"256", fmt.Sprintf("%.2f", totalf/float64(total256)), "t256", t256,
					//"256_minus3", fmt.Sprintf("%.2f", totalf/float64(total256_minus3)), "t256_minus3", t256_minus3,
					//"512", fmt.Sprintf("%.2f", totalf/float64(total512)), "t512", t512,
					//"512_minus3", fmt.Sprintf("%.2f", totalf/float64(total512_minus3)), "t512_minus3", t512_minus3,
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
