package migrations

import (
	"bytes"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

var historyAccBitmap = Migration{
	Name: "history_account_bitmap",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) (err error) {
		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
		logPrefix := "history_account_bitmap"

		const loadStep = "load"
		var prevK []byte
		blocks := roaring64.New()
		buf := bytes.NewBuffer(nil)

		collectorB, err1 := etl.NewCollectorFromFiles(tmpdir + "1") // B - stands for blocks
		if err1 != nil {
			return err1
		}
		switch string(progress) {
		case "":
			// can't use files if progress field not set, clear them
			if collectorB != nil {
				collectorB.Close(logPrefix)
				collectorB = nil
			}

		case loadStep:
			if collectorB == nil {
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
				collectorB.Close(logPrefix)
			}()
			goto LoadStep
		}

		collectorB = etl.NewCriticalCollector(tmpdir+"1", etl.NewSortableBuffer(etl.BufferOptimalSize))
		defer func() {
			// don't clean if error or panic happened
			if err != nil {
				return
			}
			if rec := recover(); rec != nil {
				panic(rec)
			}
			collectorB.Close(logPrefix)
		}()
		if err = db.Walk(dbutils.AccountsHistoryBucket, nil, 0, func(k, v []byte) (bool, error) {
			select {
			default:
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[%s] Progress2", logPrefix), "key", fmt.Sprintf("%x", k))
			}
			index := dbutils.WrapHistoryIndex(v)

			blockNums, _, errInner := index.Decode()
			if errInner != nil {
				return false, errInner
			}

			if prevK != nil && !bytes.Equal(k[:20], prevK[:20]) {
				if err = bitmapdb.WalkChunkWithKeys64(prevK[:20], blocks, bitmapdb.ChunkLimit, func(chunkKey []byte, chunk *roaring64.Bitmap) error {
					buf.Reset()
					if _, err = chunk.WriteTo(buf); err != nil {
						return err
					}
					return collectorB.Collect(chunkKey, buf.Bytes())
				}); err != nil {
					return false, err
				}
				blocks.Clear()
			}

			blocks.AddMany(blockNums)
			prevK = k
			return true, nil
		}); err != nil {
			return err
		}

		if prevK != nil {

			if err = bitmapdb.WalkChunkWithKeys64(prevK[:20], blocks, bitmapdb.ChunkLimit, func(chunkKey []byte, chunk *roaring64.Bitmap) error {
				buf.Reset()
				if _, err = chunk.WriteTo(buf); err != nil {
					return err
				}
				return collectorB.Collect(chunkKey, buf.Bytes())
			}); err != nil {
				return err
			}
		}
		if err = db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.AccountsHistoryBucket); err != nil {
			return fmt.Errorf("clearing the receipt bucket: %w", err)
		}

		// Commit clearing of the bucket - freelist should now be written to the database
		if err = CommitProgress(db, []byte(loadStep), false); err != nil {
			return fmt.Errorf("committing the removal of receipt table: %w", err)
		}

	LoadStep:
		// Now transaction would have been re-opened, and we should be re-using the space
		if err = collectorB.Load(logPrefix, db.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.AccountsHistoryBucket, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
			return fmt.Errorf("loading the transformed data back into the bodies table: %w", err)
		}
		return CommitProgress(db, nil, true)
	},
}

var historyStorageBitmap = Migration{
	Name: "history_storage_bitmap",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) (err error) {
		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
		logPrefix := "history_storage_bitmap"

		const loadStep = "load"
		var prevK []byte
		blocks := roaring64.New()
		buf := bytes.NewBuffer(nil)

		collectorB, err1 := etl.NewCollectorFromFiles(tmpdir + "1") // B - stands for blocks
		if err1 != nil {
			return err1
		}
		switch string(progress) {
		case "":
			// can't use files if progress field not set, clear them
			if collectorB != nil {
				collectorB.Close(logPrefix)
				collectorB = nil
			}

		case loadStep:
			if collectorB == nil {
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
				collectorB.Close(logPrefix)
			}()
			goto LoadStep
		}

		collectorB = etl.NewCriticalCollector(tmpdir+"1", etl.NewSortableBuffer(etl.BufferOptimalSize))
		defer func() {
			// don't clean if error or panic happened
			if err != nil {
				return
			}
			if rec := recover(); rec != nil {
				panic(rec)
			}
			collectorB.Close(logPrefix)
		}()
		if err = db.Walk(dbutils.StorageHistoryBucket, nil, 0, func(k, v []byte) (bool, error) {
			select {
			default:
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[%s] Progress2", logPrefix), "key", fmt.Sprintf("%x", k))
			}
			index := dbutils.WrapHistoryIndex(v)

			blockNums, _, errInner := index.Decode()
			if errInner != nil {
				return false, errInner
			}

			if prevK != nil && !bytes.Equal(k[:52], prevK[:52]) {
				if err = bitmapdb.WalkChunkWithKeys64(prevK[:52], blocks, bitmapdb.ChunkLimit, func(chunkKey []byte, chunk *roaring64.Bitmap) error {
					buf.Reset()
					if _, err = chunk.WriteTo(buf); err != nil {
						return err
					}
					return collectorB.Collect(chunkKey, buf.Bytes())
				}); err != nil {
					return false, err
				}
				blocks.Clear()
			}

			blocks.AddMany(blockNums)
			prevK = k
			return true, nil
		}); err != nil {
			return err
		}

		if prevK != nil {
			if err = bitmapdb.WalkChunkWithKeys64(prevK[:52], blocks, bitmapdb.ChunkLimit, func(chunkKey []byte, chunk *roaring64.Bitmap) error {
				buf.Reset()
				if _, err = chunk.WriteTo(buf); err != nil {
					return err
				}
				return collectorB.Collect(chunkKey, buf.Bytes())
			}); err != nil {
				return err
			}
		}

		if err = db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.StorageHistoryBucket); err != nil {
			return fmt.Errorf("clearing the receipt bucket: %w", err)
		}

		// Commit clearing of the bucket - freelist should now be written to the database
		if err = CommitProgress(db, []byte(loadStep), false); err != nil {
			return fmt.Errorf("committing the removal of receipt table: %w", err)
		}

	LoadStep:
		// Now transaction would have been re-opened, and we should be re-using the space
		if err = collectorB.Load(logPrefix, db.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.StorageHistoryBucket, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
			return fmt.Errorf("loading the transformed data back into the bodies table: %w", err)
		}
		return CommitProgress(db, nil, true)
	},
}
