package migrations

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var headerPrefixToSeparateBuckets = Migration{
	Name: "header_prefix_to_separate_buckets",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) (err error) {
		exists, err := db.(ethdb.BucketsMigrator).BucketExists(dbutils.HeaderPrefixOld)
		if err != nil {
			return err
		}
		if !exists {
			return CommitProgress(db, nil, true)
		}

		if err = db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.HeaderCanonicalBucket, dbutils.HeaderCanonicalBucket, dbutils.HeaderTDBucket); err != nil {
			return err
		}
		logPrefix := "split_header_prefix_bucket"
		const loadStep = "load"

		canonicalCollector, err := etl.NewCollectorFromFiles(tmpdir + "canonical")
		if err != nil {
			return err
		}
		tdCollector, err := etl.NewCollectorFromFiles(tmpdir + "td")
		if err != nil {
			return err
		}
		headersCollector, err := etl.NewCollectorFromFiles(tmpdir + "headers")
		if err != nil {
			return err
		}

		switch string(progress) {
		case "":
			// can't use files if progress field not set, clear them
			if canonicalCollector != nil {
				canonicalCollector.Close(logPrefix)
				canonicalCollector = nil
			}

			if tdCollector != nil {
				tdCollector.Close(logPrefix)
				tdCollector = nil
			}
			if headersCollector != nil {
				headersCollector.Close(logPrefix)
				headersCollector = nil
			}
		case loadStep:
			if headersCollector == nil || canonicalCollector == nil || tdCollector == nil {
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
				canonicalCollector.Close(logPrefix)
				tdCollector.Close(logPrefix)
				headersCollector.Close(logPrefix)
			}()
			goto LoadStep
		}

		canonicalCollector = etl.NewCriticalCollector(tmpdir+"canonical", etl.NewSortableBuffer(etl.BufferOptimalSize*4))
		tdCollector = etl.NewCriticalCollector(tmpdir+"td", etl.NewSortableBuffer(etl.BufferOptimalSize*4))
		headersCollector = etl.NewCriticalCollector(tmpdir+"headers", etl.NewSortableBuffer(etl.BufferOptimalSize*4))
		defer func() {
			// don't clean if error or panic happened
			if err != nil {
				return
			}
			if rec := recover(); rec != nil {
				panic(rec)
			}
			canonicalCollector.Close(logPrefix)
			tdCollector.Close(logPrefix)
			headersCollector.Close(logPrefix)
		}()

		err = db.Walk(dbutils.HeaderPrefixOld, []byte{}, 0, func(k, v []byte) (bool, error) {
			var innerErr error
			switch {
			case IsHeaderKey(k):
				innerErr = headersCollector.Collect(k, v)
			case IsHeaderTDKey(k):
				innerErr = tdCollector.Collect(bytes.TrimSuffix(k, HeaderTDSuffix), v)
			case IsHeaderHashKey(k):
				innerErr = canonicalCollector.Collect(bytes.TrimSuffix(k, HeaderHashSuffix), v)
			default:
				return false, fmt.Errorf("incorrect header prefix key: %v", common.Bytes2Hex(k))
			}
			if innerErr != nil {
				return false, innerErr
			}
			return true, nil
		})
		if err = db.(ethdb.BucketsMigrator).DropBuckets(dbutils.HeaderPrefixOld); err != nil {
			return err
		}

	LoadStep:
		// Now transaction would have been re-opened, and we should be re-using the space
		if err = canonicalCollector.Load(logPrefix, db.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.HeaderCanonicalBucket, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
			return fmt.Errorf("loading the transformed data back into the storage table: %w", err)
		}
		if err = tdCollector.Load(logPrefix, db.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.HeaderTDBucket, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
			return fmt.Errorf("loading the transformed data back into the acc table: %w", err)
		}
		if err = headersCollector.Load(logPrefix, db.(ethdb.HasTx).Tx().(ethdb.RwTx), dbutils.HeadersBucket, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
			return fmt.Errorf("loading the transformed data back into the acc table: %w", err)
		}
		return CommitProgress(db, nil, true)
	},
}

func IsHeaderKey(k []byte) bool {
	l := common.BlockNumberLength + common.HashLength
	if len(k) != l {
		return false
	}

	return !IsHeaderHashKey(k) && !IsHeaderTDKey(k)
}

func IsHeaderTDKey(k []byte) bool {
	l := common.BlockNumberLength + common.HashLength + 1
	return len(k) == l && bytes.Equal(k[l-1:], HeaderTDSuffix)
}

// headerHashKey = headerPrefix + num (uint64 big endian) + headerHashSuffix
func HeaderHashKey(number uint64) []byte {
	return append(dbutils.EncodeBlockNumber(number), HeaderHashSuffix...)
}

func CheckCanonicalKey(k []byte) bool {
	return len(k) == 8+len(HeaderHashSuffix) && bytes.Equal(k[8:], HeaderHashSuffix)
}

func IsHeaderHashKey(k []byte) bool {
	l := common.BlockNumberLength + 1
	return len(k) == l && bytes.Equal(k[l-1:], HeaderHashSuffix)
}

var (
	HeaderTDSuffix   = []byte("t") // block_num_u64 + hash + headerTDSuffix -> td
	HeaderHashSuffix = []byte("n") // block_num_u64 + headerHashSuffix -> hash
)
