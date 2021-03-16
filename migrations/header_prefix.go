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
		if exists, err := db.(ethdb.BucketsMigrator).BucketExists(dbutils.HeaderPrefixOld); err != nil {
			return err
		} else if !exists {
			return CommitProgress(db, nil, true)
		}

		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.HeaderCanonicalBucket, dbutils.HeaderCanonicalBucket, dbutils.HeaderTDBucket); err != nil {
			return err
		}
		logPrefix := "split_header_prefix_bucket"
		const loadStep = "load"

		canonicalCollector, err:=etl.NewCollectorFromFiles(tmpdir+"canonical")
		if err!=nil {
			return err
		}
		tdCollector, err:=etl.NewCollectorFromFiles(tmpdir+"td")
		if err!=nil {
			return err
		}
		headersCollector, err:=etl.NewCollectorFromFiles(tmpdir+"headers")
		if err!=nil {
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
			if headersCollector == nil || canonicalCollector == nil|| tdCollector == nil {
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
		db.Walk(dbutils.HeaderPrefixOld, []byte{}, 0, func(k, v []byte) (bool, error) {
			var err error
			switch {
			case IsHeaderKey(k):

			case IsHeaderTDKey(k):
				err = canonicalCollector.Collect(bytes.TrimSuffix(k, HeaderTDSuffix),v)
			case IsHeaderHashKey(k):
				err = canonicalCollector.Collect(k,v)
			case CheckCanonicalKey(k):
				err = canonicalCollector.Collect(k,v)
			}
			if err!=nil {
				return false, err
			}
			return true,  nil
		})

		if err := etl.Transform(
			"canonical headers",
			db,
			dbutils.HeaderPrefixOld,
			dbutils.HeaderCanonicalBucket,
			tmpdir,
			func(k []byte, v []byte, next etl.ExtractNextFunc) error {
				if dbutils.CheckCanonicalKey(k) {
					return next(k, k, v)
				}
				return nil
			},
			etl.IdentityLoadFunc,
			etl.TransformArgs{OnLoadCommit: CommitProgress},
		); err != nil {
			return err
		}

		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.HeadersBucket); err != nil {
			return err
		}

		if err := etl.Transform(
			"headers",
			db,
			dbutils.HeaderPrefixOld,
			dbutils.HeadersBucket,
			tmpdir,
			func(k []byte, v []byte, next etl.ExtractNextFunc) error {
				if dbutils.CheckCanonicalKey(k) {
					return next(k, k, v)
				}
				return nil
			},
			etl.IdentityLoadFunc,
			etl.TransformArgs{OnLoadCommit: CommitProgress},
		); err != nil {
			return err
		}

		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.HeaderTDBucket); err != nil {
			return err
		}
		if err := etl.Transform(
			"headers td",
			db,
			dbutils.HeaderPrefixOld,
			dbutils.HeaderTDBucket,
			tmpdir,
			func(k []byte, v []byte, next etl.ExtractNextFunc) error {
				if dbutils.CheckCanonicalKey(k) {
					return next(k, k, v)
				}
				return nil
			},
			etl.IdentityLoadFunc,
			etl.TransformArgs{OnLoadCommit: CommitProgress},
		); err != nil {
			return err
		}

		if err := db.(ethdb.BucketsMigrator).DropBuckets(dbutils.CurrentStateBucketOld1); err != nil {
			return err
		}

	LoadStep:
		// Now transaction would have been re-opened, and we should be re-using the space
		if err = canonicalCollector.Load(logPrefix, db, dbutils.HeaderCanonicalBucket, etl.IdentityLoadFunc, etl.TransformArgs{
			OnLoadCommit: CommitProgress,
		}); err != nil {
			return fmt.Errorf("loading the transformed data back into the storage table: %w", err)
		}
		if err = tdCollector.Load(logPrefix, db, dbutils.HeaderTDBucket, etl.IdentityLoadFunc, etl.TransformArgs{
			OnLoadCommit: CommitProgress,
		}); err != nil {
			return fmt.Errorf("loading the transformed data back into the acc table: %w", err)
		}
		if err = headersCollector.Load(logPrefix, db, dbutils.HeadersBucket, etl.IdentityLoadFunc, etl.TransformArgs{
			OnLoadCommit: CommitProgress,
		}); err != nil {
			return fmt.Errorf("loading the transformed data back into the acc table: %w", err)
		}
		return nil
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
	HeaderTDSuffix     = []byte("t") // block_num_u64 + hash + headerTDSuffix -> td
	HeaderHashSuffix   = []byte("n") // block_num_u64 + headerHashSuffix -> hash
)

