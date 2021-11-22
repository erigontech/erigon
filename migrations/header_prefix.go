package migrations

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
)

var headerPrefixToSeparateBuckets = Migration{
	Name: "header_prefix_to_separate_buckets",
	Up: func(db kv.RwDB, tmpdir string, progress []byte, BeforeCommit Callback) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		exists, err := tx.ExistsBucket(kv.HeaderPrefixOld)
		if err != nil {
			return err
		}
		if !exists {
			if err := BeforeCommit(tx, nil, true); err != nil {
				return err
			}
			return tx.Commit()
		}

		if err = tx.ClearBucket(kv.HeaderCanonical); err != nil {
			return err
		}
		if err = tx.ClearBucket(kv.HeaderTD); err != nil {
			return err
		}
		logPrefix := "split_header_prefix_bucket"
		const loadStep = "load"

		canonicalCollector, err := etl.NewCollectorFromFiles(logPrefix, tmpdir+"canonical")
		if err != nil {
			return err
		}
		tdCollector, err := etl.NewCollectorFromFiles(logPrefix, tmpdir+"td")
		if err != nil {
			return err
		}
		headersCollector, err := etl.NewCollectorFromFiles(logPrefix, tmpdir+"headers")
		if err != nil {
			return err
		}

		switch string(progress) {
		case "":
			// can't use files if progress field not set, clear them
			if canonicalCollector != nil {
				canonicalCollector.Close()
				canonicalCollector = nil
			}

			if tdCollector != nil {
				tdCollector.Close()
				tdCollector = nil
			}
			if headersCollector != nil {
				headersCollector.Close()
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
				canonicalCollector.Close()
				tdCollector.Close()
				headersCollector.Close()
			}()
			goto LoadStep
		}

		canonicalCollector = etl.NewCriticalCollector(logPrefix, tmpdir+"canonical", etl.NewSortableBuffer(etl.BufferOptimalSize*4))
		tdCollector = etl.NewCriticalCollector(logPrefix, tmpdir+"td", etl.NewSortableBuffer(etl.BufferOptimalSize*4))
		headersCollector = etl.NewCriticalCollector(logPrefix, tmpdir+"headers", etl.NewSortableBuffer(etl.BufferOptimalSize*4))
		defer func() {
			// don't clean if error or panic happened
			if err != nil {
				return
			}
			if rec := recover(); rec != nil {
				panic(rec)
			}
			canonicalCollector.Close()
			tdCollector.Close()
			headersCollector.Close()
		}()

		err = tx.ForEach(kv.HeaderPrefixOld, []byte{}, func(k, v []byte) error {
			var innerErr error
			switch {
			case IsHeaderKey(k):
				innerErr = headersCollector.Collect(k, v)
			case IsHeaderTDKey(k):
				innerErr = tdCollector.Collect(bytes.TrimSuffix(k, HeaderTDSuffix), v)
			case IsHeaderHashKey(k):
				innerErr = canonicalCollector.Collect(bytes.TrimSuffix(k, HeaderHashSuffix), v)
			default:
				return fmt.Errorf("incorrect header prefix key: %v", common.Bytes2Hex(k))
			}
			if innerErr != nil {
				return innerErr
			}
			return nil
		})
		if err = tx.DropBucket(kv.HeaderPrefixOld); err != nil {
			return err
		}

	LoadStep:
		// Now transaction would have been re-opened, and we should be re-using the space
		if err = canonicalCollector.Load(tx, kv.HeaderCanonical, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
			return fmt.Errorf("loading the transformed data back into the storage table: %w", err)
		}
		if err = tdCollector.Load(tx, kv.HeaderTD, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
			return fmt.Errorf("loading the transformed data back into the acc table: %w", err)
		}
		if err = headersCollector.Load(tx, kv.Headers, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
			return fmt.Errorf("loading the transformed data back into the acc table: %w", err)
		}
		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
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
