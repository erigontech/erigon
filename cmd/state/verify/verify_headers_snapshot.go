package verify

import (
	"context"
	"errors"

	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

func HeadersSnapshot(snapshotPath string) error {
	snKV := ethdb.NewLMDB().Path(snapshotPath).Flags(func(flags uint) uint { return flags | lmdb.Readonly }).WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeadersBucket:             dbutils.BucketConfigItem{},
			dbutils.HeadersSnapshotInfoBucket: dbutils.BucketConfigItem{},
		}
	}).MustOpen()
	var prevHeader *types.Header
	err := snKV.View(context.Background(), func(tx ethdb.Tx) error {
		c, err := tx.Cursor(dbutils.HeadersBucket)
		if err != nil {
			return err
		}
		k, v, innerErr := c.First()
		for {
			if len(k) == 0 && len(v) == 0 {
				break
			}
			if innerErr != nil {
				return innerErr
			}

			header := new(types.Header)
			innerErr := rlp.DecodeBytes(v, header)
			if innerErr != nil {
				return innerErr
			}

			if prevHeader != nil {
				if prevHeader.Number.Uint64()+1 != header.Number.Uint64() {
					log.Error("invalid header number", "p", prevHeader.Number.Uint64(), "c", header.Number.Uint64())
					return errors.New("invalid header number")
				}
				if prevHeader.HashCache() != header.ParentHash {
					log.Error("invalid parent hash", "p", prevHeader.HashCache(), "c", header.ParentHash)
					return errors.New("invalid parent hash")
				}
			}
			k, v, innerErr = c.Next() //nolint
			prevHeader = header
		}
		return nil
	})
	return err
}
