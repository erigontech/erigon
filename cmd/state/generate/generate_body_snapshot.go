package generate

import (
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func BodySnapshot(dbPath, snapshotPath string, toBlock uint64) error {
	kv := ethdb.NewLMDB().Path(dbPath).MustOpen()
	snkv := ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.BlockBodyPrefix:    dbutils.BucketConfigItem{},
			dbutils.SnapshotInfoBucket: dbutils.BucketConfigItem{},
		}
	}).Path(snapshotPath).MustOpen()
	db := ethdb.NewObjectDatabase(kv)
	sndb := ethdb.NewObjectDatabase(snkv)

	t := time.Now()
	chunkFile := 30000
	tuples := make(ethdb.MultiPutTuples, 0, chunkFile*3+100)
	var hash common.Hash
	var err error
	for i := uint64(1); i <= toBlock; i++ {
		hash, err = rawdb.ReadCanonicalHash(db, i)
		if err != nil {
			return fmt.Errorf("getting canonical hash for block %d: %v", i, err)
		}
		body := rawdb.ReadBodyRLP(db, hash, i)
		tuples = append(tuples, []byte(dbutils.BlockBodyPrefix), dbutils.BlockBodyKey(i, hash), body)
		if len(tuples) >= chunkFile {
			log.Info("Committed", "block", i)
			_, err := sndb.MultiPut(tuples...)
			if err != nil {
				log.Crit("Multiput error", "err", err)
				return err
			}
			tuples = tuples[:0]
		}
	}

	if len(tuples) > 0 {
		if _, err = sndb.MultiPut(tuples...); err != nil {
			log.Crit("Multiput error", "err", err)
			return err
		}
	}

	err = sndb.Put(dbutils.SnapshotInfoBucket, []byte(dbutils.SnapshotBodyHeadNumber), big.NewInt(0).SetUint64(toBlock).Bytes())
	if err != nil {
		log.Crit("SnapshotBodyHeadNumber error", "err", err)
		return err
	}
	err = sndb.Put(dbutils.SnapshotInfoBucket, []byte(dbutils.SnapshotBodyHeadHash), hash.Bytes())
	if err != nil {
		log.Crit("SnapshotBodyHeadHash error", "err", err)
		return err
	}
	sndb.Close()
	err = os.Remove(snapshotPath + "/lock.mdb")
	if err != nil {
		log.Warn("Remove lock", "err", err)
		return err
	}

	log.Info("Finished", "duration", time.Since(t))
	return nil
}
