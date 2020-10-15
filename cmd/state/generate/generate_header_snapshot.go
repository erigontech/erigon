package generate

import (
	"errors"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func HeaderSnapshot(dbPath, snapshotPath string, toBlock uint64) error {
	if snapshotPath == "" {
		return errors.New("empty snapshot path")
	}
	err := os.RemoveAll(snapshotPath)
	if err != nil {
		return err
	}
	kv := ethdb.NewLMDB().Path(dbPath).MustOpen()
	snkv := ethdb.NewLMDB().WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
		return dbutils.BucketsCfg{
			dbutils.HeaderPrefix:       dbutils.BucketConfigItem{},
			dbutils.SnapshotInfoBucket: dbutils.BucketConfigItem{},
		}
	}).Path(snapshotPath).MustOpen()

	db := ethdb.NewObjectDatabase(kv)
	sndb := ethdb.NewObjectDatabase(snkv)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	t := time.Now()
	chunkFile := 30000
	tuples := make(ethdb.MultiPutTuples, 0, chunkFile*3)
	var hash common.Hash
	var header []byte
	for i := uint64(1); i <= toBlock; i++ {
		select {
		case <-ch:
			return errors.New("interrupted")
		default:

		}
		hash, err = rawdb.ReadCanonicalHash(db, i)
		if err != nil {
			return fmt.Errorf("getting canonical hash for block %d: %v", i, err)
		}
		header = rawdb.ReadHeaderRLP(db, hash, i)
		tuples = append(tuples, []byte(dbutils.HeaderPrefix), dbutils.HeaderKey(i, hash), header)
		if len(tuples) >= chunkFile {
			log.Info("Committed", "block", i)
			_, err = sndb.MultiPut(tuples...)
			if err != nil {
				log.Crit("Multiput error", "err", err)
				return err
			}
			tuples = tuples[:0]
		}
	}

	if len(tuples) > 0 {
		_, err = sndb.MultiPut(tuples...)
		if err != nil {
			log.Crit("Multiput error", "err", err)
			return err
		}
	}

	err = sndb.Put(dbutils.SnapshotInfoBucket, []byte(dbutils.SnapshotHeadersHeadNumber), big.NewInt(0).SetUint64(toBlock).Bytes())
	if err != nil {
		log.Crit("SnapshotHeadersHeadNumber error", "err", err)
		return err
	}
	err = sndb.Put(dbutils.SnapshotInfoBucket, []byte(dbutils.SnapshotHeadersHeadHash), hash.Bytes())
	if err != nil {
		log.Crit("SnapshotHeadersHeadHash error", "err", err)
		return err
	}

	log.Info("Finished", "duration", time.Since(t))
	sndb.Close()
	err = os.Remove(snapshotPath + "/lock.mdb")
	if err != nil {
		log.Warn("Remove lock", "err", err)
		return err
	}

	return nil
}
