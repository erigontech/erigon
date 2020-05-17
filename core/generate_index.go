package core

import (
	"bytes"
	"sort"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func NewIndexGenerator(db ethdb.Database, changeSetBucket []byte, indexBucket []byte, walkerAdapter func([]byte) ChangesetWalker) *IndexGenerator {
	fixedBits := uint(common.HashLength)
	if bytes.Equal(changeSetBucket, dbutils.StorageChangeSetBucket) {
		fixedBits = common.HashLength*2 + common.IncarnationLength
	}
	return &IndexGenerator{
		db:            db,
		csBucket:      changeSetBucket,
		bucketToWrite: indexBucket,
		fixedBits:     fixedBits,
		csWalker:      walkerAdapter,
		cache:         nil,
	}
}

type IndexGenerator struct {
	db            ethdb.Database
	csBucket      []byte
	bucketToWrite []byte
	fixedBits     uint
	csWalker      func([]byte) ChangesetWalker
	cache         map[string][]IndexWithKey
}

type IndexWithKey struct {
	Val dbutils.HistoryIndexBytes
}

func (ig *IndexGenerator) changeSetWalker(blockNum uint64) func([]byte, []byte) error {
	return func(k, v []byte) error {
		indexes, ok := ig.cache[string(k)]
		if !ok || len(indexes) == 0 {

			indexBytes, err := ig.db.GetIndexChunk(ig.bucketToWrite, k, blockNum)
			if err != nil && err != ethdb.ErrKeyNotFound {
				return err
			}
			var index dbutils.HistoryIndexBytes

			if len(indexBytes) == 0 {
				index = dbutils.NewHistoryIndex()
			} else if dbutils.CheckNewIndexChunk(indexBytes, blockNum) {
				index = dbutils.NewHistoryIndex()
			} else {
				index = dbutils.WrapHistoryIndex(indexBytes)
			}

			indexes = append(indexes, IndexWithKey{
				Val: index,
			})
			ig.cache[string(k)] = indexes
		}

		lastIndex := indexes[len(indexes)-1]
		if dbutils.CheckNewIndexChunk(lastIndex.Val, blockNum) {
			lastIndex.Val = dbutils.NewHistoryIndex()
			indexes = append(indexes, lastIndex)
			ig.cache[string(k)] = indexes
		}
		lastIndex.Val = lastIndex.Val.Append(blockNum, len(v) == 0)
		return nil
	}
}

/*
1) Ключ - адрес контрата/ключа + инвертированный номер первого блока в куске индекса
2) Поиск - Walk(bucket, contractAddress+blocknum, fixedBits=32/64, walker{
	в k будет нужный индекс, если он есть
	return false, nil
})
*/
func (ig *IndexGenerator) GenerateIndex() error {
	startTime := time.Now()
	batchSize := ig.db.IdealBatchSize() * 3
	//addrHash - > index or addhash + inverted firshBlock for full chunk contracts
	ig.cache = make(map[string][]IndexWithKey, batchSize)

	//todo add truncate to all db
	if bolt, ok := ig.db.(*ethdb.BoltDatabase); ok {
		log.Warn("Remove bucket", "bucket", string(ig.bucketToWrite))
		err := bolt.DeleteBucket(ig.bucketToWrite)
		if err != nil {
			return err
		}
	}

	commit := func() error {
		tuples := make(ethdb.MultiPutTuples, 0, len(ig.cache)*3)
		for key, vals := range ig.cache {
			for _, val := range vals {
				var (
					chunkKey []byte
					err      error
				)
				chunkKey, err = val.Val.Key([]byte(key))
				if err != nil {
					return err
				}
				tuples = append(tuples, ig.bucketToWrite, chunkKey, val.Val)
			}
		}
		sort.Sort(tuples)
		_, err := ig.db.MultiPut(tuples...)
		if err != nil {
			return err
		}
		ig.cache = make(map[string][]IndexWithKey, batchSize)
		return nil
	}

	currentKey := []byte{}
	for {
		stop := true
		err := ig.db.Walk(ig.csBucket, currentKey, 0, func(k, v []byte) (b bool, e error) {
			blockNum, _ := dbutils.DecodeTimestamp(k)
			currentKey = common.CopyBytes(k)
			err := ig.csWalker(v).Walk(ig.changeSetWalker(blockNum))
			if err != nil {
				return false, err
			}

			if len(ig.cache) > batchSize {
				log.Info("Next chunk",
					"blocknum", blockNum,
					"time", time.Since(startTime),
					"chunk size", len(ig.cache),
				)
				stop = false
				return false, nil
			}

			return true, nil
		})
		if err != nil {
			return err
		}

		if len(ig.cache) > 0 {
			err = commit()
			if err != nil {
				return err
			}
		}
		if stop {
			break
		}

	}

	log.Info("Generation index finished", "bucket", string(ig.bucketToWrite))
	return nil
}

type ChangesetWalker interface {
	Walk(func([]byte, []byte) error) error
}
