package core

import (
	"bytes"
	//"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"sort"
	"time"
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
	cache         map[string][]*dbutils.HistoryIndexBytes
}

func (ig *IndexGenerator) changeSetWalker(blockNum uint64) func([]byte, []byte) error {
	return func(k, v []byte) error {
		indexes, ok := ig.cache[string(k)]
		if !ok || len(indexes) == 0 {

			indexBytes, err := ig.db.GetIndexChunk(ig.bucketToWrite, k, blockNum)
			if err != nil {
				return err
			}
			var index *dbutils.HistoryIndexBytes

			if len(indexBytes) == 0 || dbutils.CheckNewIndexChunk(indexBytes) {
				index = dbutils.NewHistoryIndex()
			} else {
				index = dbutils.WrapHistoryIndex(indexBytes)
			}

			indexes = append(indexes, index)
			ig.cache[string(k)] = indexes
		}

		lastIndex := indexes[len(indexes)-1]
		if len(*lastIndex)+8 > dbutils.MaxChunkSize {
			lastIndex = dbutils.NewHistoryIndex()
			indexes = append(indexes, lastIndex)
			ig.cache[string(k)] = indexes
		}
		lastIndex.Append(blockNum)
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
	ig.cache = make(map[string][]*dbutils.HistoryIndexBytes, batchSize)

	//todo add truncate to all db
	if bolt, ok := ig.db.(*ethdb.BoltDatabase); ok {
		err := bolt.DeleteBucket(ig.bucketToWrite)
		if err != nil {
			return err
		}
	}

	commit := func() error {
		tuples := common.NewTuples(len(ig.cache), 3, 1)
		for key, vals := range ig.cache {
			for _, val := range vals {
				chunkKey, err := val.Key([]byte(key))
				if err != nil {
					return err
				}
				if err := tuples.Append(ig.bucketToWrite, chunkKey, *val); err != nil {
					return err
				}

			}
		}
		sort.Sort(tuples)
		_, err := ig.db.MultiPut(tuples.Values...)
		if err != nil {
			return err
		}
		ig.cache = make(map[string][]*dbutils.HistoryIndexBytes, batchSize)
		return nil
	}

	currentKey := []byte{}
	for {
		stop := true
		err := ig.db.Walk(ig.csBucket, currentKey, 0, func(k, v []byte) (b bool, e error) {
			blockNum, _ := dbutils.DecodeTimestamp(k)
			fmt.Println(blockNum, len(ig.cache), batchSize)
			currentKey = common.CopyBytes(k)
			err := ig.csWalker(v).Walk(ig.changeSetWalker(blockNum))
			if err != nil {
				return false, err
			}

			if len(ig.cache) > batchSize {
				log.Info("Next chunk", "currentKey", common.Bytes2Hex(currentKey), "time", time.Since(startTime))
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
