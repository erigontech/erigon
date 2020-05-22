package core

import (
	"encoding/binary"
	"errors"
	"sort"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func NewIndexGenerator(db ethdb.Database) *IndexGenerator {
	return &IndexGenerator{
		db:    db,
		cache: nil,
	}
}

type IndexGenerator struct {
	db    ethdb.Database
	cache map[string][]IndexWithKey
}

type IndexWithKey struct {
	Val dbutils.HistoryIndexBytes
}

func (ig *IndexGenerator) changeSetWalker(blockNum uint64, indexBucket []byte) func([]byte, []byte) error {
	return func(k, v []byte) error {
		cacheKey := k
		indexes, ok := ig.cache[string(cacheKey)]
		if !ok || len(indexes) == 0 {

			indexBytes, err := ig.db.GetIndexChunk(indexBucket, k, blockNum)
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
			ig.cache[string(cacheKey)] = indexes
		}

		lastIndex := indexes[len(indexes)-1]
		if dbutils.CheckNewIndexChunk(lastIndex.Val, blockNum) {
			lastIndex.Val = dbutils.NewHistoryIndex()
			indexes = append(indexes, lastIndex)
			ig.cache[string(cacheKey)] = indexes
		}
		lastIndex.Val = lastIndex.Val.Append(blockNum, len(v) == 0)
		indexes[len(indexes)-1] = lastIndex
		ig.cache[string(cacheKey)] = indexes

		return nil
	}
}

func (ig *IndexGenerator) GenerateIndex(from uint64, changeSetBucket []byte, indexBucket []byte, walkerAdapter func([]byte) ChangesetWalker, commitHook func(db ethdb.Database, blockNum uint64) error) error {
	startTime := time.Now()
	batchSize := ig.db.IdealBatchSize()
	//addrHash - > index or addhash + last block for full chunk contracts
	ig.cache = make(map[string][]IndexWithKey, batchSize)

	log.Info("Index generation started", "from", from)
	commit := func() error {
		tuples := make(ethdb.MultiPutTuples, 0, len(ig.cache)*3)
		for key, vals := range ig.cache {
			for i, val := range vals {
				var (
					chunkKey []byte
					err      error
				)
				if i == len(vals)-1 {
					chunkKey = dbutils.CurrentChunkKey([]byte(key))
				} else {
					chunkKey, err = val.Val.Key([]byte(key))
					if err != nil {
						return err
					}
				}
				tuples = append(tuples, indexBucket, chunkKey, val.Val)
			}
		}
		sort.Sort(tuples)
		_, err := ig.db.MultiPut(tuples...)
		if err != nil {
			log.Error("Unable to put index", "err", err)
			return err
		}
		ig.cache = make(map[string][]IndexWithKey, batchSize)
		return nil
	}

	var blockNum uint64
	currentKey := dbutils.EncodeBlockNumber(from)
	for {
		stop := true
		err := ig.db.Walk(changeSetBucket, currentKey, 0, func(k, v []byte) (b bool, e error) {
			blockNum, _ = dbutils.DecodeTimestamp(k)

			currentKey = common.CopyBytes(k)
			err := walkerAdapter(v).Walk(ig.changeSetWalker(blockNum, indexBucket))
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
			log.Info("Commit batch",
				"blocknum", blockNum,
				"time", time.Since(startTime),
				"chunk size", len(ig.cache),
			)
			err = commit()
			if err != nil {
				return err
			}
		}
		if commitHook != nil {
			err = commitHook(ig.db, blockNum)
			if err != nil {
				return err
			}
		}

		if stop {
			break
		}

	}

	log.Info("Generation index finished", "bucket", string(indexBucket))
	return nil
}

func (ig *IndexGenerator) Truncate(timestampTo uint64, changeSetBucket []byte, indexBucket []byte, walkerAdapter func([]byte) ChangesetWalker) error {
	currentKey := dbutils.EncodeBlockNumber(timestampTo)
	keys := make(map[string]struct{})
	err := ig.db.Walk(changeSetBucket, currentKey, 0, func(k, v []byte) (b bool, e error) {
		currentKey = common.CopyBytes(k)
		err := walkerAdapter(v).Walk(func(kk []byte, _ []byte) error {
			keys[string(kk)] = struct{}{}
			return nil
		})
		if err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		return err
	}

	accountHistoryEffects := make(map[string][]byte)
	var startKey = make([]byte, common.HashLength+8)

	for key := range keys {
		key := common.CopyBytes([]byte(key))
		copy(startKey, key)

		binary.BigEndian.PutUint64(startKey[common.HashLength:], timestampTo)
		if err := ig.db.Walk(indexBucket, startKey, 8*common.HashLength, func(k, v []byte) (bool, error) {
			timestamp := binary.BigEndian.Uint64(k[common.HashLength:]) // the last timestamp in the chunk
			kStr := string(common.CopyBytes(k))
			//fmt.Println("Truncate", common.Bytes2Hex(k), timestamp, timestampTo)
			if timestamp > timestampTo {
				accountHistoryEffects[kStr] = nil
				// truncate the chunk
				index := dbutils.WrapHistoryIndex(v)
				index = index.TruncateGreater(timestampTo)
				if len(index) > 8 { // If the chunk is empty after truncation, it gets simply deleted
					// Truncated chunk becomes "the last chunk" with the timestamp 0xffff....ffff
					lastK, err := index.Key(key)
					if err != nil {
						return false, err
					}
					accountHistoryEffects[string(lastK)] = common.CopyBytes(index)
				}
			}
			return true, nil
		}); err != nil {
			return err
		}
	}

	for key, value := range accountHistoryEffects {
		if value == nil {
			//fmt.Println("drop", common.Bytes2Hex([]byte(key)), binary.BigEndian.Uint64([]byte(key)[common.HashLength:]))
			if err := ig.db.Delete(indexBucket, []byte(key)); err != nil {
				return err
			}
		} else {
			//fmt.Println("write", common.Bytes2Hex([]byte(key)), binary.BigEndian.Uint64([]byte(key)[common.HashLength:]))
			if err := ig.db.Put(indexBucket, []byte(key), value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ig *IndexGenerator) DropIndex(bucket []byte) error {
	//todo add truncate to all db
	if bolt, ok := ig.db.(*ethdb.BoltDatabase); ok {
		log.Warn("Remove bucket", "bucket", string(bucket))
		err := bolt.DeleteBucket(bucket)
		if err != nil {
			return err
		}
	}
	return errors.New("imposible to drop")
}

type ChangesetWalker interface {
	Walk(func([]byte, []byte) error) error
}
