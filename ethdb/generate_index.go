package ethdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"
	"sort"
	"time"
)

type Walker interface {
	Walk(func([]byte, []byte) error) error
}

const maxChunkSize = 1000

type IndexGenerator struct {
	db            Database
	csBucket      []byte
	bucketToWrite []byte
	fixedBits     uint
	csWalker      func([]byte) Walker
	cache         map[string][]*dbutils.HistoryIndexBytes
}

func (ig *IndexGenerator) changeSetWalker(blockNum uint64) func([]byte, []byte) error {
	return func(k, v []byte) error {
		indexes, ok := ig.cache[string(k)]
		if !ok || len(indexes) == 0 {
			blockBin := make([]byte, 8)
			binary.BigEndian.PutUint64(blockBin, ^(blockNum))
			var index *dbutils.HistoryIndexBytes
			key := common.CopyBytes(k)
			key = append(key, blockBin...)
			err := ig.db.Walk(ig.bucketToWrite, key, 0, func(kk []byte, vv []byte) (b bool, e error) {
				index = dbutils.WrapHistoryIndex(common.CopyBytes(vv))
				return false, nil
			})
			if err != nil {
				return err
			}
			if index == nil || len(*index)+8 > maxChunkSize {
				index = dbutils.NewHistoryIndex()
			}

			indexes = append(indexes, index)
			ig.cache[string(k)] = indexes
		}

		lastIndex := indexes[len(indexes)-1]
		if len(*lastIndex)+8 > maxChunkSize {
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
func (ig *IndexGenerator) generateIndex() error {
	ts := time.Now()
	defer func() {
		fmt.Println("end:", time.Now().Sub(ts))
	}()
	batchSize := ig.db.IdealBatchSize() * 3
	//addrHash - > index or addhash + inverted firshBlock for full chunk contracts
	ig.cache = make(map[string][]*dbutils.HistoryIndexBytes, batchSize)

	commit := func() error {
		blockNumBytes := make([]byte, 8)
		tuples := common.NewTuples(len(ig.cache), 3, 1)
		for key, vals := range ig.cache {
			for _, val := range vals {
				blockNum, ok := val.FirstElement()
				if !ok {
					return errors.New("empty index")
				}
				binary.BigEndian.PutUint64(blockNumBytes, ^(blockNum))
				if err := tuples.Append(ig.bucketToWrite, append([]byte(key), blockNumBytes...), *val); err != nil {
					return err
				}

			}
		}
		sort.Sort(tuples)
		fmt.Println("Multiput")
		_, err := ig.db.MultiPut(tuples.Values...)
		fmt.Println("Multiput end")
		if err != nil {
			return err
		}
		ig.cache = make(map[string][]*dbutils.HistoryIndexBytes, batchSize)
		return nil
	}
	maxSize := 0
	defer func() {
		fmt.Println("maxSize", maxSize)
	}()

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
				stop = false
				return false, nil
			}

			return true, nil
		})

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
