package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

const DeleteLimit = 70000

type BlockChainer interface {
	CurrentBlock() *types.Block
}

func NewBasicPruner(database ethdb.Database, chainer BlockChainer, config *CacheConfig) (*BasicPruner, error) {
	if config.BlocksToPrune == 0 || config.PruneTimeout.Seconds() < 1 {
		return nil, errors.New("incorrect config")
	}

	return &BasicPruner{
		wg:                 new(sync.WaitGroup),
		db:                 database,
		chain:              chainer,
		config:             config,
		LastPrunedBlockNum: 0,
		stop:               make(chan struct{}, 1),
	}, nil
}

type BasicPruner struct {
	wg   *sync.WaitGroup
	stop chan struct{}

	db                 ethdb.Database
	chain              BlockChainer
	LastPrunedBlockNum uint64
	config             *CacheConfig
}

func (p *BasicPruner) Start() error {
	db := p.db
	p.LastPrunedBlockNum = p.ReadLastPrunedBlockNum()
	p.wg.Add(1)
	go p.pruningLoop(db)
	log.Info("Pruner started")

	return nil
}
func (p *BasicPruner) pruningLoop(db ethdb.Database) {
	prunerRun := time.NewTicker(p.config.PruneTimeout)
	saveLastPrunedBlockNum := time.NewTicker(time.Minute * 5)
	defer prunerRun.Stop()
	defer saveLastPrunedBlockNum.Stop()
	defer p.wg.Done()
	for {
		select {
		case <-p.stop:
			p.WriteLastPrunedBlockNum(p.LastPrunedBlockNum)
			log.Info("Pruning stopped")
			return
		case <-saveLastPrunedBlockNum.C:
			log.Info("Save last pruned block num", "num", p.LastPrunedBlockNum)
			p.WriteLastPrunedBlockNum(p.LastPrunedBlockNum)
		case <-prunerRun.C:
			cb := p.chain.CurrentBlock()
			if cb == nil || cb.Number() == nil {
				continue
			}
			from, to, ok := calculateNumOfPrunedBlocks(cb.Number().Uint64(), p.LastPrunedBlockNum, p.config.BlocksBeforePruning, p.config.BlocksToPrune)
			if !ok {
				continue
			}
			log.Debug("Pruning history", "from", from, "to", to)
			err := Prune(db, from, to)
			if err != nil {
				log.Error("Pruning error", "err", err)
				return
			}
			p.LastPrunedBlockNum = to
		}
	}
}

func calculateNumOfPrunedBlocks(curentBlock, lastPrunedBlock uint64, blocksBeforePruning uint64, blocksBatch uint64) (uint64, uint64, bool) {
	diff := curentBlock - lastPrunedBlock - blocksBeforePruning
	switch {
	case diff >= blocksBatch:
		return lastPrunedBlock, lastPrunedBlock + blocksBatch, true
	case diff > 0 && diff < blocksBatch:
		return lastPrunedBlock, lastPrunedBlock + diff, true
	default:
		return lastPrunedBlock, lastPrunedBlock, false
	}
}
func (p *BasicPruner) Stop() {
	p.stop <- struct{}{}
	p.wg.Wait()
	log.Info("Pruning stopped")
}

func (p *BasicPruner) ReadLastPrunedBlockNum() uint64 {
	data, _ := p.db.Get(dbutils.LastPrunedBlockKey, dbutils.LastPrunedBlockKey)
	if len(data) == 0 {
		return 0
	}
	return binary.LittleEndian.Uint64(data)
}

// WriteHeadBlockHash stores the head block's hash.
func (p *BasicPruner) WriteLastPrunedBlockNum(num uint64) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, num)
	if err := p.db.Put(dbutils.LastPrunedBlockKey, dbutils.LastPrunedBlockKey, b); err != nil {
		log.Crit("Failed to store last pruned block's num", "err", err)
	}
}

func Prune(db ethdb.Database, blockNumFrom uint64, blockNumTo uint64) error {
	keysToRemove := newKeysToRemove()
	err := db.Walk(dbutils.ChangeSetBucket, []byte{}, 0, func(key, v []byte) (b bool, e error) {
		timestamp, _ := dbutils.DecodeTimestamp(key)
		if timestamp < blockNumFrom {
			return true, nil
		}
		if timestamp > blockNumTo {
			return false, nil
		}

		keysToRemove.Suffix = append(keysToRemove.Suffix, key)

		changedKeys, err := dbutils.Decode(v)
		if err != nil {
			return false, err
		}

		err = changedKeys.Walk(func(cKey, _ []byte) error {
			compKey, _ := dbutils.CompositeKeySuffix(cKey, timestamp)
			if bytes.HasSuffix(cKey, dbutils.AccountsHistoryBucket) {
				keysToRemove.AccountHistoryKeys = append(keysToRemove.AccountHistoryKeys, compKey)
			}
			if bytes.HasSuffix(cKey, dbutils.StorageHistoryBucket) {
				keysToRemove.StorageHistoryKeys = append(keysToRemove.StorageHistoryKeys, compKey)
			}
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
	err = batchDelete(db, keysToRemove)
	if err != nil {
		return err
	}

	return nil
}

func batchDelete(db ethdb.Database, keys *keysToRemove) error {
	log.Debug("Removing: ", "accounts", len(keys.AccountHistoryKeys), "storage", len(keys.StorageHistoryKeys), "suffix", len(keys.Suffix))
	iterator := LimitIterator(keys, DeleteLimit)
	for iterator.HasMore() {
		iterator.ResetLimit()
		batch := db.NewBatch()
		for {
			key, bucketKey, ok := iterator.GetNext()
			if !ok {
				break
			}
			err := batch.Delete(bucketKey, key)
			if err != nil {
				log.Warn("Unable to remove", "bucket", bucketKey, "addr", common.Bytes2Hex(key), "err", err)
				continue
			}
		}
		_, err := batch.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

func newKeysToRemove() *keysToRemove {
	return &keysToRemove{
		AccountHistoryKeys: make([][]byte, 0),
		StorageHistoryKeys: make([][]byte, 0),
		Suffix:             make([][]byte, 0),
	}
}

type keysToRemove struct {
	AccountHistoryKeys [][]byte
	StorageHistoryKeys [][]byte
	Suffix             [][]byte
}

func LimitIterator(k *keysToRemove, limit int) *limitIterator {
	return &limitIterator{
		k:     k,
		limit: limit,
	}
}

type limitIterator struct {
	k             *keysToRemove
	counter       uint64
	currentBucket []byte
	currentNum    int
	limit         int
}

func (i *limitIterator) GetNext() ([]byte, []byte, bool) {
	if i.limit <= i.currentNum {
		return nil, nil, false
	}
	i.updateBucket()
	if !i.HasMore() {
		return nil, nil, false
	}
	defer func() {
		i.currentNum++
		i.counter++
	}()
	if bytes.Equal(i.currentBucket, dbutils.AccountsHistoryBucket) {
		return i.k.AccountHistoryKeys[i.currentNum], dbutils.AccountsHistoryBucket, true
	}
	if bytes.Equal(i.currentBucket, dbutils.StorageHistoryBucket) {
		return i.k.StorageHistoryKeys[i.currentNum], dbutils.StorageHistoryBucket, true
	}
	if bytes.Equal(i.currentBucket, dbutils.ChangeSetBucket) {
		return i.k.Suffix[i.currentNum], dbutils.ChangeSetBucket, true
	}
	return nil, nil, false
}

func (i *limitIterator) ResetLimit() {
	i.counter = 0
}

func (i *limitIterator) HasMore() bool {
	if bytes.Equal(i.currentBucket, dbutils.ChangeSetBucket) && len(i.k.Suffix) == i.currentNum {
		return false
	}
	return true
}

func (i *limitIterator) updateBucket() {
	if i.currentBucket == nil {
		i.currentBucket = dbutils.AccountsHistoryBucket
	}
	if bytes.Equal(i.currentBucket, dbutils.AccountsHistoryBucket) && len(i.k.AccountHistoryKeys) == i.currentNum {
		i.currentBucket = dbutils.StorageHistoryBucket
		i.currentNum = 0
	}

	if bytes.Equal(i.currentBucket, dbutils.StorageHistoryBucket) && len(i.k.StorageHistoryKeys) == i.currentNum {
		i.currentBucket = dbutils.ChangeSetBucket
		i.currentNum = 0
	}
}
