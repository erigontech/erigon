package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/ledgerwatch/turbo-geth/common/changeset"

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
		return nil, fmt.Errorf("incorrect config BlocksToPrune - %v, PruneTimeout - %v", config.BlocksToPrune, config.PruneTimeout.Seconds())
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
			err = PruneStorageOfSelfDestructedAccounts(db)
			if err != nil {
				log.Error("PruneStorageOfSelfDestructedAccounts error", "err", err)
				return
			}
			p.LastPrunedBlockNum = to
		}
	}
}

func calculateNumOfPrunedBlocks(currentBlock, lastPrunedBlock uint64, blocksBeforePruning uint64, blocksBatch uint64) (uint64, uint64, bool) {
	//underflow see https://github.com/ledgerwatch/turbo-geth/issues/115
	if currentBlock <= lastPrunedBlock {
		return lastPrunedBlock, lastPrunedBlock, false
	}

	diff := currentBlock - lastPrunedBlock
	if diff <= blocksBeforePruning {
		return lastPrunedBlock, lastPrunedBlock, false
	}
	diff = diff - blocksBeforePruning
	switch {
	case diff >= blocksBatch:
		return lastPrunedBlock, lastPrunedBlock + blocksBatch, true
	case diff < blocksBatch:
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
	data, _ := p.db.Get(dbutils.DatabaseInfoBucket, dbutils.LastPrunedBlockKey)
	if len(data) == 0 {
		return 0
	}
	return binary.LittleEndian.Uint64(data)
}

// WriteHeadBlockHash stores the head block's hash.
func (p *BasicPruner) WriteLastPrunedBlockNum(num uint64) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, num)
	if err := p.db.Put(dbutils.DatabaseInfoBucket, dbutils.LastPrunedBlockKey, b); err != nil {
		log.Crit("Failed to store last pruned block's num", "err", err)
	}
}

func PruneStorageOfSelfDestructedAccounts(db ethdb.Database) error {
	keysToRemove := newKeysToRemove()
	if err := db.Walk(dbutils.IntermediateTrieHashBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		if len(v) > 0 && len(k) != common.HashLength { // marker of self-destructed account is - empty value
			return true, nil
		}

		if err := db.Walk(dbutils.CurrentStateBucket, k, common.HashLength*8, func(k, _ []byte) (b bool, e error) {
			keysToRemove.StorageKeys = append(keysToRemove.StorageKeys, common.CopyBytes(k))
			return true, nil
		}); err != nil {
			return false, err
		}

		if err := db.Walk(dbutils.IntermediateTrieHashBucket, k, common.HashLength*8, func(k, _ []byte) (b bool, e error) {
			keysToRemove.IntermediateTrieHashKeys = append(keysToRemove.IntermediateTrieHashKeys, common.CopyBytes(k))
			return true, nil
		}); err != nil {
			return false, err
		}

		return true, nil
	}); err != nil {
		return err
	}

	//return batchDelete(db, keysToRemove)
	log.Debug("PruneStorageOfSelfDestructedAccounts can remove rows amount", "storage_bucket", len(keysToRemove.StorageKeys), "intermediate_bucket", len(keysToRemove.IntermediateTrieHashKeys))

	return nil
}

func Prune(db ethdb.Database, blockNumFrom uint64, blockNumTo uint64) error {
	keysToRemove := newKeysToRemove()
	err := db.Walk(dbutils.AccountChangeSetBucket, []byte{}, 0, func(key, v []byte) (b bool, e error) {
		timestamp, _ := dbutils.DecodeTimestamp(key)
		if timestamp < blockNumFrom {
			return true, nil
		}
		if timestamp > blockNumTo {
			return false, nil
		}

		keysToRemove.AccountChangeSet = append(keysToRemove.AccountChangeSet, common.CopyBytes(key))

		innerErr := changeset.AccountChangeSetBytes(v).Walk(func(cKey, _ []byte) error {
			compKey, _ := dbutils.CompositeKeySuffix(cKey, timestamp)
			keysToRemove.AccountHistoryKeys = append(keysToRemove.AccountHistoryKeys, compKey)
			return nil
		})
		if innerErr != nil {
			return false, innerErr
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	err = db.Walk(dbutils.StorageChangeSetBucket, []byte{}, 0, func(key, v []byte) (b bool, e error) {
		timestamp, _ := dbutils.DecodeTimestamp(key)
		if timestamp < blockNumFrom {
			return true, nil
		}
		if timestamp > blockNumTo {
			return false, nil
		}

		keysToRemove.StorageChangeSet = append(keysToRemove.StorageChangeSet, common.CopyBytes(key))

		noop := func(cKey, _ []byte) error {
			//todo implement pruning for thin history
			return nil
		}

		if innerErr := changeset.StorageChangeSetBytes(v).Walk(noop); innerErr != nil {
			return false, innerErr
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
	log.Debug("Removing: ", "accounts", len(keys.AccountHistoryKeys), "storage", len(keys.StorageHistoryKeys), "suffix", len(keys.AccountChangeSet))
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
		AccountHistoryKeys:       make(Keys, 0),
		StorageHistoryKeys:       make(Keys, 0),
		AccountChangeSet:         make(Keys, 0),
		StorageChangeSet:         make(Keys, 0),
		StorageKeys:              make(Keys, 0),
		IntermediateTrieHashKeys: make(Keys, 0),
	}
}

type Keys [][]byte
type Batch struct {
	bucket []byte
	keys   Keys
}

type keysToRemove struct {
	AccountHistoryKeys       Keys
	StorageHistoryKeys       Keys
	AccountChangeSet         Keys
	StorageChangeSet         Keys
	StorageKeys              Keys
	IntermediateTrieHashKeys Keys
}

func LimitIterator(k *keysToRemove, limit int) *limitIterator {
	i := &limitIterator{
		k:     k,
		limit: limit,
	}

	i.batches = []Batch{
		{bucket: dbutils.AccountsHistoryBucket, keys: i.k.AccountHistoryKeys},
		{bucket: dbutils.StorageHistoryBucket, keys: i.k.StorageHistoryKeys},
		{bucket: dbutils.CurrentStateBucket, keys: i.k.StorageKeys},
		{bucket: dbutils.AccountChangeSetBucket, keys: i.k.AccountChangeSet},
		{bucket: dbutils.StorageChangeSetBucket, keys: i.k.StorageChangeSet},
		{bucket: dbutils.IntermediateTrieHashBucket, keys: i.k.IntermediateTrieHashKeys},
	}

	return i
}

type limitIterator struct {
	k             *keysToRemove
	counter       uint64
	currentBucket []byte
	currentNum    int
	limit         int
	batches       []Batch
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

	for batchIndex, batch := range i.batches {
		if batchIndex == len(i.batches)-1 {
			break
		}
		if bytes.Equal(i.currentBucket, batch.bucket) {
			return batch.keys[i.currentNum], batch.bucket, true
		}
	}
	return nil, nil, false
}

func (i *limitIterator) ResetLimit() {
	i.counter = 0
}

func (i *limitIterator) HasMore() bool {
	lastBatch := i.batches[len(i.batches)-1]
	if bytes.Equal(i.currentBucket, lastBatch.bucket) && len(lastBatch.keys) == i.currentNum {
		return false
	}

	return true
}

func (i *limitIterator) updateBucket() {
	if i.currentBucket == nil {
		i.currentBucket = i.batches[0].bucket
	}

	for batchIndex, batch := range i.batches {
		if batchIndex == len(i.batches)-1 {
			break
		}

		if bytes.Equal(i.currentBucket, batch.bucket) && len(batch.keys) == i.currentNum {
			i.currentBucket = i.batches[batchIndex+1].bucket
			i.currentNum = 0
		}
	}
}
