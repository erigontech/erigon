package core

import (
	"bytes"
	"errors"
	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"math/big"
	"sync"
	"time"
)

const NumOfPrunedBlocks = 10

type BlockChainer interface {
	CurrentBlock() *types.Block
}

func NewBasicPruner(database ethdb.Database, chainer BlockChainer, config *CacheConfig) *BasicPruner {
	return &BasicPruner{
		wg:                 new(sync.WaitGroup),
		db:                 database,
		chain:              chainer,
		config:             config,
		LastPrunedBlockNum: new(big.Int),
		stop:               make(chan struct{}, 1),
	}
}

type BasicPruner struct {
	sync.RWMutex
	wg   *sync.WaitGroup
	stop chan struct{}

	db                 ethdb.Database
	chain              BlockChainer
	LastPrunedBlockNum *big.Int
	config             *CacheConfig
}

func (p *BasicPruner) Start() error {
	db, ok := p.db.(*ethdb.BoltDatabase)
	if !ok {
		return errors.New("it's not ethdb.BoltDatabase")
	}
	if p.config.BlocksToPrune == 0 || p.config.PruneTimeout.Seconds() < 1 {
		return errors.New("incorrect config")
	}
	p.Lock()
	p.LastPrunedBlockNum = p.ReadLastPrunedBlockNum()
	p.Unlock()
	p.wg.Add(1)
	go p.pruningLoop(db)
	log.Info("Pruner started")

	return nil
}
func (p *BasicPruner) pruningLoop(db *ethdb.BoltDatabase) {
	prunerRun := time.NewTicker(p.config.PruneTimeout)
	saveLastPrunedBlockNum := time.NewTicker(time.Minute * 5)
	defer prunerRun.Stop()
	defer saveLastPrunedBlockNum.Stop()
	defer p.wg.Done()
	for {
		select {
		case <-p.stop:
			p.Lock()
			p.WriteLastPrunedBlockNum(p.LastPrunedBlockNum)
			p.Unlock()
			log.Info("Pruning stopped")
			return
		case <-saveLastPrunedBlockNum.C:
			p.Lock()
			log.Info("Save last pruned block num", "num", p.LastPrunedBlockNum.Uint64())
			p.WriteLastPrunedBlockNum(p.LastPrunedBlockNum)
			p.Unlock()
		case <-prunerRun.C:
			cb := p.chain.CurrentBlock()
			if cb == nil || cb.Number() == nil {
				continue
			}
			p.RLock()
			numOfBlocks := calculateNumOfPrunedBlocks(cb.Number().Uint64(), p.LastPrunedBlockNum.Uint64(), p.config.BlocksBeforePruning, p.config.BlocksToPrune)
			p.RUnlock()
			log.Debug("Run pruning", "numOfBlocks", numOfBlocks)
			if numOfBlocks == 0 {
				continue
			}
			p.RLock()
			from := p.LastPrunedBlockNum.Uint64()
			to := p.LastPrunedBlockNum.Uint64() + numOfBlocks
			p.RUnlock()
			log.Debug("Pruning history", "from", from, "to", to)
			err := Prune(db, from, to)
			if err != nil {
				log.Error("Pruning error", "err", err)
				return
			}
			p.Lock()
			p.LastPrunedBlockNum.SetUint64(to)
			p.Unlock()
		}
	}
}

func calculateNumOfPrunedBlocks(curentBlock, lastPrunedBlock uint64, blocksBeforePruning uint64, blocksBatch uint64) uint64 {
	diff := curentBlock - lastPrunedBlock - blocksBeforePruning
	switch {
	case diff >= blocksBatch:
		return blocksBatch
	case diff > 0 && diff < blocksBatch:
		return diff
	default:
		return 0
	}
}
func (p *BasicPruner) Stop() {
	p.stop <- struct{}{}
	p.wg.Wait()
	log.Info("Pruning stopped")
}

func (p *BasicPruner) ReadLastPrunedBlockNum() *big.Int {
	data, _ := p.db.Get(dbutils.LastPrunedBlockKey, dbutils.LastPrunedBlockKey)
	if len(data) == 0 {
		return new(big.Int)
	}
	return new(big.Int).SetBytes(data)
}

// WriteHeadBlockHash stores the head block's hash.
func (p *BasicPruner) WriteLastPrunedBlockNum(num *big.Int) {
	if err := p.db.Put(dbutils.LastPrunedBlockKey, dbutils.LastPrunedBlockKey, num.Bytes()); err != nil {
		log.Crit("Failed to store last pruned block's num", "err", err)
	}
}

func Prune(db *ethdb.BoltDatabase, blockNumFrom uint64, blockNumTo uint64) error {
	keysToRemove := newKeysToRemove()
	err := db.Walk(dbutils.SuffixBucket, []byte{}, 0, func(key, v []byte) (b bool, e error) {
		timestamp, _ := dbutils.DecodeTimestamp(key)
		if timestamp < blockNumFrom {
			return true, nil
		}
		if timestamp > blockNumTo {
			return false, nil
		}

		keysToRemove.Suffix = append(keysToRemove.Suffix, key)

		changedKeys := dbutils.Suffix(v)

		err := changedKeys.Walk(func(addrHash []byte) error {
			compKey, _ := dbutils.CompositeKeySuffix(addrHash, timestamp)
			ck := make([]byte, len(compKey))
			copy(ck, compKey)
			if bytes.HasSuffix(key, dbutils.AccountsHistoryBucket) {
				keysToRemove.Account = append(keysToRemove.Account, ck)
			}
			if bytes.HasSuffix(key, dbutils.StorageHistoryBucket) {
				keysToRemove.Storage = append(keysToRemove.Storage, ck)
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
	err = batchDelete(db.DB(), keysToRemove)
	if err != nil {
		return err
	}

	return nil
}

func batchDelete(db *bolt.DB, keys *keysToRemove) error {
	log.Debug("Removed: ", "accounts", len(keys.Account), "storage", len(keys.Storage), "suffix", len(keys.Suffix))
	return db.Update(func(tx *bolt.Tx) error {
		accountHistoryBucket := tx.Bucket(dbutils.AccountsHistoryBucket)
		for i := range keys.Account {
			err := accountHistoryBucket.Delete(keys.Account[i])
			if err != nil {
				log.Warn("Unable to remove ", "addr", common.Bytes2Hex(keys.Account[i]), "err", err)
				continue
			}
		}
		storageHistoryBucket := tx.Bucket(dbutils.StorageHistoryBucket)
		for i := range keys.Storage {
			err := storageHistoryBucket.Delete(keys.Storage[i])
			if err != nil {
				log.Warn("Unable to remove storage key", "storage", common.Bytes2Hex(keys.Account[i]), "err", err)
				continue
			}
		}
		suffixBucket := tx.Bucket(dbutils.SuffixBucket)
		for i := range keys.Suffix {
			err := suffixBucket.Delete(keys.Suffix[i])
			if err != nil {
				log.Warn("Unable to remove suffix", "suffix", common.Bytes2Hex(keys.Account[i]), "err", err)
				continue
			}
		}
		return nil
	})
}

func newKeysToRemove() *keysToRemove {
	return &keysToRemove{
		Account: make([][]byte, 0),
		Storage: make([][]byte, 0),
		Suffix:  make([][]byte, 0),
	}
}

type keysToRemove struct {
	Account [][]byte
	Storage [][]byte
	Suffix  [][]byte
}
