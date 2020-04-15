package state

import (
	"bytes"
	"fmt"
	"time"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/trie"
)

var (
	InsertCounter          = metrics.NewRegisteredCounter("db/ih/insert", nil)
	DeleteCounter          = metrics.NewRegisteredCounter("db/ih/delete", nil)
	InsertTombstoneCounter = metrics.NewRegisteredCounter("db/ih/tombstone_insert", nil)
	DeleteTombstoneCounter = metrics.NewRegisteredCounter("db/ih/tombstone_delete", nil)
	ClearTombstoneTimer    = metrics.NewRegisteredTimer("db/ih/tombstone_clear", nil)
)

const keyBufferSize = 64

type IntermediateHashes struct {
	trie.NoopObserver // make sure that we don't need to subscribe to unnecessary methods
	putter            ethdb.Putter
	deleter           ethdb.Deleter
}

func NewIntermediateHashes(putter ethdb.Putter, deleter ethdb.Deleter) *IntermediateHashes {
	return &IntermediateHashes{putter: putter, deleter: deleter}
}

func (ih *IntermediateHashes) WillUnloadBranchNode(prefixAsNibbles []byte, nodeHash common.Hash) {
	// only put to bucket prefixes with even number of nibbles
	if len(prefixAsNibbles) == 0 || len(prefixAsNibbles)%2 == 1 {
		return
	}
	InsertCounter.Inc(1)

	key := pool.GetBuffer(keyBufferSize)
	defer pool.PutBuffer(key)
	trie.CompressNibbles(prefixAsNibbles, &key.B)

	if err := ih.putter.Put(dbutils.IntermediateTrieHashBucket, common.CopyBytes(key.B), common.CopyBytes(nodeHash[:])); err != nil {
		log.Warn("could not put intermediate trie hash", "err", err)
	}
}

func (ih *IntermediateHashes) BranchNodeLoaded(prefixAsNibbles []byte) {
	// only put to bucket prefixes with even number of nibbles
	if len(prefixAsNibbles) == 0 || len(prefixAsNibbles)%2 == 1 {
		return
	}
	DeleteCounter.Inc(1)

	key := pool.GetBuffer(keyBufferSize)
	defer pool.PutBuffer(key)
	trie.CompressNibbles(prefixAsNibbles, &key.B)

	if err := ih.deleter.Delete(dbutils.IntermediateTrieHashBucket, common.CopyBytes(key.B)); err != nil {
		log.Warn("could not delete intermediate trie hash", "err", err)
		return
	}
}

// PutTombstoneForDeletedAccount - placing tombstone only if given account has storage in database
func PutTombstoneForDeletedAccount(db ethdb.Database, addrHash []byte) error {
	if len(addrHash) != common.HashLength {
		return nil
	}

	hasIH := false
	if err := db.Walk(dbutils.IntermediateTrieHashBucket, addrHash, 32*8, func(_ []byte, _ []byte) (bool, error) {
		hasIH = true
		return false, nil
	}); err != nil {
		return err
	}

	if !hasIH {
		return nil
	}

	InsertTombstoneCounter.Inc(1)
	return db.Put(dbutils.IntermediateTrieHashBucket, common.CopyBytes(addrHash), []byte{})
}

func ClearTombstonesForNewStorage(db ethdb.MinDatabase, storageKeyNoInc []byte) error {
	var boltDb *bolt.DB
	if hasKV, ok := db.(ethdb.HasKV); ok {
		boltDb = hasKV.KV()
	} else {
		return fmt.Errorf("only Bolt supported yet, given: %T", db)
	}

	var toPut [][]byte
	toDelete := map[string]struct{}{}

	start := time.Now()
	defer ClearTombstoneTimer.UpdateSince(start)

	if err := boltDb.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(dbutils.IntermediateTrieHashBucket).Cursor()

		i := common.HashLength
		var k, v []byte
		for ; i < len(storageKeyNoInc); i++ {
			k, v = c.Seek(storageKeyNoInc[:i])
			if k == nil {
				return nil
			}

			isTombstone := v != nil && len(v) == 0
			if isTombstone && bytes.HasPrefix(storageKeyNoInc, k) {
				break
			}
		}

		for ; bytes.HasPrefix(k, storageKeyNoInc[:i]); k, v = c.Next() {
			isTombstone := v != nil && len(v) == 0
			if isTombstone && bytes.HasPrefix(storageKeyNoInc, k) {
				toDelete[string(k)] = struct{}{}
			} else {
				for j := i; j < len(k); j++ {
					if storageKeyNoInc[j] != k[j] {
						toPut = append(toPut, common.CopyBytes(k[:j+1]))
						break
					}
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	InsertTombstoneCounter.Inc(int64(len(toPut)))
	for _, k := range toPut {
		if err := db.Put(dbutils.IntermediateTrieHashBucket, k, []byte{}); err != nil {
			return err
		}
	}

	DeleteTombstoneCounter.Inc(int64(len(toDelete)))
	for k := range toDelete {
		if err := db.Delete(dbutils.IntermediateTrieHashBucket, []byte(k)); err != nil {
			return err
		}
	}

	return nil
}
