package state

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/trie"
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

	key := pool.GetBuffer(keyBufferSize)
	defer pool.PutBuffer(key)
	trie.CompressNibbles(prefixAsNibbles, &key.B)

	if err := ih.deleter.Delete(dbutils.IntermediateTrieHashBucket, common.CopyBytes(key.B)); err != nil {
		log.Warn("could not delete intermediate trie hash", "err", err)
		return
	}
}

// PutTombstoneForDeletedAccount - placing tombstone only if given account has storage in database
func PutTombstoneForDeletedAccount(db ethdb.MinDatabase, addrHash []byte) error {
	if len(addrHash) != common.HashLength {
		return nil
	}

	var boltDb *bolt.DB
	if hasKV, ok := db.(ethdb.HasKV); ok {
		boltDb = hasKV.KV()
	} else {
		return fmt.Errorf("only Bolt supported yet, given: %T", db)
	}

	hasIH := false
	if err := boltDb.View(func(tx *bolt.Tx) error {
		k, _ := tx.Bucket(dbutils.IntermediateTrieHashBucket).Cursor().Seek(addrHash)
		if !bytes.HasPrefix(k, addrHash) {
			k = nil
		}
		if k != nil {
			hasIH = true
		}

		return nil
	}); err != nil {
		return err
	}

	if !hasIH {
		return nil
	}

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
	//var toDelete [][]byte

	if err := boltDb.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(dbutils.IntermediateTrieHashBucket).Cursor()
		for k, v := c.Seek(storageKeyNoInc[:common.HashLength]); k != nil; k, v = c.Next() {
			if !bytes.HasPrefix(k, storageKeyNoInc[:common.HashLength]) {
				k = nil
			}
			if k == nil {
				break
			}

			isTombstone := v != nil && len(v) == 0
			if isTombstone {
				continue
			}

			for i := common.HashLength; i < len(k); i++ {
				if storageKeyNoInc[i] == k[i] {
					continue
				}

				toPut = append(toPut, common.CopyBytes(k[:i+1]))
				break
			}
		}
		return nil
	}); err != nil {
		return err
	}

	for _, k := range toPut {
		if err := db.Put(dbutils.IntermediateTrieHashBucket, k, []byte{}); err != nil {
			return err
		}
	}

	for i := common.HashLength; i <= len(storageKeyNoInc); i++ {
		if err := db.Delete(dbutils.IntermediateTrieHashBucket, common.CopyBytes(storageKeyNoInc[:i])); err != nil {
			return err
		}
	}

	return nil
}
