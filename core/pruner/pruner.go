package pruner

import (
	"bytes"
	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

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
	return db.Update(func(tx *bolt.Tx) error {
		accountHistoryBucket := tx.Bucket(dbutils.AccountsHistoryBucket)
		for i := range keys.Account {
			err := accountHistoryBucket.Delete(keys.Account[i])
			if err != nil {
				log.Warn("Unable to remove ", "addr", common.Bytes2Hex(keys.Account[i]))
				return err
			}
		}
		storageHistoryBucket := tx.Bucket(dbutils.StorageHistoryBucket)
		for i := range keys.Storage {
			err := storageHistoryBucket.Delete(keys.Storage[i])
			if err != nil {
				log.Warn("Unable to remove storage key", "storage", common.Bytes2Hex(keys.Account[i]))
				return err
			}
		}
		suffixBucket := tx.Bucket(dbutils.SuffixBucket)
		for i := range keys.Suffix {
			err := suffixBucket.Delete(keys.Suffix[i])
			if err != nil {
				log.Warn("Unable to remove suffix", "suffix", common.Bytes2Hex(keys.Account[i]))
				return err
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
