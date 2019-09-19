package pruner

import (
	"bytes"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
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

	for i := range keysToRemove.Account {
		err := db.Delete(dbutils.AccountsHistoryBucket, keysToRemove.Account[i])
		if err != nil {
			fmt.Println("Unable to remove ", common.Bytes2Hex(keysToRemove.Account[i]))
		}
	}
	for i := range keysToRemove.Storage {
		err := db.Delete(dbutils.StorageHistoryBucket, keysToRemove.Storage[i])
		if err != nil {
			fmt.Println("Unable to remove ", common.Bytes2Hex(keysToRemove.Storage[i]))
		}
	}
	for i := range keysToRemove.Suffix {
		err := db.Delete(dbutils.SuffixBucket, keysToRemove.Suffix[i])
		if err != nil {
			fmt.Println("Unable to remove ", common.Bytes2Hex(keysToRemove.Suffix[i]))
		}
	}
	return nil
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
