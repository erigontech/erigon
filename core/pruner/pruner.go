package pruner

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func Prune(db *ethdb.BoltDatabase, blockNumFrom uint64, blockNumTo uint64) error {
	fmt.Println("prune start")
	keysToRemove :=make([][]byte, 0)
	err:= db.Walk(dbutils.SuffixBucket, []byte{}, 0, func(key, v []byte) (b bool, e error) {
		timestamp, _ := dbutils.DecodeTimestamp(key)
		fmt.Println(timestamp)
		if timestamp < blockNumFrom {
			return true, nil
		}
		if timestamp > blockNumTo {
			return false, nil
		}
		changedAccounts := dbutils.Suffix(v)

		err := changedAccounts.Walk(func(addrHash []byte) error {
			compKey, _ := dbutils.CompositeKeySuffix(addrHash, timestamp)
			ck:=make([]byte, len(compKey))
			copy(ck,compKey)
			keysToRemove = append(keysToRemove, ck)
			return nil
		})
		if err != nil {
			return false, err
		}
		return true, nil
	})
	if err!=nil {
		return err
	}

	for i:=range keysToRemove {
		err:=db.Delete(dbutils.AccountsHistoryBucket, keysToRemove[i])
		if err!=nil {
			fmt.Println("Unable to remove ", common.Bytes2Hex(keysToRemove[i]))
		}
	}
	return nil
}
