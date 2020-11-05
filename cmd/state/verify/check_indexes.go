package verify

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func CheckIndex(chaindata string, changeSetBucket string, indexBucket string) error {
	db := ethdb.MustOpen(chaindata)
	startTime := time.Now()

	var walker func([]byte) changeset.Walker
	if dbutils.AccountChangeSetBucket2 == changeSetBucket {
		walker = func(cs []byte) changeset.Walker {
			return changeset.AccountChangeSetBytes(cs)
		}
	}

	if dbutils.StorageChangeSetBucket2 == changeSetBucket {
		walker = func(cs []byte) changeset.Walker {
			return changeset.StorageChangeSetBytes(cs)
		}
	}

	if err := db.Walk(changeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		blockNum, _ := dbutils.DecodeTimestamp(k)
		if blockNum%100_000 == 0 {
			fmt.Printf("Processed %dK, %s\n", blockNum/1000, time.Since(startTime))
		}

		if err := walker(v).Walk(func(key, val []byte) error {
			indexBytes, innerErr := db.GetIndexChunk(indexBucket, key, blockNum)
			if innerErr != nil {
				return innerErr
			}

			index := dbutils.WrapHistoryIndex(indexBytes)
			if findVal, _, ok := index.Search(blockNum); !ok {
				return fmt.Errorf("%v,%v,%v", blockNum, findVal, common.Bytes2Hex(key))
			}
			return nil
		}); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return err
	}

	fmt.Println("Check was succesful")
	return nil
}
