package verify

import (
	"bytes"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"log"
	"time"
)

func CheckIndex(chaindata string, changeSetBucket []byte, indexBucket []byte) error {
	db, err := ethdb.NewBoltDatabase(chaindata)
	if err != nil {
		log.Fatal(err)
	}
	startTime := time.Now()

	var walker func([]byte) core.ChangesetWalker
	if bytes.Equal(dbutils.AccountChangeSetBucket, changeSetBucket) {
		walker = func(cs []byte) core.ChangesetWalker {
			return changeset.AccountChangeSetBytes(cs)
		}
	}

	if bytes.Equal(dbutils.StorageChangeSetBucket, changeSetBucket) {
		walker = func(cs []byte) core.ChangesetWalker {
			return changeset.StorageChangeSetBytes(cs)
		}
	}

	err = db.Walk(changeSetBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
		blockNum, _ := dbutils.DecodeTimestamp(k)
		if blockNum%100_000 == 0 {
			fmt.Printf("Processed %dK, %s\n", blockNum/1000, time.Since(startTime))
		}

		err = walker(v).Walk(func(key, val []byte) error {
			indexBytes, innerErr := db.GetIndexChunk(indexBucket, key, blockNum)
			if err != nil {
				return innerErr
			}

			index := dbutils.WrapHistoryIndex(indexBytes)
			if findVal, _, ok := index.Search(blockNum); !ok {
				return fmt.Errorf("%v,%v,%v", blockNum, findVal, common.Bytes2Hex(key))
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

	fmt.Println("Check was succesful")
	return nil
}
