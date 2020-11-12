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

	if err := changeset.Walk(db, changeSetBucket, nil, 0, func(blockN uint64, k, v []byte) (bool, error) {
		if blockN%100_000 == 0 {
			fmt.Printf("Processed %dK, %s\n", blockN/1000, time.Since(startTime))
		}

		indexBytes, innerErr := db.GetIndexChunk(indexBucket, k, blockN)
		if innerErr != nil {
			return false, innerErr
		}

		index := dbutils.WrapHistoryIndex(indexBytes)
		if findVal, _, ok := index.Search(blockN); !ok {
			return false, fmt.Errorf("%v,%v,%v", blockN, findVal, common.Bytes2Hex(k))
		}
		return true, nil
	}); err != nil {
		return err
	}

	fmt.Println("Check was succesful")
	return nil
}
