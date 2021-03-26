package verify

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
)

func CheckIndex(ctx context.Context, chaindata string, changeSetBucket string, indexBucket string) error {
	db := ethdb.MustOpen(chaindata)
	startTime := time.Now()

	i := 0
	if err := changeset.Walk(db, changeSetBucket, nil, 0, func(blockN uint64, k, v []byte) (bool, error) {
		i++
		if i%100_000 == 0 {
			fmt.Printf("Processed %dK, %s\n", blockN/1000, time.Since(startTime))
		}
		select {
		default:
		case <-ctx.Done():
			return false, ctx.Err()
		}

		bm, innerErr := bitmapdb.Get64(db, indexBucket, dbutils.CompositeKeyWithoutIncarnation(k), blockN-1, blockN+1)
		if innerErr != nil {
			return false, innerErr
		}
		if !bm.Contains(blockN) {
			return false, fmt.Errorf("%v,%v", blockN, common.Bytes2Hex(k))
		}
		return true, nil
	}); err != nil {
		return err
	}

	fmt.Println("Check was successful")
	return nil
}
