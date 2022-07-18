package verify

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/ethdb/bitmapdb"
	"github.com/ledgerwatch/log/v3"
)

func CheckIndex(ctx context.Context, chaindata string, changeSetBucket string, indexBucket string, startBlock uint64) error {
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	fmt.Printf("Checking index %s against changeset %s\n", indexBucket, changeSetBucket)
	startTime := time.Now()

	i := 0

	var blockNumBytes []byte

	if startBlock > 0 {
		log.Info("Processing", "blockNum", startBlock)
		blockNumBytes = dbutils.EncodeBlockNumber(startBlock)
	}

	if len(blockNumBytes) > 0 {
		fmt.Printf("Using startkey[] %v\n", common.Bytes2Hex(blockNumBytes))
	}

	if err := changeset.ForEach(tx, changeSetBucket, blockNumBytes, func(blockN uint64, k, v []byte) error {
		i++
		if i%100_000 == 0 {
			log.Info("Processed", "changes", i, "block", blockN, "changeset", changeSetBucket, "index", indexBucket)
		} else if i == 1 {
			fmt.Printf("First change: block %v, key %v\n", blockN, common.Bytes2Hex(k))
		}

		select {
		default:
		case <-ctx.Done():
			err2 := ctx.Err()
			log.Info("Done", "ctx.Err", err2)
			return err2
		}

		kk := dbutils.CompositeKeyWithoutIncarnation(k)
		bm, innerErr := bitmapdb.Get64(tx, indexBucket, kk, blockN-1, blockN+1)
		if innerErr != nil {
			return innerErr
		}
		if !bm.Contains(blockN) {
			khex := common.Bytes2Hex(k)
			log.Error("Failed", "change", i, "block", blockN, "key", khex, "key_without_incarnation", common.Bytes2Hex(kk))
			return fmt.Errorf("%v,%v", blockN, khex)
		}
		return nil
	}); err != nil {
		return err
	}

	log.Info("Finished", "duration", time.Since(startTime))
	fmt.Println("Check was successful")
	return nil
}
