package migrations

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"time"
)

var logIndex = Migration{
	Name: "receipt_logs_index",
	Up: func(tx ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
		if err := tx.(ethdb.BucketsMigrator).ClearBuckets(dbutils.LogIndex); err != nil {
			return err
		}

		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()

		const memLimit = uint64(1 * datasize.GB)

		bitmaps := map[string]*roaring.Bitmap{}
		if err := tx.Walk(dbutils.BlockReceiptsPrefix, nil, 0, func(k, v []byte) (bool, error) {
			blockHashBytes := k[len(k)-32:]
			blockNum64Bytes := k[:len(k)-32]
			blockNum := binary.BigEndian.Uint64(blockNum64Bytes)
			canonicalHash := rawdb.ReadCanonicalHash(tx, blockNum)
			if !bytes.Equal(blockHashBytes, canonicalHash[:]) {
				return true, nil
			}

			select {
			default:
			case <-logEvery.C:
				log.Info("progress", "blockNum", blockNum)

				if needFlush(bitmaps, memLimit) {
					bitmaps = flushBitmaps(tx, dbutils.LogIndex, bitmaps)
				}
			}

			// Convert the receipts from their storage form to their internal representation
			storageReceipts := []*types.ReceiptForStorage{}
			if err := rlp.DecodeBytes(v, &storageReceipts); err != nil {
				return false, fmt.Errorf("invalid receipt array RLP: %w, hash=%x", err, blockHashBytes)
			}

			// Index Index2 Index4
			logIdx := uint32(0) // logIdx - indexed IN THE BLOCK and starting from 0.
			for _, storageReceipt := range storageReceipts {
				for _, log := range storageReceipt.Logs {

					for _, topic := range log.Topics {
						m, ok := bitmaps[string(topic.Bytes())]
						if !ok {
							m = roaring.New()
							bitmaps[string(topic.Bytes())] = m
						}
						m.Add(uint32(blockNum))
					}

					m, ok := bitmaps[string(log.Address.Bytes())]
					if !ok {
						m = roaring.New()
						bitmaps[string(log.Address.Bytes())] = m
					}
					m.Add(uint32(blockNum))

					logIdx++
				}
			}
			return true, nil
		}); err != nil {
			return err
		}

		_ = flushBitmaps(tx, dbutils.LogIndex, bitmaps)

		return OnLoadCommit(tx, nil, true)
	},
}

func needFlush(bitmaps map[string]*roaring.Bitmap, memLimit uint64) bool {
	sz := uint64(0)
	for _, m := range bitmaps {
		sz += m.GetSizeInBytes()
	}

	const memoryNeedsForKey = 32 * 2 // each key stored in RAM: as string ang slice of bytes
	return uint64(len(bitmaps)*memoryNeedsForKey)+sz > memLimit
}

func flushBitmaps(db ethdb.MinDatabase, bucket string, inMem map[string]*roaring.Bitmap) map[string]*roaring.Bitmap {
	for kStr, b := range inMem {
		if err := bitmapdb.PutMergeByOr(db, bucket, []byte(kStr), b); err != nil {
			panic(err)
		}
	}

	return map[string]*roaring.Bitmap{}
}
