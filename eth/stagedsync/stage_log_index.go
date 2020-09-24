package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"github.com/RoaringBitmap/gocroaring"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

const (
	logIndicesMemLimit       = 512 * datasize.MB
	logIndicesCheckSizeEvery = 10 * time.Second
)

func SpawnLogIndex(s *StageState, db ethdb.Database, datadir string, quit <-chan struct{}) error {
	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	endBlock, err := s.ExecutionAt(tx)
	if err != nil {
		return fmt.Errorf("logs index: getting last executed block: %w", err)
	}
	if endBlock == s.BlockNumber {
		s.Done()
		return nil
	}

	start := s.BlockNumber
	if start > 0 {
		start++
	}

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	indices := map[string]*gocroaring.Bitmap{}
	logIndexCursor := tx.(ethdb.HasTx).Tx().Cursor(dbutils.LogIndex)
	receipts := tx.(ethdb.HasTx).Tx().Cursor(dbutils.BlockReceiptsPrefix)
	checkFlushEvery := time.NewTicker(logIndicesCheckSizeEvery)
	defer checkFlushEvery.Stop()

	for k, v, err := receipts.Seek(dbutils.EncodeBlockNumber(start)); k != nil; k, v, err = receipts.Next() {
		if err != nil {
			return err
		}

		if err := common.Stopped(quit); err != nil {
			return err
		}
		blockNum64Bytes := k[:len(k)-32]
		blockNum := binary.BigEndian.Uint64(blockNum64Bytes)

		select {
		default:
		case <-logEvery.C:
			sz, err := tx.(ethdb.HasTx).Tx().BucketSize(dbutils.LogIndex)
			if err != nil {
				return err
			}
			log.Info("Progress", "blockNum", blockNum, "bucketSize", common.StorageSize(sz))
		case <-checkFlushEvery.C:
			if needFlush(indices, logIndicesMemLimit, bitmapdb.HotShardLimit/2) {
				if err := flushBitmaps(logIndexCursor, indices); err != nil {
					return err
				}

				indices = map[string]*gocroaring.Bitmap{}
			}
		}

		// Convert the receipts from their storage form to their internal representation
		storageReceipts := []*types.ReceiptForStorage{}
		if err := rlp.DecodeBytes(v, &storageReceipts); err != nil {
			return fmt.Errorf("invalid receipt array RLP: %w, blocl=%d", err, blockNum)
		}

		for _, receipt := range storageReceipts {
			for _, log := range receipt.Logs {
				for _, topic := range log.Topics {
					topicStr := string(topic.Bytes())
					m, ok := indices[topicStr]
					if !ok {
						m = gocroaring.New()
						indices[topicStr] = m
					}
					m.Add(uint32(blockNum))
				}

				accStr := string(log.Address.Bytes())
				m, ok := indices[accStr]
				if !ok {
					m = gocroaring.New()
					indices[accStr] = m
				}
				m.Add(uint32(blockNum))
			}
		}
	}

	if err := flushBitmaps(logIndexCursor, indices); err != nil {
		return err
	}

	if err := s.DoneAndUpdate(tx, endBlock); err != nil {
		return err
	}
	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func UnwindLogIndex(u *UnwindState, s *StageState, db ethdb.Database, quitCh <-chan struct{}) error {
	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logsIndexKeys := map[string]bool{}
	logIndexCursor := tx.(ethdb.HasTx).Tx().Cursor(dbutils.LogIndex)

	receipts := tx.(ethdb.HasTx).Tx().Cursor(dbutils.BlockReceiptsPrefix)
	start := dbutils.EncodeBlockNumber(u.UnwindPoint + 1)
	for k, v, err := receipts.Seek(start); k != nil; k, v, err = receipts.Next() {
		if err != nil {
			return err
		}
		if err := common.Stopped(quitCh); err != nil {
			return err
		}
		// Convert the receipts from their storage form to their internal representation
		storageReceipts := []*types.ReceiptForStorage{}
		if err := rlp.DecodeBytes(v, &storageReceipts); err != nil {
			return fmt.Errorf("invalid receipt array RLP: %w, k=%x", err, k)
		}

		for _, storageReceipt := range storageReceipts {
			for _, log := range storageReceipt.Logs {
				for _, topic := range log.Topics {
					logsIndexKeys[string(topic.Bytes())] = true
				}
				logsIndexKeys[string(log.Address.Bytes())] = true
			}
		}
	}

	if err := truncateBitmaps(logIndexCursor, logsIndexKeys, u.UnwindPoint, s.BlockNumber+1); err != nil {
		return err
	}
	if err := u.Done(tx); err != nil {
		return fmt.Errorf("unwind AccountHistorytIndex: %w", err)
	}

	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func needFlush(bitmaps map[string]*gocroaring.Bitmap, memLimit, singleLimit datasize.ByteSize) bool {
	sz := uint64(0)
	for _, m := range bitmaps {
		sz1 := uint64(m.SerializedSizeInBytes())
		if sz1 > uint64(singleLimit) {
			return true
		}
		sz += sz1
	}
	const memoryNeedsForKey = 32 * 2 // each key stored in RAM: as string ang slice of bytes
	return uint64(len(bitmaps)*memoryNeedsForKey)+sz > uint64(memLimit)
}

func flushBitmaps(c ethdb.Cursor, inMem map[string]*gocroaring.Bitmap) error {
	t := time.Now()
	keys := make([]string, 0, len(inMem))
	for k := range inMem {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		b := inMem[k]
		if err := bitmapdb.AppendMergeByOr(c, []byte(k), b); err != nil {
			return err
		}
	}

	fmt.Printf("flush: %s, %d\n", time.Since(t), len(keys))

	return nil
}

func truncateBitmaps(c ethdb.Cursor, inMem map[string]bool, from, to uint64) error {
	t := time.Now()
	keys := make([]string, 0, len(inMem))
	for k := range inMem {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := bitmapdb.TruncateRange(c, []byte(k), from, to); err != nil {
			return nil
		}
	}
	fmt.Printf("truncateBitmaps: %s, %d\n", time.Since(t), len(keys))

	return nil
}
