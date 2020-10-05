package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"sort"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/cbor"
	"github.com/ledgerwatch/turbo-geth/log"
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

	if err := promoteLogIndex(tx, start, datadir, quit); err != nil {
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

func promoteLogIndex(db ethdb.DbWithPendingMutations, start uint64, datadir string, quit <-chan struct{}) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	tx := db.(ethdb.HasTx).Tx()
	topics := map[string]*roaring.Bitmap{}
	addresses := map[string]*roaring.Bitmap{}
	receipts := tx.Cursor(dbutils.BlockReceiptsPrefix)
	defer receipts.Close()
	checkFlushEvery := time.NewTicker(logIndicesCheckSizeEvery)
	defer checkFlushEvery.Stop()

	collectorTopics := etl.NewCollector(datadir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	collectorAddrs := etl.NewCollector(datadir, etl.NewSortableBuffer(etl.BufferOptimalSize))

	for k, v, err := receipts.Seek(dbutils.EncodeBlockNumber(start)); k != nil; k, v, err = receipts.Next() {
		if err != nil {
			return err
		}

		if err := common.Stopped(quit); err != nil {
			return err
		}
		blockNum := binary.BigEndian.Uint64(k[:8])

		select {
		default:
		case <-logEvery.C:
			sz, err := tx.BucketSize(dbutils.LogTopicIndex)
			if err != nil {
				return err
			}
			sz2, err := tx.BucketSize(dbutils.LogAddressIndex)
			if err != nil {
				return err
			}
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info("Progress", "blockNum", blockNum, dbutils.LogTopicIndex, common.StorageSize(sz), dbutils.LogAddressIndex, common.StorageSize(sz2), "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
		case <-checkFlushEvery.C:
			if needFlush(topics, logIndicesMemLimit) {
				if err := flushBitmaps2(collectorTopics, topics); err != nil {
					return err
				}
				topics = map[string]*roaring.Bitmap{}
			}

			if needFlush(addresses, logIndicesMemLimit) {
				if err := flushBitmaps2(collectorAddrs, addresses); err != nil {
					return err
				}
				addresses = map[string]*roaring.Bitmap{}
			}
		}

		receipts := types.Receipts{}
		if err := cbor.Unmarshal(&receipts, v); err != nil {
			return fmt.Errorf("receipt unmarshal failed: %w, blocl=%d", err, blockNum)
		}

		for _, receipt := range receipts {
			for _, log := range receipt.Logs {
				for _, topic := range log.Topics {
					topicStr := string(topic.Bytes())
					m, ok := topics[topicStr]
					if !ok {
						m = roaring.New()
						topics[topicStr] = m
					}
					m.Add(uint32(blockNum))
				}

				accStr := string(log.Address.Bytes())
				m, ok := addresses[accStr]
				if !ok {
					m = roaring.New()
					addresses[accStr] = m
				}
				m.Add(uint32(blockNum))
			}
		}
	}

	if err := flushBitmaps2(collectorTopics, topics); err != nil {
		return err
	}
	if err := flushBitmaps2(collectorAddrs, addresses); err != nil {
		return err
	}

	var currentKey []byte
	var currentBitmap = roaring.New()
	var tmp = roaring.New()
	var buf = bytes.NewBuffer(nil)

	var loaderFunc = func(k []byte, v []byte, state etl.State, next etl.LoadNextFunc) error {
		if currentKey == nil {
			currentKey = k
			if _, err := currentBitmap.FromBuffer(v); err != nil {
				return err
			}
			return nil
		}

		if bytes.Equal(k, currentKey) {
			tmp.Clear()
			if _, err := tmp.FromBuffer(v); err != nil {
				return err
			}
			currentBitmap.Or(tmp)
			return nil
		}

		// TODO: get last shard from db

		for {
			lft := bitmapdb.CutLeft(currentBitmap, bitmapdb.ShardLimit, 500)
			buf.Reset()
			if _, err := lft.WriteTo(buf); err != nil {
				return err
			}

			shardKey := make([]byte, len(k)+2)
			copy(shardKey, currentKey)
			if currentBitmap == nil {
				binary.BigEndian.PutUint32(shardKey[:len(k)], ^uint32(0))
			} else {
				binary.BigEndian.PutUint32(shardKey[:len(k)], lft.Maximum())
			}

			if err := next(currentKey, shardKey, common.CopyBytes(buf.Bytes())); err != nil {
				return err
			}

			if currentBitmap == nil {
				break
			}
		}

		currentKey = k
		currentBitmap.Clear()

		return nil
	}
	if err := collectorTopics.Load(db, dbutils.LogTopicIndex, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}
	if err := collectorAddrs.Load(db, dbutils.LogAddressIndex, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
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

	if err := unwindLogIndex(tx, s.BlockNumber, u.UnwindPoint, quitCh); err != nil {
		return err
	}

	if err := u.Done(tx); err != nil {
		return fmt.Errorf("unwind LogIndex: %w", err)
	}

	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func unwindLogIndex(db ethdb.DbWithPendingMutations, from, to uint64, quitCh <-chan struct{}) error {
	topics := map[string]bool{}
	addrs := map[string]bool{}

	tx := db.(ethdb.HasTx).Tx()
	receipts := tx.Cursor(dbutils.BlockReceiptsPrefix)
	defer receipts.Close()
	start := dbutils.EncodeBlockNumber(to + 1)
	for k, v, err := receipts.Seek(start); k != nil; k, v, err = receipts.Next() {
		if err != nil {
			return err
		}
		if err := common.Stopped(quitCh); err != nil {
			return err
		}
		receipts := types.Receipts{}
		if err := cbor.Unmarshal(&receipts, v); err != nil {
			return fmt.Errorf("receipt unmarshal failed: %w, k=%x", err, k)
		}

		for _, receipt := range receipts {
			for _, log := range receipt.Logs {
				for _, topic := range log.Topics {
					topics[string(topic.Bytes())] = true
				}
				addrs[string(log.Address.Bytes())] = true
			}
		}
	}

	if err := truncateBitmaps(tx, dbutils.LogTopicIndex, topics, to+1, from+1); err != nil {
		return err
	}
	if err := truncateBitmaps(tx, dbutils.LogAddressIndex, addrs, to+1, from+1); err != nil {
		return err
	}
	return nil
}

func needFlush(bitmaps map[string]*roaring.Bitmap, memLimit datasize.ByteSize) bool {
	sz := uint64(0)
	for _, m := range bitmaps {
		sz += m.GetSerializedSizeInBytes()
	}
	const memoryNeedsForKey = 32 * 2 // each key stored in RAM: as string ang slice of bytes
	return uint64(len(bitmaps)*memoryNeedsForKey)+sz > uint64(memLimit)
}

func flushBitmaps(c ethdb.Cursor, inMem map[string]*roaring.Bitmap) error {
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

	return nil
}

func flushBitmaps2(c *etl.Collector, inMem map[string]*roaring.Bitmap) error {
	for k, v := range inMem {
		newV := bytes.NewBuffer(make([]byte, 0, v.GetSerializedSizeInBytes()))
		if _, err := v.WriteTo(newV); err != nil {
			return err
		}
		if err := c.Collect([]byte(k), newV.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

func truncateBitmaps(tx ethdb.Tx, bucket string, inMem map[string]bool, from, to uint64) error {
	keys := make([]string, 0, len(inMem))
	for k := range inMem {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := bitmapdb.TruncateRange(tx, bucket, []byte(k), from, to); err != nil {
			return nil
		}
	}

	return nil
}
