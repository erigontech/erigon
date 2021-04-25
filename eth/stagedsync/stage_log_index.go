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
	bitmapsBufLimit   = 256 * datasize.MB // limit how much memory can use bitmaps before flushing to DB
	bitmapsFlushEvery = 10 * time.Second
)

type LogIndexCfg struct {
	bufLimit   datasize.ByteSize
	flushEvery time.Duration
	tmpdir     string
}

func StageLogIndexCfg(tmpDir string) LogIndexCfg {
	return LogIndexCfg{
		bufLimit:   bitmapsBufLimit,
		flushEvery: bitmapsFlushEvery,
		tmpdir:     tmpDir,
	}
}

func SpawnLogIndex(s *StageState, db ethdb.Database, cfg LogIndexCfg, quit <-chan struct{}) error {
	var tx ethdb.RwTx
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = hasTx.Tx().(ethdb.RwTx)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.(ethdb.HasRwKV).RwKV().BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	endBlock, err := s.ExecutionAt(tx)
	logPrefix := s.state.LogPrefix()
	if err != nil {
		return fmt.Errorf("%s: getting last executed block: %w", logPrefix, err)
	}
	if endBlock == s.BlockNumber {
		s.Done()
		return nil
	}

	start := s.BlockNumber
	if start > 0 {
		start++
	}

	if err := promoteLogIndex(logPrefix, tx, start, cfg, quit); err != nil {
		return err
	}

	if err := s.DoneAndUpdate(tx, endBlock); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func promoteLogIndex(logPrefix string, tx ethdb.RwTx, start uint64, cfg LogIndexCfg, quit <-chan struct{}) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	topics := map[string]*roaring.Bitmap{}
	addresses := map[string]*roaring.Bitmap{}
	logs, err := tx.Cursor(dbutils.Log)
	if err != nil {
		return err
	}
	defer logs.Close()
	checkFlushEvery := time.NewTicker(cfg.flushEvery)
	defer checkFlushEvery.Stop()

	collectorTopics := etl.NewCollector(cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	collectorAddrs := etl.NewCollector(cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))

	reader := bytes.NewReader(nil)

	for k, v, err := logs.Seek(dbutils.LogKey(start, 0)); k != nil; k, v, err = logs.Next() {
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
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info(fmt.Sprintf("[%s] Progress", logPrefix), "number", blockNum, "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
		case <-checkFlushEvery.C:
			if needFlush(topics, cfg.bufLimit) {
				if err := flushBitmaps(collectorTopics, topics); err != nil {
					return err
				}
				topics = map[string]*roaring.Bitmap{}
			}

			if needFlush(addresses, cfg.bufLimit) {
				if err := flushBitmaps(collectorAddrs, addresses); err != nil {
					return err
				}
				addresses = map[string]*roaring.Bitmap{}
			}
		}

		var ll types.Logs
		reader.Reset(v)
		if err := cbor.Unmarshal(&ll, reader); err != nil {
			return fmt.Errorf("%s: receipt unmarshal failed: %w, blocl=%d", logPrefix, err, blockNum)
		}

		for _, l := range ll {
			for _, topic := range l.Topics {
				topicStr := string(topic.Bytes())
				m, ok := topics[topicStr]
				if !ok {
					m = roaring.New()
					topics[topicStr] = m
				}
				m.Add(uint32(blockNum))
			}

			accStr := string(l.Address.Bytes())
			m, ok := addresses[accStr]
			if !ok {
				m = roaring.New()
				addresses[accStr] = m
			}
			m.Add(uint32(blockNum))
		}
	}

	if err := flushBitmaps(collectorTopics, topics); err != nil {
		return err
	}
	if err := flushBitmaps(collectorAddrs, addresses); err != nil {
		return err
	}

	var currentBitmap = roaring.New()
	var buf = bytes.NewBuffer(nil)

	lastChunkKey := make([]byte, 128)
	var loaderFunc = func(k []byte, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		lastChunkKey = lastChunkKey[:len(k)+4]
		copy(lastChunkKey, k)
		binary.BigEndian.PutUint32(lastChunkKey[len(k):], ^uint32(0))
		lastChunkBytes, err := table.Get(lastChunkKey)
		if err != nil {
			return fmt.Errorf("%s: find last chunk failed: %w", logPrefix, err)
		}

		lastChunk := roaring.New()
		if len(lastChunkBytes) > 0 {
			_, err = lastChunk.FromBuffer(lastChunkBytes)
			if err != nil {
				return fmt.Errorf("%s: couldn't read last log index chunk: %w, len(lastChunkBytes)=%d", logPrefix, err, len(lastChunkBytes))
			}
		}

		if _, err := currentBitmap.FromBuffer(v); err != nil {
			return err
		}
		currentBitmap.Or(lastChunk) // merge last existing chunk from db - next loop will overwrite it
		return bitmapdb.WalkChunkWithKeys(k, currentBitmap, bitmapdb.ChunkLimit, func(chunkKey []byte, chunk *roaring.Bitmap) error {
			buf.Reset()
			if _, err := chunk.WriteTo(buf); err != nil {
				return err
			}
			return next(k, chunkKey, buf.Bytes())
		})
	}

	if err := collectorTopics.Load(logPrefix, tx, dbutils.LogTopicIndex, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}

	if err := collectorAddrs.Load(logPrefix, tx, dbutils.LogAddressIndex, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}

	return nil
}

func UnwindLogIndex(u *UnwindState, s *StageState, db ethdb.Database, cfg LogIndexCfg, quitCh <-chan struct{}) error {
	var tx ethdb.RwTx
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = hasTx.Tx().(ethdb.RwTx)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.(ethdb.HasRwKV).RwKV().BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logPrefix := s.state.LogPrefix()
	if err := unwindLogIndex(logPrefix, tx, u.UnwindPoint, cfg, quitCh); err != nil {
		return err
	}

	if err := u.Done(tx); err != nil {
		return fmt.Errorf("%s: %w", logPrefix, err)
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func unwindLogIndex(logPrefix string, db ethdb.RwTx, to uint64, cfg LogIndexCfg, quitCh <-chan struct{}) error {
	topics := map[string]struct{}{}
	addrs := map[string]struct{}{}

	c, err := db.Cursor(dbutils.Log)
	if err != nil {
		return err
	}
	defer c.Close()
	for k, v, err := c.Seek(dbutils.EncodeBlockNumber(to + 1)); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}

		if err := common.Stopped(quitCh); err != nil {
			return err
		}
		var logs types.Logs
		if err := cbor.Unmarshal(&logs, bytes.NewReader(v)); err != nil {
			return fmt.Errorf("%s: receipt unmarshal failed: %w, block=%d", logPrefix, err, binary.BigEndian.Uint64(k))
		}

		for _, l := range logs {
			for _, topic := range l.Topics {
				topics[string(topic.Bytes())] = struct{}{}
			}
			addrs[string(l.Address.Bytes())] = struct{}{}
		}
	}

	if err := truncateBitmaps(db, dbutils.LogTopicIndex, topics, to); err != nil {
		return err
	}
	if err := truncateBitmaps(db, dbutils.LogAddressIndex, addrs, to); err != nil {
		return err
	}
	return nil
}

func needFlush(bitmaps map[string]*roaring.Bitmap, memLimit datasize.ByteSize) bool {
	sz := uint64(0)
	for _, m := range bitmaps {
		sz += m.GetSizeInBytes()
	}
	const memoryNeedsForKey = 32 * 2 // each key stored in RAM: as string ang slice of bytes
	return uint64(len(bitmaps)*memoryNeedsForKey)+sz > uint64(memLimit)
}

func flushBitmaps(c *etl.Collector, inMem map[string]*roaring.Bitmap) error {
	for k, v := range inMem {
		v.RunOptimize()
		if v.GetCardinality() == 0 {
			continue
		}
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

func truncateBitmaps(tx ethdb.RwTx, bucket string, inMem map[string]struct{}, to uint64) error {
	keys := make([]string, 0, len(inMem))
	for k := range inMem {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := bitmapdb.TruncateRange(tx, bucket, []byte(k), uint32(to+1)); err != nil {
			return fmt.Errorf("fail TruncateRange: bucket=%s, %w", bucket, err)
		}
	}

	return nil
}
