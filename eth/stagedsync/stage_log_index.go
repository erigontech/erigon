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
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/bitmapdb"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/log/v3"
)

const (
	bitmapsBufLimit   = 256 * datasize.MB // limit how much memory can use bitmaps before flushing to DB
	bitmapsFlushEvery = 10 * time.Second
)

type LogIndexCfg struct {
	tmpdir     string
	db         kv.RwDB
	prune      prune.Mode
	bufLimit   datasize.ByteSize
	flushEvery time.Duration
}

func StageLogIndexCfg(db kv.RwDB, prune prune.Mode, tmpDir string) LogIndexCfg {
	return LogIndexCfg{
		db:         db,
		prune:      prune,
		bufLimit:   bitmapsBufLimit,
		flushEvery: bitmapsFlushEvery,
		tmpdir:     tmpDir,
	}
}

func SpawnLogIndex(s *StageState, tx kv.RwTx, cfg LogIndexCfg, ctx context.Context) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	endBlock, err := s.ExecutionAt(tx)
	logPrefix := s.LogPrefix()
	if err != nil {
		return fmt.Errorf("getting last executed block: %w", err)
	}
	if endBlock == s.BlockNumber {
		return nil
	}

	startBlock := s.BlockNumber
	pruneTo := cfg.prune.Receipts.PruneTo(endBlock)
	if startBlock < pruneTo {
		startBlock = pruneTo
	}
	if startBlock > 0 {
		startBlock++
	}

	if err = promoteLogIndex(logPrefix, tx, startBlock, cfg, ctx); err != nil {
		return err
	}
	if err = s.Update(tx, endBlock); err != nil {
		return err
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func promoteLogIndex(logPrefix string, tx kv.RwTx, start uint64, cfg LogIndexCfg, ctx context.Context) error {
	quit := ctx.Done()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	topics := map[string]*roaring.Bitmap{}
	addresses := map[string]*roaring.Bitmap{}
	logs, err := tx.Cursor(kv.Log)
	if err != nil {
		return err
	}
	defer logs.Close()
	checkFlushEvery := time.NewTicker(cfg.flushEvery)
	defer checkFlushEvery.Stop()

	collectorTopics := etl.NewCollector(cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collectorTopics.Close(logPrefix)
	collectorAddrs := etl.NewCollector(cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collectorAddrs.Close(logPrefix)

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
			return fmt.Errorf("receipt unmarshal failed: %w, blocl=%d", err, blockNum)
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
			return fmt.Errorf("find last chunk: %w", err)
		}

		lastChunk := roaring.New()
		if len(lastChunkBytes) > 0 {
			_, err = lastChunk.FromBuffer(lastChunkBytes)
			if err != nil {
				return fmt.Errorf("couldn't read last log index chunk: %w, len(lastChunkBytes)=%d", err, len(lastChunkBytes))
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

	if err := collectorTopics.Load(logPrefix, tx, kv.LogTopicIndex, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}

	if err := collectorAddrs.Load(logPrefix, tx, kv.LogAddressIndex, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}

	return nil
}

func UnwindLogIndex(u *UnwindState, s *StageState, tx kv.RwTx, cfg LogIndexCfg, ctx context.Context) (err error) {
	quitCh := ctx.Done()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logPrefix := s.LogPrefix()
	if err := unwindLogIndex(logPrefix, tx, u.UnwindPoint, cfg, quitCh); err != nil {
		return err
	}

	if err := u.Done(tx); err != nil {
		return fmt.Errorf("%w", err)
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindLogIndex(logPrefix string, db kv.RwTx, to uint64, cfg LogIndexCfg, quitCh <-chan struct{}) error {
	topics := map[string]struct{}{}
	addrs := map[string]struct{}{}

	reader := bytes.NewReader(nil)
	c, err := db.Cursor(kv.Log)
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
		reader.Reset(v)
		if err := cbor.Unmarshal(&logs, reader); err != nil {
			return fmt.Errorf("receipt unmarshal: %w, block=%d", err, binary.BigEndian.Uint64(k))
		}

		for _, l := range logs {
			for _, topic := range l.Topics {
				topics[string(topic.Bytes())] = struct{}{}
			}
			addrs[string(l.Address.Bytes())] = struct{}{}
		}
	}

	if err := truncateBitmaps(db, kv.LogTopicIndex, topics, to); err != nil {
		return err
	}
	if err := truncateBitmaps(db, kv.LogAddressIndex, addrs, to); err != nil {
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

func truncateBitmaps(tx kv.RwTx, bucket string, inMem map[string]struct{}, to uint64) error {
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

func pruneOldLogChunks(tx kv.RwTx, bucket string, inMem map[string]struct{}, pruneTo uint64, logPrefix string, ctx context.Context) error {
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	keys := make([]string, 0, len(inMem))
	for k := range inMem {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	c, err := tx.RwCursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()
	for _, kS := range keys {
		seek := []byte(kS)
		for k, _, err := c.Seek(seek); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			blockNum := uint64(binary.BigEndian.Uint32(k[len(seek):]))
			if !bytes.HasPrefix(k, seek) || blockNum >= pruneTo {
				break
			}
			select {
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[%s]", logPrefix), "table", kv.AccountsHistory, "block", blockNum)
			case <-ctx.Done():
				return common.ErrStopped
			default:
			}
			if err = c.DeleteCurrent(); err != nil {
				return fmt.Errorf("failed delete, block=%d: %w", blockNum, err)
			}
		}
	}
	return nil
}

func PruneLogIndex(s *PruneState, tx kv.RwTx, cfg LogIndexCfg, ctx context.Context) (err error) {
	if !cfg.prune.Receipts.Enabled() {
		return nil
	}
	logPrefix := s.LogPrefix()

	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	pruneTo := cfg.prune.Receipts.PruneTo(s.ForwardProgress)
	if err = pruneLogIndex(logPrefix, tx, cfg.tmpdir, pruneTo, ctx); err != nil {
		return err
	}
	if err = s.Done(tx); err != nil {
		return err
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func pruneLogIndex(logPrefix string, tx kv.RwTx, tmpDir string, pruneTo uint64, ctx context.Context) error {
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	topics := map[string]struct{}{}
	addrs := map[string]struct{}{}

	reader := bytes.NewReader(nil)
	{
		c, err := tx.Cursor(kv.Log)
		if err != nil {
			return err
		}
		defer c.Close()

		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			blockNum := binary.BigEndian.Uint64(k)
			if blockNum >= pruneTo {
				break
			}
			select {
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[%s]", logPrefix), "table", kv.Log, "block", blockNum)
			case <-ctx.Done():
				return common.ErrStopped
			default:
			}

			var logs types.Logs
			reader.Reset(v)
			if err := cbor.Unmarshal(&logs, reader); err != nil {
				return fmt.Errorf("receipt unmarshal failed: %w, block=%d", err, binary.BigEndian.Uint64(k))
			}

			for _, l := range logs {
				for _, topic := range l.Topics {
					topics[string(topic.Bytes())] = struct{}{}
				}
				addrs[string(l.Address.Bytes())] = struct{}{}
			}
		}
	}

	if err := pruneOldLogChunks(tx, kv.LogTopicIndex, topics, pruneTo, logPrefix, ctx); err != nil {
		return err
	}
	if err := pruneOldLogChunks(tx, kv.LogAddressIndex, addrs, pruneTo, logPrefix, ctx); err != nil {
		return err
	}
	return nil
}
