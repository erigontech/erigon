package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/bitmapdb"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/log/v3"
)

type HistoryCfg struct {
	db         kv.RwDB
	bufLimit   datasize.ByteSize
	prune      prune.Mode
	flushEvery time.Duration
	tmpdir     string
}

func StageHistoryCfg(db kv.RwDB, prune prune.Mode, tmpDir string) HistoryCfg {
	return HistoryCfg{
		db:         db,
		prune:      prune,
		bufLimit:   bitmapsBufLimit,
		flushEvery: bitmapsFlushEvery,
		tmpdir:     tmpDir,
	}
}

func SpawnAccountHistoryIndex(s *StageState, tx kv.RwTx, cfg HistoryCfg, ctx context.Context) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	quitCh := ctx.Done()

	endBlock, err := s.ExecutionAt(tx)
	logPrefix := s.LogPrefix()
	if err != nil {
		return fmt.Errorf(" getting last executed block: %w", err)
	}
	if endBlock <= s.BlockNumber {
		return nil
	}

	var startBlock uint64
	if s.BlockNumber > 0 {
		startBlock = s.BlockNumber + 1
	}
	stopChangeSetsLookupAt := endBlock + 1

	pruneTo := cfg.prune.History.PruneTo(endBlock)
	if startBlock < pruneTo {
		startBlock = pruneTo
	}

	if err := promoteHistory(logPrefix, tx, kv.AccountChangeSet, startBlock, stopChangeSetsLookupAt, cfg, quitCh); err != nil {
		return err
	}

	if err := s.Update(tx, endBlock); err != nil {
		return err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func SpawnStorageHistoryIndex(s *StageState, tx kv.RwTx, cfg HistoryCfg, ctx context.Context) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	quitCh := ctx.Done()

	executionAt, err := s.ExecutionAt(tx)
	logPrefix := s.LogPrefix()
	if err != nil {
		return fmt.Errorf("getting last executed block: %w", err)
	}
	if executionAt <= s.BlockNumber {
		return nil
	}

	var startChangeSetsLookupAt uint64
	if s.BlockNumber > 0 {
		startChangeSetsLookupAt = s.BlockNumber + 1
	}
	stopChangeSetsLookupAt := executionAt + 1

	if err := promoteHistory(logPrefix, tx, kv.StorageChangeSet, startChangeSetsLookupAt, stopChangeSetsLookupAt, cfg, quitCh); err != nil {
		return err
	}

	if err := s.Update(tx, executionAt); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func promoteHistory(logPrefix string, tx kv.RwTx, changesetBucket string, start, stop uint64, cfg HistoryCfg, quit <-chan struct{}) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	updates := map[string]*roaring64.Bitmap{}
	checkFlushEvery := time.NewTicker(cfg.flushEvery)
	defer checkFlushEvery.Stop()

	collectorUpdates := etl.NewCollector(cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collectorUpdates.Close(logPrefix)

	if err := changeset.Walk(tx, changesetBucket, dbutils.EncodeBlockNumber(start), 0, func(blockN uint64, k, v []byte) (bool, error) {
		if blockN >= stop {
			return false, nil
		}
		if err := common.Stopped(quit); err != nil {
			return false, err
		}

		k = dbutils.CompositeKeyWithoutIncarnation(k)

		select {
		default:
		case <-logEvery.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info(fmt.Sprintf("[%s] Progress", logPrefix), "number", blockN, "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
		case <-checkFlushEvery.C:
			if needFlush64(updates, cfg.bufLimit) {
				if err := flushBitmaps64(collectorUpdates, updates); err != nil {
					return false, err
				}
				updates = map[string]*roaring64.Bitmap{}
			}
		}

		m, ok := updates[string(k)]
		if !ok {
			m = roaring64.New()
			updates[string(k)] = m
		}
		m.Add(blockN)

		return true, nil
	}); err != nil {
		return err
	}

	if err := flushBitmaps64(collectorUpdates, updates); err != nil {
		return err
	}

	var currentBitmap = roaring64.New()
	var buf = bytes.NewBuffer(nil)

	lastChunkKey := make([]byte, 128)
	var loaderFunc = func(k []byte, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if _, err := currentBitmap.ReadFrom(bytes.NewReader(v)); err != nil {
			return err
		}

		lastChunkKey = lastChunkKey[:len(k)+8]
		copy(lastChunkKey, k)
		binary.BigEndian.PutUint64(lastChunkKey[len(k):], ^uint64(0))
		lastChunkBytes, err := table.Get(lastChunkKey)
		if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
			return fmt.Errorf("find last chunk failed: %w", err)
		}
		if len(lastChunkBytes) > 0 {
			lastChunk := roaring64.New()
			_, err = lastChunk.ReadFrom(bytes.NewReader(lastChunkBytes))
			if err != nil {
				return fmt.Errorf("couldn't read last log index chunk: %w, len(lastChunkBytes)=%d", err, len(lastChunkBytes))
			}

			currentBitmap.Or(lastChunk) // merge last existing chunk from tx - next loop will overwrite it
		}
		if err = bitmapdb.WalkChunkWithKeys64(k, currentBitmap, bitmapdb.ChunkLimit, func(chunkKey []byte, chunk *roaring64.Bitmap) error {
			buf.Reset()
			if _, err = chunk.WriteTo(buf); err != nil {
				return err
			}
			return next(k, chunkKey, buf.Bytes())
		}); err != nil {
			return err
		}
		currentBitmap.Clear()
		return nil
	}

	if err := collectorUpdates.Load(logPrefix, tx, changeset.Mapper[changesetBucket].IndexBucket, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}
	return nil
}

func UnwindAccountHistoryIndex(u *UnwindState, s *StageState, tx kv.RwTx, cfg HistoryCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	quitCh := ctx.Done()
	logPrefix := s.LogPrefix()
	if err := unwindHistory(logPrefix, tx, kv.AccountChangeSet, u.UnwindPoint, cfg, quitCh); err != nil {
		return err
	}

	if err := u.Done(tx); err != nil {
		return err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func UnwindStorageHistoryIndex(u *UnwindState, s *StageState, tx kv.RwTx, cfg HistoryCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	quitCh := ctx.Done()

	logPrefix := s.LogPrefix()
	if err := unwindHistory(logPrefix, tx, kv.StorageChangeSet, u.UnwindPoint, cfg, quitCh); err != nil {
		return err
	}

	if err := u.Done(tx); err != nil {
		return err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindHistory(logPrefix string, db kv.RwTx, csBucket string, to uint64, cfg HistoryCfg, quitCh <-chan struct{}) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	updates := map[string]struct{}{}
	if err := changeset.Walk(db, csBucket, dbutils.EncodeBlockNumber(to), 0, func(blockN uint64, k, v []byte) (bool, error) {
		select {
		case <-logEvery.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info(fmt.Sprintf("[%s] Progress", logPrefix), "number", blockN, "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
		case <-quitCh:
			return false, common.ErrStopped
		default:
		}
		k = dbutils.CompositeKeyWithoutIncarnation(k)
		updates[string(k)] = struct{}{}
		return true, nil
	}); err != nil {
		return err
	}

	if err := truncateBitmaps64(db, changeset.Mapper[csBucket].IndexBucket, updates, to); err != nil {
		return err
	}
	return nil
}

func needFlush64(bitmaps map[string]*roaring64.Bitmap, memLimit datasize.ByteSize) bool {
	sz := uint64(0)
	for _, m := range bitmaps {
		sz += m.GetSizeInBytes()
	}
	const memoryNeedsForKey = 32 * 2 // each key stored in RAM: as string ang slice of bytes
	return uint64(len(bitmaps)*memoryNeedsForKey)+sz > uint64(memLimit)
}

func flushBitmaps64(c *etl.Collector, inMem map[string]*roaring64.Bitmap) error {
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

func truncateBitmaps64(tx kv.RwTx, bucket string, inMem map[string]struct{}, to uint64) error {
	keys := make([]string, 0, len(inMem))
	for k := range inMem {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := bitmapdb.TruncateRange64(tx, bucket, []byte(k), to+1); err != nil {
			return fmt.Errorf("fail TruncateRange: bucket=%s, %w", bucket, err)
		}
	}

	return nil
}

func PruneAccountHistoryIndex(s *PruneState, tx kv.RwTx, cfg HistoryCfg, ctx context.Context) (err error) {
	if !cfg.prune.History.Enabled() {
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

	pruneTo := cfg.prune.History.PruneTo(s.ForwardProgress)
	if err = pruneHistoryIndex(tx, kv.AccountChangeSet, logPrefix, cfg.tmpdir, pruneTo, ctx); err != nil {
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

func PruneStorageHistoryIndex(s *PruneState, tx kv.RwTx, cfg HistoryCfg, ctx context.Context) (err error) {
	if !cfg.prune.History.Enabled() {
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
	pruneTo := cfg.prune.History.PruneTo(s.ForwardProgress)
	if err = pruneHistoryIndex(tx, kv.StorageChangeSet, logPrefix, cfg.tmpdir, pruneTo, ctx); err != nil {
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

func pruneHistoryIndex(tx kv.RwTx, csTable, logPrefix, tmpDir string, pruneTo uint64, ctx context.Context) error {
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	collector := etl.NewCollector(tmpDir, etl.NewOldestEntryBuffer(etl.BufferOptimalSize))
	defer collector.Close(logPrefix)

	if err := changeset.Walk(tx, csTable, nil, 0, func(blockNum uint64, k, _ []byte) (bool, error) {
		if blockNum >= pruneTo {
			return false, nil
		}
		if err := collector.Collect(k, nil); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return err
	}

	c, err := tx.RwCursor(changeset.Mapper[csTable].IndexBucket)
	if err != nil {
		return fmt.Errorf("failed to create cursor for pruning %w", err)
	}
	defer c.Close()
	prefixLen := common.AddressLength
	if csTable == kv.StorageChangeSet {
		prefixLen = common.HashLength
	}
	if err := collector.Load(logPrefix, tx, "", func(addr, _ []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		select {
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s]", logPrefix), "table", changeset.Mapper[csTable].IndexBucket, "key", fmt.Sprintf("%x", addr))
		case <-ctx.Done():
			return common.ErrStopped
		default:
		}
		for k, _, err := c.Seek(addr); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			blockNum := binary.BigEndian.Uint64(k[prefixLen:])
			if !bytes.HasPrefix(k, addr) || blockNum >= pruneTo {
				break
			}
			if err = c.DeleteCurrent(); err != nil {
				return fmt.Errorf("failed to remove for block %d: %w", blockNum, err)
			}
		}
		return nil
	}, etl.TransformArgs{}); err != nil {
		return err
	}

	return nil
}
