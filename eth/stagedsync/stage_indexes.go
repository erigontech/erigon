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
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func SpawnAccountHistoryIndex(s *StageState, db ethdb.Database, tmpdir string, quitCh <-chan struct{}) error {
	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	executionAt, err := s.ExecutionAt(tx)
	logPrefix := s.state.LogPrefix()
	if err != nil {
		return fmt.Errorf("%s: getting last executed block: %w", logPrefix, err)
	}
	if executionAt == s.BlockNumber {
		s.Done()
		return nil
	}

	var startChangeSetsLookupAt uint64
	if s.BlockNumber > 0 {
		startChangeSetsLookupAt = s.BlockNumber + 1
	}
	stopChangeSetsLookupAt := executionAt + 1

	if err := promoteHistory(logPrefix, tx, dbutils.PlainAccountChangeSetBucket, startChangeSetsLookupAt, stopChangeSetsLookupAt, bitmapsBufLimit, bitmapsFlushEvery, tmpdir, quitCh); err != nil {
		return fmt.Errorf("[%s] %w", logPrefix, err)
	}

	if err := s.DoneAndUpdate(tx, executionAt); err != nil {
		return fmt.Errorf("[%s] %w", logPrefix, err)
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func SpawnStorageHistoryIndex(s *StageState, db ethdb.Database, tmpdir string, quitCh <-chan struct{}) error {
	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	executionAt, err := s.ExecutionAt(tx)
	logPrefix := s.state.LogPrefix()
	if err != nil {
		return fmt.Errorf("%s: logs index: getting last executed block: %w", logPrefix, err)
	}
	if executionAt == s.BlockNumber {
		s.Done()
		return nil
	}

	var startChangeSetsLookupAt uint64
	if s.BlockNumber > 0 {
		startChangeSetsLookupAt = s.BlockNumber + 1
	}
	stopChangeSetsLookupAt := executionAt + 1

	if err := promoteHistory(logPrefix, tx, dbutils.PlainStorageChangeSetBucket, startChangeSetsLookupAt, stopChangeSetsLookupAt, bitmapsBufLimit, bitmapsFlushEvery, tmpdir, quitCh); err != nil {
		return fmt.Errorf("[%s] %w", logPrefix, err)
	}

	if err := s.DoneAndUpdate(tx, executionAt); err != nil {
		return fmt.Errorf("[%s] %w", logPrefix, err)
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func promoteHistory(logPrefix string, db ethdb.Database, changesetBucket string, start, stop uint64, bufLimit datasize.ByteSize, flushEvery time.Duration, tmpdir string, quit <-chan struct{}) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	updates := map[string]*roaring64.Bitmap{}
	checkFlushEvery := time.NewTicker(flushEvery)
	defer checkFlushEvery.Stop()

	collectorUpdates := etl.NewCollector(tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))

	if err := changeset.Walk(db, changesetBucket, dbutils.EncodeBlockNumber(start), 0, func(blockN uint64, k, v []byte) (bool, error) {
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
			if needFlush64(updates, bufLimit) {
				if err := flushBitmaps64(collectorUpdates, updates); err != nil {
					return false, err
				}
				updates = map[string]*roaring64.Bitmap{}
			}
		}

		kStr := string(k)
		m, ok := updates[kStr]
		if !ok {
			m = roaring64.New()
			updates[kStr] = m
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

			currentBitmap.Or(lastChunk) // merge last existing chunk from db - next loop will overwrite it
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

	if err := collectorUpdates.Load(logPrefix, db, changeset.Mapper[changesetBucket].IndexBucket, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}
	return nil
}

func UnwindAccountHistoryIndex(u *UnwindState, s *StageState, db ethdb.Database, quitCh <-chan struct{}) error {
	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logPrefix := s.state.LogPrefix()
	if err := unwindHistory(logPrefix, tx, dbutils.PlainAccountChangeSetBucket, u.UnwindPoint, quitCh); err != nil {
		return fmt.Errorf("[%s] %w", logPrefix, err)
	}

	if err := u.Done(tx); err != nil {
		return fmt.Errorf("[%s] %w", logPrefix, err)
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func UnwindStorageHistoryIndex(u *UnwindState, s *StageState, db ethdb.Database, quitCh <-chan struct{}) error {
	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logPrefix := s.state.LogPrefix()
	if err := unwindHistory(logPrefix, tx, dbutils.PlainStorageChangeSetBucket, u.UnwindPoint, quitCh); err != nil {
		return fmt.Errorf("[%s] %w", logPrefix, err)
	}

	if err := u.Done(tx); err != nil {
		return fmt.Errorf("[%s] %w", logPrefix, err)
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindHistory(logPrefix string, db ethdb.Database, csBucket string, to uint64, quitCh <-chan struct{}) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	updates := map[string]struct{}{}
	if err := changeset.Walk(db, csBucket, dbutils.EncodeBlockNumber(to), 0, func(blockN uint64, k, v []byte) (bool, error) {
		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}
		select {
		default:
		case <-logEvery.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info(fmt.Sprintf("[%s] Progress", logPrefix), "number", blockN, "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
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

func truncateBitmaps64(tx ethdb.Database, bucket string, inMem map[string]struct{}, to uint64) error {
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
