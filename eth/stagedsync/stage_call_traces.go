package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"runtime"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/c2h5oh/datasize"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/params"
)

type CallTracesCfg struct {
	db      kv.RwDB
	prune   prune.Mode
	ToBlock uint64 // not setting this params means no limit
	tmpdir  string
}

func StageCallTracesCfg(
	db kv.RwDB,
	prune prune.Mode,
	toBlock uint64,
	tmpdir string,
) CallTracesCfg {
	return CallTracesCfg{
		db:      db,
		prune:   prune,
		ToBlock: toBlock,
		tmpdir:  tmpdir,
	}
}

func SpawnCallTraces(s *StageState, tx kv.RwTx, cfg CallTracesCfg, ctx context.Context) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	quit := ctx.Done()
	endBlock, err := s.ExecutionAt(tx)
	if cfg.ToBlock > 0 && cfg.ToBlock < endBlock {
		endBlock = cfg.ToBlock
	}
	logPrefix := s.LogPrefix()
	if err != nil {
		return fmt.Errorf("getting last executed block: %w", err)
	}
	if endBlock == s.BlockNumber {
		return nil
	}

	if err := promoteCallTraces(logPrefix, tx, s.BlockNumber+1, endBlock, bitmapsBufLimit, bitmapsFlushEvery, quit, cfg.tmpdir); err != nil {
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

func promoteCallTraces(logPrefix string, tx kv.RwTx, startBlock, endBlock uint64, bufLimit datasize.ByteSize, flushEvery time.Duration, quit <-chan struct{}, tmpdir string) error {
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	froms := map[string]*roaring64.Bitmap{}
	tos := map[string]*roaring64.Bitmap{}
	collectorFrom := etl.NewCollector(logPrefix, tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collectorFrom.Close()
	collectorTo := etl.NewCollector(logPrefix, tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collectorTo.Close()
	checkFlushEvery := time.NewTicker(flushEvery)
	defer checkFlushEvery.Stop()

	traceCursor, err := tx.RwCursorDupSort(kv.CallTraceSet)
	if err != nil {
		return fmt.Errorf("failed to create cursor: %w", err)
	}
	defer traceCursor.Close()

	var k, v []byte
	prev := startBlock
	for k, v, err = traceCursor.Seek(hexutility.EncodeTs(startBlock)); k != nil; k, v, err = traceCursor.Next() {
		if err != nil {
			return err
		}
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum > endBlock {
			break
		}
		if len(v) != length.Addr+1 {
			return fmt.Errorf(" wrong size of value in CallTraceSet: %x (size %d)", v, len(v))
		}
		mapKey := string(v[:length.Addr])
		if v[length.Addr]&1 > 0 {
			m, ok := froms[mapKey]
			if !ok {
				m = roaring64.New()
				froms[mapKey] = m
			}
			m.Add(blockNum)
		}
		if v[length.Addr]&2 > 0 {
			m, ok := tos[mapKey]
			if !ok {
				m = roaring64.New()
				tos[mapKey] = m
			}
			m.Add(blockNum)
		}
		select {
		default:
		case <-logEvery.C:
			var m runtime.MemStats
			dbg.ReadMemStats(&m)
			speed := float64(blockNum-prev) / float64(logInterval/time.Second)
			prev = blockNum

			log.Info(fmt.Sprintf("[%s] Progress", logPrefix), "number", blockNum,
				"blk/second", speed,
				"alloc", libcommon.ByteCount(m.Alloc),
				"sys", libcommon.ByteCount(m.Sys))
		case <-checkFlushEvery.C:
			if needFlush64(froms, bufLimit) {
				if err := flushBitmaps64(collectorFrom, froms); err != nil {
					return err
				}

				froms = map[string]*roaring64.Bitmap{}
			}

			if needFlush64(tos, bufLimit) {
				if err := flushBitmaps64(collectorTo, tos); err != nil {
					return err
				}

				tos = map[string]*roaring64.Bitmap{}
			}
		}
	}
	if err = flushBitmaps64(collectorFrom, froms); err != nil {
		return err
	}
	if err = flushBitmaps64(collectorTo, tos); err != nil {
		return err
	}

	// Clean up before loading call traces to reclaim space
	var prunedMin uint64 = math.MaxUint64
	var prunedMax uint64 = 0
	for k, _, err = traceCursor.First(); k != nil; k, _, err = traceCursor.NextNoDup() {
		if err != nil {
			return err
		}
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum+params.FullImmutabilityThreshold >= endBlock {
			break
		}
		select {
		default:
		case <-logEvery.C:
			var m runtime.MemStats
			dbg.ReadMemStats(&m)
			log.Info(fmt.Sprintf("[%s] Pruning call trace table", logPrefix), "number", blockNum,
				"alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys))
		}
		if err = traceCursor.DeleteCurrentDuplicates(); err != nil {
			return fmt.Errorf("remove trace call set for block %d: %w", blockNum, err)
		}
		if blockNum < prunedMin {
			prunedMin = blockNum
		}
		if blockNum > prunedMax {
			prunedMax = blockNum
		}
	}
	if prunedMax != 0 && prunedMax > prunedMin+16 {
		log.Info(fmt.Sprintf("[%s] Pruned call trace intermediate table", logPrefix), "from", prunedMin, "to", prunedMax)
	}

	if err := finaliseCallTraces(collectorFrom, collectorTo, logPrefix, tx, quit); err != nil {
		return err
	}

	return nil
}

func finaliseCallTraces(collectorFrom, collectorTo *etl.Collector, logPrefix string, tx kv.RwTx, quit <-chan struct{}) error {
	var buf = bytes.NewBuffer(nil)
	lastChunkKey := make([]byte, 128)
	reader := bytes.NewReader(nil)
	reader2 := bytes.NewReader(nil)
	var loaderFunc = func(k []byte, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		reader.Reset(v)
		currentBitmap := roaring64.New()
		if _, err := currentBitmap.ReadFrom(reader); err != nil {
			return err
		}
		lastChunkKey = lastChunkKey[:len(k)+8]
		copy(lastChunkKey, k)
		binary.BigEndian.PutUint64(lastChunkKey[len(k):], ^uint64(0))
		lastChunkBytes, err := table.Get(lastChunkKey)
		if err != nil {
			return fmt.Errorf("find last chunk failed: %w", err)
		}

		if len(lastChunkBytes) > 0 {
			lastChunk := roaring64.New()
			reader2.Reset(lastChunkBytes)
			_, err = lastChunk.ReadFrom(reader2)
			if err != nil {
				return fmt.Errorf("couldn't read last log index chunk: %w, len(lastChunkBytes)=%d", err, len(lastChunkBytes))
			}
			currentBitmap.Or(lastChunk) // merge last existing chunk from db - next loop will overwrite it
		}
		if err := bitmapdb.WalkChunkWithKeys64(k, currentBitmap, bitmapdb.ChunkLimit, func(chunkKey []byte, chunk *roaring64.Bitmap) error {
			buf.Reset()
			if _, err := chunk.WriteTo(buf); err != nil {
				return err
			}
			return next(k, chunkKey, buf.Bytes())
		}); err != nil {
			return err
		}
		return nil
	}
	if err := collectorFrom.Load(tx, kv.CallFromIndex, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}
	if err := collectorTo.Load(tx, kv.CallToIndex, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
		return err
	}
	return nil
}

func UnwindCallTraces(u *UnwindState, s *StageState, tx kv.RwTx, cfg CallTracesCfg, ctx context.Context) (err error) {
	if s.BlockNumber <= u.UnwindPoint {
		return nil
	}
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logPrefix := u.LogPrefix()
	if s.BlockNumber-u.UnwindPoint > 16 {
		log.Info(fmt.Sprintf("[%s] Unwind", logPrefix), "from", s.BlockNumber, "to", u.UnwindPoint)
	}
	if err := DoUnwindCallTraces(logPrefix, tx, s.BlockNumber, u.UnwindPoint, ctx, cfg.tmpdir); err != nil {
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

func DoUnwindCallTraces(logPrefix string, db kv.RwTx, from, to uint64, ctx context.Context, tmpdir string) error {
	froms := etl.NewCollector(logPrefix, tmpdir, etl.NewOldestEntryBuffer(etl.BufferOptimalSize))
	defer froms.Close()
	tos := etl.NewCollector(logPrefix, tmpdir, etl.NewOldestEntryBuffer(etl.BufferOptimalSize))
	defer tos.Close()

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	traceCursor, err := db.RwCursorDupSort(kv.CallTraceSet)
	if err != nil {
		return fmt.Errorf("create cursor for call traces: %w", err)
	}
	defer traceCursor.Close()

	var k, v []byte
	prev := to + 1
	for k, v, err = traceCursor.Seek(hexutility.EncodeTs(to + 1)); k != nil; k, v, err = traceCursor.Next() {
		if err != nil {
			return err
		}
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= from {
			break
		}
		if len(v) != length.Addr+1 {
			return fmt.Errorf("wrong size of value in CallTraceSet: %x (size %d)", v, len(v))
		}
		mapKey := v[:length.Addr]
		if v[length.Addr]&1 > 0 {
			if err = froms.Collect(mapKey, nil); err != nil {
				return err
			}
		}
		if v[length.Addr]&2 > 0 {
			if err = tos.Collect(mapKey, nil); err != nil {
				return err
			}
		}
		select {
		case <-logEvery.C:
			var m runtime.MemStats
			dbg.ReadMemStats(&m)
			speed := float64(blockNum-prev) / float64(logInterval/time.Second)
			prev = blockNum

			log.Info(fmt.Sprintf("[%s] Progress", logPrefix), "number", blockNum,
				"blk/second", speed,
				"alloc", libcommon.ByteCount(m.Alloc),
				"sys", libcommon.ByteCount(m.Sys))
		case <-ctx.Done():
			return libcommon.ErrStopped
		default:
		}
	}

	if err = froms.Load(db, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		return bitmapdb.TruncateRange64(db, kv.CallFromIndex, k, to+1)
	}, etl.TransformArgs{}); err != nil {
		return fmt.Errorf("TruncateRange: bucket=%s, %w", kv.CallFromIndex, err)
	}

	if err = tos.Load(db, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		return bitmapdb.TruncateRange64(db, kv.CallToIndex, k, to+1)
	}, etl.TransformArgs{}); err != nil {
		return fmt.Errorf("TruncateRange: bucket=%s, %w", kv.CallFromIndex, err)
	}
	return nil
}

func PruneCallTraces(s *PruneState, tx kv.RwTx, cfg CallTracesCfg, ctx context.Context) (err error) {
	logPrefix := s.LogPrefix()

	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if cfg.prune.CallTraces.Enabled() {
		if err = pruneCallTraces(tx, logPrefix, cfg.prune.CallTraces.PruneTo(s.ForwardProgress), ctx, cfg.tmpdir); err != nil {
			return err
		}
	}
	if err := s.Done(tx); err != nil {
		return err
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func pruneCallTraces(tx kv.RwTx, logPrefix string, pruneTo uint64, ctx context.Context, tmpdir string) error {
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	froms := etl.NewCollector(logPrefix, tmpdir, etl.NewOldestEntryBuffer(etl.BufferOptimalSize))
	defer froms.Close()
	tos := etl.NewCollector(logPrefix, tmpdir, etl.NewOldestEntryBuffer(etl.BufferOptimalSize))
	defer tos.Close()

	{
		traceCursor, err := tx.CursorDupSort(kv.CallTraceSet)
		if err != nil {
			return fmt.Errorf("create cursor for call traces: %w", err)
		}
		defer traceCursor.Close()

		var k, v []byte
		for k, v, err = traceCursor.First(); k != nil; k, v, err = traceCursor.Next() {
			if err != nil {
				return err
			}
			blockNum := binary.BigEndian.Uint64(k)
			if blockNum >= pruneTo {
				break
			}
			if len(v) != length.Addr+1 {
				return fmt.Errorf("wrong size of value in CallTraceSet: %x (size %d)", v, len(v))
			}
			mapKey := v[:length.Addr]
			if v[length.Addr]&1 > 0 {
				if err := froms.Collect(mapKey, nil); err != nil {
					return err
				}
			}
			if v[length.Addr]&2 > 0 {
				if err := tos.Collect(mapKey, nil); err != nil {
					return err
				}
			}
			select {
			case <-logEvery.C:
				var m runtime.MemStats
				dbg.ReadMemStats(&m)
				log.Info(fmt.Sprintf("[%s] Progress", logPrefix), "number", blockNum, "alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys))
			case <-ctx.Done():
				return libcommon.ErrStopped
			default:
			}
		}
	}

	{
		c, err := tx.RwCursor(kv.CallFromIndex)
		if err != nil {
			return err
		}
		defer c.Close()

		if err := froms.Load(tx, "", func(from, _ []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
			for k, _, err := c.Seek(from); k != nil; k, _, err = c.Next() {
				if err != nil {
					return err
				}
				blockNum := binary.BigEndian.Uint64(k[length.Addr:])
				if !bytes.HasPrefix(k, from) || blockNum >= pruneTo {
					break
				}
				if err = c.DeleteCurrent(); err != nil {
					return fmt.Errorf("failed delete, block=%d: %w", blockNum, err)
				}
			}
			select {
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[%s]", logPrefix), "table", kv.CallFromIndex, "key", hex.EncodeToString(from))
			case <-ctx.Done():
				return libcommon.ErrStopped
			default:
			}
			return nil
		}, etl.TransformArgs{}); err != nil {
			return err
		}
	}
	{
		c, err := tx.RwCursor(kv.CallToIndex)
		if err != nil {
			return err
		}
		defer c.Close()

		if err := tos.Load(tx, "", func(to, _ []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
			for k, _, err := c.Seek(to); k != nil; k, _, err = c.Next() {
				if err != nil {
					return err
				}
				blockNum := binary.BigEndian.Uint64(k[length.Addr:])
				if !bytes.HasPrefix(k, to) || blockNum >= pruneTo {
					break
				}
				if err = c.DeleteCurrent(); err != nil {
					return fmt.Errorf("failed delete, block=%d: %w", blockNum, err)
				}
			}
			select {
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[%s]", logPrefix), "table", kv.CallToIndex, "key", hex.EncodeToString(to))
			case <-ctx.Done():
				return libcommon.ErrStopped
			default:
			}
			return nil
		}, etl.TransformArgs{}); err != nil {
			return err
		}
	}
	return nil
}
