package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/c2h5oh/datasize"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/bitmapdb2"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/worker"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
	"github.com/ledgerwatch/erigon/ethdb/prune"
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
	bitmapDB2  *bitmapdb2.DB
}

func StageLogIndexCfg(db kv.RwDB, prune prune.Mode, tmpDir string, bitmapDB2 *bitmapdb2.DB) LogIndexCfg {
	return LogIndexCfg{
		db:         db,
		prune:      prune,
		bufLimit:   bitmapsBufLimit,
		flushEvery: bitmapsFlushEvery,
		tmpdir:     tmpDir,
		bitmapDB2:  bitmapDB2,
	}
}

func SpawnLogIndex(s *StageState, tx kv.RwTx, cfg LogIndexCfg, ctx context.Context, prematureEndBlock uint64) error {
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
	// if prematureEndBlock is nonzero and less than the latest executed block,
	// then we only run the log index stage until prematureEndBlock
	if prematureEndBlock != 0 && prematureEndBlock < endBlock {
		endBlock = prematureEndBlock
	}
	// It is possible that prematureEndBlock < s.BlockNumber,
	// in which case it is important that we skip this stage,
	// or else we could overwrite stage_at with prematureEndBlock
	if endBlock <= s.BlockNumber {
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
	if err = promoteLogIndex(logPrefix, tx, startBlock, endBlock, cfg, ctx, !useExternalTx); err != nil {
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

const LogIndexParallelWorkers = 32

func promoteLogIndex(logPrefix string, tx kv.RwTx, start uint64, endBlock uint64, cfg LogIndexCfg, ctx context.Context,
	parallel bool) error {
	if endBlock == 0 {
		parallel = false
		log.Info(fmt.Sprintf("[%s] Not parallel", logPrefix), "reason", "endBlock == 0")
	}
	quit := ctx.Done()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	topics := map[string]*roaring.Bitmap{}
	addresses := map[string]*roaring.Bitmap{}
	checkFlushEvery := time.NewTicker(cfg.flushEvery)
	defer checkFlushEvery.Stop()

	collectorTopics := etl.NewCollector(logPrefix, cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collectorTopics.Close()
	collectorAddrs := etl.NewCollector(logPrefix, cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collectorAddrs.Close()

	if endBlock != 0 && endBlock-start > 100 {
		log.Info(fmt.Sprintf("[%s] processing", logPrefix), "from", start, "to", endBlock)
	}
	var numTxs uint64
	handleOnePart := func(blockNum uint64, ll types.Logs) {
		numTxs++
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

	if parallel {
		startTime := time.Now()
		type txLogPart struct {
			blockNum uint64
			logs     types.Logs
		}
		resChan := make(chan txLogPart, 10_000)
		g := worker.SpawnWorkersWithRoTx(ctx, logPrefix, cfg.db, start, endBlock, LogIndexParallelWorkers, func(start, end uint64, partition int, gctx context.Context, roTx kv.Tx) error {
			logs, err := roTx.Cursor(kv.Log)
			if err != nil {
				return err
			}
			defer logs.Close()
			reader := bytes.NewReader(nil)
			for k, v, err := logs.Seek(dbutils.LogKey(start, 0)); k != nil; k, v, err = logs.Next() {
				if err != nil {
					return err
				}
				if err := libcommon.Stopped(quit); err != nil {
					return err
				}
				blockNum := binary.BigEndian.Uint64(k[:8])
				if blockNum > end {
					break
				}

				var ll types.Logs
				reader.Reset(v)
				if err := cbor.Unmarshal(&ll, reader); err != nil {
					return fmt.Errorf("receipt unmarshal failed: %w, blocl=%d", err, blockNum)
				}
				resChan <- txLogPart{
					blockNum: blockNum,
					logs:     ll,
				}
			}
			return nil
		}, func() {
			close(resChan)
		})
	outer:
		for {
			select {
			case <-logEvery.C:
				var m runtime.MemStats
				dbg.ReadMemStats(&m)
				log.Info(fmt.Sprintf("[%s] Progress", logPrefix), "numTxs", numTxs, "alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys))
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
			case part, ok := <-resChan:
				if !ok {
					break outer
				}
				handleOnePart(part.blockNum, part.logs)
			}
		}
		if err := <-g.Done; err != nil {
			log.Info(fmt.Sprintf("[%s] Parallel worker error", logPrefix), "err", err)
			return err
		}
		log.Info(fmt.Sprintf("[%s] Finished parallel collecting bitmaps", logPrefix), "numTxs", numTxs, "elapsed", time.Since(startTime))
	} else {
		logs, err := tx.Cursor(kv.Log)
		if err != nil {
			return err
		}
		defer logs.Close()
		reader := bytes.NewReader(nil)
		for k, v, err := logs.Seek(dbutils.LogKey(start, 0)); k != nil; k, v, err = logs.Next() {
			if err != nil {
				return err
			}

			if err := libcommon.Stopped(quit); err != nil {
				return err
			}
			blockNum := binary.BigEndian.Uint64(k[:8])

			// if endBlock is positive, we only run the stage up until endBlock
			// if endBlock is zero, we run the stage for all available blocks
			if endBlock != 0 && blockNum > endBlock {
				log.Info(fmt.Sprintf("[%s] Reached user-specified end block", logPrefix), "endBlock", endBlock)
				break
			}

			select {
			default:
			case <-logEvery.C:
				var m runtime.MemStats
				dbg.ReadMemStats(&m)
				log.Info(fmt.Sprintf("[%s] Progress", logPrefix), "blockNum", blockNum, "numTxs", numTxs,
					"alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys))
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
			handleOnePart(blockNum, ll)
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
		// Make a copy of lastChunkBytes.  Someone is modifying the underlying buffer for some reason that I do not know yet.
		// Probably caused by reading and writing on the same key multiple times.
		// This fixes err: the fundamental arrayContainer assumption of sorted ac.content was broken, trace: [stageloop.go:133 panic.go:890 arraycontainer.go:972 arraycontainer.go:990 roaringarray.go:120 roaring.go:160 bitmapdb.go:90 bitmapdb.go:115 bitmapdb.go:123 stage_log_index.go:252 collector.go:228 collector.go:279 collector.go:230 stage_log_index.go:261 stage_log_index.go:89 default_stages.go:191 sync.go:353 sync.go:255 stageloop.go:167 stageloop.go:95 asm_amd64.s:1598]
		copyLastChunkBytes := libcommon.Copy(lastChunkBytes)

		lastChunk := roaring.New()
		if len(copyLastChunkBytes) > 0 {
			_, err = lastChunk.FromBuffer(copyLastChunkBytes)
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
	if cfg.bitmapDB2 != nil {
		pl := bitmapdb2.NewParallelLoader(cfg.bitmapDB2, 16, 64*1024*1024, 64)
		defer pl.Close()
		loadTopic := pl.ETLLoadFunc(kv.LogTopicIndex)
		loadAddress := pl.ETLLoadFunc(kv.LogAddressIndex)
		if err := collectorTopics.Load(tx, kv.LogTopicIndex, loadTopic, etl.TransformArgs{Quit: quit}); err != nil {
			return err
		}
		if err := collectorAddrs.Load(tx, kv.LogAddressIndex, loadAddress, etl.TransformArgs{Quit: quit}); err != nil {
			return err
		}
		if err := pl.Commit(); err != nil {
			return err
		}
		log.Info(fmt.Sprintf("[%s] LogIndex commit", logPrefix), "bitmapDB2Summary", pl.Summary)
		return nil
	} else {
		if err := collectorTopics.Load(tx, kv.LogTopicIndex, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
			return err
		}
		if err := collectorAddrs.Load(tx, kv.LogAddressIndex, loaderFunc, etl.TransformArgs{Quit: quit}); err != nil {
			return err
		}
		return nil
	}
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
	for k, v, err := c.Seek(hexutility.EncodeTs(to + 1)); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}

		if err := libcommon.Stopped(quitCh); err != nil {
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

	if err := truncateBitmaps(db, kv.LogTopicIndex, topics, to, cfg.bitmapDB2); err != nil {
		return err
	}
	if err := truncateBitmaps(db, kv.LogAddressIndex, addrs, to, cfg.bitmapDB2); err != nil {
		return err
	}
	return nil
}

func needFlush(bitmaps map[string]*roaring.Bitmap, memLimit datasize.ByteSize) bool {
	sz := uint64(0)
	for _, m := range bitmaps {
		sz += m.GetSizeInBytes() * 2 // for golang's overhead
	}
	const memoryNeedsForKey = 32 * 2 * 2 //  len(key) * (string and bytes) overhead * go's map overhead
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

func truncateBitmaps(tx kv.RwTx, bucket string, inMem map[string]struct{}, to uint64, bitmapDB2 *bitmapdb2.DB) error {
	keys := make([]string, 0, len(inMem))
	for k := range inMem {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		if err := bitmapdb.TruncateRange(tx, bucket, []byte(k), uint32(to+1)); err != nil {
			return fmt.Errorf("fail TruncateRange: bucket=%s, %w", bucket, err)
		}
	}

	if bitmapDB2 != nil {
		batch := bitmapDB2.NewBatch()
		defer batch.Close()
		for k := range inMem {
			if err := batch.TruncateBitmap(bucket, []byte(k), to+1); err != nil {
				return fmt.Errorf("fail TruncateRange (bitmapDB2): bucket=%s, %w", bucket, err)
			}
		}
		if err := batch.Commit(); err != nil {
			return fmt.Errorf("fail TruncateRange (bitmapDB2): bucket=%s, %w", bucket, err)
		}
	}

	return nil
}

func pruneOldLogChunks(tx kv.RwTx, bucket string, inMem *etl.Collector, pruneTo uint64, ctx context.Context, batch *bitmapdb2.Batch) error {
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	c, err := tx.RwCursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()

	if err := inMem.Load(tx, bucket, func(key, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if batch != nil {
			if err := batch.TruncateBitmap(bucket, key, pruneTo+1); err != nil {
				return err
			}
		}
		for k, _, err := c.Seek(key); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			blockNum := uint64(binary.BigEndian.Uint32(k[len(key):]))
			if !bytes.HasPrefix(k, key) || blockNum >= pruneTo {
				break
			}

			if err = c.DeleteCurrent(); err != nil {
				return fmt.Errorf("failed delete, block=%d: %w", blockNum, err)
			}
		}
		return nil
	}, etl.TransformArgs{
		Quit: ctx.Done(),
	}); err != nil {
		return err
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
	if err = pruneLogIndex(logPrefix, tx, cfg.tmpdir, pruneTo, ctx, cfg.bitmapDB2); err != nil {
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

func pruneLogIndex(logPrefix string, tx kv.RwTx, tmpDir string, pruneTo uint64, ctx context.Context, bitmapDB2 *bitmapdb2.DB) error {
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	bufferSize := etl.BufferOptimalSize
	topics := etl.NewCollector(logPrefix, tmpDir, etl.NewOldestEntryBuffer(bufferSize))
	defer topics.Close()
	addrs := etl.NewCollector(logPrefix, tmpDir, etl.NewOldestEntryBuffer(bufferSize))
	defer addrs.Close()

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
				return libcommon.ErrStopped
			default:
			}

			var logs types.Logs
			reader.Reset(v)
			if err := cbor.Unmarshal(&logs, reader); err != nil {
				return fmt.Errorf("receipt unmarshal failed: %w, block=%d", err, binary.BigEndian.Uint64(k))
			}

			for _, l := range logs {
				for _, topic := range l.Topics {
					if err := topics.Collect(topic.Bytes(), nil); err != nil {
						return err
					}
				}
				if err := addrs.Collect(l.Address.Bytes(), nil); err != nil {
					return err
				}
			}
		}
	}

	var batch *bitmapdb2.Batch
	if bitmapDB2 != nil {
		batch := bitmapDB2.NewBatch()
		defer batch.Close()
	}

	if err := pruneOldLogChunks(tx, kv.LogTopicIndex, topics, pruneTo, ctx, batch); err != nil {
		return err
	}
	if err := pruneOldLogChunks(tx, kv.LogAddressIndex, addrs, pruneTo, ctx, batch); err != nil {
		return err
	}
	if batch != nil {
		if err := batch.Commit(); err != nil {
			return fmt.Errorf("Commit (bitmapdb2): %w", err)
		}
	}
	return nil
}
