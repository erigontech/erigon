package freezeblocks

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon/cmd/hack/tool/fromdb"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapcfg"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
)

const (
	spanLength    = 6400 // Number of blocks in a span
	zerothSpanEnd = 255  // End block of 0th span
)

type BorEventSegment struct {
	seg           *compress.Decompressor // value: event_rlp
	IdxBorTxnHash *recsplit.Index        // bor_transaction_hash  -> bor_event_segment_offset
	ranges        Range
}

func (sn *BorEventSegment) closeIdx() {
	if sn.IdxBorTxnHash != nil {
		sn.IdxBorTxnHash.Close()
		sn.IdxBorTxnHash = nil
	}
}
func (sn *BorEventSegment) closeSeg() {
	if sn.seg != nil {
		sn.seg.Close()
		sn.seg = nil
	}
}
func (sn *BorEventSegment) close() {
	sn.closeSeg()
	sn.closeIdx()
}
func (sn *BorEventSegment) reopenSeg(dir string) (err error) {
	sn.closeSeg()
	fileName := snaptype.SegmentFileName(sn.ranges.from, sn.ranges.to, snaptype.BorEvents)
	sn.seg, err = compress.NewDecompressor(path.Join(dir, fileName))
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, fileName)
	}
	return nil
}
func (sn *BorEventSegment) reopenIdx(dir string) (err error) {
	sn.closeIdx()
	if sn.seg == nil {
		return nil
	}

	fileName := snaptype.IdxFileName(sn.ranges.from, sn.ranges.to, snaptype.BorEvents.String())
	sn.IdxBorTxnHash, err = recsplit.OpenIndex(path.Join(dir, fileName))
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, fileName)
	}
	if sn.IdxBorTxnHash.ModTime().Before(sn.seg.ModTime()) {
		// Index has been created before the segment file, needs to be ignored (and rebuilt) as inconsistent
		sn.IdxBorTxnHash.Close()
		sn.IdxBorTxnHash = nil
	}
	return nil
}

func (sn *BorEventSegment) reopenIdxIfNeed(dir string, optimistic bool) (err error) {
	if sn.IdxBorTxnHash != nil {
		return nil
	}
	err = sn.reopenIdx(dir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			if optimistic {
				log.Warn("[snapshots] open index", "err", err)
			} else {
				return err
			}
		}
	}
	return nil
}

type borEventSegments struct {
	lock     sync.RWMutex
	segments []*BorEventSegment
}

type BorSpanSegment struct {
	seg    *compress.Decompressor // value: span_json
	idx    *recsplit.Index        // span_id -> offset
	ranges Range
}

func (sn *BorSpanSegment) closeIdx() {
	if sn.idx != nil {
		sn.idx.Close()
		sn.idx = nil
	}
}
func (sn *BorSpanSegment) closeSeg() {
	if sn.seg != nil {
		sn.seg.Close()
		sn.seg = nil
	}
}
func (sn *BorSpanSegment) close() {
	sn.closeSeg()
	sn.closeIdx()
}
func (sn *BorSpanSegment) reopenSeg(dir string) (err error) {
	sn.closeSeg()
	fileName := snaptype.SegmentFileName(sn.ranges.from, sn.ranges.to, snaptype.BorSpans)
	sn.seg, err = compress.NewDecompressor(path.Join(dir, fileName))
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, fileName)
	}
	return nil
}
func (sn *BorSpanSegment) reopenIdx(dir string) (err error) {
	sn.closeIdx()
	if sn.seg == nil {
		return nil
	}
	fileName := snaptype.IdxFileName(sn.ranges.from, sn.ranges.to, snaptype.BorSpans.String())
	sn.idx, err = recsplit.OpenIndex(path.Join(dir, fileName))
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, fileName)
	}
	if sn.idx.ModTime().Before(sn.seg.ModTime()) {
		// Index has been created before the segment file, needs to be ignored (and rebuilt) as inconsistent
		sn.idx.Close()
		sn.idx = nil
	}
	return nil
}

func (sn *BorSpanSegment) reopenIdxIfNeed(dir string, optimistic bool) (err error) {
	if sn.idx != nil {
		return nil
	}
	err = sn.reopenIdx(dir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			if optimistic {
				log.Warn("[snapshots] open index", "err", err)
			} else {
				return err
			}
		}
	}
	return nil
}

type borSpanSegments struct {
	lock     sync.RWMutex
	segments []*BorSpanSegment
}

func (br *BlockRetire) RetireBorBlocks(ctx context.Context, blockFrom, blockTo uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []services.DownloadRequest) error) error {
	chainConfig := fromdb.ChainConfig(br.db)
	notifier, logger, blockReader, tmpDir, db, workers := br.notifier, br.logger, br.blockReader, br.tmpDir, br.db, br.workers
	logger.Log(lvl, "[bor snapshots] Retire Bor Blocks", "range", fmt.Sprintf("%dk-%dk", blockFrom/1000, blockTo/1000))
	snapshots := br.borSnapshots()
	firstTxNum := blockReader.(*BlockReader).FirstTxNumNotInSnapshots()

	if err := DumpBorBlocks(ctx, chainConfig, blockFrom, blockTo, snaptype.Erigon2SegmentSize, tmpDir, snapshots.Dir(), firstTxNum, db, workers, lvl, logger, blockReader); err != nil {
		return fmt.Errorf("DumpBorBlocks: %w", err)
	}
	if err := snapshots.ReopenFolder(); err != nil {
		return fmt.Errorf("reopen: %w", err)
	}
	snapshots.LogStat()
	if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
		notifier.OnNewSnapshot()
	}
	merger := NewBorMerger(tmpDir, workers, lvl, db, chainConfig, notifier, logger)
	rangesToMerge := merger.FindMergeRanges(snapshots.Ranges())
	if len(rangesToMerge) == 0 {
		return nil
	}
	err := merger.Merge(ctx, snapshots, rangesToMerge, snapshots.Dir(), true /* doIndex */)
	if err != nil {
		return err
	}
	if err := snapshots.ReopenFolder(); err != nil {
		return fmt.Errorf("reopen: %w", err)
	}
	snapshots.LogStat()
	if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
		notifier.OnNewSnapshot()
	}
	downloadRequest := make([]services.DownloadRequest, 0, len(rangesToMerge))
	for i := range rangesToMerge {
		r := &services.Range{From: rangesToMerge[i].from, To: rangesToMerge[i].to}
		downloadRequest = append(downloadRequest, services.NewDownloadRequest(r, "", "", true /* Bor */))
	}

	if seedNewSnapshots != nil {
		if err := seedNewSnapshots(downloadRequest); err != nil {
			return err
		}
	}
	return nil
}

func DumpBorBlocks(ctx context.Context, chainConfig *chain.Config, blockFrom, blockTo, blocksPerFile uint64, tmpDir, snapDir string, firstTxNum uint64, chainDB kv.RoDB, workers int, lvl log.Lvl, logger log.Logger, blockReader services.FullBlockReader) error {
	if blocksPerFile == 0 {
		return nil
	}

	for i := blockFrom; i < blockTo; i = chooseSegmentEnd(i, blockTo, blocksPerFile) {
		if err := dumpBorBlocksRange(ctx, i, chooseSegmentEnd(i, blockTo, blocksPerFile), tmpDir, snapDir, firstTxNum, chainDB, *chainConfig, workers, lvl, logger, blockReader); err != nil {
			return err
		}
	}
	return nil
}

func dumpBorBlocksRange(ctx context.Context, blockFrom, blockTo uint64, tmpDir, snapDir string, firstTxNum uint64, chainDB kv.RoDB, chainConfig chain.Config, workers int, lvl log.Lvl, logger log.Logger, blockReader services.FullBlockReader) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	{
		segName := snaptype.SegmentFileName(blockFrom, blockTo, snaptype.BorEvents)
		f, _ := snaptype.ParseFileName(snapDir, segName)

		sn, err := compress.NewCompressor(ctx, "Snapshot BorEvents", f.Path, tmpDir, compress.MinPatternScore, workers, log.LvlTrace, logger)
		if err != nil {
			return err
		}
		defer sn.Close()
		if err := DumpBorEvents(ctx, chainDB, blockFrom, blockTo, workers, lvl, logger, func(v []byte) error {
			return sn.AddWord(v)
		}); err != nil {
			return fmt.Errorf("DumpBorEvents: %w", err)
		}
		if err := sn.Compress(); err != nil {
			return fmt.Errorf("compress: %w", err)
		}

		p := &background.Progress{}
		if err := buildIdx(ctx, f, &chainConfig, tmpDir, p, lvl, logger); err != nil {
			return err
		}
	}
	{
		segName := snaptype.SegmentFileName(blockFrom, blockTo, snaptype.BorSpans)
		f, _ := snaptype.ParseFileName(snapDir, segName)

		sn, err := compress.NewCompressor(ctx, "Snapshot BorSpans", f.Path, tmpDir, compress.MinPatternScore, workers, log.LvlTrace, logger)
		if err != nil {
			return err
		}
		defer sn.Close()
		if err := DumpBorSpans(ctx, chainDB, blockFrom, blockTo, workers, lvl, logger, func(v []byte) error {
			return sn.AddWord(v)
		}); err != nil {
			return fmt.Errorf("DumpBorSpans: %w", err)
		}
		if err := sn.Compress(); err != nil {
			return fmt.Errorf("compress: %w", err)
		}

		p := &background.Progress{}
		if err := buildIdx(ctx, f, &chainConfig, tmpDir, p, lvl, logger); err != nil {
			return err
		}
	}
	return nil
}

func dumpBorEventRange(startEventId, endEventId uint64, tx kv.Tx, blockNum uint64, blockHash common2.Hash, collect func([]byte) error) error {
	var blockNumBuf [8]byte
	var eventIdBuf [8]byte
	txnHash := types.ComputeBorTxHash(blockNum, blockHash)
	binary.BigEndian.PutUint64(blockNumBuf[:], blockNum)
	for eventId := startEventId; eventId < endEventId; eventId++ {
		binary.BigEndian.PutUint64(eventIdBuf[:], eventId)
		event, err := tx.GetOne(kv.BorEvents, eventIdBuf[:])
		if err != nil {
			return err
		}
		snapshotRecord := make([]byte, len(event)+length.Hash+length.BlockNum+8)
		copy(snapshotRecord, txnHash[:])
		copy(snapshotRecord[length.Hash:], blockNumBuf[:])
		binary.BigEndian.PutUint64(snapshotRecord[length.Hash+length.BlockNum:], eventId)
		copy(snapshotRecord[length.Hash+length.BlockNum+8:], event)
		if err := collect(snapshotRecord); err != nil {
			return err
		}
	}
	return nil
}

// DumpBorEvents - [from, to)
func DumpBorEvents(ctx context.Context, db kv.RoDB, blockFrom, blockTo uint64, workers int, lvl log.Lvl, logger log.Logger, collect func([]byte) error) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	from := hexutility.EncodeTs(blockFrom)
	var first bool = true
	var prevBlockNum uint64
	var startEventId uint64
	var lastEventId uint64
	if err := kv.BigChunks(db, kv.BorEventNums, from, func(tx kv.Tx, blockNumBytes, eventIdBytes []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(blockNumBytes)
		if first {
			startEventId = binary.BigEndian.Uint64(eventIdBytes)
			first = false
			prevBlockNum = blockNum
		} else if blockNum != prevBlockNum {
			endEventId := binary.BigEndian.Uint64(eventIdBytes)
			blockHash, e := rawdb.ReadCanonicalHash(tx, prevBlockNum)
			if e != nil {
				return false, e
			}
			if e := dumpBorEventRange(startEventId, endEventId, tx, prevBlockNum, blockHash, collect); e != nil {
				return false, e
			}
			startEventId = endEventId
			prevBlockNum = blockNum
		}
		if blockNum >= blockTo {
			return false, nil
		}
		lastEventId = binary.BigEndian.Uint64(eventIdBytes)
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-logEvery.C:
			var m runtime.MemStats
			if lvl >= log.LvlInfo {
				dbg.ReadMemStats(&m)
			}
			logger.Log(lvl, "[bor snapshots] Dumping bor events", "block num", blockNum,
				"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys),
			)
		default:
		}
		return true, nil
	}); err != nil {
		return err
	}
	if lastEventId > startEventId {
		if err := db.View(ctx, func(tx kv.Tx) error {
			blockHash, e := rawdb.ReadCanonicalHash(tx, prevBlockNum)
			if e != nil {
				return e
			}
			return dumpBorEventRange(startEventId, lastEventId+1, tx, prevBlockNum, blockHash, collect)
		}); err != nil {
			return err
		}
	}
	return nil
}

// DumpBorEvents - [from, to)
func DumpBorSpans(ctx context.Context, db kv.RoDB, blockFrom, blockTo uint64, workers int, lvl log.Lvl, logger log.Logger, collect func([]byte) error) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	var spanFrom, spanTo uint64
	if blockFrom > zerothSpanEnd {
		spanFrom = 1 + (blockFrom-zerothSpanEnd-1)/spanLength
	}
	if blockTo > zerothSpanEnd {
		spanTo = 1 + (blockTo-zerothSpanEnd-1)/spanLength
	}
	from := hexutility.EncodeTs(spanFrom)
	if err := kv.BigChunks(db, kv.BorSpans, from, func(tx kv.Tx, spanIdBytes, spanBytes []byte) (bool, error) {
		spanId := binary.BigEndian.Uint64(spanIdBytes)
		if spanId >= spanTo {
			return false, nil
		}
		if e := collect(spanBytes); e != nil {
			return false, e
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-logEvery.C:
			var m runtime.MemStats
			if lvl >= log.LvlInfo {
				dbg.ReadMemStats(&m)
			}
			logger.Log(lvl, "[bor snapshots] Dumping bor spans", "spanId", spanId,
				"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys),
			)
		default:
		}
		return true, nil
	}); err != nil {
		return err
	}
	return nil
}

func BorEventsIdx(ctx context.Context, segmentFilePath string, blockFrom, blockTo uint64, snapDir string, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("BorEventsIdx: at=%d-%d, %v, %s", blockFrom, blockTo, rec, dbg.Stack())
		}
	}()
	// Calculate how many records there will be in the index
	d, err := compress.NewDecompressor(segmentFilePath)
	if err != nil {
		return err
	}
	defer d.Close()
	g := d.MakeGetter()
	var blockNumBuf [length.BlockNum]byte
	var first bool = true
	word := make([]byte, 0, 4096)
	var blockCount int
	var baseEventId uint64
	for g.HasNext() {
		word, _ = g.Next(word[:0])
		if first || !bytes.Equal(blockNumBuf[:], word[length.Hash:length.Hash+length.BlockNum]) {
			blockCount++
			copy(blockNumBuf[:], word[length.Hash:length.Hash+length.BlockNum])
		}
		if first {
			baseEventId = binary.BigEndian.Uint64(word[length.Hash+length.BlockNum : length.Hash+length.BlockNum+8])
			first = false
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	var idxFilePath = filepath.Join(snapDir, snaptype.IdxFileName(blockFrom, blockTo, snaptype.BorEvents.String()))

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   blockCount,
		Enums:      blockCount > 0,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpDir,
		IndexFile:  idxFilePath,
		BaseDataID: baseEventId,
	}, logger)
	if err != nil {
		return err
	}
	rs.LogLvl(log.LvlDebug)

	defer d.EnableMadvNormal().DisableReadAhead()
RETRY:
	g.Reset(0)
	first = true
	var i, offset, nextPos uint64
	for g.HasNext() {
		word, nextPos = g.Next(word[:0])
		i++
		if first || !bytes.Equal(blockNumBuf[:], word[length.Hash:length.Hash+length.BlockNum]) {
			if err = rs.AddKey(word[:length.Hash], offset); err != nil {
				return err
			}
			copy(blockNumBuf[:], word[length.Hash:length.Hash+length.BlockNum])
		}
		if first {
			first = false
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		offset = nextPos
	}
	if err = rs.Build(ctx); err != nil {
		if errors.Is(err, recsplit.ErrCollision) {
			logger.Info("Building recsplit. Collision happened. It's ok. Restarting with another salt...", "err", err)
			rs.ResetNextSalt()
			goto RETRY
		}
		return err
	}

	return nil
}

func BorSpansIdx(ctx context.Context, segmentFilePath string, blockFrom, blockTo uint64, snapDir string, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("BorSpansIdx: at=%d-%d, %v, %s", blockFrom, blockTo, rec, dbg.Stack())
		}
	}()
	// Calculate how many records there will be in the index
	d, err := compress.NewDecompressor(segmentFilePath)
	if err != nil {
		return err
	}
	defer d.Close()
	g := d.MakeGetter()
	var idxFilePath = filepath.Join(snapDir, snaptype.IdxFileName(blockFrom, blockTo, snaptype.BorSpans.String()))

	var baseSpanId uint64
	if blockFrom > zerothSpanEnd {
		baseSpanId = 1 + (blockFrom-zerothSpanEnd-1)/spanLength
	}

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   d.Count(),
		Enums:      d.Count() > 0,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpDir,
		IndexFile:  idxFilePath,
		BaseDataID: baseSpanId,
	}, logger)
	if err != nil {
		return err
	}
	rs.LogLvl(log.LvlDebug)

	defer d.EnableMadvNormal().DisableReadAhead()
RETRY:
	g.Reset(0)
	var i, offset, nextPos uint64
	var key [8]byte
	for g.HasNext() {
		nextPos, _ = g.Skip()
		binary.BigEndian.PutUint64(key[:], i)
		i++
		if err = rs.AddKey(key[:], offset); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		offset = nextPos
	}
	if err = rs.Build(ctx); err != nil {
		if errors.Is(err, recsplit.ErrCollision) {
			logger.Info("Building recsplit. Collision happened. It's ok. Restarting with another salt...", "err", err)
			rs.ResetNextSalt()
			goto RETRY
		}
		return err
	}

	return nil
}

type BorRoSnapshots struct {
	indicesReady  atomic.Bool
	segmentsReady atomic.Bool

	Events *borEventSegments
	Spans  *borSpanSegments

	dir         string
	segmentsMax atomic.Uint64 // all types of .seg files are available - up to this number
	idxMax      atomic.Uint64 // all types of .idx files are available - up to this number
	cfg         ethconfig.BlocksFreezing
	logger      log.Logger
}

// NewBorRoSnapshots - opens all bor snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewBorRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, logger log.Logger) *BorRoSnapshots {
	return &BorRoSnapshots{dir: snapDir, cfg: cfg, Events: &borEventSegments{}, Spans: &borSpanSegments{}, logger: logger}
}

func (s *BorRoSnapshots) Cfg() ethconfig.BlocksFreezing { return s.cfg }
func (s *BorRoSnapshots) Dir() string                   { return s.dir }
func (s *BorRoSnapshots) SegmentsReady() bool           { return s.segmentsReady.Load() }
func (s *BorRoSnapshots) IndicesReady() bool            { return s.indicesReady.Load() }
func (s *BorRoSnapshots) IndicesMax() uint64            { return s.idxMax.Load() }
func (s *BorRoSnapshots) SegmentsMax() uint64           { return s.segmentsMax.Load() }
func (s *BorRoSnapshots) BlocksAvailable() uint64 {
	return cmp.Min(s.segmentsMax.Load(), s.idxMax.Load())
}
func (s *BorRoSnapshots) LogStat() {
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	s.logger.Info("[bor snapshots] Blocks Stat",
		"blocks", fmt.Sprintf("%dk", (s.SegmentsMax()+1)/1000),
		"indices", fmt.Sprintf("%dk", (s.IndicesMax()+1)/1000),
		"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
}

func BorSegments(dir string) (res []snaptype.FileInfo, missingSnapshots []Range, err error) {
	list, err := snaptype.Segments(dir)
	if err != nil {
		return nil, missingSnapshots, err
	}
	{
		var l []snaptype.FileInfo
		var m []Range
		for _, f := range list {
			if f.T != snaptype.BorEvents {
				continue
			}
			l = append(l, f)
		}
		l, m = noGaps(noOverlaps(borSegmentsMustExist(dir, l)))
		res = append(res, l...)
		missingSnapshots = append(missingSnapshots, m...)
	}
	{
		var l []snaptype.FileInfo
		for _, f := range list {
			if f.T != snaptype.BorSpans {
				continue
			}
			l = append(l, f)
		}
		l, _ = noGaps(noOverlaps(borSegmentsMustExist(dir, l)))
		res = append(res, l...)
	}

	return res, missingSnapshots, nil
}

func (s *BorRoSnapshots) ScanDir() (map[string]struct{}, []*services.Range, error) {
	existingFiles, missingSnapshots, err := BorSegments(s.dir)
	if err != nil {
		return nil, nil, err
	}
	existingFilesMap := map[string]struct{}{}
	for _, existingFile := range existingFiles {
		_, fname := filepath.Split(existingFile.Path)
		existingFilesMap[fname] = struct{}{}
	}

	res := make([]*services.Range, 0, len(missingSnapshots))
	for _, sn := range missingSnapshots {
		res = append(res, &services.Range{From: sn.from, To: sn.to})
	}
	return existingFilesMap, res, nil
}
func (s *BorRoSnapshots) EnsureExpectedBlocksAreAvailable(cfg *snapcfg.Cfg) error {
	if s.BlocksAvailable() < cfg.ExpectBlocks {
		return fmt.Errorf("app must wait until all expected bor snapshots are available. Expected: %d, Available: %d", cfg.ExpectBlocks, s.BlocksAvailable())
	}
	return nil
}

// DisableReadAhead - usage: `defer d.EnableReadAhead().DisableReadAhead()`. Please don't use this funcs without `defer` to avoid leak.
func (s *BorRoSnapshots) DisableReadAhead() {
	s.Events.lock.RLock()
	defer s.Events.lock.RUnlock()
	s.Spans.lock.RLock()
	defer s.Spans.lock.RUnlock()
	for _, sn := range s.Events.segments {
		sn.seg.DisableReadAhead()
	}
	for _, sn := range s.Spans.segments {
		sn.seg.DisableReadAhead()
	}
}
func (s *BorRoSnapshots) EnableReadAhead() *BorRoSnapshots {
	s.Events.lock.RLock()
	defer s.Events.lock.RUnlock()
	s.Spans.lock.RLock()
	defer s.Spans.lock.RUnlock()
	for _, sn := range s.Events.segments {
		sn.seg.EnableReadAhead()
	}
	for _, sn := range s.Spans.segments {
		sn.seg.EnableReadAhead()
	}
	return s
}
func (s *BorRoSnapshots) EnableMadvWillNeed() *BorRoSnapshots {
	s.Events.lock.RLock()
	defer s.Events.lock.RUnlock()
	s.Spans.lock.RLock()
	defer s.Spans.lock.RUnlock()
	for _, sn := range s.Events.segments {
		sn.seg.EnableWillNeed()
	}
	for _, sn := range s.Spans.segments {
		sn.seg.EnableWillNeed()
	}
	return s
}
func (s *BorRoSnapshots) EnableMadvNormal() *BorRoSnapshots {
	s.Events.lock.RLock()
	defer s.Events.lock.RUnlock()
	s.Spans.lock.RLock()
	defer s.Spans.lock.RUnlock()
	for _, sn := range s.Events.segments {
		sn.seg.EnableMadvNormal()
	}
	for _, sn := range s.Spans.segments {
		sn.seg.EnableMadvNormal()
	}
	return s
}

func (s *BorRoSnapshots) idxAvailability() uint64 {
	var events, spans uint64
	for _, seg := range s.Events.segments {
		if seg.IdxBorTxnHash == nil {
			break
		}
		events = seg.ranges.to - 1
	}
	for _, seg := range s.Spans.segments {
		if seg.idx == nil {
			break
		}
		spans = seg.ranges.to - 1
	}
	return cmp.Min(events, spans)
}

// OptimisticReopenWithDB - optimistically open snapshots (ignoring error), useful at App startup because:
// - user must be able: delete any snapshot file and Erigon will self-heal by re-downloading
// - RPC return Nil for historical blocks if snapshots are not open
func (s *BorRoSnapshots) OptimisticReopenWithDB(db kv.RoDB) {
	_ = db.View(context.Background(), func(tx kv.Tx) error {
		snList, _, err := rawdb.ReadSnapshots(tx)
		if err != nil {
			return err
		}
		return s.ReopenList(snList, true)
	})
}

func (s *BorRoSnapshots) Files() (list []string) {
	s.Events.lock.RLock()
	defer s.Events.lock.RUnlock()
	s.Spans.lock.RLock()
	defer s.Spans.lock.RUnlock()
	max := s.BlocksAvailable()
	for _, seg := range s.Events.segments {
		if seg.seg == nil {
			continue
		}
		if seg.ranges.from > max {
			continue
		}
		_, fName := filepath.Split(seg.seg.FilePath())
		list = append(list, fName)
	}
	for _, seg := range s.Spans.segments {
		if seg.seg == nil {
			continue
		}
		if seg.ranges.from > max {
			continue
		}
		_, fName := filepath.Split(seg.seg.FilePath())
		list = append(list, fName)
	}
	slices.Sort(list)
	return list
}

// ReopenList stops on optimistic=false, continue opening files on optimistic=true
func (s *BorRoSnapshots) ReopenList(fileNames []string, optimistic bool) error {
	s.Events.lock.Lock()
	defer s.Events.lock.Unlock()
	s.Spans.lock.RLock()
	defer s.Spans.lock.RUnlock()

	s.closeWhatNotInList(fileNames)
	var segmentsMax uint64
	var segmentsMaxSet bool
Loop:
	for _, fName := range fileNames {
		f, ok := snaptype.ParseFileName(s.dir, fName)
		if !ok {
			s.logger.Trace("BorRoSnapshots.ReopenList: skip", "file", fName)
			continue
		}

		var processed bool = true
		switch f.T {
		case snaptype.BorEvents:
			var sn *BorEventSegment
			var exists bool
			for _, sn2 := range s.Events.segments {
				if sn2.seg == nil { // it's ok if some segment was not able to open
					continue
				}
				if fName == sn2.seg.FileName() {
					sn = sn2
					exists = true
					break
				}
			}
			if !exists {
				sn = &BorEventSegment{ranges: Range{f.From, f.To}}
			}
			if err := sn.reopenSeg(s.dir); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					if optimistic {
						continue Loop
					} else {
						break Loop
					}
				}
				if optimistic {
					s.logger.Warn("[bor snapshots] open segment", "err", err)
					continue Loop
				} else {
					return err
				}
			}

			if !exists {
				// it's possible to iterate over .seg file even if you don't have index
				// then make segment availabe even if index open may fail
				s.Events.segments = append(s.Events.segments, sn)
			}
			if err := sn.reopenIdxIfNeed(s.dir, optimistic); err != nil {
				return err
			}
		case snaptype.BorSpans:
			var sn *BorSpanSegment
			var exists bool
			for _, sn2 := range s.Spans.segments {
				if sn2.seg == nil { // it's ok if some segment was not able to open
					continue
				}
				if fName == sn2.seg.FileName() {
					sn = sn2
					exists = true
					break
				}
			}
			if !exists {
				sn = &BorSpanSegment{ranges: Range{f.From, f.To}}
			}
			if err := sn.reopenSeg(s.dir); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					if optimistic {
						continue Loop
					} else {
						break Loop
					}
				}
				if optimistic {
					s.logger.Warn("[bor snapshots] open segment", "err", err)
					continue Loop
				} else {
					return err
				}
			}

			if !exists {
				// it's possible to iterate over .seg file even if you don't have index
				// then make segment availabe even if index open may fail
				s.Spans.segments = append(s.Spans.segments, sn)
			}
			if err := sn.reopenIdxIfNeed(s.dir, optimistic); err != nil {
				return err
			}
		default:
			processed = false
		}

		if processed {
			if f.To > 0 {
				segmentsMax = f.To - 1
			} else {
				segmentsMax = 0
			}
			segmentsMaxSet = true
		}
	}
	if segmentsMaxSet {
		s.segmentsMax.Store(segmentsMax)
	}
	s.segmentsReady.Store(true)
	s.idxMax.Store(s.idxAvailability())
	s.indicesReady.Store(true)

	return nil
}

func (s *BorRoSnapshots) Ranges() (ranges []Range) {
	view := s.View()
	defer view.Close()

	for _, sn := range view.Events() {
		ranges = append(ranges, sn.ranges)
	}
	return ranges
}

func (s *BorRoSnapshots) OptimisticalyReopenFolder()           { _ = s.ReopenFolder() }
func (s *BorRoSnapshots) OptimisticalyReopenWithDB(db kv.RoDB) { _ = s.ReopenWithDB(db) }
func (s *BorRoSnapshots) ReopenFolder() error {
	files, _, err := BorSegments(s.dir)
	if err != nil {
		return err
	}
	list := make([]string, 0, len(files))
	for _, f := range files {
		_, fName := filepath.Split(f.Path)
		list = append(list, fName)
	}
	return s.ReopenList(list, false)
}
func (s *BorRoSnapshots) ReopenWithDB(db kv.RoDB) error {
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		snList, _, err := rawdb.ReadSnapshots(tx)
		if err != nil {
			return err
		}
		return s.ReopenList(snList, true)
	}); err != nil {
		return err
	}
	return nil
}

func (s *BorRoSnapshots) Close() {
	s.Events.lock.Lock()
	defer s.Events.lock.Unlock()
	s.closeWhatNotInList(nil)
}

func (s *BorRoSnapshots) closeWhatNotInList(l []string) {
Loop1:
	for i, sn := range s.Events.segments {
		if sn.seg == nil {
			continue Loop1
		}
		_, name := filepath.Split(sn.seg.FilePath())
		for _, fName := range l {
			if fName == name {
				continue Loop1
			}
		}
		sn.close()
		s.Events.segments[i] = nil
	}
Loop2:
	for i, sn := range s.Spans.segments {
		if sn.seg == nil {
			continue Loop2
		}
		_, name := filepath.Split(sn.seg.FilePath())
		for _, fName := range l {
			if fName == name {
				continue Loop2
			}
		}
		sn.close()
		s.Spans.segments[i] = nil
	}
	var i int
	for i = 0; i < len(s.Events.segments) && s.Events.segments[i] != nil && s.Events.segments[i].seg != nil; i++ {
	}
	tail := s.Events.segments[i:]
	s.Events.segments = s.Events.segments[:i]
	for i = 0; i < len(tail); i++ {
		if tail[i] != nil {
			tail[i].close()
			tail[i] = nil
		}
	}
	for i = 0; i < len(s.Spans.segments) && s.Spans.segments[i] != nil && s.Spans.segments[i].seg != nil; i++ {
	}
	tailS := s.Spans.segments[i:]
	s.Spans.segments = s.Spans.segments[:i]
	for i = 0; i < len(tailS); i++ {
		if tailS[i] != nil {
			tailS[i].close()
			tailS[i] = nil
		}
	}
}

func (s *BorRoSnapshots) PrintDebug() {
	s.Events.lock.RLock()
	defer s.Events.lock.RUnlock()
	s.Spans.lock.RLock()
	defer s.Spans.lock.RUnlock()
	fmt.Println("    == BorSnapshots, Event")
	for _, sn := range s.Events.segments {
		fmt.Printf("%d,  %t\n", sn.ranges.from, sn.IdxBorTxnHash == nil)
	}
	fmt.Println("    == BorSnapshots, Span")
	for _, sn := range s.Spans.segments {
		fmt.Printf("%d,  %t\n", sn.ranges.from, sn.idx == nil)
	}
}

type BorView struct {
	s      *BorRoSnapshots
	closed bool
}

func (s *BorRoSnapshots) View() *BorView {
	v := &BorView{s: s}
	v.s.Events.lock.RLock()
	v.s.Spans.lock.RLock()
	return v
}

func (v *BorView) Close() {
	if v.closed {
		return
	}
	v.closed = true
	v.s.Events.lock.RUnlock()
	v.s.Spans.lock.RUnlock()
}
func (v *BorView) Events() []*BorEventSegment { return v.s.Events.segments }
func (v *BorView) Spans() []*BorSpanSegment   { return v.s.Spans.segments }
func (v *BorView) EventsSegment(blockNum uint64) (*BorEventSegment, bool) {
	for _, seg := range v.Events() {
		if !(blockNum >= seg.ranges.from && blockNum < seg.ranges.to) {
			continue
		}
		return seg, true
	}
	return nil, false
}
func (v *BorView) SpansSegment(blockNum uint64) (*BorSpanSegment, bool) {
	for _, seg := range v.Spans() {
		if !(blockNum >= seg.ranges.from && blockNum < seg.ranges.to) {
			continue
		}
		return seg, true
	}
	return nil, false
}

type BorMerger struct {
	lvl             log.Lvl
	compressWorkers int
	tmpDir          string
	chainConfig     *chain.Config
	chainDB         kv.RoDB
	notifier        services.DBEventNotifier
	logger          log.Logger
}

func NewBorMerger(tmpDir string, compressWorkers int, lvl log.Lvl, chainDB kv.RoDB, chainConfig *chain.Config, notifier services.DBEventNotifier, logger log.Logger) *BorMerger {
	return &BorMerger{tmpDir: tmpDir, compressWorkers: compressWorkers, lvl: lvl, chainDB: chainDB, chainConfig: chainConfig, notifier: notifier, logger: logger}
}

func (*BorMerger) FindMergeRanges(currentRanges []Range) (toMerge []Range) {
	for i := len(currentRanges) - 1; i > 0; i-- {
		r := currentRanges[i]
		if r.to-r.from >= snaptype.Erigon2SegmentSize { // is complete .seg
			continue
		}

		for _, span := range []uint64{500_000, 100_000, 10_000} {
			if r.to%span != 0 {
				continue
			}
			if r.to-r.from == span {
				break
			}
			aggFrom := r.to - span
			toMerge = append(toMerge, Range{from: aggFrom, to: r.to})
			for currentRanges[i].from > aggFrom {
				i--
			}
			break
		}
	}
	slices.SortFunc(toMerge, func(i, j Range) bool { return i.from < j.from })
	return toMerge
}

func (m *BorMerger) filesByRange(snapshots *BorRoSnapshots, from, to uint64) (map[snaptype.Type][]string, error) {
	toMerge := map[snaptype.Type][]string{}
	view := snapshots.View()
	defer view.Close()

	eSegments := view.Events()
	sSegments := view.Spans()

	for i, sn := range eSegments {
		if sn.ranges.from < from {
			continue
		}
		if sn.ranges.to > to {
			break
		}
		toMerge[snaptype.BorEvents] = append(toMerge[snaptype.BorEvents], eSegments[i].seg.FilePath())
		toMerge[snaptype.BorSpans] = append(toMerge[snaptype.BorSpans], sSegments[i].seg.FilePath())
	}

	return toMerge, nil
}

// Merge does merge segments in given ranges
func (m *BorMerger) Merge(ctx context.Context, snapshots *BorRoSnapshots, mergeRanges []Range, snapDir string, doIndex bool) error {
	if len(mergeRanges) == 0 {
		return nil
	}
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	for _, r := range mergeRanges {
		toMerge, err := m.filesByRange(snapshots, r.from, r.to)
		if err != nil {
			return err
		}

		for _, t := range []snaptype.Type{snaptype.BorEvents, snaptype.BorSpans} {
			segName := snaptype.SegmentFileName(r.from, r.to, t)
			f, ok := snaptype.ParseFileName(snapDir, segName)
			if !ok {
				continue
			}
			if err := m.merge(ctx, toMerge[t], f.Path, logEvery); err != nil {
				return fmt.Errorf("mergeByAppendSegments: %w", err)
			}
			if doIndex {
				p := &background.Progress{}
				if err := buildIdx(ctx, f, m.chainConfig, m.tmpDir, p, m.lvl, m.logger); err != nil {
					return err
				}
			}
		}
		if err := snapshots.ReopenFolder(); err != nil {
			return fmt.Errorf("ReopenSegments: %w", err)
		}
		snapshots.LogStat()
		if m.notifier != nil { // notify about new snapshots of any size
			m.notifier.OnNewSnapshot()
			time.Sleep(1 * time.Second) // i working on blocking API - to ensure client does not use old snapsthos - and then delete them
		}
		for _, t := range []snaptype.Type{snaptype.BorEvents} {
			m.removeOldFiles(toMerge[t], snapDir)
		}
	}
	m.logger.Log(m.lvl, "[bor snapshots] Merge done", "from", mergeRanges[0].from, "to", mergeRanges[0].to)
	return nil
}

func (m *BorMerger) merge(ctx context.Context, toMerge []string, targetFile string, logEvery *time.Ticker) error {
	var word = make([]byte, 0, 4096)
	var expectedTotal int
	cList := make([]*compress.Decompressor, len(toMerge))
	for i, cFile := range toMerge {
		d, err := compress.NewDecompressor(cFile)
		if err != nil {
			return err
		}
		defer d.Close()
		cList[i] = d
		expectedTotal += d.Count()
	}

	f, err := compress.NewCompressor(ctx, "Bor Snapshots merge", targetFile, m.tmpDir, compress.MinPatternScore, m.compressWorkers, log.LvlTrace, m.logger)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, d := range cList {
		if err := d.WithReadAhead(func() error {
			g := d.MakeGetter()
			for g.HasNext() {
				word, _ = g.Next(word[:0])
				if err := f.AddWord(word); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if f.Count() != expectedTotal {
		return fmt.Errorf("unexpected amount after bor segments merge. got: %d, expected: %d", f.Count(), expectedTotal)
	}
	if err = f.Compress(); err != nil {
		return err
	}
	return nil
}

func (m *BorMerger) removeOldFiles(toDel []string, snapDir string) {
	for _, f := range toDel {
		_ = os.Remove(f)
		ext := filepath.Ext(f)
		withoutExt := f[:len(f)-len(ext)]
		_ = os.Remove(withoutExt + ".idx")
	}
	tmpFiles, err := snaptype.TmpFiles(snapDir)
	if err != nil {
		return
	}
	for _, f := range tmpFiles {
		_ = os.Remove(f)
	}
}
