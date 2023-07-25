package freezeblocks

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon/cmd/hack/tool/fromdb"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapcfg"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
)

type BorEventSegment struct {
	seg                 *compress.Decompressor // value: event_rlp
	IdxTxnHash2BlockNum *recsplit.Index        // bor_transaction_hash  -> block_number
	ranges              Range
}

func (sn *BorEventSegment) closeIdx() {
	if sn.IdxTxnHash2BlockNum != nil {
		sn.IdxTxnHash2BlockNum.Close()
		sn.IdxTxnHash2BlockNum = nil
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

	fileName := snaptype.IdxFileName(sn.ranges.from, sn.ranges.to, snaptype.BorEvents2Block.String())
	sn.IdxTxnHash2BlockNum, err = recsplit.OpenIndex(path.Join(dir, fileName))
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, fileName)
	}
	if sn.IdxTxnHash2BlockNum.ModTime().Before(sn.seg.ModTime()) {
		// Index has been created before the segment file, needs to be ignored (and rebuilt) as inconsistent
		sn.IdxTxnHash2BlockNum.Close()
		sn.IdxTxnHash2BlockNum = nil
	}
	return nil
}

func (sn *BorEventSegment) reopenIdxIfNeed(dir string, optimistic bool) (err error) {
	if sn.IdxTxnHash2BlockNum != nil {
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

type BorRetire struct {
	working               atomic.Bool
	needSaveFilesListInDB atomic.Bool

	workers int
	tmpDir  string
	db      kv.RoDB

	notifier    services.DBEventNotifier
	logger      log.Logger
	blockReader services.FullBlockReader
	blockWriter *blockio.BlockWriter
	dirs        datadir.Dirs
}

func NewBorRetire(workers int, dirs datadir.Dirs, blockReader services.FullBlockReader, blockWriter *blockio.BlockWriter, db kv.RoDB, notifier services.DBEventNotifier, logger log.Logger) *BorRetire {
	return &BorRetire{workers: workers, tmpDir: dirs.Tmp, dirs: dirs, blockReader: blockReader, blockWriter: blockWriter, db: db, notifier: notifier, logger: logger}
}

func (br *BorRetire) snapshots() *BorRoSnapshots {
	return br.blockReader.BorSnapshots().(*BorRoSnapshots)
}

func (br *BorRetire) PruneAncientBlocks(tx kv.RwTx, limit int) error {
	if br.blockReader.FreezingCfg().KeepBlocks {
		return nil
	}
	currentProgress, err := stages.GetStageProgress(tx, stages.Senders)
	if err != nil {
		return err
	}
	canDeleteTo := CanDeleteTo(currentProgress, br.blockReader.FrozenBorBlocks())
	if err := br.blockWriter.PruneBorBlocks(context.Background(), tx, canDeleteTo, limit); err != nil {
		return nil
	}
	return nil
}

func (br *BorRetire) RetireBlocks(ctx context.Context, blockFrom, blockTo uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []services.DownloadRequest) error) error {
	chainConfig := fromdb.ChainConfig(br.db)
	logger, blockReader, tmpDir, db, workers := br.logger, br.blockReader, br.tmpDir, br.db, br.workers
	logger.Log(lvl, "[snapshots] Retire Bor Blocks", "range", fmt.Sprintf("%dk-%dk", blockFrom/1000, blockTo/1000))
	snapshots := br.snapshots()
	firstTxNum := blockReader.(*BlockReader).FirstTxNumNotInSnapshots()

	if err := DumpBorBlocks(ctx, chainConfig, blockFrom, blockTo, snaptype.Erigon2SegmentSize, tmpDir, snapshots.Dir(), firstTxNum, db, workers, lvl, logger, blockReader); err != nil {
		return fmt.Errorf("DumpBorBlocks: %w", err)
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
	return nil
}

// DumpBorEvents - [from, to)
func DumpBorEvents(ctx context.Context, db kv.RoDB, blockFrom, blockTo uint64, workers int, lvl log.Lvl, logger log.Logger, collect func([]byte) error) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	from := hexutility.EncodeTs(blockFrom)
	if err := kv.BigChunks(db, kv.BorEventNums, from, func(tx kv.Tx, blockNumBytes, eventIdBytes []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(blockNumBytes)
		if blockNum >= blockTo {
			return false, nil
		}
		event, e := tx.GetOne(kv.BorEvents, eventIdBytes)
		if e != nil {
			return false, e
		}
		if err := collect(event); err != nil {
			return false, err
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-logEvery.C:
			var m runtime.MemStats
			if lvl >= log.LvlInfo {
				dbg.ReadMemStats(&m)
			}
			logger.Log(lvl, "[snapshots] Dumping bor events", "block num", blockNum,
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

func BorEventsIdx(ctx context.Context, segmentFilePath string, firstBlockNumInSegment uint64, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			_, fName := filepath.Split(segmentFilePath)
			err = fmt.Errorf("BorEventsIdx: at=%s, %v, %s", fName, rec, dbg.Stack())
		}
	}()

	num := make([]byte, 8)

	d, err := compress.NewDecompressor(segmentFilePath)
	if err != nil {
		return err
	}
	defer d.Close()

	_, fname := filepath.Split(segmentFilePath)
	p.Name.Store(&fname)
	p.Total.Store(uint64(d.Count()))

	if err := Idx(ctx, d, firstBlockNumInSegment, tmpDir, log.LvlDebug, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		p.Processed.Add(1)
		n := binary.PutUvarint(num, i)
		if err := idx.AddKey(num[:n], offset); err != nil {
			return err
		}
		return nil
	}, logger); err != nil {
		return fmt.Errorf("EventNumberIdx: %w", err)
	}
	return nil
}

func (br *BorRetire) RetireBlocksInBackground(ctx context.Context, forwardProgress uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []services.DownloadRequest) error) {
	ok := br.working.CompareAndSwap(false, true)
	br.logger.Info("Bor RetireBlocksInBackground", "ok", ok)
	if !ok {
		// go-routine is still working
		return
	}
	go func() {
		defer br.working.Store(false)

		blockFrom, blockTo, ok := CanRetire(forwardProgress, br.blockReader.FrozenBorBlocks())
		br.logger.Info("Bor CanRetire", "from", blockFrom, "to", blockTo, "ok", ok, "forwardProgress", forwardProgress, "frozenBorBlocks", br.blockReader.FrozenBorBlocks())
		if !ok {
			return
		}

		err := br.RetireBlocks(ctx, blockFrom, blockTo, lvl, seedNewSnapshots)
		if err != nil {
			br.logger.Warn("[snapshots] retire blocks", "err", err, "fromBlock", blockFrom, "toBlock", blockTo)
		}
	}()
}
func (br *BorRetire) BuildMissedIndicesIfNeed(ctx context.Context, logPrefix string, notifier services.DBEventNotifier, cc *chain.Config) error {
	return nil
}

type BorRoSnapshots struct {
	indicesReady  atomic.Bool
	segmentsReady atomic.Bool

	Events *borEventSegments

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
	return &BorRoSnapshots{dir: snapDir, cfg: cfg, Events: &borEventSegments{}, logger: logger}
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
		"blocks", fmt.Sprintf("%dk", (s.BlocksAvailable()+1)/1000),
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
		l, m = noGaps(noOverlaps(allTypeOfSegmentsMustExist(dir, l)))
		res = append(res, l...)
		missingSnapshots = append(missingSnapshots, m...)
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
	for _, sn := range s.Events.segments {
		sn.seg.DisableReadAhead()
	}
}
func (s *BorRoSnapshots) EnableReadAhead() *BorRoSnapshots {
	s.Events.lock.RLock()
	defer s.Events.lock.RUnlock()
	for _, sn := range s.Events.segments {
		sn.seg.EnableReadAhead()
	}
	return s
}
func (s *BorRoSnapshots) EnableMadvWillNeed() *BorRoSnapshots {
	s.Events.lock.RLock()
	defer s.Events.lock.RUnlock()
	for _, sn := range s.Events.segments {
		sn.seg.EnableWillNeed()
	}
	return s
}
func (s *BorRoSnapshots) EnableMadvNormal() *BorRoSnapshots {
	s.Events.lock.RLock()
	defer s.Events.lock.RUnlock()
	for _, sn := range s.Events.segments {
		sn.seg.EnableMadvNormal()
	}
	return s
}

func (s *BorRoSnapshots) idxAvailability() uint64 {
	var events uint64
	for _, seg := range s.Events.segments {
		if seg.IdxTxnHash2BlockNum == nil {
			break
		}
		events = seg.ranges.to - 1
	}
	return events
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
	slices.Sort(list)
	return list
}

// ReopenList stops on optimistic=false, continue opening files on optimistic=true
func (s *BorRoSnapshots) ReopenList(fileNames []string, optimistic bool) error {
	s.Events.lock.Lock()
	defer s.Events.lock.Unlock()

	s.closeWhatNotInList(fileNames)
	var segmentsMax uint64
	var segmentsMaxSet bool
Loop:
	for _, fName := range fileNames {
		f, err := snaptype.ParseFileName(s.dir, fName)
		if err != nil {
			s.logger.Warn("invalid segment name", "err", err, "name", fName)
			continue
		}

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
		}

		if f.To > 0 {
			segmentsMax = f.To - 1
		} else {
			segmentsMax = 0
		}
		segmentsMaxSet = true
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
}

func (s *BorRoSnapshots) PrintDebug() {
	s.Events.lock.RLock()
	defer s.Events.lock.RUnlock()
	fmt.Println("    == BorSnapshots, Event")
	for _, sn := range s.Events.segments {
		fmt.Printf("%d,  %t\n", sn.ranges.from, sn.IdxTxnHash2BlockNum == nil)
	}
}

type BorView struct {
	s      *BorRoSnapshots
	closed bool
}

func (s *BorRoSnapshots) View() *BorView {
	v := &BorView{s: s}
	v.s.Events.lock.RLock()
	return v
}

func (v *BorView) Close() {
	if v.closed {
		return
	}
	v.closed = true
	v.s.Events.lock.RUnlock()
}
func (v *BorView) Events() []*BorEventSegment { return v.s.Events.segments }
func (v *BorView) EventsSegment(blockNum uint64) (*BorEventSegment, bool) {
	for _, seg := range v.Events() {
		if !(blockNum >= seg.ranges.from && blockNum < seg.ranges.to) {
			continue
		}
		return seg, true
	}
	return nil, false
}
