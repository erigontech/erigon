package freezeblocks

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/background"
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
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/turbo/services"
)

func (br *BlockRetire) retireBorBlocks(ctx context.Context, minBlockNum uint64, maxBlockNum uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []services.DownloadRequest) error, onDelete func(l []string) error) (bool, error) {
	chainConfig := fromdb.ChainConfig(br.db)
	notifier, logger, blockReader, tmpDir, db, workers := br.notifier, br.logger, br.blockReader, br.tmpDir, br.db, br.workers
	snapshots := br.borSnapshots()

	blockFrom, blockTo, ok := CanRetire(maxBlockNum, minBlockNum, br.chainConfig)
	if ok {
		logger.Log(lvl, "[bor snapshots] Retire Bor Blocks", "range", fmt.Sprintf("%dk-%dk", blockFrom/1000, blockTo/1000))
		if err := DumpBorBlocks(ctx, blockFrom, blockTo, chainConfig, tmpDir, snapshots.Dir(), db, workers, lvl, logger, blockReader); err != nil {
			return ok, fmt.Errorf("DumpBorBlocks: %w", err)
		}
		if err := snapshots.ReopenFolder(); err != nil {
			return ok, fmt.Errorf("reopen: %w", err)
		}
		snapshots.LogStat("retire")
		if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
			notifier.OnNewSnapshot()
		}
	}

	merger := NewMerger(tmpDir, workers, lvl, db, chainConfig, logger)
	rangesToMerge := merger.FindMergeRanges(snapshots.Ranges(), snapshots.BlocksAvailable())
	logger.Log(lvl, "[bor snapshots] Retire Bor Blocks", "rangesToMerge", Ranges(rangesToMerge))
	if len(rangesToMerge) == 0 {
		return ok, nil
	}
	ok = true // have something to merge
	onMerge := func(r Range) error {
		if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
			notifier.OnNewSnapshot()
		}

		if seedNewSnapshots != nil {
			downloadRequest := []services.DownloadRequest{
				services.NewDownloadRequest("", ""),
			}
			if err := seedNewSnapshots(downloadRequest); err != nil {
				return err
			}
		}
		return nil
	}

	err := merger.Merge(ctx, &snapshots.RoSnapshots, snaptype.BorSnapshotTypes, rangesToMerge, snapshots.Dir(), true /* doIndex */, onMerge, onDelete)

	if err != nil {
		return ok, err
	}
	return ok, nil
}

func DumpBorBlocks(ctx context.Context, blockFrom, blockTo uint64, chainConfig *chain.Config, tmpDir, snapDir string, chainDB kv.RoDB, workers int, lvl log.Lvl, logger log.Logger, blockReader services.FullBlockReader) error {
	for i := blockFrom; i < blockTo; i = chooseSegmentEnd(i, blockTo, chainConfig) {
		if err := dumpBorBlocksRange(ctx, i, chooseSegmentEnd(i, blockTo, chainConfig), tmpDir, snapDir, chainDB, chainConfig, workers, lvl, logger, blockReader); err != nil {
			return err
		}
	}

	return nil
}

func dumpBorBlocksRange(ctx context.Context, blockFrom, blockTo uint64, tmpDir, snapDir string, chainDB kv.RoDB, chainConfig *chain.Config, workers int, lvl log.Lvl, logger log.Logger, blockReader services.FullBlockReader) error {

	if _, err := dumpRange(ctx, snaptype.BorEvents.FileInfo(snapDir, blockFrom, blockTo),
		DumpBorEvents, nil, chainDB, chainConfig, tmpDir, workers, lvl, logger); err != nil {
		return err
	}

	if _, err := dumpRange(ctx, snaptype.BorSpans.FileInfo(snapDir, blockFrom, blockTo),
		DumpBorSpans, nil, chainDB, chainConfig, tmpDir, workers, lvl, logger); err != nil {
		return err
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
func DumpBorEvents(ctx context.Context, db kv.RoDB, chainConfig *chain.Config, blockFrom, blockTo uint64, _ firstKeyGetter, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
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
		return 0, err
	}
	if lastEventId > startEventId {
		if err := db.View(ctx, func(tx kv.Tx) error {
			blockHash, e := rawdb.ReadCanonicalHash(tx, prevBlockNum)
			if e != nil {
				return e
			}
			return dumpBorEventRange(startEventId, lastEventId+1, tx, prevBlockNum, blockHash, collect)
		}); err != nil {
			return 0, err
		}
	}

	return lastEventId, nil
}

// DumpBorSpans - [from, to)
func DumpBorSpans(ctx context.Context, db kv.RoDB, chainConfig *chain.Config, blockFrom, blockTo uint64, _ firstKeyGetter, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	spanFrom := uint64(heimdall.SpanIdAt(blockFrom))
	spanTo := uint64(heimdall.SpanIdAt(blockTo))

	if err := kv.BigChunks(db, kv.BorSpans, hexutility.EncodeTs(spanFrom), func(tx kv.Tx, spanIdBytes, spanBytes []byte) (bool, error) {
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
		return spanTo, err
	}
	return spanTo, nil
}

func BorEventsIdx(ctx context.Context, sn snaptype.FileInfo, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("BorEventsIdx: at=%d-%d, %v, %s", sn.From, sn.To, rec, dbg.Stack())
		}
	}()
	// Calculate how many records there will be in the index
	d, err := compress.NewDecompressor(sn.Path)
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

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   blockCount,
		Enums:      blockCount > 0,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpDir,
		IndexFile:  filepath.Join(sn.Dir(), snaptype.IdxFileName(sn.Version, sn.From, sn.To, snaptype.BorEvents.String())),
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

func BorSpansIdx(ctx context.Context, sn snaptype.FileInfo, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("BorSpansIdx: at=%d-%d, %v, %s", sn.From, sn.To, rec, dbg.Stack())
		}
	}()
	// Calculate how many records there will be in the index
	d, err := compress.NewDecompressor(sn.Path)
	if err != nil {
		return err
	}
	defer d.Close()

	baseSpanId := heimdall.SpanIdAt(sn.From)

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   d.Count(),
		Enums:      d.Count() > 0,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpDir,
		IndexFile:  filepath.Join(sn.Dir(), sn.Type.IdxFileName(sn.Version, sn.From, sn.To)),
		BaseDataID: uint64(baseSpanId),
	}, logger)
	if err != nil {
		return err
	}
	rs.LogLvl(log.LvlDebug)

	defer d.EnableMadvNormal().DisableReadAhead()
RETRY:
	g := d.MakeGetter()
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

// Bor Events
// value: event_rlp
// bor_transaction_hash  -> bor_event_segment_offset

// Bor Spans
// value: span_json
// span_id -> offset

type BorRoSnapshots struct {
	RoSnapshots
}

// NewBorRoSnapshots - opens all bor snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewBorRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, segmentsMin uint64, logger log.Logger) *BorRoSnapshots {
	return &BorRoSnapshots{*newRoSnapshots(cfg, snapDir, snaptype.BorSnapshotTypes, segmentsMin, logger)}
}

func (s *BorRoSnapshots) Ranges() []Range {
	view := s.View()
	defer view.Close()
	return view.base.Ranges()
}

// this is one off code to fix an issue in 2.49.x->2.52.x which missed
// removal of intermediate segments after a merge operation
func removeBorOverlaps(dir string, active []snaptype.FileInfo, max uint64) {
	list, err := snaptype.Segments(dir)

	if err != nil {
		return
	}

	var toDel []string
	l := make([]snaptype.FileInfo, 0, len(list))

	for _, f := range list {
		if !(f.Type.Enum() == snaptype.Enums.BorSpans || f.Type.Enum() == snaptype.Enums.BorEvents) {
			continue
		}
		l = append(l, f)
	}

	// added overhead to make sure we don't delete in the
	// current 500k block segment
	if max > 500_001 {
		max -= 500_001
	}

	for _, f := range l {
		if max < f.From {
			continue
		}

		for _, a := range active {
			if a.Type.Enum() != snaptype.Enums.BorSpans {
				continue
			}

			if f.From < a.From {
				continue
			}

			if f.From == a.From {
				if f.To < a.To {
					toDel = append(toDel, f.Path)
				}

				break
			}

			if f.From < a.To {
				toDel = append(toDel, f.Path)
				break
			}
		}
	}

	for _, f := range toDel {
		_ = os.Remove(f)
		ext := filepath.Ext(f)
		withoutExt := f[:len(f)-len(ext)]
		_ = os.Remove(withoutExt + ".idx")
	}
}

func (s *BorRoSnapshots) ReopenFolder() error {
	files, _, err := typedSegments(s.dir, s.segmentsMin.Load(), snaptype.BorSnapshotTypes)
	if err != nil {
		return err
	}

	// this is one off code to fix an issue in 2.49.x->2.52.x which missed
	// removal of intermediate segments after a merge operation
	removeBorOverlaps(s.dir, files, s.BlocksAvailable())

	list := make([]string, 0, len(files))
	for _, f := range files {
		_, fName := filepath.Split(f.Path)
		list = append(list, fName)
	}
	return s.ReopenList(list, false)
}

type BorView struct {
	base *View
}

func (s *BorRoSnapshots) View() *BorView {
	v := &BorView{base: s.RoSnapshots.View()}
	v.base.baseSegType = snaptype.BorSpans
	return v
}

func (v *BorView) Close() {
	v.base.Close()
}

func (v *BorView) Events() []*Segment { return v.base.Segments(snaptype.BorEvents) }
func (v *BorView) Spans() []*Segment  { return v.base.Segments(snaptype.BorSpans) }

func (v *BorView) EventsSegment(blockNum uint64) (*Segment, bool) {
	return v.base.Segment(snaptype.BorEvents, blockNum)
}

func (v *BorView) SpansSegment(blockNum uint64) (*Segment, bool) {
	return v.base.Segment(snaptype.BorSpans, blockNum)
}
