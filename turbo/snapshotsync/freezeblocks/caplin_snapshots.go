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
	"sync"
	"sync/atomic"

	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/persistence/format/snapshot_format"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/log/v3"
)

type BeaconBlockSegment struct {
	seg     *compress.Decompressor // value: chunked(ssz(SignedBeaconBlocks))
	idxSlot *recsplit.Index        // slot       -> beacon_slot_segment_offset
	ranges  Range
}

func (sn *BeaconBlockSegment) closeIdx() {
	if sn.idxSlot != nil {
		sn.idxSlot.Close()
		sn.idxSlot = nil
	}
}
func (sn *BeaconBlockSegment) closeSeg() {
	if sn.seg != nil {
		sn.seg.Close()
		sn.seg = nil
	}
}
func (sn *BeaconBlockSegment) close() {
	sn.closeSeg()
	sn.closeIdx()
}
func (sn *BeaconBlockSegment) reopenSeg(dir string) (err error) {
	sn.closeSeg()
	fileName := snaptype.SegmentFileName(sn.ranges.from, sn.ranges.to, snaptype.BeaconBlocks)
	sn.seg, err = compress.NewDecompressor(path.Join(dir, fileName))
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, fileName)
	}
	return nil
}
func (sn *BeaconBlockSegment) reopenIdxIfNeed(dir string, optimistic bool) (err error) {
	if sn.idxSlot != nil {
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

func (sn *BeaconBlockSegment) reopenIdx(dir string) (err error) {
	sn.closeIdx()
	if sn.seg == nil {
		return nil
	}
	fileName := snaptype.IdxFileName(sn.ranges.from, sn.ranges.to, snaptype.BeaconBlocks.String())
	sn.idxSlot, err = recsplit.OpenIndex(path.Join(dir, fileName))
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, fileName)
	}
	return nil
}

type beaconBlockSegments struct {
	lock     sync.RWMutex
	segments []*BeaconBlockSegment
}

func (s *beaconBlockSegments) View(f func(segments []*BeaconBlockSegment) error) error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return f(s.segments)
}

func BeaconBlocksIdx(ctx context.Context, sn snaptype.FileInfo, segmentFilePath string, blockFrom, blockTo uint64, snapDir string, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("BeaconBlocksIdx: at=%d-%d, %v, %s", blockFrom, blockTo, rec, dbg.Stack())
		}
	}()

	// Calculate how many records there will be in the index
	d, err := compress.NewDecompressor(path.Join(snapDir, segmentFilePath))
	if err != nil {
		return err
	}
	defer d.Close()

	_, fname := filepath.Split(segmentFilePath)
	p.Name.Store(&fname)
	p.Total.Store(uint64(d.Count()))

	if err := Idx(ctx, d, sn.From, tmpDir, log.LvlDebug, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		if i%100_000 == 0 {
			logger.Log(lvl, "Compressing beacon blocks", "progress", i)
		}
		p.Processed.Add(1)
		num := make([]byte, 8)
		n := binary.PutUvarint(num, i)
		if err := idx.AddKey(num[:n], offset); err != nil {
			return err
		}
		return nil
	}, logger); err != nil {
		return fmt.Errorf("BodyNumberIdx: %w", err)
	}

	return nil
}

type CaplinSnapshots struct {
	indicesReady  atomic.Bool
	segmentsReady atomic.Bool

	BeaconBlocks *beaconBlockSegments

	dir         string
	segmentsMax atomic.Uint64 // all types of .seg files are available - up to this number
	idxMax      atomic.Uint64 // all types of .idx files are available - up to this number
	cfg         ethconfig.BlocksFreezing
	logger      log.Logger
}

// NewCaplinSnapshots - opens all snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewCaplinSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, logger log.Logger) *CaplinSnapshots {
	return &CaplinSnapshots{dir: snapDir, cfg: cfg, BeaconBlocks: &beaconBlockSegments{}, logger: logger}
}

func (s *CaplinSnapshots) IndicesMax() uint64  { return s.idxMax.Load() }
func (s *CaplinSnapshots) SegmentsMax() uint64 { return s.segmentsMax.Load() }
func (s *CaplinSnapshots) BlocksAvailable() uint64 {
	return cmp.Min(s.segmentsMax.Load(), s.idxMax.Load())
}

// ReopenList stops on optimistic=false, continue opening files on optimistic=true
func (s *CaplinSnapshots) ReopenList(fileNames []string, optimistic bool) error {
	s.BeaconBlocks.lock.Lock()
	defer s.BeaconBlocks.lock.Unlock()

	s.closeWhatNotInList(fileNames)
	var segmentsMax uint64
	var segmentsMaxSet bool
Loop:
	for _, fName := range fileNames {
		f, ok := snaptype.ParseFileName(s.dir, fName)
		if !ok {
			continue
		}
		var processed bool = true

		switch f.T {
		case snaptype.BeaconBlocks:
			var sn *BeaconBlockSegment
			var exists bool
			for _, sn2 := range s.BeaconBlocks.segments {
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
				sn = &BeaconBlockSegment{ranges: Range{f.From, f.To}}
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
					s.logger.Warn("[snapshots] open segment", "err", err)
					continue Loop
				} else {
					return err
				}
			}

			if !exists {
				// it's possible to iterate over .seg file even if you don't have index
				// then make segment available even if index open may fail
				s.BeaconBlocks.segments = append(s.BeaconBlocks.segments, sn)
			}
			if err := sn.reopenIdxIfNeed(s.dir, optimistic); err != nil {
				return err
			}
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

func (s *CaplinSnapshots) idxAvailability() uint64 {
	var beaconBlocks uint64
	for _, seg := range s.BeaconBlocks.segments {
		if seg.idxSlot == nil {
			break
		}
		beaconBlocks = seg.ranges.to - 1
	}
	return beaconBlocks
}

func (s *CaplinSnapshots) ReopenFolder() error {
	files, _, err := SegmentsCaplin(s.dir)
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

func (s *CaplinSnapshots) closeWhatNotInList(l []string) {
Loop1:
	for i, sn := range s.BeaconBlocks.segments {
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
		s.BeaconBlocks.segments[i] = nil
	}
	var i int
	for i = 0; i < len(s.BeaconBlocks.segments) && s.BeaconBlocks.segments[i] != nil && s.BeaconBlocks.segments[i].seg != nil; i++ {
	}
	tail := s.BeaconBlocks.segments[i:]
	s.BeaconBlocks.segments = s.BeaconBlocks.segments[:i]
	for i = 0; i < len(tail); i++ {
		if tail[i] != nil {
			tail[i].close()
			tail[i] = nil
		}
	}
}

type CaplinView struct {
	s      *CaplinSnapshots
	closed bool
}

func (s *CaplinSnapshots) View() *CaplinView {
	v := &CaplinView{s: s}
	v.s.BeaconBlocks.lock.RLock()
	return v
}

func (v *CaplinView) Close() {
	if v.closed {
		return
	}
	v.closed = true
	v.s.BeaconBlocks.lock.RUnlock()

}

func (v *CaplinView) BeaconBlocks() []*BeaconBlockSegment { return v.s.BeaconBlocks.segments }

func (v *CaplinView) BeaconBlocksSegment(slot uint64) (*BeaconBlockSegment, bool) {
	for _, seg := range v.BeaconBlocks() {
		if !(slot >= seg.ranges.from && slot < seg.ranges.to) {
			continue
		}
		return seg, true
	}
	return nil, false
}

func dumpBeaconBlocksRange(ctx context.Context, db kv.RoDB, b persistence.BlockSource, fromSlot uint64, toSlot uint64, tmpDir, snapDir string, workers int, lvl log.Lvl, logger log.Logger) error {
	segName := snaptype.SegmentFileName(fromSlot, toSlot, snaptype.BeaconBlocks)
	f, _ := snaptype.ParseFileName(snapDir, segName)

	sn, err := compress.NewCompressor(ctx, "Snapshot BeaconBlocks", f.Path, tmpDir, compress.MinPatternScore, workers, lvl, logger)
	if err != nil {
		return err
	}
	defer sn.Close()

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Generate .seg file, which is just the list of beacon blocks.
	for i := fromSlot; i < toSlot; i++ {
		obj, err := b.GetBlock(ctx, tx, i)
		if err != nil {
			return err
		}
		if i%20_000 == 0 {
			logger.Log(lvl, "Dumping beacon blocks", "progress", i)
		}
		if obj == nil {

			if err := sn.AddWord(nil); err != nil {
				return err
			}
			continue
		}
		var buf bytes.Buffer
		if err := snapshot_format.WriteBlockForSnapshot(obj.Data, &buf); err != nil {
			return err
		}
		word := buf.Bytes()

		if err := sn.AddWord(word); err != nil {
			return err
		}

	}
	if err := sn.Compress(); err != nil {
		return fmt.Errorf("compress: %w", err)
	}
	// Generate .idx file, which is the slot => offset mapping.
	p := &background.Progress{}

	return BeaconBlocksIdx(ctx, f, segName, fromSlot, toSlot, snapDir, tmpDir, p, lvl, logger)
}

func DumpBeaconBlocks(ctx context.Context, db kv.RoDB, b persistence.BlockSource, fromSlot, toSlot, blocksPerFile uint64, tmpDir, snapDir string, workers int, lvl log.Lvl, logger log.Logger) error {
	if blocksPerFile == 0 {
		return nil
	}

	for i := fromSlot; i < toSlot; i = chooseSegmentEnd(i, toSlot, blocksPerFile) {
		if toSlot-i < blocksPerFile {
			break
		}
		to := chooseSegmentEnd(i, toSlot, blocksPerFile)
		logger.Log(lvl, "Dumping beacon blocks", "from", i, "to", to)
		if err := dumpBeaconBlocksRange(ctx, db, b, i, to, tmpDir, snapDir, workers, lvl, logger); err != nil {
			return err
		}
	}
	return nil
}
