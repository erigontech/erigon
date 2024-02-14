package freezeblocks

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/persistence/format/snapshot_format"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/log/v3"
)

func BeaconBlocksIdx(ctx context.Context, sn snaptype.FileInfo, segmentFilePath string, blockFrom, blockTo uint64, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {

	if err := Idx(ctx, sn, sn.From, tmpDir, log.LvlDebug, p, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		if i%20_000 == 0 {
			logger.Log(lvl, "Generating idx for beacon blocks", "progress", i)
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

// value: chunked(ssz(SignedBeaconBlocks))
// slot       -> beacon_slot_segment_offset

type CaplinSnapshots struct {
	indicesReady  atomic.Bool
	segmentsReady atomic.Bool

	BeaconBlocks *segments

	dir         string
	segmentsMax atomic.Uint64 // all types of .seg files are available - up to this number
	idxMax      atomic.Uint64 // all types of .idx files are available - up to this number
	cfg         ethconfig.BlocksFreezing
	logger      log.Logger
	// allows for pruning segments - this is the min availible segment
	segmentsMin atomic.Uint64
	// chain cfg
	beaconCfg *clparams.BeaconChainConfig
}

// NewCaplinSnapshots - opens all snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewCaplinSnapshots(cfg ethconfig.BlocksFreezing, beaconCfg *clparams.BeaconChainConfig, snapDir string, logger log.Logger) *CaplinSnapshots {
	return &CaplinSnapshots{dir: snapDir, cfg: cfg, BeaconBlocks: &segments{}, logger: logger, beaconCfg: beaconCfg}
}

func (s *CaplinSnapshots) IndicesMax() uint64  { return s.idxMax.Load() }
func (s *CaplinSnapshots) SegmentsMax() uint64 { return s.segmentsMax.Load() }

func (s *CaplinSnapshots) SegFilePaths(from, to uint64) []string {
	var res []string
	for _, seg := range s.BeaconBlocks.segments {
		if seg.from >= from && seg.to <= to {
			res = append(res, seg.FilePath())
		}
	}
	return res
}

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

		switch f.Type.Enum() {
		case snaptype.Enums.BeaconBlocks:
			var sn *Segment
			var exists bool
			for _, sn2 := range s.BeaconBlocks.segments {
				if sn2.Decompressor == nil { // it's ok if some segment was not able to open
					continue
				}
				if fName == sn2.FileName() {
					sn = sn2
					exists = true
					break
				}
			}
			if !exists {
				sn = &Segment{segType: snaptype.BeaconBlocks, version: f.Version, Range: Range{f.From, f.To}}
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
		if seg.Index() == nil {
			break
		}
		beaconBlocks = seg.to - 1
	}
	return beaconBlocks
}

func (s *CaplinSnapshots) ReopenFolder() error {
	files, _, err := SegmentsCaplin(s.dir, s.segmentsMin.Load())
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
		if sn.Decompressor == nil {
			continue Loop1
		}
		_, name := filepath.Split(sn.FilePath())
		for _, fName := range l {
			if fName == name {
				continue Loop1
			}
		}
		sn.close()
		s.BeaconBlocks.segments[i] = nil
	}
	var i int
	for i = 0; i < len(s.BeaconBlocks.segments) && s.BeaconBlocks.segments[i] != nil && s.BeaconBlocks.segments[i].Decompressor != nil; i++ {
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

func (v *CaplinView) BeaconBlocks() []*Segment { return v.s.BeaconBlocks.segments }

func (v *CaplinView) BeaconBlocksSegment(slot uint64) (*Segment, bool) {
	for _, seg := range v.BeaconBlocks() {
		if !(slot >= seg.from && slot < seg.to) {
			continue
		}
		return seg, true
	}
	return nil, false
}

func dumpBeaconBlocksRange(ctx context.Context, db kv.RoDB, b persistence.BlockSource, fromSlot uint64, toSlot uint64, tmpDir, snapDir string, workers int, lvl log.Lvl, logger log.Logger) error {
	segName := snaptype.BeaconBlocks.FileName(0, fromSlot, toSlot)
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
	var w bytes.Buffer
	compressor, err := zstd.NewWriter(&w, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		return err
	}
	defer compressor.Close()
	// Just make a reusable buffer
	buf := make([]byte, 2048)
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

		if buf, err = snapshot_format.WriteBlockForSnapshot(compressor, obj.Data, buf); err != nil {
			return err
		}
		if err := compressor.Close(); err != nil {
			return err
		}
		word := w.Bytes()

		if err := sn.AddWord(word); err != nil {
			return err
		}
		w.Reset()
		compressor.Reset(&w)
	}
	if err := sn.Compress(); err != nil {
		return fmt.Errorf("compress: %w", err)
	}
	// Generate .idx file, which is the slot => offset mapping.
	p := &background.Progress{}

	return BeaconBlocksIdx(ctx, f, filepath.Join(snapDir, segName), fromSlot, toSlot, tmpDir, p, lvl, logger)
}

func DumpBeaconBlocks(ctx context.Context, db kv.RoDB, b persistence.BlockSource, fromSlot, toSlot uint64, tmpDir, snapDir string, workers int, lvl log.Lvl, logger log.Logger) error {

	for i := fromSlot; i < toSlot; i = chooseSegmentEnd(i, toSlot, nil) {
		blocksPerFile := snapcfg.MergeLimit("", i)

		if toSlot-i < blocksPerFile {
			break
		}
		to := chooseSegmentEnd(i, toSlot, nil)
		logger.Log(lvl, "Dumping beacon blocks", "from", i, "to", to)
		if err := dumpBeaconBlocksRange(ctx, db, b, i, to, tmpDir, snapDir, workers, lvl, logger); err != nil {
			return err
		}
	}
	return nil
}

func (s *CaplinSnapshots) BuildMissingIndices(ctx context.Context, logger log.Logger, lvl log.Lvl) error {
	// if !s.segmentsReady.Load() {
	// 	return fmt.Errorf("not all snapshot segments are available")
	// }

	// wait for Downloader service to download all expected snapshots
	segments, _, err := SegmentsCaplin(s.dir, 0)
	if err != nil {
		return err
	}
	for index := range segments {
		segment := segments[index]
		if segment.Type.Enum() != snaptype.Enums.BeaconBlocks {
			continue
		}
		if hasIdxFile(segment, logger) {
			continue
		}
		p := &background.Progress{}

		if err := BeaconBlocksIdx(ctx, segment, segment.Path, segment.From, segment.To, s.dir, p, log.LvlDebug, logger); err != nil {
			return err
		}
	}

	return s.ReopenFolder()
}

func (s *CaplinSnapshots) ReadHeader(slot uint64) (*cltypes.SignedBeaconBlockHeader, uint64, libcommon.Hash, error) {
	view := s.View()
	defer view.Close()

	var buf []byte

	seg, ok := view.BeaconBlocksSegment(slot)
	if !ok {
		return nil, 0, libcommon.Hash{}, nil
	}

	idxSlot := seg.Index()

	if idxSlot == nil {
		return nil, 0, libcommon.Hash{}, nil
	}
	blockOffset := idxSlot.OrdinalLookup(slot - idxSlot.BaseDataID())

	gg := seg.MakeGetter()
	gg.Reset(blockOffset)
	if !gg.HasNext() {
		return nil, 0, libcommon.Hash{}, nil
	}

	buf, _ = gg.Next(buf)
	if len(buf) == 0 {
		return nil, 0, libcommon.Hash{}, nil
	}
	// Decompress this thing
	buffer := buffersPool.Get().(*bytes.Buffer)
	defer buffersPool.Put(buffer)

	buffer.Reset()
	buffer.Write(buf)
	reader := decompressorPool.Get().(*zstd.Decoder)
	defer decompressorPool.Put(reader)
	reader.Reset(buffer)

	// Use pooled buffers and readers to avoid allocations.
	return snapshot_format.ReadBlockHeaderFromSnapshotWithExecutionData(reader, s.beaconCfg)
}
