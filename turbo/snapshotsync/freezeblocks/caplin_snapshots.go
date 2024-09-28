// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package freezeblocks

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/chain/snapcfg"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/dbutils"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format"
	"github.com/erigontech/erigon/eth/ethconfig"
)

var sidecarSSZSize = (&cltypes.BlobSidecar{}).EncodingSizeSSZ()

func BeaconSimpleIdx(ctx context.Context, sn snaptype.FileInfo, salt uint32, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
	num := make([]byte, binary.MaxVarintLen64)
	cfg := recsplit.RecSplitArgs{
		Enums:      true,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpDir,
		Salt:       &salt,
		BaseDataID: sn.From,
	}
	if err := snaptype.BuildIndex(ctx, sn, cfg, log.LvlDebug, p, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		if i%20_000 == 0 {
			logger.Log(lvl, "Generating idx for "+sn.Type.Name(), "progress", i)
		}
		p.Processed.Add(1)
		n := binary.PutUvarint(num, i)
		if err := idx.AddKey(num[:n], offset); err != nil {
			return err
		}
		return nil
	}, logger); err != nil {
		return fmt.Errorf("idx: %w", err)
	}

	return nil
}

// value: chunked(ssz(SignedBeaconBlocks))
// slot       -> beacon_slot_segment_offset

type CaplinSnapshots struct {
	indicesReady  atomic.Bool
	segmentsReady atomic.Bool

	Salt uint32

	dirtySegmentsLock   sync.RWMutex
	visibleSegmentsLock sync.RWMutex

	BeaconBlocks *segments
	BlobSidecars *segments

	dir         string
	tmpdir      string
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
func NewCaplinSnapshots(cfg ethconfig.BlocksFreezing, beaconCfg *clparams.BeaconChainConfig, dirs datadir.Dirs, logger log.Logger) *CaplinSnapshots {
	BeaconBlocks := &segments{
		DirtySegments: btree.NewBTreeGOptions[*DirtySegment](DirtySegmentLess, btree.Options{Degree: 128, NoLocks: false}),
	}
	BlobSidecars := &segments{
		DirtySegments: btree.NewBTreeGOptions[*DirtySegment](DirtySegmentLess, btree.Options{Degree: 128, NoLocks: false}),
	}
	c := &CaplinSnapshots{dir: dirs.Snap, tmpdir: dirs.Tmp, cfg: cfg, BeaconBlocks: BeaconBlocks, BlobSidecars: BlobSidecars, logger: logger, beaconCfg: beaconCfg}
	c.recalcVisibleFiles()
	return c
}

func (s *CaplinSnapshots) IndicesMax() uint64  { return s.idxMax.Load() }
func (s *CaplinSnapshots) SegmentsMax() uint64 { return s.segmentsMax.Load() }

func (s *CaplinSnapshots) LogStat(str string) {
	s.logger.Info(fmt.Sprintf("[snapshots:%s] Stat", str),
		"blocks", libcommon.PrettyCounter(s.SegmentsMax()+1), "indices", libcommon.PrettyCounter(s.IndicesMax()+1))
}

func (s *CaplinSnapshots) LS() {
	if s == nil {
		return
	}
	view := s.View()
	defer view.Close()

	if view.BeaconBlockRotx != nil {
		for _, seg := range view.BeaconBlockRotx.VisibleSegments {
			log.Info("[agg] ", "f", seg.src.Decompressor.FileName(), "words", seg.src.Decompressor.Count())
		}
	}
	if view.BlobSidecarRotx != nil {
		for _, seg := range view.BlobSidecarRotx.VisibleSegments {
			log.Info("[agg] ", "f", seg.src.Decompressor.FileName(), "words", seg.src.Decompressor.Count())
		}
	}
}

func (s *CaplinSnapshots) SegFileNames(from, to uint64) []string {
	view := s.View()
	defer view.Close()

	var res []string
	for _, seg := range view.BeaconBlockRotx.VisibleSegments {
		if seg.from >= from && seg.to <= to {
			res = append(res, seg.src.FileName())
		}
	}
	for _, seg := range view.BlobSidecarRotx.VisibleSegments {
		if seg.from >= from && seg.to <= to {
			res = append(res, seg.src.FileName())
		}
	}
	return res
}

func (s *CaplinSnapshots) BlocksAvailable() uint64 {
	return min(s.segmentsMax.Load(), s.idxMax.Load())
}

func (s *CaplinSnapshots) Close() {
	if s == nil {
		return
	}
	s.dirtySegmentsLock.Lock()
	defer s.dirtySegmentsLock.Unlock()

	s.closeWhatNotInList(nil)
}

// ReopenList stops on optimistic=false, continue opening files on optimistic=true
func (s *CaplinSnapshots) ReopenList(fileNames []string, optimistic bool) error {
	defer s.recalcVisibleFiles()

	s.dirtySegmentsLock.Lock()
	defer s.dirtySegmentsLock.Unlock()

	s.closeWhatNotInList(fileNames)
	var segmentsMax uint64
	var segmentsMaxSet bool
Loop:
	for _, fName := range fileNames {
		f, _, ok := snaptype.ParseFileName(s.dir, fName)
		if !ok {
			continue
		}
		var processed bool = true

		switch f.Type.Enum() {
		case snaptype.CaplinEnums.BeaconBlocks:
			var sn *DirtySegment
			var exists bool
			s.BeaconBlocks.DirtySegments.Walk(func(segments []*DirtySegment) bool {
				for _, sn2 := range segments {
					if sn2.Decompressor == nil { // it's ok if some segment was not able to open
						continue
					}
					if fName == sn2.FileName() {
						sn = sn2
						exists = true
						break
					}
				}
				return true
			})
			if !exists {
				sn = &DirtySegment{
					segType: snaptype.BeaconBlocks,
					version: f.Version,
					Range:   Range{f.From, f.To},
					frozen:  snapcfg.Seedable(s.cfg.ChainName, f),
				}
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
				s.BeaconBlocks.DirtySegments.Set(sn)
			}
			if err := sn.reopenIdxIfNeed(s.dir, optimistic); err != nil {
				return err
			}
			// Only bob sidecars count for progression
			if processed {
				if f.To > 0 {
					segmentsMax = f.To - 1
				} else {
					segmentsMax = 0
				}
				segmentsMaxSet = true
			}
		case snaptype.CaplinEnums.BlobSidecars:
			var sn *DirtySegment
			var exists bool
			s.BlobSidecars.DirtySegments.Walk(func(segments []*DirtySegment) bool {
				for _, sn2 := range segments {
					if sn2.Decompressor == nil { // it's ok if some segment was not able to open
						continue
					}
					if fName == sn2.FileName() {
						sn = sn2
						exists = true
						break
					}
				}
				return true
			})
			if !exists {
				sn = &DirtySegment{
					segType: snaptype.BlobSidecars,
					version: f.Version,
					Range:   Range{f.From, f.To},
					frozen:  snapcfg.Seedable(s.cfg.ChainName, f),
				}
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
				s.BlobSidecars.DirtySegments.Set(sn)
			}
			if err := sn.reopenIdxIfNeed(s.dir, optimistic); err != nil {
				return err
			}
		}

	}
	if segmentsMaxSet {
		s.segmentsMax.Store(segmentsMax)
	}
	s.segmentsReady.Store(true)
	return nil
}

func (s *CaplinSnapshots) recalcVisibleFiles() {
	defer func() {
		s.idxMax.Store(s.idxAvailability())
		s.indicesReady.Store(true)
	}()

	s.visibleSegmentsLock.Lock()
	defer s.visibleSegmentsLock.Unlock()

	getNewVisibleSegments := func(dirtySegments *btree.BTreeG[*DirtySegment]) []*VisibleSegment {
		newVisibleSegments := make([]*VisibleSegment, 0, dirtySegments.Len())
		dirtySegments.Walk(func(segments []*DirtySegment) bool {
			for _, sn := range segments {
				if sn.canDelete.Load() {
					continue
				}
				if sn.Decompressor == nil {
					continue
				}
				if sn.indexes == nil {
					continue
				}
				for len(newVisibleSegments) > 0 && newVisibleSegments[len(newVisibleSegments)-1].src.isSubSetOf(sn) {
					newVisibleSegments[len(newVisibleSegments)-1].src = nil
					newVisibleSegments = newVisibleSegments[:len(newVisibleSegments)-1]
				}
				newVisibleSegments = append(newVisibleSegments, &VisibleSegment{
					Range:   sn.Range,
					segType: sn.segType,
					src:     sn,
				})
			}
			return true
		})
		return newVisibleSegments
	}
	s.BeaconBlocks.VisibleSegments = getNewVisibleSegments(s.BeaconBlocks.DirtySegments)
	s.BlobSidecars.VisibleSegments = getNewVisibleSegments(s.BlobSidecars.DirtySegments)

	var maxIdx uint64
	if len(s.BeaconBlocks.VisibleSegments) > 0 {
		maxIdx = s.BeaconBlocks.VisibleSegments[len(s.BeaconBlocks.VisibleSegments)-1].to - 1
	}
	s.BeaconBlocks.maxVisibleBlock.Store(maxIdx)
}

func (s *CaplinSnapshots) idxAvailability() uint64 {
	return s.BeaconBlocks.maxVisibleBlock.Load()
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
	toClose := make([]*DirtySegment, 0)
	s.BeaconBlocks.DirtySegments.Walk(func(segments []*DirtySegment) bool {
	Loop1:
		for _, sn := range segments {
			if sn.Decompressor == nil {
				continue Loop1
			}
			_, name := filepath.Split(sn.FilePath())
			for _, fName := range l {
				if fName == name {
					continue Loop1
				}
			}
			toClose = append(toClose, sn)
		}
		return true
	})
	for _, sn := range toClose {
		sn.close()
		s.BeaconBlocks.DirtySegments.Delete(sn)
	}

	toClose = make([]*DirtySegment, 0)
	s.BlobSidecars.DirtySegments.Walk(func(segments []*DirtySegment) bool {
	Loop2:
		for _, sn := range segments {
			if sn.Decompressor == nil {
				continue Loop2
			}
			_, name := filepath.Split(sn.FilePath())
			for _, fName := range l {
				if fName == name {
					continue Loop2
				}
			}
			toClose = append(toClose, sn)
		}
		return true
	})
	for _, sn := range toClose {
		sn.close()
		s.BlobSidecars.DirtySegments.Delete(sn)
	}
}

type CaplinView struct {
	s               *CaplinSnapshots
	BeaconBlockRotx *segmentsRotx
	BlobSidecarRotx *segmentsRotx
	closed          bool
}

func (s *CaplinSnapshots) View() *CaplinView {
	s.visibleSegmentsLock.RLock()
	defer s.visibleSegmentsLock.RUnlock()

	v := &CaplinView{s: s}
	if s.BeaconBlocks != nil {
		v.BeaconBlockRotx = s.BeaconBlocks.BeginRotx()
	}
	if s.BlobSidecars != nil {
		v.BlobSidecarRotx = s.BlobSidecars.BeginRotx()
	}
	return v
}

func (v *CaplinView) Close() {
	if v.closed {
		return
	}
	v.BeaconBlockRotx.Close()
	v.BlobSidecarRotx.Close()
	v.s = nil
	v.closed = true
}

func (v *CaplinView) BeaconBlocks() []*VisibleSegment {
	return v.BeaconBlockRotx.VisibleSegments
}
func (v *CaplinView) BlobSidecars() []*VisibleSegment { return v.BlobSidecarRotx.VisibleSegments }

func (v *CaplinView) BeaconBlocksSegment(slot uint64) (*VisibleSegment, bool) {
	for _, seg := range v.BeaconBlocks() {
		if !(slot >= seg.from && slot < seg.to) {
			continue
		}
		return seg, true
	}
	return nil, false
}

func (v *CaplinView) BlobSidecarsSegment(slot uint64) (*VisibleSegment, bool) {
	for _, seg := range v.BlobSidecars() {
		if !(slot >= seg.from && slot < seg.to) {
			continue
		}
		return seg, true
	}
	return nil, false
}

func dumpBeaconBlocksRange(ctx context.Context, db kv.RoDB, fromSlot uint64, toSlot uint64, salt uint32, dirs datadir.Dirs, workers int, lvl log.Lvl, logger log.Logger) error {
	tmpDir, snapDir := dirs.Tmp, dirs.Snap

	segName := snaptype.BeaconBlocks.FileName(0, fromSlot, toSlot)
	f, _, _ := snaptype.ParseFileName(snapDir, segName)

	compressCfg := seg.DefaultCfg
	compressCfg.Workers = workers
	sn, err := seg.NewCompressor(ctx, "Snapshot BeaconBlocks", f.Path, tmpDir, compressCfg, lvl, logger)
	if err != nil {
		return err
	}
	defer sn.Close()

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	skippedInARow := 0
	var prevBlockRoot libcommon.Hash

	// Generate .seg file, which is just the list of beacon blocks.
	for i := fromSlot; i < toSlot; i++ {
		// read root.
		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, i)
		if err != nil {
			return err
		}
		parentRoot, err := beacon_indicies.ReadParentBlockRoot(ctx, tx, blockRoot)
		if err != nil {
			return err
		}
		if blockRoot != (libcommon.Hash{}) && prevBlockRoot != (libcommon.Hash{}) && parentRoot != prevBlockRoot {
			return fmt.Errorf("parent block root mismatch at slot %d", i)
		}

		dump, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(i, blockRoot))
		if err != nil {
			return err
		}
		if i%20_000 == 0 {
			logger.Log(lvl, "Dumping beacon blocks", "progress", i)
		}
		if dump == nil {
			skippedInARow++
		} else {
			prevBlockRoot = blockRoot
			skippedInARow = 0
		}
		if skippedInARow > 1000 {
			return fmt.Errorf("skipped too many blocks in a row during snapshot generation, range %d-%d at slot %d", fromSlot, toSlot, i)
		}
		if err := sn.AddWord(dump); err != nil {
			return err
		}
	}
	if sn.Count() != snaptype.CaplinMergeLimit {
		return fmt.Errorf("expected %d blocks, got %d", snaptype.CaplinMergeLimit, sn.Count())
	}
	if err := sn.Compress(); err != nil {
		return fmt.Errorf("compress: %w", err)
	}
	// Generate .idx file, which is the slot => offset mapping.
	p := &background.Progress{}

	// Ugly hack to wait for fsync
	time.Sleep(15 * time.Second)

	return BeaconSimpleIdx(ctx, f, salt, tmpDir, p, lvl, logger)
}

func dumpBlobSidecarsRange(ctx context.Context, db kv.RoDB, storage blob_storage.BlobStorage, fromSlot uint64, toSlot uint64, salt uint32, dirs datadir.Dirs, workers int, lvl log.Lvl, logger log.Logger) error {
	tmpDir, snapDir := dirs.Tmp, dirs.Snap

	segName := snaptype.BlobSidecars.FileName(0, fromSlot, toSlot)
	f, _, _ := snaptype.ParseFileName(snapDir, segName)

	compressCfg := seg.DefaultCfg
	compressCfg.Workers = workers
	sn, err := seg.NewCompressor(ctx, "Snapshot BlobSidecars", f.Path, tmpDir, compressCfg, lvl, logger)
	if err != nil {
		return err
	}
	defer sn.Close()

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	reusableBuf := []byte{}

	// Generate .seg file, which is just the list of beacon blocks.
	for i := fromSlot; i < toSlot; i++ {
		// read root.
		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, i)
		if err != nil {
			return err
		}

		commitmentsCount, err := storage.KzgCommitmentsCount(ctx, blockRoot)
		if err != nil {
			return err
		}
		if commitmentsCount == 0 {
			sn.AddWord(nil)
			continue
		}
		sidecars, found, err := storage.ReadBlobSidecars(ctx, i, blockRoot)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("blob sidecars not found for block %d", i)
		}
		reusableBuf = reusableBuf[:0]
		// Make a concatenated SSZ of all sidecars.
		for _, sidecar := range sidecars {
			reusableBuf, err = sidecar.EncodeSSZ(reusableBuf)
			if err != nil {
				return err
			}
		}

		if i%20_000 == 0 {
			logger.Log(lvl, "Dumping beacon blobs", "progress", i)
		}
		if err := sn.AddWord(reusableBuf); err != nil {
			return err
		}

	}
	if err := sn.Compress(); err != nil {
		return fmt.Errorf("compress: %w", err)
	}
	// Generate .idx file, which is the slot => offset mapping.
	p := &background.Progress{}

	return BeaconSimpleIdx(ctx, f, salt, tmpDir, p, lvl, logger)
}

func DumpBeaconBlocks(ctx context.Context, db kv.RoDB, fromSlot, toSlot uint64, salt uint32, dirs datadir.Dirs, workers int, lvl log.Lvl, logger log.Logger) error {
	cfg := snapcfg.KnownCfg("")
	for i := fromSlot; i < toSlot; i = chooseSegmentEnd(i, toSlot, snaptype.CaplinEnums.BeaconBlocks, nil) {
		blocksPerFile := snapcfg.MergeLimitFromCfg(cfg, snaptype.CaplinEnums.BeaconBlocks, i)

		if toSlot-i < blocksPerFile {
			break
		}
		to := chooseSegmentEnd(i, toSlot, snaptype.CaplinEnums.BeaconBlocks, nil)
		logger.Log(lvl, "Dumping beacon blocks", "from", i, "to", to)
		if err := dumpBeaconBlocksRange(ctx, db, i, to, salt, dirs, workers, lvl, logger); err != nil {
			return err
		}
	}
	return nil
}

func DumpBlobsSidecar(ctx context.Context, blobStorage blob_storage.BlobStorage, db kv.RoDB, fromSlot, toSlot uint64, salt uint32, dirs datadir.Dirs, compressWorkers int, lvl log.Lvl, logger log.Logger) error {
	cfg := snapcfg.KnownCfg("")
	for i := fromSlot; i < toSlot; i = chooseSegmentEnd(i, toSlot, snaptype.CaplinEnums.BlobSidecars, nil) {
		blocksPerFile := snapcfg.MergeLimitFromCfg(cfg, snaptype.CaplinEnums.BlobSidecars, i)

		if toSlot-i < blocksPerFile {
			break
		}
		to := chooseSegmentEnd(i, toSlot, snaptype.CaplinEnums.BlobSidecars, nil)
		logger.Log(lvl, "Dumping blobs sidecars", "from", i, "to", to)
		if err := dumpBlobSidecarsRange(ctx, db, blobStorage, i, to, salt, dirs, compressWorkers, lvl, logger); err != nil {
			return err
		}
	}
	return nil
}

func (s *CaplinSnapshots) BuildMissingIndices(ctx context.Context, logger log.Logger) error {
	if s == nil {
		return nil
	}
	// if !s.segmentsReady.Load() {
	// 	return fmt.Errorf("not all snapshot segments are available")
	// }

	// wait for Downloader service to download all expected snapshots
	segments, _, err := SegmentsCaplin(s.dir, 0)
	if err != nil {
		return err
	}
	noneDone := true
	for index := range segments {
		segment := segments[index]
		// The same slot=>offset mapping is used for both beacon blocks and blob sidecars.
		if segment.Type.Enum() != snaptype.CaplinEnums.BeaconBlocks && segment.Type.Enum() != snaptype.CaplinEnums.BlobSidecars {
			continue
		}
		if segment.Type.HasIndexFiles(segment, logger) {
			continue
		}
		p := &background.Progress{}
		noneDone = false
		if err := BeaconSimpleIdx(ctx, segment, s.Salt, s.tmpdir, p, log.LvlDebug, logger); err != nil {
			return err
		}
	}
	if noneDone {
		return nil
	}

	return s.ReopenFolder()
}

func (s *CaplinSnapshots) ReadHeader(slot uint64) (*cltypes.SignedBeaconBlockHeader, uint64, libcommon.Hash, error) {
	defer func() {
		if rec := recover(); rec != nil {
			panic(fmt.Sprintf("ReadHeader(%d), %s, %s\n", slot, rec, dbg.Stack()))
		}
	}()

	view := s.View()
	defer view.Close()

	var buf []byte

	seg, ok := view.BeaconBlocksSegment(slot)
	if !ok {
		return nil, 0, libcommon.Hash{}, nil
	}

	idxSlot := seg.src.Index()

	if idxSlot == nil {
		return nil, 0, libcommon.Hash{}, nil
	}
	blockOffset := idxSlot.OrdinalLookup(slot - idxSlot.BaseDataID())

	gg := seg.src.MakeGetter()
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

func (s *CaplinSnapshots) ReadBlobSidecars(slot uint64) ([]*cltypes.BlobSidecar, error) {
	view := s.View()
	defer view.Close()

	var buf []byte

	seg, ok := view.BlobSidecarsSegment(slot)
	if !ok {
		return nil, nil
	}

	idxSlot := seg.src.Index()

	if idxSlot == nil {
		return nil, nil
	}
	blockOffset := idxSlot.OrdinalLookup(slot - idxSlot.BaseDataID())

	gg := seg.src.MakeGetter()
	gg.Reset(blockOffset)
	if !gg.HasNext() {
		return nil, nil
	}

	buf, _ = gg.Next(buf)
	if len(buf) == 0 {
		return nil, nil
	}
	if len(buf)%sidecarSSZSize != 0 {
		return nil, errors.New("invalid sidecar list length")
	}
	sidecars := make([]*cltypes.BlobSidecar, len(buf)/sidecarSSZSize)
	for i := 0; i < len(buf); i += sidecarSSZSize {
		sidecars[i/sidecarSSZSize] = &cltypes.BlobSidecar{}
		if err := sidecars[i/sidecarSSZSize].DecodeSSZ(buf[i:i+sidecarSSZSize], int(clparams.DenebVersion)); err != nil {
			return nil, err
		}
	}
	return sidecars, nil
}

func (s *CaplinSnapshots) FrozenBlobs() uint64 {
	if s.beaconCfg.DenebForkEpoch == math.MaxUint64 {
		return 0
	}
	potentialMinSegFroms := []uint64{
		((s.beaconCfg.SlotsPerEpoch * s.beaconCfg.DenebForkEpoch) / snaptype.Erigon2MergeLimit) * snaptype.Erigon2MergeLimit,
		((s.beaconCfg.SlotsPerEpoch * s.beaconCfg.DenebForkEpoch) / snaptype.CaplinMergeLimit) * snaptype.CaplinMergeLimit,
	}
	foundMinSeg := false
	ret := uint64(0)
	for _, seg := range s.BlobSidecars.VisibleSegments {
		if slices.Contains(potentialMinSegFroms, seg.from) {
			foundMinSeg = true
		}
		ret = max(ret, seg.to)
	}
	if !foundMinSeg {
		return 0
	}
	return ret
}
