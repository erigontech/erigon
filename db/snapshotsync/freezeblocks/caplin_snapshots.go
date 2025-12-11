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
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/eth/ethconfig"
)

var sidecarSSZSize = (&cltypes.BlobSidecar{}).EncodingSizeSSZ()

// value: chunked(ssz(SignedBeaconBlocks))
// slot       -> beacon_slot_segment_offset

type CaplinSnapshots struct {
	Salt uint32

	dirtyLock sync.RWMutex                                // guards `dirty` field
	dirty     []*btree.BTreeG[*snapshotsync.DirtySegment] // ordered map `type.Enum()` -> DirtySegments

	visibleLock sync.RWMutex                   // guards  `visible` field
	visible     []snapshotsync.VisibleSegments // ordered map `type.Enum()` -> VisbileSegments

	dir         string
	tmpdir      string
	segmentsMax atomic.Uint64 // all types of .seg files are available - up to this number
	idxMax      atomic.Uint64 // all types of .idx files are available - up to this number
	cfg         ethconfig.BlocksFreezing
	logger      log.Logger
	// chain cfg
	beaconCfg *clparams.BeaconChainConfig
}

// NewCaplinSnapshots - opens all snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewCaplinSnapshots(cfg ethconfig.BlocksFreezing, beaconCfg *clparams.BeaconChainConfig, dirs datadir.Dirs, logger log.Logger) *CaplinSnapshots {
	if cfg.ChainName == "" {
		log.Debug("[dbg] NewCaplinSnapshots created with empty ChainName", "stack", dbg.Stack())
	}
	c := &CaplinSnapshots{dir: dirs.Snap, tmpdir: dirs.Tmp, cfg: cfg, logger: logger, beaconCfg: beaconCfg,
		dirty:   make([]*btree.BTreeG[*snapshotsync.DirtySegment], snaptype.MaxEnum),
		visible: make([]snapshotsync.VisibleSegments, snaptype.MaxEnum),
	}
	c.dirty[snaptype.BeaconBlocks.Enum()] = btree.NewBTreeGOptions[*snapshotsync.DirtySegment](snapshotsync.DirtySegmentLess, btree.Options{Degree: 128, NoLocks: false})
	c.dirty[snaptype.BlobSidecars.Enum()] = btree.NewBTreeGOptions[*snapshotsync.DirtySegment](snapshotsync.DirtySegmentLess, btree.Options{Degree: 128, NoLocks: false})
	c.recalcVisibleFiles()
	return c
}

func (s *CaplinSnapshots) IndicesMax() uint64  { return s.idxMax.Load() }
func (s *CaplinSnapshots) SegmentsMax() uint64 { return s.segmentsMax.Load() }

func (s *CaplinSnapshots) LogStat(str string) {
	s.logger.Info(fmt.Sprintf("[snapshots:%s] Stat", str),
		"blocks", common.PrettyCounter(s.SegmentsMax()+1), "indices", common.PrettyCounter(s.IndicesMax()+1))
}

func (s *CaplinSnapshots) LS() {
	if s == nil {
		return
	}
	view := s.View()
	defer view.Close()

	if view.BeaconBlockRotx != nil {
		for _, seg := range view.BeaconBlockRotx.Segments {
			log.Info("[agg] ", "f", seg.Src().Decompressor.FileName(), "words", seg.Src().Decompressor.Count())
		}
	}
	if view.BlobSidecarRotx != nil {
		for _, seg := range view.BlobSidecarRotx.Segments {
			log.Info("[agg] ", "f", seg.Src().Decompressor.FileName(), "words", seg.Src().Decompressor.Count())
		}
	}
}

func (s *CaplinSnapshots) SegFileNames(from, to uint64) []string {
	view := s.View()
	defer view.Close()

	var res []string
	for _, seg := range view.BeaconBlockRotx.Segments {
		if seg.From() >= from && seg.To() <= to {
			res = append(res, seg.Src().FileName())
		}
	}
	for _, seg := range view.BlobSidecarRotx.Segments {
		if seg.From() >= from && seg.To() <= to {
			res = append(res, seg.Src().FileName())
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
	s.dirtyLock.Lock()
	defer s.dirtyLock.Unlock()

	s.closeWhatNotInList(nil)
}

// OpenList stops on optimistic=false, continue opening files on optimistic=true
func (s *CaplinSnapshots) OpenList(fileNames []string, optimistic bool) error {
	defer s.recalcVisibleFiles()

	s.dirtyLock.Lock()
	defer s.dirtyLock.Unlock()

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
			var sn *snapshotsync.DirtySegment
			var exists bool
			s.dirty[snaptype.BeaconBlocks.Enum()].Walk(func(segments []*snapshotsync.DirtySegment) bool {
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
				sn = snapshotsync.NewDirtySegment(
					snaptype.BeaconBlocks,
					f.Version,
					f.From, f.To,
					true)
			}
			if err := sn.Open(s.dir); err != nil {
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
				s.dirty[snaptype.BeaconBlocks.Enum()].Set(sn)
			}
			if err := sn.OpenIdxIfNeed(s.dir, optimistic); err != nil {
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
			var sn *snapshotsync.DirtySegment
			var exists bool
			s.dirty[snaptype.BlobSidecars.Enum()].Walk(func(segments []*snapshotsync.DirtySegment) bool {
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
				sn = snapshotsync.NewDirtySegment(
					snaptype.BlobSidecars,
					f.Version,
					f.From, f.To,
					true)
			}
			if err := sn.Open(s.dir); err != nil {
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
				s.dirty[snaptype.BlobSidecars.Enum()].Set(sn)
			}
			if err := sn.OpenIdxIfNeed(s.dir, optimistic); err != nil {
				return err
			}
		}

	}
	if segmentsMaxSet {
		s.segmentsMax.Store(segmentsMax)
	}
	return nil
}

func (s *CaplinSnapshots) recalcVisibleFiles() {
	defer func() {
		s.idxMax.Store(s.idxAvailability())
	}()

	s.visibleLock.Lock()
	defer s.visibleLock.Unlock()
	s.visible = make([]snapshotsync.VisibleSegments, snaptype.MaxEnum) // create new pointer - only new readers will see it. old-alive readers will continue use previous pointer
	s.visible[snaptype.BeaconBlocks.Enum()] = snapshotsync.RecalcVisibleSegments(s.dirty[snaptype.BeaconBlocks.Enum()])
	s.visible[snaptype.BlobSidecars.Enum()] = snapshotsync.RecalcVisibleSegments(s.dirty[snaptype.BlobSidecars.Enum()])
}

func (s *CaplinSnapshots) idxAvailability() uint64 {
	s.visibleLock.RLock()
	defer s.visibleLock.RUnlock()
	if len(s.visible[snaptype.BeaconBlocks.Enum()]) == 0 {
		return 0
	}
	return s.visible[snaptype.BeaconBlocks.Enum()][len(s.visible[snaptype.BeaconBlocks.Enum()])-1].To()
}

func (s *CaplinSnapshots) OpenFolder() error {
	files, _, err := snapshotsync.SegmentsCaplin(s.dir)
	if err != nil {
		return err
	}
	list := make([]string, 0, len(files))
	for _, f := range files {
		_, fName := filepath.Split(f.Path)
		list = append(list, fName)
	}
	return s.OpenList(list, false)
}

func (s *CaplinSnapshots) closeWhatNotInList(l []string) {
	protectFiles := make(map[string]struct{}, len(l))
	for _, fName := range l {
		protectFiles[fName] = struct{}{}
	}
	toClose := make([]*snapshotsync.DirtySegment, 0)
	s.dirty[snaptype.BeaconBlocks.Enum()].Walk(func(segments []*snapshotsync.DirtySegment) bool {
		for _, sn := range segments {
			if sn.Decompressor == nil {
				continue
			}
			_, name := filepath.Split(sn.FilePath())
			if _, ok := protectFiles[name]; ok {
				continue
			}
			toClose = append(toClose, sn)
		}
		return true
	})
	for _, sn := range toClose {
		sn.Close()
		s.dirty[snaptype.BeaconBlocks.Enum()].Delete(sn)
	}

	toClose = make([]*snapshotsync.DirtySegment, 0)
	s.dirty[snaptype.BlobSidecars.Enum()].Walk(func(segments []*snapshotsync.DirtySegment) bool {
		for _, sn := range segments {
			if sn.Decompressor == nil {
				continue
			}
			_, name := filepath.Split(sn.FilePath())
			if _, ok := protectFiles[name]; ok {
				continue
			}
			toClose = append(toClose, sn)
		}
		return true
	})
	for _, sn := range toClose {
		sn.Close()
		s.dirty[snaptype.BlobSidecars.Enum()].Delete(sn)
	}
}

type CaplinView struct {
	s               *CaplinSnapshots
	BeaconBlockRotx *snapshotsync.RoTx
	BlobSidecarRotx *snapshotsync.RoTx
	closed          bool
}

func (s *CaplinSnapshots) View() *CaplinView {
	s.visibleLock.RLock()
	defer s.visibleLock.RUnlock()

	v := &CaplinView{s: s}
	if s.visible[snaptype.BeaconBlocks.Enum()] != nil {
		v.BeaconBlockRotx = s.visible[snaptype.BeaconBlocks.Enum()].BeginRo()
	}
	if s.visible[snaptype.BlobSidecars.Enum()] != nil {
		v.BlobSidecarRotx = s.visible[snaptype.BlobSidecars.Enum()].BeginRo()
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

func (v *CaplinView) BeaconBlocks() []*snapshotsync.VisibleSegment {
	return v.BeaconBlockRotx.Segments
}
func (v *CaplinView) BlobSidecars() []*snapshotsync.VisibleSegment { return v.BlobSidecarRotx.Segments }

func (v *CaplinView) BeaconBlocksSegment(slot uint64) (*snapshotsync.VisibleSegment, bool) {
	for _, seg := range v.BeaconBlocks() {
		if !(slot >= seg.From() && slot < seg.To()) {
			continue
		}
		return seg, true
	}
	return nil, false
}

func (v *CaplinView) BlobSidecarsSegment(slot uint64) (*snapshotsync.VisibleSegment, bool) {
	for _, seg := range v.BlobSidecars() {
		if !(slot >= seg.From() && slot < seg.To()) {
			continue
		}
		return seg, true
	}
	return nil, false
}

func dumpBeaconBlocksRange(ctx context.Context, db kv.RoDB, fromSlot uint64, toSlot uint64, salt uint32, dirs datadir.Dirs, workers int, lvl log.Lvl, logger log.Logger) error {
	tmpDir, snapDir := dirs.Tmp, dirs.Snap

	segName := snaptype.BeaconBlocks.FileName(version.ZeroVersion, fromSlot, toSlot)
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
	var prevBlockRoot common.Hash

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
		if blockRoot != (common.Hash{}) && prevBlockRoot != (common.Hash{}) && parentRoot != prevBlockRoot {
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
	tx.Rollback()
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

	return snapshotsync.BeaconSimpleIdx(ctx, f, salt, tmpDir, p, lvl, logger)
}

func DumpBlobSidecarsRange(ctx context.Context, db kv.RoDB, storage blob_storage.BlobStorage, fromSlot uint64, toSlot uint64, salt uint32, dirs datadir.Dirs, workers int, blobCountFn BlobCountBySlotFn, lvl log.Lvl, logger log.Logger) error {
	tmpDir, snapDir := dirs.Tmp, dirs.Snap

	segName := snaptype.BlobSidecars.FileName(version.ZeroVersion, fromSlot, toSlot)
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

	sanityCheckBlobCount := blobCountFn != nil

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
		var blobCount uint64
		if sanityCheckBlobCount {
			blobCount, err = blobCountFn(i)
			if err != nil {
				return err
			}
			if blobCount != uint64(commitmentsCount) {
				return fmt.Errorf("blob storage count mismatch at slot %d: %d != %d", i, blobCount, commitmentsCount)
			}
		}
		if commitmentsCount == 0 {
			sn.AddWord(nil)
			continue
		}
		sidecars, found, err := storage.ReadBlobSidecars(ctx, i, blockRoot)
		if err != nil {
			return err
		}
		if sanityCheckBlobCount && uint64(len(sidecars)) != blobCount {
			return fmt.Errorf("blob sidecars count mismatch at slot %d: %d != %d", i, len(sidecars), blobCount)
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
	tx.Rollback()
	if err := sn.Compress(); err != nil {
		return fmt.Errorf("compress: %w", err)
	}
	// Generate .idx file, which is the slot => offset mapping.
	p := &background.Progress{}

	return snapshotsync.BeaconSimpleIdx(ctx, f, salt, tmpDir, p, lvl, logger)
}

func DumpBeaconBlocks(ctx context.Context, db kv.RoDB, fromSlot, toSlot uint64, salt uint32, dirs datadir.Dirs, workers int, lvl log.Lvl, logger log.Logger) error {
	cfg, _ := snapcfg.KnownCfg("")
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

type BlobCountBySlotFn func(slot uint64) (uint64, error)

func DumpBlobsSidecar(ctx context.Context, blobStorage blob_storage.BlobStorage, db kv.RoDB, fromSlot, toSlot uint64, salt uint32, dirs datadir.Dirs, compressWorkers int, blobCountFn BlobCountBySlotFn, lvl log.Lvl, logger log.Logger) error {
	cfg, _ := snapcfg.KnownCfg("")
	for i := fromSlot; i < toSlot; i = chooseSegmentEnd(i, toSlot, snaptype.CaplinEnums.BlobSidecars, nil) {
		blocksPerFile := snapcfg.MergeLimitFromCfg(cfg, snaptype.CaplinEnums.BlobSidecars, i)

		if toSlot-i < blocksPerFile {
			break
		}
		to := chooseSegmentEnd(i, toSlot, snaptype.CaplinEnums.BlobSidecars, nil)
		logger.Log(lvl, "Dumping blobs sidecars", "from", i, "to", to)
		if err := DumpBlobSidecarsRange(ctx, db, blobStorage, i, to, salt, dirs, compressWorkers, blobCountFn, lvl, logger); err != nil {
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
	segments, _, err := snapshotsync.SegmentsCaplin(s.dir)
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
		if err := snapshotsync.BeaconSimpleIdx(ctx, segment, s.Salt, s.tmpdir, p, log.LvlDebug, logger); err != nil {
			return err
		}
	}
	if noneDone {
		return nil
	}

	return s.OpenFolder()
}

func (s *CaplinSnapshots) ReadHeader(slot uint64) (*cltypes.SignedBeaconBlockHeader, uint64, common.Hash, error) {
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
		return nil, 0, common.Hash{}, nil
	}

	idxSlot := seg.Src().Index()

	if idxSlot == nil {
		return nil, 0, common.Hash{}, nil
	}
	blockOffset := idxSlot.OrdinalLookup(slot - idxSlot.BaseDataID())

	gg := seg.Src().MakeGetter()
	gg.Reset(blockOffset)
	if !gg.HasNext() {
		return nil, 0, common.Hash{}, nil
	}

	buf, _ = gg.Next(buf[:0])
	if len(buf) == 0 {
		return nil, 0, common.Hash{}, nil
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

	idxSlot := seg.Src().Index()

	if idxSlot == nil {
		return nil, nil
	}
	blockOffset := idxSlot.OrdinalLookup(slot - idxSlot.BaseDataID())

	gg := seg.Src().MakeGetter()
	gg.Reset(blockOffset)
	if !gg.HasNext() {
		return nil, nil
	}

	buf, _ = gg.Next(buf[:0])
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
	ret := uint64(0)
	for _, seg := range s.visible[snaptype.BlobSidecars.Enum()] {
		ret = max(ret, seg.To())
	}

	return ret
}
