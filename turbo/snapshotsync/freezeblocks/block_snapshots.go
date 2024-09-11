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
	"cmp"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
	"github.com/tidwall/btree"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	common2 "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	dir2 "github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/diagnostics"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
	types2 "github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/rawdb/blockio"
	coresnaptype "github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/silkworm"
)

type SortedRange interface {
	GetRange() (from, to uint64)
	GetType() snaptype.Type
}

// noOverlaps - keep largest ranges and avoid overlap
func noOverlaps[T SortedRange](in []T) (res []T) {
	for i := 0; i < len(in); i++ {
		r := in[i]
		iFrom, iTo := r.GetRange()
		if iFrom == iTo {
			continue
		}
		for j := i + 1; j < len(in); j++ {
			r2 := in[j]
			jFrom, jTo := r2.GetRange()
			if jFrom == jTo {
				continue
			}
			if jFrom > iFrom {
				break
			}
			r = r2
			i++
		}
		res = append(res, r)
	}
	return res
}

func noGaps[T SortedRange](in []T) (out []T, missingRanges []Range) {
	if len(in) == 0 {
		return nil, nil
	}
	prevTo, _ := in[0].GetRange()
	for _, f := range in {
		from, to := f.GetRange()
		if to <= prevTo {
			continue
		}
		if from != prevTo { // no gaps
			missingRanges = append(missingRanges, Range{prevTo, from})
			continue
		}
		prevTo = to
		out = append(out, f)
	}
	return out, missingRanges
}

func findOverlaps[T SortedRange](in []T) (res []T, overlapped []T) {
	for i := 0; i < len(in); i++ {
		f := in[i]
		iFrom, iTo := f.GetRange()
		if iFrom == iTo {
			overlapped = append(overlapped, f)
			continue
		}

		for j := i + 1; j < len(in); i, j = i+1, j+1 { // if there is file with larger range - use it instead
			f2 := in[j]
			jFrom, jTo := f2.GetRange()

			if f.GetType().Enum() != f2.GetType().Enum() {
				break
			}
			if jFrom == jTo {
				overlapped = append(overlapped, f2)
				continue
			}
			if jFrom > iFrom && jTo > iTo {
				break
			}

			if iTo >= jTo && iFrom <= jFrom {
				overlapped = append(overlapped, f2)
				continue
			}
			if i < len(in)-1 && (jTo >= iTo && jFrom <= iFrom) {
				overlapped = append(overlapped, f)
			}
			f = f2
			iFrom, iTo = f.GetRange()
		}
		res = append(res, f)
	}
	return res, overlapped
}

func FindOverlaps(in []snaptype.FileInfo) (res []snaptype.FileInfo, overlapped []snaptype.FileInfo) {
	for i := 0; i < len(in); i++ {
		f := in[i]

		if f.From == f.To {
			overlapped = append(overlapped, f)
			continue
		}

		for j := i + 1; j < len(in); i, j = i+1, j+1 { // if there is file with larger range - use it instead
			f2 := in[j]

			if f.Type.Enum() != f2.Type.Enum() {
				break
			}

			if f2.From == f2.To {
				overlapped = append(overlapped, f2)
				continue
			}

			if f2.From > f.From && f2.To > f.To {
				break
			}

			if f.To >= f2.To && f.From <= f2.From {
				overlapped = append(overlapped, f2)
				continue
			}

			if i < len(in)-1 && (f2.To >= f.To && f2.From <= f.From) {
				overlapped = append(overlapped, f)
			}

			f = f2
		}

		res = append(res, f)
	}

	return res, overlapped
}

type Range struct {
	from, to uint64
}

func (r Range) From() uint64 { return r.from }
func (r Range) To() uint64   { return r.to }

type Ranges []Range

func (r Ranges) String() string {
	return fmt.Sprintf("%d", r)
}

type DirtySegment struct {
	Range
	*seg.Decompressor
	indexes []*recsplit.Index
	segType snaptype.Type
	version snaptype.Version

	frozen   bool
	refcount atomic.Int32

	canDelete atomic.Bool
}

type VisibleSegment struct {
	Range
	segType snaptype.Type
	src     *DirtySegment
}

func DirtySegmentLess(i, j *DirtySegment) bool {
	if i.from != j.from {
		return i.from < j.from
	}
	if i.to != j.to {
		return i.to < j.to
	}
	return int(i.version) < int(j.version)
}

func (s *DirtySegment) Type() snaptype.Type {
	return s.segType
}

func (s *DirtySegment) Version() snaptype.Version {
	return s.version
}

func (s *DirtySegment) Index(index ...snaptype.Index) *recsplit.Index {
	if len(index) == 0 {
		index = []snaptype.Index{{}}
	}

	if len(s.indexes) <= index[0].Offset {
		return nil
	}

	return s.indexes[index[0].Offset]
}

func (s *DirtySegment) FileName() string {
	return s.Type().FileName(s.version, s.from, s.to)
}

func (s *DirtySegment) FileInfo(dir string) snaptype.FileInfo {
	return s.Type().FileInfo(dir, s.from, s.to)
}

func (s *DirtySegment) GetRange() (from, to uint64) { return s.from, s.to }
func (s *DirtySegment) GetType() snaptype.Type      { return s.segType }
func (s *DirtySegment) isSubSetOf(j *DirtySegment) bool {
	return (j.from <= s.from && s.to <= j.to) && (j.from != s.from || s.to != j.to)
}

func (s *DirtySegment) reopenSeg(dir string) (err error) {
	s.closeSeg()
	s.Decompressor, err = seg.NewDecompressor(filepath.Join(dir, s.FileName()))
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, s.FileName())
	}
	return nil
}

func (s *DirtySegment) closeSeg() {
	if s.Decompressor != nil {
		s.Close()
		s.Decompressor = nil
	}
}

func (s *DirtySegment) closeIdx() {
	for _, index := range s.indexes {
		index.Close()
	}

	s.indexes = nil
}

func (s *DirtySegment) close() {
	if s != nil {
		s.closeSeg()
		s.closeIdx()
	}
}

func (s *DirtySegment) closeAndRemoveFiles() {
	if s != nil {
		f := s.FilePath()
		s.closeSeg()
		s.closeIdx()

		snapDir := filepath.Dir(f)
		removeOldFiles([]string{f}, snapDir)
	}
}

func (s *DirtySegment) openFiles() []string {
	files := make([]string, 0, len(s.indexes)+1)

	if s.IsOpen() {
		files = append(files, s.FilePath())
	}

	for _, index := range s.indexes {
		files = append(files, index.FilePath())
	}

	return files
}

func (s *DirtySegment) reopenIdxIfNeed(dir string, optimistic bool) (err error) {
	if len(s.Type().IdxFileNames(s.version, s.from, s.to)) == 0 {
		return nil
	}

	err = s.reopenIdx(dir)

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

func (s *DirtySegment) reopenIdx(dir string) (err error) {
	s.closeIdx()
	if s.Decompressor == nil {
		return nil
	}

	for _, fileName := range s.Type().IdxFileNames(s.version, s.from, s.to) {
		index, err := recsplit.OpenIndex(filepath.Join(dir, fileName))

		if err != nil {
			return fmt.Errorf("%w, fileName: %s", err, fileName)
		}

		s.indexes = append(s.indexes, index)
	}

	return nil
}

func (sn *DirtySegment) mappedHeaderSnapshot() *silkworm.MappedHeaderSnapshot {
	segmentRegion := silkworm.NewMemoryMappedRegion(sn.FilePath(), sn.DataHandle(), sn.Size())
	idxRegion := silkworm.NewMemoryMappedRegion(sn.Index().FilePath(), sn.Index().DataHandle(), sn.Index().Size())
	return silkworm.NewMappedHeaderSnapshot(segmentRegion, idxRegion)
}

func (sn *DirtySegment) mappedBodySnapshot() *silkworm.MappedBodySnapshot {
	segmentRegion := silkworm.NewMemoryMappedRegion(sn.FilePath(), sn.DataHandle(), sn.Size())
	idxRegion := silkworm.NewMemoryMappedRegion(sn.Index().FilePath(), sn.Index().DataHandle(), sn.Index().Size())
	return silkworm.NewMappedBodySnapshot(segmentRegion, idxRegion)
}

func (sn *DirtySegment) mappedTxnSnapshot() *silkworm.MappedTxnSnapshot {
	segmentRegion := silkworm.NewMemoryMappedRegion(sn.FilePath(), sn.DataHandle(), sn.Size())
	idxTxnHash := sn.Index(coresnaptype.Indexes.TxnHash)
	idxTxnHashRegion := silkworm.NewMemoryMappedRegion(idxTxnHash.FilePath(), idxTxnHash.DataHandle(), idxTxnHash.Size())
	idxTxnHash2BlockNum := sn.Index(coresnaptype.Indexes.TxnHash2BlockNum)
	idxTxnHash2BlockRegion := silkworm.NewMemoryMappedRegion(idxTxnHash2BlockNum.FilePath(), idxTxnHash2BlockNum.DataHandle(), idxTxnHash2BlockNum.Size())
	return silkworm.NewMappedTxnSnapshot(segmentRegion, idxTxnHashRegion, idxTxnHash2BlockRegion)
}

// headers
// value: first_byte_of_header_hash + header_rlp
// header_hash       -> headers_segment_offset

// bodies
// value: rlp(types.BodyForStorage)
// block_num_u64     -> bodies_segment_offset

// transactions
// value: first_byte_of_transaction_hash + sender_address + transaction_rlp
// transaction_hash  -> transactions_segment_offset
// transaction_hash  -> block_number

type segments struct {
	DirtySegments   *btree.BTreeG[*DirtySegment]
	VisibleSegments []*VisibleSegment
	maxVisibleBlock atomic.Uint64
}

func (s *segments) View(f func(segments []*VisibleSegment) error) error {
	return f(s.VisibleSegments)
}

// no caller yet
func (s *segments) Segment(blockNum uint64, f func(*VisibleSegment) error) (found bool, err error) {
	for _, seg := range s.VisibleSegments {
		if !(blockNum >= seg.from && blockNum < seg.to) {
			continue
		}
		return true, f(seg)
	}
	return false, nil
}

func (s *segments) BeginRotx() *segmentsRotx {
	for _, seg := range s.VisibleSegments {
		if !seg.src.frozen {
			seg.src.refcount.Add(1)
		}
	}
	return &segmentsRotx{segments: s, VisibleSegments: s.VisibleSegments}
}

func (s *segmentsRotx) Close() {
	if s.VisibleSegments == nil {
		return
	}
	VisibleSegments := s.VisibleSegments
	s.VisibleSegments = nil

	for i := range VisibleSegments {
		src := VisibleSegments[i].src
		if src == nil || src.frozen {
			continue
		}
		refCnt := src.refcount.Add(-1)
		if refCnt == 0 && src.canDelete.Load() {
			src.closeAndRemoveFiles()
		}
	}
}

type segmentsRotx struct {
	segments        *segments
	VisibleSegments []*VisibleSegment
}

type RoSnapshots struct {
	indicesReady  atomic.Bool
	segmentsReady atomic.Bool

	types []snaptype.Type

	dirtySegmentsLock   sync.RWMutex
	visibleSegmentsLock sync.RWMutex

	segments btree.Map[snaptype.Enum, *segments]

	dir         string
	segmentsMax atomic.Uint64 // all types of .seg files are available - up to this number
	idxMax      atomic.Uint64 // all types of .idx files are available - up to this number
	cfg         ethconfig.BlocksFreezing
	logger      log.Logger

	// allows for pruning segments - this is the min available segment
	segmentsMin atomic.Uint64
}

// NewRoSnapshots - opens all snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, segmentsMin uint64, logger log.Logger) *RoSnapshots {
	return newRoSnapshots(cfg, snapDir, coresnaptype.BlockSnapshotTypes, segmentsMin, logger)
}

func newRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, types []snaptype.Type, segmentsMin uint64, logger log.Logger) *RoSnapshots {
	var segs btree.Map[snaptype.Enum, *segments]
	for _, snapType := range types {
		segs.Set(snapType.Enum(), &segments{
			DirtySegments: btree.NewBTreeGOptions[*DirtySegment](DirtySegmentLess, btree.Options{Degree: 128, NoLocks: false}),
		})
	}

	s := &RoSnapshots{dir: snapDir, cfg: cfg, segments: segs, logger: logger, types: types}
	s.segmentsMin.Store(segmentsMin)
	s.recalcVisibleFiles()

	return s
}

func (s *RoSnapshots) Cfg() ethconfig.BlocksFreezing { return s.cfg }
func (s *RoSnapshots) Dir() string                   { return s.dir }
func (s *RoSnapshots) SegmentsReady() bool           { return s.segmentsReady.Load() }
func (s *RoSnapshots) IndicesReady() bool            { return s.indicesReady.Load() }
func (s *RoSnapshots) IndicesMax() uint64            { return s.idxMax.Load() }
func (s *RoSnapshots) SegmentsMax() uint64           { return s.segmentsMax.Load() }
func (s *RoSnapshots) SegmentsMin() uint64           { return s.segmentsMin.Load() }
func (s *RoSnapshots) SetSegmentsMin(min uint64)     { s.segmentsMin.Store(min) }
func (s *RoSnapshots) BlocksAvailable() uint64 {
	if s == nil {
		return 0
	}

	return s.idxMax.Load()
}
func (s *RoSnapshots) LogStat(label string) {
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	s.logger.Info(fmt.Sprintf("[snapshots:%s] Stat", label),
		"blocks", common2.PrettyCounter(s.SegmentsMax()+1), "indices", common2.PrettyCounter(s.IndicesMax()+1),
		"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
}

func (s *RoSnapshots) EnsureExpectedBlocksAreAvailable(cfg *snapcfg.Cfg) error {
	if s.BlocksAvailable() < cfg.ExpectBlocks {
		return fmt.Errorf("app must wait until all expected snapshots are available. Expected: %d, Available: %d", cfg.ExpectBlocks, s.BlocksAvailable())
	}
	return nil
}

func (s *RoSnapshots) Types() []snaptype.Type { return s.types }
func (s *RoSnapshots) HasType(in snaptype.Type) bool {
	for _, t := range s.types {
		if t.Enum() == in.Enum() {
			return true
		}
	}
	return false
}

// DisableReadAhead - usage: `defer d.EnableReadAhead().DisableReadAhead()`. Please don't use this funcs without `defer` to avoid leak.
func (s *RoSnapshots) DisableReadAhead() *RoSnapshots {
	v := s.View()
	defer v.Close()

	s.segments.Scan(func(segtype snaptype.Enum, value *segments) bool {
		for _, sn := range value.VisibleSegments {
			sn.src.DisableReadAhead()
		}
		return true
	})

	return s
}

func (s *RoSnapshots) EnableReadAhead() *RoSnapshots {
	v := s.View()
	defer v.Close()

	s.segments.Scan(func(segtype snaptype.Enum, value *segments) bool {
		for _, sn := range value.VisibleSegments {
			sn.src.EnableReadAhead()
		}
		return true
	})

	return s
}

func (s *RoSnapshots) EnableMadvWillNeed() *RoSnapshots {
	v := s.View()
	defer v.Close()

	s.segments.Scan(func(segtype snaptype.Enum, value *segments) bool {
		for _, sn := range value.VisibleSegments {
			sn.src.EnableMadvWillNeed()
		}
		return true
	})
	return s
}

func (s *RoSnapshots) recalcVisibleFiles() {
	defer func() {
		s.idxMax.Store(s.idxAvailability())
		s.indicesReady.Store(true)
	}()

	s.visibleSegmentsLock.Lock()
	defer s.visibleSegmentsLock.Unlock()

	var maxVisibleBlocks []uint64
	s.segments.Scan(func(segtype snaptype.Enum, value *segments) bool {
		dirtySegments := value.DirtySegments
		newVisibleSegments := make([]*VisibleSegment, 0, dirtySegments.Len())
		dirtySegments.Walk(func(segs []*DirtySegment) bool {
			for _, seg := range segs {
				if seg.canDelete.Load() {
					continue
				}
				if seg.Decompressor == nil {
					continue
				}
				if seg.indexes == nil {
					continue
				}
				for len(newVisibleSegments) > 0 && newVisibleSegments[len(newVisibleSegments)-1].src.isSubSetOf(seg) {
					newVisibleSegments[len(newVisibleSegments)-1].src = nil
					newVisibleSegments = newVisibleSegments[:len(newVisibleSegments)-1]
				}
				newVisibleSegments = append(newVisibleSegments, &VisibleSegment{
					Range:   seg.Range,
					segType: seg.segType,
					src:     seg,
				})

			}
			return true
		})

		value.VisibleSegments = newVisibleSegments
		var to uint64
		if len(newVisibleSegments) > 0 {
			to = newVisibleSegments[len(newVisibleSegments)-1].to - 1
		}
		maxVisibleBlocks = append(maxVisibleBlocks, to)
		return true
	})

	minMaxVisibleBlock := slices.Min(maxVisibleBlocks)
	s.segments.Scan(func(segtype snaptype.Enum, value *segments) bool {
		if minMaxVisibleBlock == 0 {
			value.VisibleSegments = []*VisibleSegment{}
		} else {
			for i, seg := range value.VisibleSegments {
				if seg.to > minMaxVisibleBlock+1 {
					value.VisibleSegments = value.VisibleSegments[:i]
					break
				}
			}
		}
		value.maxVisibleBlock.Store(minMaxVisibleBlock)
		return true
	})
}

// minimax of existing indices
func (s *RoSnapshots) idxAvailability() uint64 {
	// Use-Cases:
	//   1. developers can add new types in future. and users will not have files of this type
	//   2. some types are network-specific. example: borevents exists only on Bor-consensus networks
	//   3. user can manually remove 1 .idx file: `rm snapshots/v1-type1-0000-1000.idx`
	//   4. user can manually remove all .idx files of given type: `rm snapshots/*type1*.idx`
	//   5. file-types may have different height: 10 headers, 10 bodies, 9 transactions (for example if `kill -9` came during files building/merge). still need index all 3 types.

	var maxIdx uint64
	s.segments.Scan(func(segtype snaptype.Enum, value *segments) bool {
		if !s.HasType(segtype.Type()) {
			return true
		}
		maxIdx = value.maxVisibleBlock.Load()
		return false // all types of segments have the same height. stop here
	})

	return maxIdx
}

func (s *RoSnapshots) LS() {
	view := s.View()
	defer view.Close()

	view.VisibleSegments.Scan(func(segtype snaptype.Enum, value *segmentsRotx) bool {
		for _, seg := range value.VisibleSegments {
			if seg.src.Decompressor == nil {
				continue
			}
			log.Info("[snapshots] ", "f", seg.src.Decompressor.FileName(), "from", seg.from, "to", seg.to)
		}
		return true
	})
}

func (s *RoSnapshots) Files() (list []string) {
	view := s.View()
	defer view.Close()

	view.VisibleSegments.Scan(func(segtype snaptype.Enum, value *segmentsRotx) bool {
		for _, seg := range value.VisibleSegments {
			list = append(list, seg.src.FileName())
		}
		return true
	})

	return
}

func (s *RoSnapshots) OpenFiles() (list []string) {
	s.dirtySegmentsLock.RLock()
	defer s.dirtySegmentsLock.RUnlock()

	log.Warn("[dbg] OpenFiles")
	defer log.Warn("[dbg] OpenFiles end")
	s.segments.Scan(func(segtype snaptype.Enum, value *segments) bool {
		value.DirtySegments.Walk(func(segs []*DirtySegment) bool {
			for _, seg := range segs {
				if seg.Decompressor == nil {
					continue
				}
				list = append(list, seg.FilePath())
			}
			return true
		})
		return true
	})

	return list
}

// ReopenList stops on optimistic=false, continue opening files on optimistic=true
func (s *RoSnapshots) ReopenList(fileNames []string, optimistic bool) error {
	defer s.recalcVisibleFiles()

	s.dirtySegmentsLock.Lock()
	defer s.dirtySegmentsLock.Unlock()

	s.closeWhatNotInList(fileNames)
	if err := s.rebuildSegments(fileNames, true, optimistic); err != nil {
		return err
	}
	return nil
}

func (s *RoSnapshots) InitSegments(fileNames []string) error {
	defer s.recalcVisibleFiles()

	s.dirtySegmentsLock.Lock()
	defer s.dirtySegmentsLock.Unlock()

	s.closeWhatNotInList(fileNames)
	if err := s.rebuildSegments(fileNames, false, true); err != nil {
		return err
	}
	return nil
}

func (s *RoSnapshots) rebuildSegments(fileNames []string, open bool, optimistic bool) error {
	var segmentsMax uint64
	var segmentsMaxSet bool

	for _, fName := range fileNames {
		f, isState, ok := snaptype.ParseFileName(s.dir, fName)
		if !ok || isState {
			continue
		}
		if !s.HasType(f.Type) {
			continue
		}

		segtype, ok := s.segments.Get(f.Type.Enum())
		if !ok {
			segtype = &segments{
				DirtySegments: btree.NewBTreeGOptions[*DirtySegment](DirtySegmentLess, btree.Options{Degree: 128, NoLocks: false}),
			}
			s.segments.Set(f.Type.Enum(), segtype)
		}

		var sn *DirtySegment
		var exists bool
		segtype.DirtySegments.Walk(func(segs []*DirtySegment) bool {
			for _, sn2 := range segs {
				if sn2.Decompressor == nil { // it's ok if some segment was not able to open
					continue
				}
				if fName == sn2.FileName() {
					sn = sn2
					exists = true
					return false
				}
			}
			return true
		})

		if !exists {
			sn = &DirtySegment{segType: f.Type, version: f.Version, Range: Range{f.From, f.To}, frozen: snapcfg.Seedable(s.cfg.ChainName, f)}
		}

		if open {
			if err := sn.reopenSeg(s.dir); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					if optimistic {
						continue
					} else {
						break
					}
				}
				if optimistic {
					continue
				} else {
					return err
				}
			}
		}

		if !exists {
			// it's possible to iterate over .seg file even if you don't have index
			// then make segment available even if index open may fail
			segtype.DirtySegments.Set(sn)
		}

		if open {
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
	return nil
}

func (s *RoSnapshots) Ranges() []Range {
	view := s.View()
	defer view.Close()
	return view.Ranges()
}

func (s *RoSnapshots) OptimisticalyReopenFolder() { _ = s.ReopenFolder() }
func (s *RoSnapshots) ReopenFolder() error {
	defer s.recalcVisibleFiles()

	s.dirtySegmentsLock.Lock()
	defer s.dirtySegmentsLock.Unlock()

	fmt.Printf("[dbg] types: %s\n", s.Types())
	files, _, err := typedSegments(s.dir, s.segmentsMin.Load(), s.Types(), false)
	if err != nil {
		return err
	}

	list := make([]string, 0, len(files))
	for _, f := range files {
		_, fName := filepath.Split(f.Path)
		list = append(list, fName)
	}
	s.closeWhatNotInList(list)
	if err := s.rebuildSegments(list, true, false); err != nil {
		return err
	}
	return nil
}

func (s *RoSnapshots) ReopenSegments(types []snaptype.Type, allowGaps bool) error {
	defer s.recalcVisibleFiles()

	s.dirtySegmentsLock.Lock()
	defer s.dirtySegmentsLock.Unlock()

	files, _, err := typedSegments(s.dir, s.segmentsMin.Load(), types, allowGaps)

	if err != nil {
		return err
	}
	list := make([]string, 0, len(files))
	for _, f := range files {
		_, fName := filepath.Split(f.Path)
		list = append(list, fName)
	}

	if err := s.rebuildSegments(list, true, false); err != nil {
		return err
	}
	return nil
}

func (s *RoSnapshots) Close() {
	if s == nil {
		return
	}
	s.dirtySegmentsLock.Lock()
	defer s.dirtySegmentsLock.Unlock()

	s.closeWhatNotInList(nil)
	s.recalcVisibleFiles()
}

func (s *RoSnapshots) closeWhatNotInList(l []string) {
	toClose := make(map[snaptype.Enum][]*DirtySegment, 0)
	s.segments.Scan(func(segtype snaptype.Enum, value *segments) bool {
		value.DirtySegments.Walk(func(segs []*DirtySegment) bool {

		Loop1:
			for _, seg := range segs {
				for _, fName := range l {
					if fName == seg.FileName() {
						continue Loop1
					}
				}
				if _, ok := toClose[seg.segType.Enum()]; !ok {
					toClose[segtype] = make([]*DirtySegment, 0)
				}
				toClose[segtype] = append(toClose[segtype], seg)
			}

			return true
		})
		return true
	})

	for segtype, delSegments := range toClose {
		segs, _ := s.segments.Get(segtype)
		for _, delSeg := range delSegments {
			delSeg.close()
			segs.DirtySegments.Delete(delSeg)
		}
	}
}

func (s *RoSnapshots) removeOverlapsAfterMerge() error {
	list, err := snaptype.Segments(s.dir)

	if err != nil {
		return err
	}

	if _, toRemove := findOverlaps(list); len(toRemove) > 0 {
		filesToRemove := make([]string, 0, len(toRemove))

		for _, info := range toRemove {
			filesToRemove = append(filesToRemove, info.Path)
		}

		removeOldFiles(filesToRemove, s.dir)
	}

	return nil
}

func (s *RoSnapshots) buildMissedIndicesIfNeed(ctx context.Context, logPrefix string, notifier services.DBEventNotifier, dirs datadir.Dirs, cc *chain.Config, logger log.Logger) error {
	if s.IndicesMax() >= s.SegmentsMax() {
		return nil
	}
	if !s.Cfg().ProduceE2 && s.IndicesMax() == 0 {
		return errors.New("please remove --snap.stop, erigon can't work without creating basic indices")
	}
	if !s.Cfg().ProduceE2 {
		return nil
	}
	if !s.SegmentsReady() {
		return errors.New("not all snapshot segments are available")
	}
	s.LogStat("missed-idx")

	// wait for Downloader service to download all expected snapshots
	indexWorkers := estimate.IndexSnapshot.Workers()
	if err := s.buildMissedIndices(logPrefix, ctx, dirs, cc, indexWorkers, logger); err != nil {
		return fmt.Errorf("can't build missed indices: %w", err)
	}

	if err := s.ReopenFolder(); err != nil {
		return err
	}
	s.LogStat("missed-idx:reopen")
	if notifier != nil {
		notifier.OnNewSnapshot()
	}
	return nil
}

func (s *RoSnapshots) delete(fileName string) error {
	s.dirtySegmentsLock.Lock()
	defer s.dirtySegmentsLock.Unlock()

	var err error
	var delSeg *DirtySegment
	var dirtySegments *btree.BTreeG[*DirtySegment]

	_, fName := filepath.Split(fileName)
	s.segments.Scan(func(segtype snaptype.Enum, value *segments) bool {
		findDelSeg := false
		value.DirtySegments.Walk(func(segs []*DirtySegment) bool {
			for _, sn := range segs {
				if sn.Decompressor == nil {
					continue
				}
				if sn.segType.FileName(sn.version, sn.from, sn.to) != fName {
					continue
				}
				sn.canDelete.Store(true)
				if sn.refcount.Load() == 0 {
					sn.closeAndRemoveFiles()
				}
				delSeg = sn
				dirtySegments = value.DirtySegments
				findDelSeg = false
				return true
			}
			return true
		})
		return !findDelSeg
	})
	dirtySegments.Delete(delSeg)
	return err
}

// prune visible segments
func (s *RoSnapshots) Delete(fileName string) error {
	if s == nil {
		return nil
	}
	defer s.recalcVisibleFiles()
	if err := s.delete(fileName); err != nil {
		return fmt.Errorf("can't delete file: %w", err)
	}
	return nil
}

func (s *RoSnapshots) buildMissedIndices(logPrefix string, ctx context.Context, dirs datadir.Dirs, chainConfig *chain.Config, workers int, logger log.Logger) error {
	if s == nil {
		return nil
	}

	if _, err := snaptype.ReadAndCreateSaltIfNeeded(dirs.Snap); err != nil {
		return err
	}

	dir, tmpDir := dirs.Snap, dirs.Tmp
	//log.Log(lvl, "[snapshots] Build indices", "from", min)

	ps := background.NewProgressSet()
	startIndexingTime := time.Now()

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	finish := make(chan struct{})

	go func() {
		for {
			select {
			case <-logEvery.C:
				var m runtime.MemStats
				dbg.ReadMemStats(&m)
				sendDiagnostics(startIndexingTime, ps.DiagnosticsData(), m.Alloc, m.Sys)
				logger.Info(fmt.Sprintf("[%s] Indexing", logPrefix), "progress", ps.String(), "total-indexing-time", time.Since(startIndexingTime).Round(time.Second).String(), "alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
			case <-finish:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	var fmu sync.Mutex
	failedIndexes := make(map[string]error, 0)

	s.segments.Scan(func(segtype snaptype.Enum, value *segments) bool {
		value.DirtySegments.Walk(func(segs []*DirtySegment) bool {
			for _, segment := range segs {
				info := segment.FileInfo(dir)

				if segtype.HasIndexFiles(info, logger) {
					continue
				}

				segment.closeIdx()

				g.Go(func() error {
					p := &background.Progress{}
					ps.Add(p)
					defer notifySegmentIndexingFinished(info.Name())
					defer ps.Delete(p)
					if err := segtype.BuildIndexes(gCtx, info, chainConfig, tmpDir, p, log.LvlInfo, logger); err != nil {
						// unsuccessful indexing should allow other indexing to finish
						fmu.Lock()
						failedIndexes[info.Name()] = err
						fmu.Unlock()
					}
					return nil
				})
			}
			return true
		})
		return true
	})

	var ie error

	go func() {
		defer close(finish)
		g.Wait()

		fmu.Lock()
		for fname, err := range failedIndexes {
			logger.Error(fmt.Sprintf("[%s] Indexing failed", logPrefix), "file", fname, "error", err)
			ie = fmt.Errorf("%s: %w", fname, err) // report the last one anyway
		}
		fmu.Unlock()
	}()

	// Block main thread
	select {
	case <-finish:
		if err := g.Wait(); err != nil {
			return err
		}
		return ie
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *RoSnapshots) PrintDebug() {
	v := s.View()
	defer v.Close()
	s.segments.Scan(func(key snaptype.Enum, value *segments) bool {
		fmt.Println("    == [dbg] Snapshots,", key.String())
		printDebug := func(sn *DirtySegment) {
			args := make([]any, 0, len(sn.Type().Indexes())+1)
			args = append(args, sn.from)
			for _, index := range sn.Type().Indexes() {
				args = append(args, sn.Index(index) != nil)
			}
			fmt.Println(args...)
		}
		value.DirtySegments.Scan(func(sn *DirtySegment) bool {
			printDebug(sn)
			return true
		})
		return true
	})
}

func (s *RoSnapshots) AddSnapshotsToSilkworm(silkwormInstance *silkworm.Silkworm) error {
	v := s.View()
	defer v.Close()

	s.visibleSegmentsLock.RLock()
	defer s.visibleSegmentsLock.RUnlock()

	mappedHeaderSnapshots := make([]*silkworm.MappedHeaderSnapshot, 0)
	if headers, ok := v.VisibleSegments.Get(coresnaptype.Enums.Headers); ok {
		for _, headerSegment := range headers.VisibleSegments {
			mappedHeaderSnapshots = append(mappedHeaderSnapshots, headerSegment.src.mappedHeaderSnapshot())
		}
	}

	mappedBodySnapshots := make([]*silkworm.MappedBodySnapshot, 0)
	if bodies, ok := v.VisibleSegments.Get(coresnaptype.Enums.Bodies); ok {
		for _, bodySegment := range bodies.VisibleSegments {
			mappedBodySnapshots = append(mappedBodySnapshots, bodySegment.src.mappedBodySnapshot())
		}
		return nil

	}

	mappedTxnSnapshots := make([]*silkworm.MappedTxnSnapshot, 0)
	if txs, ok := v.VisibleSegments.Get(coresnaptype.Enums.Transactions); ok {
		for _, txnSegment := range txs.VisibleSegments {
			mappedTxnSnapshots = append(mappedTxnSnapshots, txnSegment.src.mappedTxnSnapshot())
		}
	}

	if len(mappedHeaderSnapshots) != len(mappedBodySnapshots) || len(mappedBodySnapshots) != len(mappedTxnSnapshots) {
		return errors.New("addSnapshots: the number of headers/bodies/txs snapshots must be the same")
	}

	for i := 0; i < len(mappedHeaderSnapshots); i++ {
		mappedSnapshot := &silkworm.MappedChainSnapshot{
			Headers: mappedHeaderSnapshots[i],
			Bodies:  mappedBodySnapshots[i],
			Txs:     mappedTxnSnapshots[i],
		}
		err := silkwormInstance.AddSnapshot(mappedSnapshot)
		if err != nil {
			return err
		}
	}

	return nil
}

func buildIdx(ctx context.Context, sn snaptype.FileInfo, chainConfig *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) error {
	//log.Info("[snapshots] build idx", "file", sn.Name())
	if err := sn.Type.BuildIndexes(ctx, sn, chainConfig, tmpDir, p, lvl, logger); err != nil {
		return fmt.Errorf("buildIdx: %s: %s", sn.Type, err)
	}
	//log.Info("[snapshots] finish build idx", "file", fName)
	return nil
}

func notifySegmentIndexingFinished(name string) {
	dts := []diagnostics.SnapshotSegmentIndexingStatistics{
		{
			SegmentName: name,
			Percent:     100,
			Alloc:       0,
			Sys:         0,
		},
	}
	diagnostics.Send(diagnostics.SnapshotIndexingStatistics{
		Segments:    dts,
		TimeElapsed: -1,
	})
}

func sendDiagnostics(startIndexingTime time.Time, indexPercent map[string]int, alloc uint64, sys uint64) {
	segmentsStats := make([]diagnostics.SnapshotSegmentIndexingStatistics, 0, len(indexPercent))
	for k, v := range indexPercent {
		segmentsStats = append(segmentsStats, diagnostics.SnapshotSegmentIndexingStatistics{
			SegmentName: k,
			Percent:     v,
			Alloc:       alloc,
			Sys:         sys,
		})
	}
	diagnostics.Send(diagnostics.SnapshotIndexingStatistics{
		Segments:    segmentsStats,
		TimeElapsed: time.Since(startIndexingTime).Round(time.Second).Seconds(),
	})
}

func typeOfSegmentsMustExist(dir string, in []snaptype.FileInfo, types []snaptype.Type) (res []snaptype.FileInfo) {
MainLoop:
	for _, f := range in {
		if f.From == f.To {
			continue
		}
		for _, t := range types {
			p := filepath.Join(dir, snaptype.SegmentFileName(f.Version, f.From, f.To, t.Enum()))
			exists, err := dir2.FileExist(p)
			if err != nil {
				log.Debug("[snapshots] FileExist error", "err", err, "path", p)
				continue MainLoop
			}
			if !exists {
				continue MainLoop
			}
			res = append(res, f)
		}
	}
	return res
}

func SegmentsCaplin(dir string, minBlock uint64) (res []snaptype.FileInfo, missingSnapshots []Range, err error) {
	list, err := snaptype.Segments(dir)
	if err != nil {
		return nil, missingSnapshots, err
	}

	{
		var l, lSidecars []snaptype.FileInfo
		var m []Range
		for _, f := range list {
			if f.Type.Enum() != snaptype.CaplinEnums.BeaconBlocks && f.Type.Enum() != snaptype.CaplinEnums.BlobSidecars {
				continue
			}
			if f.Type.Enum() == snaptype.CaplinEnums.BlobSidecars {
				lSidecars = append(lSidecars, f) // blobs are an exception
				continue
			}
			l = append(l, f)
		}
		l, m = noGaps(noOverlaps(l))
		if len(m) > 0 {
			lst := m[len(m)-1]
			log.Debug("[snapshots] see gap", "type", snaptype.CaplinEnums.BeaconBlocks, "from", lst.from)
		}
		res = append(res, l...)
		res = append(res, lSidecars...)
		missingSnapshots = append(missingSnapshots, m...)
	}
	return res, missingSnapshots, nil
}

func Segments(dir string, minBlock uint64) (res []snaptype.FileInfo, missingSnapshots []Range, err error) {
	return typedSegments(dir, minBlock, coresnaptype.BlockSnapshotTypes, true)
}

func typedSegments(dir string, minBlock uint64, types []snaptype.Type, allowGaps bool) (res []snaptype.FileInfo, missingSnapshots []Range, err error) {
	segmentsTypeCheck := func(dir string, in []snaptype.FileInfo) (res []snaptype.FileInfo) {
		return typeOfSegmentsMustExist(dir, in, types)
	}

	list, err := snaptype.Segments(dir)

	if err != nil {
		return nil, missingSnapshots, err
	}

	for _, segType := range types {
		{
			var l []snaptype.FileInfo
			var m []Range
			for _, f := range list {
				if f.Type.Enum() != segType.Enum() {
					continue
				}
				l = append(l, f)
			}

			if allowGaps {
				l = noOverlaps(segmentsTypeCheck(dir, l))
			} else {
				l, m = noGaps(noOverlaps(segmentsTypeCheck(dir, l)))
			}
			if len(m) > 0 {
				lst := m[len(m)-1]
				log.Debug("[snapshots] see gap", "type", segType, "from", lst.from)
			}
			res = append(res, l...)
			if len(m) > 0 {
				lst := m[len(m)-1]
				log.Debug("[snapshots] see gap", "type", segType, "from", lst.from)
			}

			missingSnapshots = append(missingSnapshots, m...)
		}
	}
	return res, missingSnapshots, nil
}

func chooseSegmentEnd(from, to uint64, snapType snaptype.Enum, chainConfig *chain.Config) uint64 {
	var chainName string

	if chainConfig != nil {
		chainName = chainConfig.ChainName
	}
	blocksPerFile := snapcfg.MergeLimitFromCfg(snapcfg.KnownCfg(chainName), snapType, from)

	next := (from/blocksPerFile + 1) * blocksPerFile
	to = min(next, to)

	if to < snaptype.Erigon2MinSegmentSize {
		return to
	}

	return to - (to % snaptype.Erigon2MinSegmentSize) // round down to the nearest 1k
}

type BlockRetire struct {
	maxScheduledBlock atomic.Uint64
	working           atomic.Bool

	// shared semaphore with AggregatorV3 to allow only one type of snapshot building at a time
	snBuildAllowed *semaphore.Weighted

	workers int
	tmpDir  string
	db      kv.RoDB

	notifier    services.DBEventNotifier
	logger      log.Logger
	blockReader services.FullBlockReader
	blockWriter *blockio.BlockWriter
	dirs        datadir.Dirs
	chainConfig *chain.Config
}

func NewBlockRetire(
	compressWorkers int,
	dirs datadir.Dirs,
	blockReader services.FullBlockReader,
	blockWriter *blockio.BlockWriter,
	db kv.RoDB,
	chainConfig *chain.Config,
	notifier services.DBEventNotifier,
	snBuildAllowed *semaphore.Weighted,
	logger log.Logger,
) *BlockRetire {
	return &BlockRetire{
		workers:        compressWorkers,
		tmpDir:         dirs.Tmp,
		dirs:           dirs,
		blockReader:    blockReader,
		blockWriter:    blockWriter,
		db:             db,
		snBuildAllowed: snBuildAllowed,
		chainConfig:    chainConfig,
		notifier:       notifier,
		logger:         logger,
	}
}

func (br *BlockRetire) SetWorkers(workers int) { br.workers = workers }
func (br *BlockRetire) GetWorkers() int        { return br.workers }

func (br *BlockRetire) IO() (services.FullBlockReader, *blockio.BlockWriter) {
	return br.blockReader, br.blockWriter
}

func (br *BlockRetire) Writer() *RoSnapshots { return br.blockReader.Snapshots().(*RoSnapshots) }

func (br *BlockRetire) snapshots() *RoSnapshots { return br.blockReader.Snapshots().(*RoSnapshots) }

func (br *BlockRetire) borSnapshots() *BorRoSnapshots {
	return br.blockReader.BorSnapshots().(*BorRoSnapshots)
}

func CanRetire(curBlockNum uint64, blocksInSnapshots uint64, snapType snaptype.Enum, chainConfig *chain.Config) (blockFrom, blockTo uint64, can bool) {
	var keep uint64 = 1024 //TODO: we will increase it to params.FullImmutabilityThreshold after some db optimizations
	if curBlockNum <= keep {
		return
	}
	blockFrom = blocksInSnapshots + 1
	return canRetire(blockFrom, curBlockNum-keep, snapType, chainConfig)
}

func canRetire(from, to uint64, snapType snaptype.Enum, chainConfig *chain.Config) (blockFrom, blockTo uint64, can bool) {
	if to <= from {
		return
	}
	blockFrom = (from / 1_000) * 1_000
	roundedTo1K := (to / 1_000) * 1_000
	var maxJump uint64 = 1_000

	var chainName string

	if chainConfig != nil {
		chainName = chainConfig.ChainName
	}

	mergeLimit := snapcfg.MergeLimitFromCfg(snapcfg.KnownCfg(chainName), snapType, blockFrom)

	if blockFrom%mergeLimit == 0 {
		maxJump = mergeLimit
	} else if blockFrom%100_000 == 0 {
		maxJump = 100_000
	} else if blockFrom%10_000 == 0 {
		maxJump = 10_000
	}
	//roundedTo1K := (to / 1_000) * 1_000
	jump := min(maxJump, roundedTo1K-blockFrom)
	switch { // only next segment sizes are allowed
	case jump >= mergeLimit:
		blockTo = blockFrom + mergeLimit
	case jump >= 100_000:
		blockTo = blockFrom + 100_000
	case jump >= 10_000:
		blockTo = blockFrom + 10_000
	case jump >= 1_000:
		blockTo = blockFrom + 1_000
	default:
		blockTo = blockFrom
	}
	return blockFrom, blockTo, blockTo-blockFrom >= 1_000
}

func CanDeleteTo(curBlockNum uint64, blocksInSnapshots uint64) (blockTo uint64) {
	if blocksInSnapshots == 0 {
		return 0
	}

	var keep uint64 = 1024 // params.FullImmutabilityThreshold //TODO: we will increase this value after db optimizations - about on-chain-tip prune speed
	if curBlockNum+999 < keep {
		// To prevent overflow of uint64 below
		return blocksInSnapshots + 1
	}
	hardLimit := (curBlockNum/1_000)*1_000 - keep
	return min(hardLimit, blocksInSnapshots+1)
}

func (br *BlockRetire) dbHasEnoughDataForBlocksRetire(ctx context.Context) (bool, error) {
	// pre-check if db has enough data
	var haveGap bool
	if err := br.db.View(ctx, func(tx kv.Tx) error {
		firstInDB, ok, err := rawdb.ReadFirstNonGenesisHeaderNumber(tx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		lastInFiles := br.snapshots().SegmentsMax() + 1
		haveGap = lastInFiles < firstInDB
		if haveGap {
			log.Debug("[snapshots] not enough blocks in db to create snapshots", "lastInFiles", lastInFiles, " firstBlockInDB", firstInDB, "recommendations", "it's ok to ignore this message. can fix by: downloading more files `rm datadir/snapshots/prohibit_new_downloads.lock datdir/snapshots/snapshots-lock.json`, or downloading old blocks to db `integration stage_headers --reset`")
		}
		return nil
	}); err != nil {
		return false, err
	}
	return !haveGap, nil
}

func (br *BlockRetire) retireBlocks(ctx context.Context, minBlockNum uint64, maxBlockNum uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []services.DownloadRequest) error, onDelete func(l []string) error) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	notifier, logger, blockReader, tmpDir, db, workers := br.notifier, br.logger, br.blockReader, br.tmpDir, br.db, br.workers
	snapshots := br.snapshots()

	blockFrom, blockTo, ok := CanRetire(maxBlockNum, minBlockNum, snaptype.Unknown, br.chainConfig)

	if ok {
		if has, err := br.dbHasEnoughDataForBlocksRetire(ctx); err != nil {
			return false, err
		} else if !has {
			return false, nil
		}
		logger.Log(lvl, "[snapshots] Retire Blocks", "range",
			fmt.Sprintf("%s-%s", common2.PrettyCounter(blockFrom), common2.PrettyCounter(blockTo)))
		// in future we will do it in background
		if err := DumpBlocks(ctx, blockFrom, blockTo, br.chainConfig, tmpDir, snapshots.Dir(), db, workers, lvl, logger, blockReader); err != nil {
			return ok, fmt.Errorf("DumpBlocks: %w", err)
		}

		if err := snapshots.ReopenFolder(); err != nil {
			return ok, fmt.Errorf("reopen: %w", err)
		}
		snapshots.LogStat("blocks:retire")
		if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
			notifier.OnNewSnapshot()
		}
	}

	merger := NewMerger(tmpDir, workers, lvl, db, br.chainConfig, logger)
	rangesToMerge := merger.FindMergeRanges(snapshots.Ranges(), snapshots.BlocksAvailable())
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
	err := merger.Merge(ctx, snapshots, snapshots.Types(), rangesToMerge, snapshots.Dir(), true /* doIndex */, onMerge, onDelete)
	if err != nil {
		return ok, err
	}

	// remove old garbage files
	if err := snapshots.removeOverlapsAfterMerge(); err != nil {
		return false, err
	}
	return ok, nil
}

var ErrNothingToPrune = errors.New("nothing to prune")

func (br *BlockRetire) PruneAncientBlocks(tx kv.RwTx, limit int) (deleted int, err error) {
	if br.blockReader.FreezingCfg().KeepBlocks {
		return deleted, nil
	}
	currentProgress, err := stages.GetStageProgress(tx, stages.Senders)
	if err != nil {
		return deleted, err
	}
	if canDeleteTo := CanDeleteTo(currentProgress, br.blockReader.FrozenBlocks()); canDeleteTo > 0 {
		br.logger.Debug("[snapshots] Prune Blocks", "to", canDeleteTo, "limit", limit)
		deletedBlocks, err := br.blockWriter.PruneBlocks(context.Background(), tx, canDeleteTo, limit)
		if err != nil {
			return deleted, err
		}
		deleted += deletedBlocks
	}

	if br.chainConfig.Bor != nil {
		if canDeleteTo := CanDeleteTo(currentProgress, br.blockReader.FrozenBorBlocks()); canDeleteTo > 0 {
			br.logger.Debug("[snapshots] Prune Bor Blocks", "to", canDeleteTo, "limit", limit)
			deletedBorBlocks, err := br.blockWriter.PruneBorBlocks(context.Background(), tx, canDeleteTo, limit,
				func(block uint64) uint64 { return uint64(heimdall.SpanIdAt(block)) })
			if err != nil {
				return deleted, err
			}
			deleted += deletedBorBlocks
		}

	}

	return deleted, nil
}

func (br *BlockRetire) RetireBlocksInBackground(ctx context.Context, minBlockNum, maxBlockNum uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []services.DownloadRequest) error, onDeleteSnapshots func(l []string) error, onFinishRetire func() error) {
	if maxBlockNum > br.maxScheduledBlock.Load() {
		br.maxScheduledBlock.Store(maxBlockNum)
	}

	if !br.working.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer br.working.Store(false)

		if br.snBuildAllowed != nil {
			//we are inside own goroutine - it's fine to block here
			if err := br.snBuildAllowed.Acquire(ctx, 1); err != nil {
				br.logger.Warn("[snapshots] retire blocks", "err", err)
				return
			}
			defer br.snBuildAllowed.Release(1)
		}

		err := br.RetireBlocks(ctx, minBlockNum, maxBlockNum, lvl, seedNewSnapshots, onDeleteSnapshots, onFinishRetire)
		if err != nil {
			br.logger.Warn("[snapshots] retire blocks", "err", err)
			return
		}
	}()
}

func (br *BlockRetire) RetireBlocks(ctx context.Context, requestedMinBlockNum uint64, requestedMaxBlockNum uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []services.DownloadRequest) error, onDeleteSnapshots func(l []string) error, onFinish func() error) error {
	if requestedMaxBlockNum > br.maxScheduledBlock.Load() {
		br.maxScheduledBlock.Store(requestedMaxBlockNum)
	}
	includeBor := br.chainConfig.Bor != nil

	if err := br.BuildMissedIndicesIfNeed(ctx, "RetireBlocks", br.notifier, br.chainConfig); err != nil {
		return err
	}

	if includeBor {
		// "bor snaps" can be behind "block snaps", it's ok:
		//      - for example because of `kill -9` in the middle of merge
		//      - or if manually delete bor files (for re-generation)
		var err error
		var okBor bool
		for {
			minBlockNum := max(br.blockReader.FrozenBlocks(), requestedMinBlockNum)
			okBor, err = br.retireBorBlocks(ctx, br.blockReader.FrozenBorBlocks(), minBlockNum, lvl, seedNewSnapshots, onDeleteSnapshots)
			if err != nil {
				return err
			}
			if !okBor {
				break
			}
		}
	}

	var err error
	for {
		var ok, okBor bool
		minBlockNum := max(br.blockReader.FrozenBlocks(), requestedMinBlockNum)
		maxBlockNum := br.maxScheduledBlock.Load()
		ok, err = br.retireBlocks(ctx, minBlockNum, maxBlockNum, lvl, seedNewSnapshots, onDeleteSnapshots)
		if err != nil {
			return err
		}

		if includeBor {
			minBorBlockNum := max(br.blockReader.FrozenBorBlocks(), requestedMinBlockNum)
			okBor, err = br.retireBorBlocks(ctx, minBorBlockNum, maxBlockNum, lvl, seedNewSnapshots, onDeleteSnapshots)
			if err != nil {
				return err
			}
		}
		if onFinish != nil {
			if err := onFinish(); err != nil {
				return err
			}
		}

		if !(ok || okBor) {
			break
		}
	}
	return nil
}

func (br *BlockRetire) BuildMissedIndicesIfNeed(ctx context.Context, logPrefix string, notifier services.DBEventNotifier, cc *chain.Config) error {
	if err := br.snapshots().buildMissedIndicesIfNeed(ctx, logPrefix, notifier, br.dirs, cc, br.logger); err != nil {
		return err
	}

	if cc.Bor != nil {
		if err := br.borSnapshots().RoSnapshots.buildMissedIndicesIfNeed(ctx, logPrefix, notifier, br.dirs, cc, br.logger); err != nil {
			return err
		}
	}

	return nil
}

func DumpBlocks(ctx context.Context, blockFrom, blockTo uint64, chainConfig *chain.Config, tmpDir, snapDir string, chainDB kv.RoDB, workers int, lvl log.Lvl, logger log.Logger, blockReader services.FullBlockReader) error {
	firstTxNum := blockReader.FirstTxnNumNotInSnapshots()
	for i := blockFrom; i < blockTo; i = chooseSegmentEnd(i, blockTo, coresnaptype.Enums.Headers, chainConfig) {
		lastTxNum, err := dumpBlocksRange(ctx, i, chooseSegmentEnd(i, blockTo, coresnaptype.Enums.Headers, chainConfig), tmpDir, snapDir, firstTxNum, chainDB, chainConfig, workers, lvl, logger)
		if err != nil {
			return err
		}
		firstTxNum = lastTxNum + 1
	}
	return nil
}

func dumpBlocksRange(ctx context.Context, blockFrom, blockTo uint64, tmpDir, snapDir string, firstTxNum uint64, chainDB kv.RoDB, chainConfig *chain.Config, workers int, lvl log.Lvl, logger log.Logger) (lastTxNum uint64, err error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	if _, err = dumpRange(ctx, coresnaptype.Headers.FileInfo(snapDir, blockFrom, blockTo),
		DumpHeaders, nil, chainDB, chainConfig, tmpDir, workers, lvl, logger); err != nil {
		return 0, err
	}

	if lastTxNum, err = dumpRange(ctx, coresnaptype.Bodies.FileInfo(snapDir, blockFrom, blockTo),
		DumpBodies, func(context.Context) uint64 { return firstTxNum }, chainDB, chainConfig, tmpDir, workers, lvl, logger); err != nil {
		return lastTxNum, err
	}

	if _, err = dumpRange(ctx, coresnaptype.Transactions.FileInfo(snapDir, blockFrom, blockTo),
		DumpTxs, func(context.Context) uint64 { return firstTxNum }, chainDB, chainConfig, tmpDir, workers, lvl, logger); err != nil {
		return lastTxNum, err
	}

	return lastTxNum, nil
}

type firstKeyGetter func(ctx context.Context) uint64
type dumpFunc func(ctx context.Context, db kv.RoDB, chainConfig *chain.Config, blockFrom, blockTo uint64, firstKey firstKeyGetter, collecter func(v []byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error)

func dumpRange(ctx context.Context, f snaptype.FileInfo, dumper dumpFunc, firstKey firstKeyGetter, chainDB kv.RoDB, chainConfig *chain.Config, tmpDir string, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
	var lastKeyValue uint64

	compressCfg := seg.DefaultCfg
	compressCfg.Workers = workers
	sn, err := seg.NewCompressor(ctx, "Snapshot "+f.Type.Name(), f.Path, tmpDir, compressCfg, log.LvlTrace, logger)
	if err != nil {
		return lastKeyValue, err
	}
	defer sn.Close()

	// E3 need to keep db smaller: earlier retire -> earlier prune.
	// Means:
	//  - build must be fast
	//  - merge can be slow and expensive
	noCompress := (f.To - f.From) < (snaptype.Erigon2MergeLimit - 1)

	lastKeyValue, err = dumper(ctx, chainDB, chainConfig, f.From, f.To, firstKey, func(v []byte) error {
		if noCompress {
			return sn.AddUncompressedWord(v)
		}
		return sn.AddWord(v)
	}, workers, lvl, logger)

	if err != nil {
		return lastKeyValue, fmt.Errorf("DumpBodies: %w", err)
	}

	ext := filepath.Ext(f.Name())
	logger.Log(lvl, "[snapshots] Compression start", "file", f.Name()[:len(f.Name())-len(ext)], "workers", sn.WorkersAmount())

	if err := sn.Compress(); err != nil {
		return lastKeyValue, fmt.Errorf("compress: %w", err)
	}

	p := &background.Progress{}

	if err := f.Type.BuildIndexes(ctx, f, chainConfig, tmpDir, p, lvl, logger); err != nil {
		return lastKeyValue, err
	}

	return lastKeyValue, nil
}

var bufPool = sync.Pool{
	New: func() any {
		bytes := [16 * 4096]byte{}
		return &bytes
	},
}

// DumpTxs - [from, to)
// Format: hash[0]_1byte + sender_address_2bytes + txnRlp
func DumpTxs(ctx context.Context, db kv.RoDB, chainConfig *chain.Config, blockFrom, blockTo uint64, _ firstKeyGetter, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (lastTx uint64, err error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	warmupCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	chainID, _ := uint256.FromBig(chainConfig.ChainID)

	numBuf := make([]byte, 8)

	parse := func(ctx *types2.TxParseContext, v, valueBuf []byte, senders []common2.Address, j int) ([]byte, error) {
		var sender [20]byte
		slot := types2.TxSlot{}

		if _, err := ctx.ParseTransaction(v, 0, &slot, sender[:], false /* hasEnvelope */, false /* wrappedWithBlobs */, nil); err != nil {
			return valueBuf, err
		}
		if len(senders) > 0 {
			sender = senders[j]
		}

		valueBuf = valueBuf[:0]
		valueBuf = append(valueBuf, slot.IDHash[:1]...)
		valueBuf = append(valueBuf, sender[:]...)
		valueBuf = append(valueBuf, v...)
		return valueBuf, nil
	}

	addSystemTx := func(ctx *types2.TxParseContext, tx kv.Tx, txId types.BaseTxnID) error {
		binary.BigEndian.PutUint64(numBuf, txId.U64())
		tv, err := tx.GetOne(kv.EthTx, numBuf)
		if err != nil {
			return err
		}
		if tv == nil {
			if err := collect(nil); err != nil {
				return err
			}
			return nil
		}

		ctx.WithSender(false)

		valueBuf := bufPool.Get().(*[16 * 4096]byte)
		defer bufPool.Put(valueBuf)

		parsed, err := parse(ctx, tv, valueBuf[:], nil, 0)
		if err != nil {
			return err
		}
		if err := collect(parsed); err != nil {
			return err
		}
		return nil
	}

	doWarmup, warmupTxs, warmupSenders := blockTo-blockFrom >= 100_000 && workers > 4, &atomic.Bool{}, &atomic.Bool{}
	from := hexutility.EncodeTs(blockFrom)
	if err := kv.BigChunks(db, kv.HeaderCanonical, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= blockTo { // [from, to)
			return false, nil
		}

		h := common2.BytesToHash(v)
		dataRLP := rawdb.ReadStorageBodyRLP(tx, h, blockNum)
		if dataRLP == nil {
			return false, fmt.Errorf("body not found: %d, %x", blockNum, h)
		}
		var body types.BodyForStorage
		if e := rlp.DecodeBytes(dataRLP, &body); e != nil {
			return false, e
		}
		if body.TxCount == 0 {
			return true, nil
		}

		if doWarmup && !warmupSenders.Load() && blockNum%1_000 == 0 {
			clean := kv.ReadAhead(warmupCtx, db, warmupSenders, kv.Senders, hexutility.EncodeTs(blockNum), 10_000)
			defer clean()
		}
		if doWarmup && !warmupTxs.Load() && blockNum%1_000 == 0 {
			clean := kv.ReadAhead(warmupCtx, db, warmupTxs, kv.EthTx, body.BaseTxnID.Bytes(), 100*10_000)
			defer clean()
		}
		senders, err := rawdb.ReadSenders(tx, h, blockNum)
		if err != nil {
			return false, err
		}

		workers := estimate.AlmostAllCPUs()

		if workers > 3 {
			workers = workers / 3 * 2
		}

		if workers > int(body.TxCount-2) {
			if int(body.TxCount-2) > 1 {
				workers = int(body.TxCount - 2)
			} else {
				workers = 1
			}
		}

		parsers := errgroup.Group{}
		parsers.SetLimit(workers)

		valueBufs := make([][]byte, workers)
		parseCtxs := make([]*types2.TxParseContext, workers)

		for i := 0; i < workers; i++ {
			valueBuf := bufPool.Get().(*[16 * 4096]byte)
			defer bufPool.Put(valueBuf)
			valueBufs[i] = valueBuf[:]
			parseCtxs[i] = types2.NewTxParseContext(*chainID)
		}

		if err := addSystemTx(parseCtxs[0], tx, body.BaseTxnID); err != nil {
			return false, err
		}

		binary.BigEndian.PutUint64(numBuf, body.BaseTxnID.First())

		collected := -1
		collectorLock := sync.Mutex{}
		collections := sync.NewCond(&collectorLock)

		var j int

		if err := tx.ForAmount(kv.EthTx, numBuf, body.TxCount-2, func(_, tv []byte) error {
			tx := j
			j++

			parsers.Go(func() error {
				parseCtx := parseCtxs[tx%workers]

				parseCtx.WithSender(len(senders) == 0)
				parseCtx.WithAllowPreEip2s(blockNum <= chainConfig.HomesteadBlock.Uint64())

				valueBuf, err := parse(parseCtx, tv, valueBufs[tx%workers], senders, tx)

				if err != nil {
					return fmt.Errorf("%w, block: %d", err, blockNum)
				}

				collectorLock.Lock()
				defer collectorLock.Unlock()

				for collected < tx-1 {
					collections.Wait()
				}

				// first tx byte => sender address => tx rlp
				if err := collect(valueBuf); err != nil {
					return err
				}

				collected = tx
				collections.Broadcast()

				return nil
			})

			return nil
		}); err != nil {
			return false, fmt.Errorf("ForAmount: %w", err)
		}

		if err := parsers.Wait(); err != nil {
			return false, fmt.Errorf("ForAmount parser: %w", err)
		}

		if err := addSystemTx(parseCtxs[0], tx, types.BaseTxnID(body.BaseTxnID.LastSystemTx(body.TxCount))); err != nil {
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
			logger.Log(lvl, "[snapshots] Dumping txs", "block num", blockNum,
				"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys),
			)
		default:
		}
		return true, nil
	}); err != nil {
		return 0, fmt.Errorf("BigChunks: %w", err)
	}
	return 0, nil
}

// DumpHeaders - [from, to)
func DumpHeaders(ctx context.Context, db kv.RoDB, _ *chain.Config, blockFrom, blockTo uint64, _ firstKeyGetter, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	key := make([]byte, 8+32)
	from := hexutility.EncodeTs(blockFrom)
	if err := kv.BigChunks(db, kv.HeaderCanonical, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= blockTo {
			return false, nil
		}
		copy(key, k)
		copy(key[8:], v)
		dataRLP, err := tx.GetOne(kv.Headers, key)
		if err != nil {
			return false, err
		}
		if dataRLP == nil {
			return false, fmt.Errorf("header missed in db: block_num=%d,  hash=%x", blockNum, v)
		}
		h := types.Header{}
		if err := rlp.DecodeBytes(dataRLP, &h); err != nil {
			return false, err
		}

		value := make([]byte, len(dataRLP)+1) // first_byte_of_header_hash + header_rlp
		value[0] = h.Hash()[0]
		copy(value[1:], dataRLP)
		if err := collect(value); err != nil {
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
			logger.Log(lvl, "[snapshots] Dumping headers", "block num", blockNum,
				"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys),
			)
		default:
		}
		return true, nil
	}); err != nil {
		return 0, err
	}
	return 0, nil
}

// DumpBodies - [from, to)
func DumpBodies(ctx context.Context, db kv.RoDB, _ *chain.Config, blockFrom, blockTo uint64, firstTxNum firstKeyGetter, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	blockNumByteLength := 8
	blockHashByteLength := 32
	key := make([]byte, blockNumByteLength+blockHashByteLength)
	from := hexutility.EncodeTs(blockFrom)

	lastTxNum := firstTxNum(ctx)

	if err := kv.BigChunks(db, kv.HeaderCanonical, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= blockTo {
			return false, nil
		}
		copy(key, k)
		copy(key[8:], v)

		// Important: DB does store canonical and non-canonical txs in same table. And using same body.BaseTxnID
		// But snapshots using canonical TxNum in field body.BaseTxnID
		// So, we manually calc this field here and serialize again.
		//
		// FYI: we also have other table to map canonical BlockNum->TxNum: kv.MaxTxNum
		body, err := rawdb.ReadBodyForStorageByKey(tx, key)
		if err != nil {
			return false, err
		}
		if body == nil {
			logger.Warn("body missed", "block_num", blockNum, "hash", hex.EncodeToString(v))
			return true, nil
		}
		body.BaseTxnID = types.BaseTxnID(lastTxNum)
		lastTxNum = body.BaseTxnID.LastSystemTx(body.TxCount) + 1 // +1 to set it on first systemTxn of next block

		dataRLP, err := rlp.EncodeToBytes(body)
		if err != nil {
			return false, err
		}

		if err := collect(dataRLP); err != nil {
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
			logger.Log(lvl, "[snapshots] Wrote into file", "block num", blockNum,
				"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys),
			)
		default:
		}
		return true, nil
	}); err != nil {
		return lastTxNum, err
	}

	return lastTxNum, nil
}

func ForEachHeader(ctx context.Context, s *RoSnapshots, walker func(header *types.Header) error) error {
	r := bytes.NewReader(nil)
	word := make([]byte, 0, 2*4096)

	view := s.View()
	defer view.Close()

	for _, sn := range view.Headers() {
		if err := sn.src.WithReadAhead(func() error {
			g := sn.src.MakeGetter()
			for g.HasNext() {
				word, _ = g.Next(word[:0])
				var header types.Header
				r.Reset(word[1:])
				if err := rlp.Decode(r, &header); err != nil {
					return err
				}
				if err := walker(&header); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}

type Merger struct {
	lvl             log.Lvl
	compressWorkers int
	tmpDir          string
	chainConfig     *chain.Config
	chainDB         kv.RoDB
	logger          log.Logger
	noFsync         bool // fsync is enabled by default, but tests can manually disable
}

func NewMerger(tmpDir string, compressWorkers int, lvl log.Lvl, chainDB kv.RoDB, chainConfig *chain.Config, logger log.Logger) *Merger {
	return &Merger{tmpDir: tmpDir, compressWorkers: compressWorkers, lvl: lvl, chainDB: chainDB, chainConfig: chainConfig, logger: logger}
}
func (m *Merger) DisableFsync() { m.noFsync = true }

func (m *Merger) FindMergeRanges(currentRanges []Range, maxBlockNum uint64) (toMerge []Range) {
	cfg := snapcfg.KnownCfg(m.chainConfig.ChainName)
	for i := len(currentRanges) - 1; i > 0; i-- {
		r := currentRanges[i]
		mergeLimit := snapcfg.MergeLimitFromCfg(cfg, snaptype.Unknown, r.from)
		if r.to-r.from >= mergeLimit {
			continue
		}
		for _, span := range snapcfg.MergeStepsFromCfg(cfg, snaptype.Unknown, r.from) {
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
	slices.SortFunc(toMerge, func(i, j Range) int { return cmp.Compare(i.from, j.from) })
	return toMerge
}

func (m *Merger) filesByRange(v *View, from, to uint64) (map[snaptype.Enum][]*DirtySegment, error) {
	toMerge := map[snaptype.Enum][]*DirtySegment{}
	v.VisibleSegments.Scan(func(key snaptype.Enum, value *segmentsRotx) bool {
		toMerge[key.Type().Enum()] = m.filesByRangeOfType(v, from, to, key.Type())
		return true
	})

	return toMerge, nil
}

func (m *Merger) filesByRangeOfType(view *View, from, to uint64, snapshotType snaptype.Type) (out []*DirtySegment) {
	for _, sn := range view.segments(snapshotType) {
		if sn.from < from {
			continue
		}
		if sn.to > to {
			break
		}

		out = append(out, sn.src)
	}
	return
}

func (m *Merger) mergeSubSegment(ctx context.Context, v *View, sn snaptype.FileInfo, toMerge []*DirtySegment, snapDir string, doIndex bool, onMerge func(r Range) error) (newDirtySegment *DirtySegment, err error) {
	defer func() {
		if err == nil {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("panic: %v", rec)
			}
		}
		if err != nil {
			f := sn.Path
			_ = os.Remove(f)
			_ = os.Remove(f + ".torrent")
			ext := filepath.Ext(f)
			withoutExt := f[:len(f)-len(ext)]
			_ = os.Remove(withoutExt + ".idx")
			isTxnType := strings.HasSuffix(withoutExt, coresnaptype.Transactions.Name())
			if isTxnType {
				_ = os.Remove(withoutExt + "-to-block.idx")
			}
		}
	}()
	if len(toMerge) == 0 {
		return
	}
	if newDirtySegment, err = m.merge(ctx, v, toMerge, sn, snapDir, nil); err != nil {
		err = fmt.Errorf("mergeByAppendSegments: %w", err)
		return
	}

	// new way to build index
	if doIndex {
		p := &background.Progress{}
		if err = buildIdx(ctx, sn, m.chainConfig, m.tmpDir, p, m.lvl, m.logger); err != nil {
			return
		}
		err = newDirtySegment.reopenIdx(snapDir)
		if err != nil {
			return
		}
	}

	return
}

// Merge does merge segments in given ranges
func (m *Merger) Merge(ctx context.Context, snapshots *RoSnapshots, snapTypes []snaptype.Type, mergeRanges []Range, snapDir string, doIndex bool, onMerge func(r Range) error, onDelete func(l []string) error) (err error) {
	v := snapshots.View()
	defer v.Close()

	if len(mergeRanges) == 0 {
		return nil
	}
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	in := make(map[snaptype.Enum][]*DirtySegment)
	out := make(map[snaptype.Enum][]*DirtySegment)

	for _, r := range mergeRanges {
		toMerge, err := m.filesByRange(v, r.from, r.to)
		if err != nil {
			return err
		}
		for snapType, t := range toMerge {
			if out[snapType] == nil {
				out[snapType] = make([]*DirtySegment, 0, len(t))
			}
			out[snapType] = append(out[snapType], t...)
		}

		for _, t := range snapTypes {
			newDirtySegment, err := m.mergeSubSegment(ctx, v, t.FileInfo(snapDir, r.from, r.to), toMerge[t.Enum()], snapDir, doIndex, onMerge)
			if err != nil {
				return err
			}
			if in[t.Enum()] == nil {
				in[t.Enum()] = make([]*DirtySegment, 0, len(toMerge[t.Enum()]))
			}
			in[t.Enum()] = append(in[t.Enum()], newDirtySegment)
		}

		snapshots.LogStat("merge")

		if onMerge != nil {
			if err := onMerge(r); err != nil {
				return err
			}
		}

		for _, t := range snapTypes {
			if len(toMerge[t.Enum()]) == 0 {
				continue
			}
			toMergeFilePaths := make([]string, 0, len(toMerge[t.Enum()]))
			for _, f := range toMerge[t.Enum()] {
				toMergeFilePaths = append(toMergeFilePaths, f.FilePath())
			}
			if onDelete != nil {
				if err := onDelete(toMergeFilePaths); err != nil {
					return err
				}
			}
		}
	}
	m.integrateMergedDirtyFiles(snapshots, in, out)
	m.logger.Log(m.lvl, "[snapshots] Merge done", "from", mergeRanges[0].from, "to", mergeRanges[0].to)
	return nil
}

func (m *Merger) integrateMergedDirtyFiles(snapshots *RoSnapshots, in, out map[snaptype.Enum][]*DirtySegment) {
	defer snapshots.recalcVisibleFiles()

	snapshots.dirtySegmentsLock.Lock()
	defer snapshots.dirtySegmentsLock.Unlock()

	// add new segments
	for enum, newSegs := range in {
		segs, b := snapshots.segments.Get(enum)
		if !b {
			m.logger.Error("[snapshots] Merge: segment not found", "enum", enum)
			continue
		}
		dirtySegments := segs.DirtySegments
		for _, newSeg := range newSegs {
			dirtySegments.Set(newSeg)
			if newSeg.frozen {
				dirtySegments.Walk(func(items []*DirtySegment) bool {
					for _, item := range items {
						if item.frozen || item.to > newSeg.to {
							continue
						}
						if out[enum] == nil {
							out[enum] = make([]*DirtySegment, 0, 1)
						}
						out[enum] = append(out[enum], item)
					}
					return true
				})
			}
		}
	}

	// delete old sub segments
	for enum, delSegs := range out {
		segs, b := snapshots.segments.Get(enum)
		if !b {
			m.logger.Error("[snapshots] Merge: segment not found", "enum", enum)
			continue
		}
		dirtySegments := segs.DirtySegments
		for _, delSeg := range delSegs {
			dirtySegments.Delete(delSeg)
			delSeg.canDelete.Store(true)
			if delSeg.refcount.Load() == 0 {
				delSeg.closeAndRemoveFiles()
			}
		}
	}
}

func (m *Merger) merge(ctx context.Context, v *View, toMerge []*DirtySegment, targetFile snaptype.FileInfo, snapDir string, logEvery *time.Ticker) (*DirtySegment, error) {
	var word = make([]byte, 0, 4096)
	var expectedTotal int
	cList := make([]*seg.Decompressor, len(toMerge))
	for i, cFile := range toMerge {
		d, err := seg.NewDecompressor(cFile.FilePath())
		if err != nil {
			return nil, err
		}
		defer d.Close()
		cList[i] = d
		expectedTotal += d.Count()
	}

	compresCfg := seg.DefaultCfg
	compresCfg.Workers = m.compressWorkers
	f, err := seg.NewCompressor(ctx, "Snapshots merge", targetFile.Path, m.tmpDir, compresCfg, log.LvlTrace, m.logger)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if m.noFsync {
		f.DisableFsync()
	}
	m.logger.Debug("[snapshots] merge", "file", targetFile.Name())

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
			return nil, err
		}
	}
	if f.Count() != expectedTotal {
		return nil, fmt.Errorf("unexpected amount after segments merge. got: %d, expected: %d", f.Count(), expectedTotal)
	}
	if err = f.Compress(); err != nil {
		return nil, err
	}
	sn := &DirtySegment{segType: targetFile.Type, version: targetFile.Version, Range: Range{targetFile.From, targetFile.To},
		frozen: snapcfg.Seedable(v.s.cfg.ChainName, targetFile)}

	err = sn.reopenSeg(snapDir)
	if err != nil {
		return nil, err
	}
	return sn, nil
}

func removeOldFiles(toDel []string, snapDir string) {
	for _, f := range toDel {
		_ = os.Remove(f)
		_ = os.Remove(f + ".torrent")
		ext := filepath.Ext(f)
		withoutExt := f[:len(f)-len(ext)]
		_ = os.Remove(withoutExt + ".idx")
		isTxnType := strings.HasSuffix(withoutExt, coresnaptype.Transactions.Name())
		if isTxnType {
			_ = os.Remove(withoutExt + "-to-block.idx")
		}
	}
	tmpFiles, err := snaptype.TmpFiles(snapDir)
	if err != nil {
		return
	}
	for _, f := range tmpFiles {
		_ = os.Remove(f)
	}
}

type View struct {
	s               *RoSnapshots
	VisibleSegments btree.Map[snaptype.Enum, *segmentsRotx]
	baseSegType     snaptype.Type
}

func (s *RoSnapshots) View() *View {
	s.visibleSegmentsLock.RLock()
	defer s.visibleSegmentsLock.RUnlock()

	var sgs btree.Map[snaptype.Enum, *segmentsRotx]
	s.segments.Scan(func(segtype snaptype.Enum, value *segments) bool {
		sgs.Set(segtype, value.BeginRotx())
		return true
	})
	return &View{s: s, VisibleSegments: sgs, baseSegType: coresnaptype.Transactions} // Transactions is the last segment to be processed, so it's the most reliable.
}

func (v *View) Close() {
	if v == nil || v.s == nil {
		return
	}
	v.s = nil

	v.VisibleSegments.Scan(func(segtype snaptype.Enum, value *segmentsRotx) bool {
		value.Close()
		return true
	})
}

var noop = func() {}

func (s *RoSnapshots) ViewType(t snaptype.Type) *segmentsRotx {
	s.visibleSegmentsLock.RLock()
	defer s.visibleSegmentsLock.RUnlock()

	seg, ok := s.segments.Get(t.Enum())
	if !ok {
		return nil
	}
	return seg.BeginRotx()
}

func (s *RoSnapshots) ViewSingleFile(t snaptype.Type, blockNum uint64) (segment *VisibleSegment, ok bool, close func()) {
	s.visibleSegmentsLock.RLock()
	defer s.visibleSegmentsLock.RUnlock()

	segs, ok := s.segments.Get(t.Enum())
	if !ok {
		return nil, false, noop
	}

	if blockNum > segs.maxVisibleBlock.Load() {
		return nil, false, noop
	}

	segmentRotx := segs.BeginRotx()
	for _, seg := range segmentRotx.VisibleSegments {
		if !(blockNum >= seg.from && blockNum < seg.to) {
			continue
		}
		return seg, true, func() { segmentRotx.Close() }
	}
	segmentRotx.Close()
	return nil, false, noop
}

func (v *View) segments(t snaptype.Type) []*VisibleSegment {
	if s, ok := v.s.segments.Get(t.Enum()); ok {
		return s.VisibleSegments
	}
	return nil
}

func (v *View) Headers() []*VisibleSegment { return v.segments(coresnaptype.Headers) }
func (v *View) Bodies() []*VisibleSegment  { return v.segments(coresnaptype.Bodies) }
func (v *View) Txs() []*VisibleSegment     { return v.segments(coresnaptype.Transactions) }

func (v *View) Segment(t snaptype.Type, blockNum uint64) (*VisibleSegment, bool) {
	if s, ok := v.s.segments.Get(t.Enum()); ok {
		if blockNum > s.maxVisibleBlock.Load() {
			return nil, false
		}
		for _, seg := range s.VisibleSegments {
			if !(blockNum >= seg.from && blockNum < seg.to) {
				continue
			}
			return seg, true
		}
	}
	return nil, false
}

func (v *View) Ranges() (ranges []Range) {
	for _, sn := range v.segments(v.baseSegType) {
		ranges = append(ranges, sn.Range)
	}

	return ranges
}

func (v *View) HeadersSegment(blockNum uint64) (*VisibleSegment, bool) {
	return v.Segment(coresnaptype.Headers, blockNum)
}

func (v *View) BodiesSegment(blockNum uint64) (*VisibleSegment, bool) {
	return v.Segment(coresnaptype.Bodies, blockNum)
}
func (v *View) TxsSegment(blockNum uint64) (*VisibleSegment, bool) {
	return v.Segment(coresnaptype.Transactions, blockNum)
}

func RemoveIncompatibleIndices(dirs datadir.Dirs) error {
	l, err := dir2.ListFiles(dirs.Snap, ".idx")
	if err != nil {
		return err
	}
	l1, err := dir2.ListFiles(dirs.SnapAccessors, ".efi")
	if err != nil {
		return err
	}
	l2, err := dir2.ListFiles(dirs.SnapAccessors, ".vi")
	if err != nil {
		return err
	}
	l = append(append(l, l1...), l2...)

	for _, fPath := range l {
		index, err := recsplit.OpenIndex(fPath)
		if err != nil {
			if errors.Is(err, recsplit.IncompatibleErr) {
				_, fName := filepath.Split(fPath)
				if err = os.Remove(fPath); err != nil {
					log.Warn("Removing incompatible index", "file", fName, "err", err)
				} else {
					log.Info("Removing incompatible index", "file", fName)
				}
				continue
			}
			return fmt.Errorf("%w, %s", err, fPath)
		}
		index.Close()
	}
	return nil
}
