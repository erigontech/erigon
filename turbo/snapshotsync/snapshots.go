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

package snapshotsync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/chansync/events"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	common2 "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	dir2 "github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/diagnostics"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon/core/rawdb"
	coresnaptype "github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	"github.com/erigontech/erigon/turbo/silkworm"
	"github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"
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

func NewRange(from, to uint64) Range {
	return Range{from, to}
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

func (s *segments) BeginRo() *roTx {
	for _, seg := range s.VisibleSegments {
		if !seg.src.frozen {
			seg.src.refcount.Add(1)
		}
	}
	return &roTx{segments: s, VisibleSegments: s.VisibleSegments}
}

func (s *roTx) Close() {
	if s == nil || s.VisibleSegments == nil {
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

type roTx struct {
	segments        *segments
	VisibleSegments []*VisibleSegment
}

type BlockSnapshots interface {
	LogStat(label string)
	ReopenFolder() error
	ReopenSegments(types []snaptype.Type, allowGaps bool) error
	SegmentsMax() uint64
	SegmentsMin() uint64
	Delete(fileName string) error
	Types() []snaptype.Type
	Close()
	SetSegmentsMin(uint64)

	DownloadComplete()
	Ready(context.Context) <-chan error
}

type retireOperators struct {
	rangeExtractor snaptype.RangeExtractor
	indexBuilder   snaptype.IndexBuilder
}

type RoSnapshots struct {
	downloadReady atomic.Bool
	indicesReady  atomic.Bool
	segmentsReady atomic.Bool

	types []snaptype.Type

	dirtySegmentsLock   sync.RWMutex
	visibleSegmentsLock sync.RWMutex

	segments btree.Map[snaptype.Enum, *Segments]

	dir         string
	segmentsMax atomic.Uint64 // all types of .seg files are available - up to this number
	idxMax      atomic.Uint64 // all types of .idx files are available - up to this number
	cfg         ethconfig.BlocksFreezing
	logger      log.Logger

	// allows for pruning segments - this is the min availible segment
	segmentsMin atomic.Uint64
	ready       ready
	operators   map[snaptype.Enum]*retireOperators
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
func (s *RoSnapshots) DownloadReady() bool           { return s.downloadReady.Load() }
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

func (s *RoSnapshots) IndexedBlocksAvailable(t snaptype.Enum) uint64 {
	return s.segIdxAvailability(t)
}

func (s *RoSnapshots) DownloadComplete() {
	wasReady := s.downloadReady.Swap(true)

	if !wasReady && s.SegmentsReady() && s.IndicesReady() {
		s.ready.set()
	}
}

func (s *RoSnapshots) IndexBuilder(t snaptype.Type) snaptype.IndexBuilder {
	if operators, ok := s.operators[t.Enum()]; ok {
		return operators.indexBuilder
	}

	return nil
}

func (s *RoSnapshots) SetIndexBuilder(t snaptype.Type, indexBuilder snaptype.IndexBuilder) {
	if operators, ok := s.operators[t.Enum()]; ok {
		operators.indexBuilder = indexBuilder
	} else {
		s.operators[t.Enum()] = &retireOperators{
			indexBuilder: indexBuilder,
		}
	}
}

func (s *RoSnapshots) RangeExtractor(t snaptype.Type) snaptype.RangeExtractor {
	if operators, ok := s.operators[t.Enum()]; ok {
		return operators.rangeExtractor
	}

	return nil
}

func (s *RoSnapshots) SetRangeExtractor(t snaptype.Type, rangeExtractor snaptype.RangeExtractor) {
	if operators, ok := s.operators[t.Enum()]; ok {
		operators.rangeExtractor = rangeExtractor
	} else {
		s.operators[t.Enum()] = &retireOperators{
			rangeExtractor: rangeExtractor,
		}

	}
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

type ready struct {
	mu     sync.Mutex
	on     chan struct{}
	state  bool
	inited bool
}

func (me *ready) On() events.Active {
	me.mu.Lock()
	defer me.mu.Unlock()
	me.init()
	return me.on
}

func (me *ready) init() {
	if me.inited {
		return
	}
	me.on = make(chan struct{})
	me.inited = true
}

func (me *ready) set() {
	me.mu.Lock()
	defer me.mu.Unlock()
	me.init()
	if me.state {
		return
	}
	me.state = true
	close(me.on)
}

func (s *RoSnapshots) Ready(ctx context.Context) <-chan error {
	errc := make(chan error)

	go func() {
		select {
		case <-ctx.Done():
			errc <- ctx.Err()
		case <-s.ready.On():
			errc <- nil
		}

		close(errc)
	}()

	return errc
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

func (s *RoSnapshots) segIdxAvailability(segtype snaptype.Enum) uint64 {
	segs, ok := s.segments.Get(segtype)

	if !ok {
		return 0
	}

	var max uint64

	for _, seg := range segs.VisibleSegments {
		if !seg.IsIndexed() {
			break
		}

		max = seg.to - 1
	}

	return max
}

func (s *RoSnapshots) Ls() {
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
	log.Warn("[dbg] OpenFiles")
	defer log.Warn("[dbg] OpenFiles end")
	s.segments.Scan(func(segtype snaptype.Enum, value *Segments) bool {
		value.RLock()
		defer value.RUnlock()

		for _, seg := range value.Segments {
			list = append(list, seg.openFiles()...)
		}
		return true
	})

	return list
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

func (s *RoSnapshots) RemoveOldFiles(filesToRemove []string) {
	removeOldFiles(filesToRemove, s.dir)
}

type snapshotNotifier interface {
	OnNewSnapshot()
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

func (s *View) WithBaseSegType(t snaptype.Type) *View {
	v := *s
	s.baseSegType = t
	return &v
}

func (s *RoSnapshots) View() *View {
	v := &View{s: s, baseSegType: coresnaptype.Transactions}
	s.segments.Scan(func(segtype snaptype.Enum, value *Segments) bool {
		value.RLock()
		return true
	})
	return v
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

func TypedSegments(dir string, minBlock uint64, types []snaptype.Type, allowGaps bool) (res []snaptype.FileInfo, missingSnapshots []Range, err error) {
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

func noGaps(in []snaptype.FileInfo) (out []snaptype.FileInfo, missingSnapshots []Range) {
	if len(in) == 0 {
		return nil, nil
	}
	prevTo := in[0].From
	for _, f := range in {
		if f.To <= prevTo {
			continue
		}
		if f.From != prevTo { // no gaps
			missingSnapshots = append(missingSnapshots, Range{prevTo, f.From})
			continue
		}
		prevTo = f.To
		out = append(out, f)
	}
	return out, missingSnapshots
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

// noOverlaps - keep largest ranges and avoid overlap
func noOverlaps(in []snaptype.FileInfo) (res []snaptype.FileInfo) {
	for i := range in {
		f := in[i]
		if f.From == f.To {
			continue
		}

		for j := i + 1; j < len(in); j++ { // if there is file with larger range - use it instead
			f2 := in[j]
			if f2.From == f2.To {
				continue
			}
			if f2.From > f.From {
				break
			}
			f = f2
			i++
		}

		res = append(res, f)
	}

	return res
}

func findOverlaps(in []snaptype.FileInfo) (res []snaptype.FileInfo, overlapped []snaptype.FileInfo) {
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
			log.Debug("[snapshots] see gap", "type", snaptype.CaplinEnums.BeaconBlocks, "from", lst.From())
		}
		res = append(res, l...)
		res = append(res, lSidecars...)
		missingSnapshots = append(missingSnapshots, m...)
	}
	return res, missingSnapshots, nil
}
