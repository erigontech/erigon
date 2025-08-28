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

	"github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/estimate"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/diagnostics/diaglib"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/chain"
)

type SortedRange interface {
	GetRange() (from, to uint64)
	GetGrouping() string
}

// NoOverlaps - keep largest ranges and avoid overlap
func NoOverlaps[T SortedRange](in []T) (res []T) {
	if len(in) == 1 {
		return in
	}

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

func NoGaps[T SortedRange](in []T) (out []T, missingRanges []Range) {
	if len(in) == 0 {
		return nil, nil
	}
	if len(in) == 1 {
		return in, nil
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

			if f.GetGrouping() != f2.GetGrouping() {
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

func CanRetire(from, to uint64, snapType snaptype.Enum, chainConfig *chain.Config) (blockFrom, blockTo uint64, can bool) {
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

	snapCfg, _ := snapcfg.KnownCfg(chainName)
	mergeLimit := snapcfg.MergeLimitFromCfg(snapCfg, snapType, blockFrom)

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

	// only caplin state
	filePath string
}

func NewDirtySegment(segType snaptype.Type, version snaptype.Version, from uint64, to uint64, frozen bool) *DirtySegment {
	return &DirtySegment{
		segType: segType,
		version: version,
		Range:   Range{from, to},
		frozen:  frozen,
	}
}

type VisibleSegment struct {
	Range
	segType snaptype.Type
	src     *DirtySegment
}

func (s *VisibleSegment) Src() *DirtySegment {
	return s.src
}

func (s *VisibleSegment) IsIndexed() bool {
	return s.src.IsIndexed()
}

func (s *VisibleSegment) Get(globalId uint64) ([]byte, error) {
	idxSlot := s.src.Index()

	if idxSlot == nil {
		return nil, nil
	}
	blockOffset := idxSlot.OrdinalLookup(globalId - idxSlot.BaseDataID())

	gg := s.src.MakeGetter()
	gg.Reset(blockOffset)
	if !gg.HasNext() {
		return nil, nil
	}
	buf, _ := gg.Next(nil)
	if len(buf) == 0 {
		return nil, nil
	}

	return buf, nil
}

func DirtySegmentLess(i, j *DirtySegment) bool {
	if i.from != j.from {
		return i.from < j.from
	}
	if i.to != j.to {
		return i.to < j.to
	}
	return i.version.Less(j.version)
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

func (s *DirtySegment) IsIndexed() bool {
	if len(s.indexes) < len(s.Type().Indexes()) {
		return false
	}

	for _, i := range s.indexes {
		if i == nil {
			return false
		}
	}

	return true
}

func (s *DirtySegment) FileName() string {
	return s.Type().FileName(s.version, s.from, s.to)
}

func (s *DirtySegment) FilePaths(basePath string) (relativePaths []string) {
	if s.Decompressor != nil {
		relativePaths = append(relativePaths, s.Decompressor.FilePath())
	}
	for _, index := range s.indexes {
		if index == nil {
			continue
		}
		relativePaths = append(relativePaths, index.FilePath())
	}
	var err error
	for i := 0; i < len(relativePaths); i++ {
		relativePaths[i], err = filepath.Rel(basePath, relativePaths[i])
		if err != nil {
			log.Warn("FilesItem.FilePaths: can't make basePath path", "err", err, "basePath", basePath, "path", relativePaths[i])
		}
	}
	return relativePaths
}

func (s *DirtySegment) FileInfo(dir string) snaptype.FileInfo {
	return s.Type().FileInfo(dir, s.from, s.to)
}

func (s *DirtySegment) GetRange() (from, to uint64) { return s.from, s.to }
func (s *DirtySegment) GetType() snaptype.Type      { return s.segType }
func (s *DirtySegment) isSubSetOf(j *DirtySegment) bool {
	return (j.from <= s.from && s.to <= j.to) && (j.from != s.from || s.to != j.to)
}

func (s *DirtySegment) Open(dir string) (err error) {
	if s.Decompressor != nil {
		return nil
	}
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
		s.closeIdx()
		s.closeSeg()
	}
}

func (s *DirtySegment) closeAndRemoveFiles() {
	if s != nil {
		f := s.FilePath()
		s.closeIdx()
		s.closeSeg()

		removeOldFiles([]string{f})
	}
}

func (s *DirtySegment) OpenIdxIfNeed(dir string, optimistic bool) (err error) {
	if len(s.Type().IdxFileNames(s.version, s.from, s.to)) == 0 {
		return nil
	}

	if s.refcount.Load() == 0 {
		err = s.openIdx(dir)

		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				if optimistic {
					log.Warn("[snapshots] open index", "err", err)
				} else {
					return err
				}
			}
		}
	}

	return nil
}

func (s *DirtySegment) openIdx(dir string) (err error) {
	if s.Decompressor == nil {
		return nil
	}

	for len(s.indexes) < len(s.Type().Indexes()) {
		s.indexes = append(s.indexes, nil)
	}

	for i, fileName := range s.Type().IdxFileNames(s.version, s.from, s.to) {
		if s.indexes[i] != nil {
			continue
		}
		index, err := recsplit.OpenIndex(filepath.Join(dir, fileName))

		if err != nil {
			return fmt.Errorf("%w, fileName: %s", err, fileName)
		}

		s.indexes[i] = index
	}

	return nil
}

type VisibleSegments []*VisibleSegment

func (s VisibleSegments) BeginRo() *RoTx {
	for _, seg := range s {
		if !seg.src.frozen {
			seg.src.refcount.Add(1)
		}
	}
	return &RoTx{Segments: s}
}

type RoTx struct {
	Segments VisibleSegments
}

func (s *RoTx) Close() {
	if s == nil || s.Segments == nil {
		return
	}
	VisibleSegments := s.Segments
	s.Segments = nil

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

	//fmt.Println("CRO", s.segments)
}

type BlockSnapshots interface {
	LogStat(label string)
	OpenFolder() error
	OpenSegments(types []snaptype.Type, allowGaps, allignMin bool) error
	SegmentsMax() uint64
	Delete(fileName string) error
	Types() []snaptype.Type
	Close()

	DownloadComplete()
	RemoveOverlaps(onDelete func(l []string) error) error
	DownloadReady() bool
	Ready(context.Context) <-chan error
}

type retireOperators struct {
	rangeExtractor snaptype.RangeExtractor
	indexBuilder   snaptype.IndexBuilder
}

type RoSnapshots struct {
	downloadReady atomic.Bool
	segmentsReady atomic.Bool

	types []snaptype.Type //immutable
	enums []snaptype.Enum //immutable

	dirtyLock   sync.RWMutex                   // guards `dirty` field
	dirty       []*btree.BTreeG[*DirtySegment] // ordered map `type.Enum()` -> DirtySegments
	visibleLock sync.RWMutex                   // guards  `visible` field
	visible     []VisibleSegments              // ordered map `type.Enum()` -> VisbileSegments

	dir         string
	segmentsMax atomic.Uint64 // all types of .seg files are available - up to this number
	idxMax      atomic.Uint64 // all types of .idx files are available - up to this number
	cfg         ethconfig.BlocksFreezing
	logger      log.Logger

	ready     ready
	operators map[snaptype.Enum]*retireOperators
	alignMin  bool // do we want to align all visible segments to the minimum available
}

// NewRoSnapshots - opens all snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, types []snaptype.Type, alignMin bool, logger log.Logger) *RoSnapshots {
	return newRoSnapshots(cfg, snapDir, types, alignMin, logger)
}

func newRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, types []snaptype.Type, alignMin bool, logger log.Logger) *RoSnapshots {
	if cfg.ChainName == "" {
		log.Debug("[dbg] newRoSnapshots created with empty ChainName", "stack", dbg.Stack())
	}
	enums := make([]snaptype.Enum, len(types))
	for i, t := range types {
		enums[i] = t.Enum()
	}
	s := &RoSnapshots{dir: snapDir, cfg: cfg, logger: logger,
		types: types, enums: enums,
		dirty:     make([]*btree.BTreeG[*DirtySegment], snaptype.MaxEnum),
		alignMin:  alignMin,
		operators: map[snaptype.Enum]*retireOperators{},
	}
	for _, snapType := range types {
		s.dirty[snapType.Enum()] = btree.NewBTreeGOptions[*DirtySegment](DirtySegmentLess, btree.Options{Degree: 128, NoLocks: false})
	}

	s.recalcVisibleFiles(s.alignMin)

	if cfg.NoDownloader {
		s.DownloadComplete()
	}

	return s
}

func (s *RoSnapshots) Cfg() ethconfig.BlocksFreezing { return s.cfg }
func (s *RoSnapshots) Dir() string                   { return s.dir }
func (s *RoSnapshots) DownloadReady() bool           { return s.downloadReady.Load() }
func (s *RoSnapshots) SegmentsReady() bool           { return s.segmentsReady.Load() }
func (s *RoSnapshots) IndicesMax() uint64            { return s.idxMax.Load() }
func (s *RoSnapshots) SegmentsMax() uint64           { return s.segmentsMax.Load() }
func (s *RoSnapshots) BlocksAvailable() uint64 {
	if s == nil {
		return 0
	}

	return s.idxMax.Load()
}

func (s *RoSnapshots) DirtyBlocksAvailable(t snaptype.Enum) uint64 {
	return s.dirtyIdxAvailability(t)
}

func (s *RoSnapshots) VisibleBlocksAvailable(t snaptype.Enum) uint64 {
	return s.visibleIdxAvailability(t)
}

func (s *RoSnapshots) DownloadComplete() {
	wasReady := s.downloadReady.Swap(true)
	if !wasReady {
		if s.SegmentsReady() {
			s.ready.set()
		}
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
		"blocks", common.PrettyCounter(s.SegmentsMax()+1), "indices", common.PrettyCounter(s.IndicesMax()+1),
		"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
}

func (s *RoSnapshots) EnsureExpectedBlocksAreAvailable(cfg *snapcfg.Cfg) error {
	if s.BlocksAvailable() < cfg.ExpectBlocks {
		return fmt.Errorf("app must wait until all expected snapshots are available. Expected: %d, Available: %d", cfg.ExpectBlocks, s.BlocksAvailable())
	}
	return nil
}

func (s *RoSnapshots) Types() []snaptype.Type { return s.types }
func (s *RoSnapshots) HasType(in snaptype.Type) bool {
	for _, t := range s.enums {
		if t == in.Enum() {
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

func (r *ready) On() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	return r.on
}

func (r *ready) init() {
	if r.inited {
		return
	}
	r.on = make(chan struct{})
	r.inited = true
}

func (r *ready) set() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if r.state {
		return
	}
	r.state = true
	close(r.on)
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

	for _, t := range s.enums {
		for _, sn := range v.segments[t].Segments {
			sn.src.DisableReadAhead()
		}
	}
	return s
}

func (s *RoSnapshots) EnableReadAhead() *RoSnapshots {
	v := s.View()
	defer v.Close()

	for _, t := range s.enums {
		for _, sn := range v.segments[t].Segments {
			sn.src.MadvSequential()
		}
	}

	return s
}
func (s *RoSnapshots) MadvNormal() *RoSnapshots {
	v := s.View()
	defer v.Close()

	for _, t := range s.enums {
		for _, sn := range v.segments[t].Segments {
			sn.src.MadvNormal()
		}
	}

	return s
}

func (s *RoSnapshots) EnableMadvWillNeed() *RoSnapshots {
	v := s.View()
	defer v.Close()

	for _, t := range s.enums {
		for _, sn := range v.segments[t].Segments {
			sn.src.MadvWillNeed()
		}
	}
	return s
}

func RecalcVisibleSegments(dirtySegments *btree.BTreeG[*DirtySegment]) []*VisibleSegment {
	newVisibleSegments := make([]*VisibleSegment, 0, dirtySegments.Len())
	dirtySegments.Walk(func(segments []*DirtySegment) bool {
		for _, sn := range segments {
			if sn.canDelete.Load() {
				continue
			}
			if !sn.IsIndexed() {
				continue
			}

			//protect from overlaps
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

	// protect from gaps
	if len(newVisibleSegments) > 0 {
		prevEnd := newVisibleSegments[0].from
		for i, seg := range newVisibleSegments {
			if seg.from != prevEnd {
				newVisibleSegments = newVisibleSegments[:i] //remove tail if see gap
				break
			}
			prevEnd = seg.to
		}
	}

	return newVisibleSegments
}

func (s *RoSnapshots) recalcVisibleFiles(alignMin bool) {
	defer func() {
		s.idxMax.Store(s.idxAvailability())
	}()

	s.visibleLock.Lock()
	defer s.visibleLock.Unlock()

	s.dirtyLock.RLock()
	defer s.dirtyLock.RUnlock()

	visible := make([]VisibleSegments, snaptype.MaxEnum) // create new pointer - only new readers will see it. old-alive readers will continue use previous pointer
	maxVisibleBlocks := make([]uint64, 0, len(s.types))

	for _, t := range s.enums {
		newVisibleSegments := RecalcVisibleSegments(s.dirty[t])
		visible[t] = newVisibleSegments
		var to uint64
		if len(newVisibleSegments) > 0 {
			to = newVisibleSegments[len(newVisibleSegments)-1].to - 1
		}
		if alignMin {
			maxVisibleBlocks = append(maxVisibleBlocks, to)
		}
	}

	if alignMin {
		// all types must have the same height
		minMaxVisibleBlock := slices.Min(maxVisibleBlocks)
		for _, t := range s.enums {
			if minMaxVisibleBlock == 0 {
				visible[t] = []*VisibleSegment{}
			} else {
				visibleSegmentsOfType := visible[t]
				for i, seg := range visibleSegmentsOfType {
					if seg.to > minMaxVisibleBlock+1 {
						visible[t] = visibleSegmentsOfType[:i]
						break
					}
				}
			}
		}
	}

	s.visible = visible
}

// minimax of existing indices
func (s *RoSnapshots) idxAvailability() uint64 {
	// Use-Cases:
	//   1. developers can add new types in future. and users will not have files of this type
	//   2. some types are network-specific. example: borevents exists only on Bor-consensus networks
	//   3. user can manually remove 1 .idx file: `rm snapshots/v1.0-type1-0000-1000.idx`
	//   4. user can manually remove all .idx files of given type: `rm snapshots/*type1*.idx`
	//   5. file-types may have different height: 10 headers, 10 bodies, 9 transactions (for example if `kill -9` came during files building/merge). still need index all 3 types.

	if len(s.enums) == 0 {
		return 0
	}

	var maxIdx uint64
	visible := s.visible[s.enums[0]]
	if len(visible) > 0 {
		maxIdx = visible[len(visible)-1].to - 1
	}

	return maxIdx
}

func (s *RoSnapshots) dirtyIdxAvailability(segtype snaptype.Enum) uint64 {
	s.dirtyLock.RLock()
	defer s.dirtyLock.RUnlock()

	dirty := s.dirty[segtype]

	if dirty == nil {
		return 0
	}

	var _max uint64

	dirty.Walk(func(segments []*DirtySegment) bool {
		for _, seg := range segments {
			if !seg.IsIndexed() {
				break
			}

			_max = seg.to - 1
		}

		return true
	})

	return _max
}

func (s *RoSnapshots) visibleIdxAvailability(segtype snaptype.Enum) (maxVisibleIdx uint64) {
	s.visibleLock.RLock()
	defer s.visibleLock.RUnlock()

	visibleFiles := s.visible[segtype]
	if len(visibleFiles) > 0 {
		maxVisibleIdx = visibleFiles[len(visibleFiles)-1].to - 1
	}

	return
}

func (s *RoSnapshots) Ls() {
	view := s.View()
	defer view.Close()

	for _, t := range s.enums {
		for _, seg := range s.visible[t] {
			if seg.src == nil || seg.src.Decompressor == nil {
				continue
			}
			log.Info("[snapshots] ", "f", seg.src.Decompressor.FileName(), "count", seg.src.Decompressor.Count())
		}
	}
}

func (s *RoSnapshots) Files() (list []string) {
	view := s.View()
	defer view.Close()
	for _, t := range s.enums {
		for _, seg := range view.segments[t].Segments {
			list = append(list, seg.src.FileName())
		}
	}

	return
}

func (s *RoSnapshots) OpenFiles() (list []string) {
	s.dirtyLock.RLock()
	defer s.dirtyLock.RUnlock()

	log.Warn("[dbg] OpenFiles")
	defer log.Warn("[dbg] OpenFiles end")
	for _, t := range s.types {
		s.dirty[t.Enum()].Walk(func(segs []*DirtySegment) bool {
			for _, seg := range segs {
				if seg.Decompressor == nil {
					continue
				}
				list = append(list, seg.FilePath())
			}
			return true
		})
	}

	return list
}

// OpenList stops on optimistic=false, continue opening files on optimistic=true
func (s *RoSnapshots) OpenList(fileNames []string, optimistic bool) error {
	defer s.recalcVisibleFiles(s.alignMin)

	s.dirtyLock.Lock()
	defer s.dirtyLock.Unlock()

	s.closeWhatNotInList(fileNames)
	if err := s.openSegments(fileNames, true, optimistic); err != nil {
		return err
	}
	return nil
}

func (s *RoSnapshots) InitSegments(fileNames []string) error {
	if err := func() error {
		s.dirtyLock.Lock()
		defer s.dirtyLock.Unlock()

		s.closeWhatNotInList(fileNames)
		return s.openSegments(fileNames, false, true)
	}(); err != nil {
		return err
	}

	s.recalcVisibleFiles(s.alignMin)
	wasReady := s.segmentsReady.Swap(true)
	if !wasReady {
		if s.downloadReady.Load() {
			s.ready.set()
		}
	}

	return nil
}

func TypedSegments(dir string, types []snaptype.Type, allowGaps bool) (res []snaptype.FileInfo, missingSnapshots []Range, err error) {
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
				l = NoOverlaps(l)
			} else {
				l, m = NoGaps(NoOverlaps(l))
			}

			if len(m) > 0 {
				lst := m[len(m)-1]
				log.Debug("[snapshots] see gap", "type", segType, "from", lst.from)
			}
			res = append(res, l...)

			missingSnapshots = append(missingSnapshots, m...)
		}
	}
	return res, missingSnapshots, nil
}

func (s *RoSnapshots) openSegments(fileNames []string, open bool, optimistic bool) error {
	var segmentsMax uint64
	var segmentsMaxSet bool

	wg := &errgroup.Group{}
	wg.SetLimit(64)
	//fmt.Println("RS", s)
	//defer fmt.Println("Done RS", s)

	snConfig, _ := snapcfg.KnownCfg(s.cfg.ChainName)

	for _, fName := range fileNames {
		f, isState, ok := snaptype.ParseFileName(s.dir, fName)
		if !ok || isState || snaptype.IsTorrentPartial(f.Ext) {
			continue
		}
		if !s.HasType(f.Type) {
			continue
		}

		segtype := s.dirty[f.Type.Enum()]
		if segtype == nil {
			log.Debug("[snapshot] rebuildSegments: unknown type", "t", f.Type.Enum().String())
			continue
		}

		var sn *DirtySegment
		var exists bool
		segtype.Walk(func(segs []*DirtySegment) bool {
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
			sn = &DirtySegment{segType: f.Type, version: f.Version, Range: Range{f.From, f.To}, frozen: snConfig.IsFrozen(f)}
		}

		if open {
			if err := sn.Open(s.dir); err != nil {
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
			segtype.Set(sn)
		}

		if open {
			wg.Go(func() error {
				if err := sn.OpenIdxIfNeed(s.dir, optimistic); err != nil {
					return err
				}
				return nil
			})
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
	if err := wg.Wait(); err != nil {
		return err
	}

	return nil
}

func (s *RoSnapshots) Ranges() []Range {
	view := s.View()
	defer view.Close()
	return view.Ranges()
}

func (s *RoSnapshots) OptimisticalyOpenFolder() { _ = s.OpenFolder() }
func (s *RoSnapshots) OpenFolder() error {
	if err := func() error {
		s.dirtyLock.Lock()
		defer s.dirtyLock.Unlock()

		files, _, err := TypedSegments(s.dir, s.Types(), false)
		if err != nil {
			return err
		}

		list := make([]string, 0, len(files))
		for _, f := range files {
			_, fName := filepath.Split(f.Path)
			list = append(list, fName)
		}
		s.closeWhatNotInList(list)
		return s.openSegments(list, true, false)
	}(); err != nil {
		return fmt.Errorf("OpenFolder: %w", err)
	}

	s.recalcVisibleFiles(s.alignMin)
	wasReady := s.segmentsReady.Swap(true)
	if !wasReady {
		if s.downloadReady.Load() {
			s.ready.set()
		}
	}
	return nil
}

func (s *RoSnapshots) OpenSegments(types []snaptype.Type, allowGaps, alignMin bool) error {
	defer s.recalcVisibleFiles(alignMin)

	s.dirtyLock.Lock()
	defer s.dirtyLock.Unlock()

	files, _, err := TypedSegments(s.dir, types, allowGaps)

	if err != nil {
		return err
	}
	list := make([]string, 0, len(files))
	for _, f := range files {
		_, fName := filepath.Split(f.Path)
		list = append(list, fName)
	}

	if err := s.openSegments(list, true, false); err != nil {
		return err
	}
	return nil
}

func (s *RoSnapshots) Close() {
	if s == nil {
		return
	}
	defer s.recalcVisibleFiles(s.alignMin)
	s.dirtyLock.Lock()
	defer s.dirtyLock.Unlock()

	s.closeWhatNotInList(nil)
}

func (s *RoSnapshots) closeWhatNotInList(l []string) {
	protectFiles := make(map[string]struct{}, len(l))
	for _, f := range l {
		protectFiles[f] = struct{}{}
	}
	toClose := make(map[snaptype.Enum][]*DirtySegment, 0)
	for _, t := range s.enums {
		s.dirty[t].Walk(func(segs []*DirtySegment) bool {

			for _, seg := range segs {
				if _, ok := protectFiles[seg.FileName()]; ok {
					continue
				}
				if _, ok := toClose[seg.segType.Enum()]; !ok {
					toClose[t] = make([]*DirtySegment, 0)
				}
				toClose[t] = append(toClose[t], seg)
			}

			return true
		})
	}

	for segtype, delSegments := range toClose {
		dirtyFiles := s.dirty[segtype]
		for _, delSeg := range delSegments {
			delSeg.close()
			dirtyFiles.Delete(delSeg)
		}
	}
}

func (s *RoSnapshots) RemoveOverlaps(onDelete func(l []string) error) error {
	list, err := snaptype.Segments(s.dir)
	if err != nil {
		return err
	}
	_, segmentsToRemove := findOverlaps(list)

	toRemove := make([]string, 0, len(segmentsToRemove))
	for _, info := range segmentsToRemove {
		toRemove = append(toRemove, info.Path)
	}

	//it's possible that .seg was remove but .idx not (kill between deletes, etc...)
	list, err = snaptype.IdxFiles(s.dir)
	if err != nil {
		return err
	}
	_, accessorsToRemove := findOverlaps(list)
	for _, info := range accessorsToRemove {
		toRemove = append(toRemove, info.Path)
	}

	{
		relativePaths, err := toRelativePaths(s.dir, toRemove)
		if err != nil {
			return err
		}
		if onDelete != nil {
			if err := onDelete(relativePaths); err != nil {
				return fmt.Errorf("onDelete: %w", err)
			}
		}
	}

	removeOldFiles(toRemove)

	// remove .tmp files
	//TODO: it may remove Caplin's useful .tmp files - re-think. Keep it here for backward-compatibility for now.
	tmpFiles, err := snaptype.TmpFiles(s.dir)
	if err != nil {
		return err
	}
	for _, f := range tmpFiles {
		_ = dir.RemoveFile(f)
	}
	return nil
}

func toRelativePaths(basePath string, absolutePaths []string) (relativePaths []string, err error) {
	relativePaths = make([]string, len(absolutePaths))
	for i, f := range absolutePaths {
		relativePaths[i], err = filepath.Rel(basePath, f)
		if err != nil {
			return nil, fmt.Errorf("rel: %w", err)
		}
	}
	return relativePaths, nil
}

type snapshotNotifier interface {
	OnNewSnapshot()
}

func (s *RoSnapshots) BuildMissedIndices(ctx context.Context, logPrefix string, notifier snapshotNotifier, dirs datadir.Dirs, cc *chain.Config, logger log.Logger) error {
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

	if err := s.OpenFolder(); err != nil {
		return err
	}
	s.LogStat("missed-idx:open")
	if notifier != nil {
		notifier.OnNewSnapshot()
	}
	return nil
}

func (s *RoSnapshots) delete(fileName string) error {
	s.dirtyLock.Lock()
	defer s.dirtyLock.Unlock()

	var err error
	var delSeg *DirtySegment
	var dirtySegments *btree.BTreeG[*DirtySegment]

	_, fName := filepath.Split(fileName)
	for _, t := range s.enums {
		findDelSeg := false
		s.dirty[t].Walk(func(segs []*DirtySegment) bool {
			for _, sn := range segs {
				if sn.Decompressor == nil {
					continue
				}
				if sn.segType.FileName(sn.version, sn.from, sn.to) != fName {
					continue
				}
				sn.canDelete.Store(true)
				delSeg = sn
				dirtySegments = s.dirty[t]
				findDelSeg = false
				return true
			}
			return true
		})
		if findDelSeg {
			break
		}
	}
	dirtySegments.Delete(delSeg)
	return err
}

// prune visible segments
func (s *RoSnapshots) Delete(fileName string) error {
	if s == nil {
		return nil
	}

	v := s.View()
	defer v.Close()

	defer s.recalcVisibleFiles(s.alignMin)
	if err := s.delete(fileName); err != nil {
		return fmt.Errorf("can't delete file: %w", err)
	}
	return nil
}

func (s *RoSnapshots) buildMissedIndices(logPrefix string, ctx context.Context, dirs datadir.Dirs, chainConfig *chain.Config, workers int, logger log.Logger) error {
	if s == nil {
		return nil
	}

	if _, err := snaptype.GetIndexSalt(dirs.Snap, logger); err != nil {
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
				logger.Info(fmt.Sprintf("[%s] Indexing", logPrefix), "progress", ps.String(), "total-indexing-time", time.Since(startIndexingTime).Round(time.Second).String(), "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
			case <-finish:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	var fmu sync.Mutex
	failedIndexes := make(map[string]error, 0)

	for _, t := range s.enums {
		s.dirty[t].Walk(func(segs []*DirtySegment) bool {
			for _, segment := range segs {
				info := segment.FileInfo(dir)

				if t.HasIndexFiles(info, logger) {
					continue
				}

				segment.closeIdx()

				indexBuilder := s.IndexBuilder(t.Type())

				g.Go(func() error {
					p := &background.Progress{}
					ps.Add(p)
					defer notifySegmentIndexingFinished(info.Name())
					defer ps.Delete(p)
					if err := t.BuildIndexes(gCtx, info, indexBuilder, chainConfig, tmpDir, p, log.LvlInfo, logger); err != nil {
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
	}

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
	for _, t := range s.types {
		fmt.Println("    == [dbg] Snapshots,", t.Enum().String())
		printDebug := func(sn *DirtySegment) {
			args := make([]any, 0, len(sn.Type().Indexes())+1)
			args = append(args, sn.from)
			for _, index := range sn.Type().Indexes() {
				args = append(args, sn.Index(index) != nil)
			}
			fmt.Println(args...)
		}
		s.dirty[t.Enum()].Scan(func(sn *DirtySegment) bool {
			printDebug(sn)
			return true
		})
	}
}

type View struct {
	s           *RoSnapshots
	segments    []*RoTx
	baseSegType snaptype.Type
}

func (s *RoSnapshots) View() *View {
	s.visibleLock.RLock()
	defer s.visibleLock.RUnlock()
	sgs := make([]*RoTx, snaptype.MaxEnum)
	for _, t := range s.enums {
		sgs[t] = s.visible[t].BeginRo()
	}
	return &View{s: s, segments: sgs, baseSegType: snaptype2.Transactions} // Transactions is the last segment to be processed, so it's the most reliable.
}

func (v *View) Close() {
	if v == nil || v.s == nil {
		return
	}
	for _, t := range v.s.enums {
		v.segments[t].Close()
	}
	v.s = nil
}

func (s *View) WithBaseSegType(t snaptype.Type) *View {
	v := *s
	v.baseSegType = t
	return &v
}

var noop = func() {}

func (s *RoSnapshots) ViewType(t snaptype.Type) *RoTx {
	s.visibleLock.RLock()
	defer s.visibleLock.RUnlock()
	return s.visible[t.Enum()].BeginRo()
}

func (s *RoSnapshots) ViewSingleFile(t snaptype.Type, blockNum uint64) (segment *VisibleSegment, ok bool, close func()) {
	s.visibleLock.RLock()
	defer s.visibleLock.RUnlock()

	segmentRotx := s.visible[t.Enum()].BeginRo()

	for _, seg := range segmentRotx.Segments {
		if !(blockNum >= seg.from && blockNum < seg.to) {
			continue
		}
		return seg, true, segmentRotx.Close
	}
	segmentRotx.Close()
	return nil, false, noop
}

func (v *View) Segments(t snaptype.Type) []*VisibleSegment {
	return v.segments[t.Enum()].Segments
}

func (v *View) Segment(t snaptype.Type, blockNum uint64) (*VisibleSegment, bool) {
	for _, seg := range v.s.visible[t.Enum()] {
		if !(blockNum >= seg.from && blockNum < seg.to) {
			continue
		}
		return seg, true
	}
	return nil, false
}

func (v *View) Ranges() (ranges []Range) {
	for _, sn := range v.Segments(v.baseSegType) {
		ranges = append(ranges, sn.Range)
	}

	return ranges
}

func notifySegmentIndexingFinished(name string) {
	dts := []diaglib.SnapshotSegmentIndexingStatistics{
		{
			SegmentName: name,
			Percent:     100,
			Alloc:       0,
			Sys:         0,
		},
	}
	diaglib.Send(diaglib.SnapshotIndexingStatistics{
		Segments:    dts,
		TimeElapsed: -1,
	})
}

func sendDiagnostics(startIndexingTime time.Time, indexPercent map[string]int, alloc uint64, sys uint64) {
	segmentsStats := make([]diaglib.SnapshotSegmentIndexingStatistics, 0, len(indexPercent))
	for k, v := range indexPercent {
		segmentsStats = append(segmentsStats, diaglib.SnapshotSegmentIndexingStatistics{
			SegmentName: k,
			Percent:     v,
			Alloc:       alloc,
			Sys:         sys,
		})
	}
	diaglib.Send(diaglib.SnapshotIndexingStatistics{
		Segments:    segmentsStats,
		TimeElapsed: time.Since(startIndexingTime).Round(time.Second).Seconds(),
	})
}

func removeOldFiles(toDel []string) {
	for _, f := range toDel {
		_ = dir.RemoveFile(f)
		_ = dir.RemoveFile(f + ".torrent")
		ext := filepath.Ext(f)
		withoutExt := f[:len(f)-len(ext)]
		_ = dir.RemoveFile(withoutExt + ".idx")
		_ = dir.RemoveFile(withoutExt + ".idx.torrent")
		isTxnType := strings.HasSuffix(withoutExt, snaptype2.Transactions.Name())
		if isTxnType {
			_ = dir.RemoveFile(withoutExt + "-to-block.idx")
			_ = dir.RemoveFile(withoutExt + "-to-block.idx.torrent")
		}
	}
}

func SegmentsCaplin(dir string) (res []snaptype.FileInfo, missingSnapshots []Range, err error) {
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
		l, m = NoGaps(NoOverlaps(l))
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
