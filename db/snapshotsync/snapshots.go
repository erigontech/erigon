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
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/diagnostics/diaglib"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/node/ethconfig"
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

func CanRetire(from, to uint64, snapType snaptype.Enum, snCfg *snapcfg.Cfg) (blockFrom, blockTo uint64, can bool) {
	if to <= from {
		return
	}
	blockFrom = (from / 1_000) * 1_000
	roundedTo1K := (to / 1_000) * 1_000
	var maxJump uint64 = 1_000

	mergeLimit := snapcfg.MergeLimitFromCfg(snCfg, snapType, blockFrom)

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

	frozen bool

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
	if s.Decompressor != nil {
		return s.Decompressor.FileName()
	}
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
	return s.Type().FileInfoByMask(dir, s.from, s.to)
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
		if index == nil {
			continue
		}
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
		toRemove := make([]string, 0, 1+len(s.indexes))
		if s.Decompressor != nil {
			toRemove = append(toRemove, s.FilePath())
		}
		for _, index := range s.indexes {
			if index == nil {
				continue
			}
			toRemove = append(toRemove, index.FilePath())
		}
		s.closeIdx()
		s.closeSeg()
		removeOldFiles(toRemove)
	}
}

func (s *DirtySegment) OpenIdxIfNeed(dir string, optimistic bool, dirEntries []string) (err error) {
	if len(s.Type().IdxFileNames(s.from, s.to)) == 0 {
		return nil
	}

	// An unindexed segment is never visible (RecalcVisibleSegments requires IsIndexed),
	// so it is never pinned by a reader — building its index races nobody.
	err = s.openIdx(dir, dirEntries)
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

func (s *DirtySegment) openIdx(dir string, dirEntries []string) (err error) {
	if s.Decompressor == nil {
		return nil
	}

	for len(s.indexes) < len(s.Type().Indexes()) {
		s.indexes = append(s.indexes, nil)
	}

	for i, fileName := range s.Type().IdxFileNames(s.from, s.to) {
		if s.indexes[i] != nil {
			continue
		}
		fPathMask, err := version.ReplaceVersionWithMask(fileName)
		if err != nil {
			return fmt.Errorf("[open index] can't replace with mask in file %s: %w", fileName, err)
		}

		var fPath string
		var ok bool
		if dirEntries != nil {
			fPath, _, ok, err = version.MatchVersionedFile(fPathMask, dirEntries, dir)
		} else {
			fPath, _, ok, err = version.FindFilesWithVersionsByPattern(filepath.Join(dir, fPathMask))
		}
		if err != nil {
			return fmt.Errorf("%w, fileName: %s", err, fileName)
		}
		if !ok {
			_, fName := filepath.Split(fPath)
			return fmt.Errorf("[open index] find files by pattern err %w fname %s", os.ErrNotExist, fName)
		}
		index, err := recsplit.OpenIndex(fPath)

		if err != nil {
			return fmt.Errorf("%w, fileName: %s", err, fileName)
		}

		s.indexes[i] = index
	}

	return nil
}

type DirtyFiles = []*btree.BTreeG[*DirtySegment]

type VisibleSegments []*VisibleSegment

func (s VisibleSegments) BeginRo() *RoTx {
	return &RoTx{Segments: s}
}

type RoTx struct {
	Segments VisibleSegments
	// release drops the pin for a standalone RoTx (ViewType/ViewSingleFile). nil for the
	// per-type children inside a View, whose single pin is dropped by View.Close.
	release func()
}

func (s *RoTx) Close() {
	if s == nil {
		return
	}
	s.Segments = nil
	if s.release != nil {
		release := s.release
		s.release = nil // idempotent: a second Close is a no-op
		release()
	}
}

type retireOperators struct {
	rangeExtractor snaptype.RangeExtractor
	indexBuilder   snaptype.IndexBuilder
}

type BaseRoSnapshots struct {
	downloadReady atomic.Bool
	segmentsReady atomic.Bool

	types []snaptype.Type //immutable
	enums []snaptype.Enum //immutable

	dirtyLock sync.RWMutex // guards `dirty` and the generation chain (oldestVisible/next/retired)
	dirty     DirtyFiles   // ordered map `type.Enum()` -> DirtySegments
	visible   atomic.Pointer[snapshotVisible]

	// oldestVisible is the chain head: reclamation walks oldest->newest from here,
	// deleting a generation's retired files once its refcnt hits 0. Mutated under dirtyLock.
	oldestVisible *snapshotVisible

	dir               string
	segmentsMinByType map[snaptype.Enum]*atomic.Uint64 // min block number per segment type
	idxMax            atomic.Uint64                    // all types of .idx files are available - up to this number
	cfg               ethconfig.BlocksFreezing
	snCfg             *snapcfg.Cfg
	logger            log.Logger

	ready     ready
	operators map[snaptype.Enum]*retireOperators
	alignMin  bool // do we want to align all visible segments to the minimum available

	// (type, from, to) of files a producer is currently building — a merge
	// output, a dump, or a missed-index rebuild. The data file can be on disk
	// before its index exists; openers skip these and other builders can't
	// claim the same file, so nobody creates a duplicate segment or races the
	// owner's index build.
	rangesInProgress sync.Map // rangeKey -> struct{}
}

type rangeKey struct {
	enum     snaptype.Enum
	from, to uint64
}

// ErrRangeBuildInProgress means another builder holds the (type, range) claim;
// the caller should retry later rather than treat it as a failure.
var ErrRangeBuildInProgress = errors.New("range build is already in progress")

// TryAcquireRange atomically claims (enum, from, to) for building. Returns false
// if another builder already holds it, in which case the caller must skip it.
func (s *BaseRoSnapshots) TryAcquireRange(enum snaptype.Enum, from, to uint64) bool {
	_, loaded := s.rangesInProgress.LoadOrStore(rangeKey{enum, from, to}, struct{}{})
	return !loaded
}

func (s *BaseRoSnapshots) ReleaseRange(enum snaptype.Enum, from, to uint64) {
	s.rangesInProgress.Delete(rangeKey{enum, from, to})
}

func (s *BaseRoSnapshots) isInProgress(enum snaptype.Enum, from, to uint64) bool {
	_, ok := s.rangesInProgress.Load(rangeKey{enum, from, to})
	return ok
}

type snapshotVisible struct {
	segments    []VisibleSegments // ordered map `type.Enum()` -> VisibleSegments
	segmentsMax uint64            // max visible (indexed, non-subsumed, gap-free) segment height across all types

	refcnt  atomic.Int32     // live readers pinning this generation
	retired []*DirtySegment  // segments this generation is the last to reference; unlinked on head-drain
	next    *snapshotVisible // oldest->newest chain link (set under dirtyLock)
}

// NewBaseRoSnapshots - opens all snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewBaseRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, types []snaptype.Type, alignMin bool, logger log.Logger) *BaseRoSnapshots {
	return newRoSnapshots(cfg, snapDir, types, alignMin, logger)
}

func newRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, types []snaptype.Type, alignMin bool, logger log.Logger) *BaseRoSnapshots {
	if cfg.ChainName == "" {
		log.Debug("[dbg] newRoSnapshots created with empty ChainName", "stack", dbg.Stack())
	}
	enums := make([]snaptype.Enum, len(types))
	for i, t := range types {
		enums[i] = t.Enum()
	}
	snCfg := snapcfg.KnownCfgOrDevnet(cfg.ChainName)
	s := &BaseRoSnapshots{dir: snapDir, cfg: cfg, snCfg: snCfg, logger: logger,
		types: types, enums: enums,
		dirty:             make(DirtyFiles, snaptype.MaxEnum),
		alignMin:          alignMin,
		operators:         map[snaptype.Enum]*retireOperators{},
		segmentsMinByType: make(map[snaptype.Enum]*atomic.Uint64),
	}
	for _, snapType := range types {
		s.dirty[snapType.Enum()] = btree.NewBTreeGOptions[*DirtySegment](DirtySegmentLess, btree.Options{Degree: 128, NoLocks: false})
	}
	empty := &snapshotVisible{segments: make([]VisibleSegments, snaptype.MaxEnum)}
	s.visible.Store(empty)
	s.oldestVisible = empty

	for _, t := range s.enums {
		u := &atomic.Uint64{}
		u.Store(math.MaxUint64)
		s.segmentsMinByType[t] = u
	}

	s.dirtyLock.Lock()
	s.recalcVisibleFiles(s.alignMin, nil)
	s.dirtyLock.Unlock()
	return s
}

func (s *BaseRoSnapshots) Cfg() ethconfig.BlocksFreezing { return s.cfg }
func (s *BaseRoSnapshots) Dir() string                   { return s.dir }
func (s *BaseRoSnapshots) DownloadReady() bool           { return s.downloadReady.Load() }
func (s *BaseRoSnapshots) SegmentsReady() bool           { return s.segmentsReady.Load() }
func (s *BaseRoSnapshots) IndicesMax() uint64            { return s.idxMax.Load() }
func (s *BaseRoSnapshots) SegmentsMax() uint64           { return s.visible.Load().segmentsMax }
func (s *BaseRoSnapshots) SegmentsMinByType(t snaptype.Enum) (min uint64, ok bool) {
	if s == nil {
		return 0, false
	}

	minStore, exists := s.segmentsMinByType[t]
	if !exists {
		return 0, false
	}

	min = minStore.Load()
	if min == math.MaxUint64 {
		return 0, false
	}

	return min, true
}
func (s *BaseRoSnapshots) BlocksAvailable() uint64 {
	if s == nil {
		return 0
	}

	return s.idxMax.Load()
}

func (s *BaseRoSnapshots) DirtyBlocksAvailable(t snaptype.Enum) uint64 {
	return s.dirtyIdxAvailability(t)
}

func (s *BaseRoSnapshots) VisibleBlocksAvailable(t snaptype.Enum) uint64 {
	return s.visibleIdxAvailability(t)
}

func (s *BaseRoSnapshots) DownloadComplete() {
	wasReady := s.downloadReady.Swap(true)
	if !wasReady {
		if s.SegmentsReady() {
			s.ready.Set()
		}
	}
}

func (s *BaseRoSnapshots) IndexBuilder(t snaptype.Type) snaptype.IndexBuilder {
	if operators, ok := s.operators[t.Enum()]; ok {
		return operators.indexBuilder
	}

	return nil
}

func (s *BaseRoSnapshots) RangeExtractor(t snaptype.Type) snaptype.RangeExtractor {
	if operators, ok := s.operators[t.Enum()]; ok {
		return operators.rangeExtractor
	}

	return nil
}

func (s *BaseRoSnapshots) SetRangeExtractor(t snaptype.Type, rangeExtractor snaptype.RangeExtractor) {
	if operators, ok := s.operators[t.Enum()]; ok {
		operators.rangeExtractor = rangeExtractor
	} else {
		s.operators[t.Enum()] = &retireOperators{
			rangeExtractor: rangeExtractor,
		}

	}
}

func (s *BaseRoSnapshots) LogStat(label string) {
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	s.logger.Info(fmt.Sprintf("[snapshots:%s] Stat", label),
		"blocks", fmt.Sprintf("%dk", (s.SegmentsMax()+1)/1_000), "indices", fmt.Sprintf("%dk", (s.IndicesMax()+1)/1_000),
		"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
}

func (s *BaseRoSnapshots) Types() []snaptype.Type { return s.types }
func (s *BaseRoSnapshots) HasType(in snaptype.Type) bool {
	return slices.Contains(s.enums, in.Enum())
}

// ready is an alias for the shared common.Ready type.
type ready = common.Ready

func (s *BaseRoSnapshots) Ready(ctx context.Context) <-chan error {
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
func (s *BaseRoSnapshots) DisableReadAhead() *BaseRoSnapshots {
	v := s.View()
	defer v.Close()

	for _, t := range s.enums {
		for _, sn := range v.segments[t].Segments {
			sn.src.DisableReadAhead()
		}
	}
	return s
}

func (s *BaseRoSnapshots) EnableReadAhead() *BaseRoSnapshots {
	v := s.View()
	defer v.Close()

	for _, t := range s.enums {
		for _, sn := range v.segments[t].Segments {
			sn.src.MadvSequential()
		}
	}

	return s
}
func (s *BaseRoSnapshots) MadvNormal() *BaseRoSnapshots {
	v := s.View()
	defer v.Close()

	for _, t := range s.enums {
		for _, sn := range v.segments[t].Segments {
			sn.src.MadvNormal()
		}
	}

	return s
}

func RecalcVisibleSegments(dirtySegments *btree.BTreeG[*DirtySegment]) VisibleSegments {
	newVisibleSegments := make(VisibleSegments, 0, dirtySegments.Len())
	dirtySegments.Walk(func(segments []*DirtySegment) bool {
		for _, sn := range segments {
			if !sn.IsIndexed() {
				continue
			}

			if len(newVisibleSegments) > 0 {
				last := newVisibleSegments[len(newVisibleSegments)-1].src
				// Same [from,to) range but different version: keep the newer one.
				// Both pass the isSubSetOf check (equal ranges are not subsets), so without
				// this guard both would be appended, causing the gap detector to truncate
				// everything after the duplicate start.
				if last.from == sn.from && last.to == sn.to {
					if last.version.Less(sn.version) {
						newVisibleSegments[len(newVisibleSegments)-1].src = sn
					}
					continue
				}
				// if this indexed segment is fully covered by the last visible
				// segment, skip it. The backward-removal loop below handles the general case,
				// but this avoids unnecessary list mutations for the common subsegment order.
				if sn.isSubSetOf(last) {
					continue
				}
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

// recalcVisibleFiles publishes a fresh visible bundle from dirty and retires `retired`
// (segments removed from dirty during the outgoing bundle's tenure): they are unlinked
// only once every reader pinning that generation has drained. Must be called with
// dirtyLock held, so the caller's dirty mutation and this publish are one atomic step.
func (s *BaseRoSnapshots) recalcVisibleFiles(alignMin bool, retired []*DirtySegment) {
	defer func() {
		s.idxMax.Store(s.idxAvailability())
	}()

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
				visible[t] = VisibleSegments{}
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

	var segmentsMax uint64
	for _, t := range s.enums {
		segs := visible[t]
		minBlock := uint64(math.MaxUint64)
		if len(segs) > 0 {
			minBlock = segs[0].from
			if to := segs[len(segs)-1].to; to > 0 && to-1 > segmentsMax {
				segmentsMax = to - 1
			}
		}
		if u, ok := s.segmentsMinByType[t]; ok {
			u.Store(minBlock)
		}
	}

	next := &snapshotVisible{segments: visible, segmentsMax: segmentsMax}
	old := s.visible.Load()
	old.retired = retired
	old.next = next
	s.visible.Store(next)

	// `recalcVisibleFiles` is rare background operation under `dirtyFilesLock`
	// it's good idea to delete files here, then hot reader-Close path will more likely be lock-free
	closeAndRemoveSegments(s.reclaimRetiredLocked())
}

// acquireVisible pins the current generation. Load and increment are not atomic together,
// so after incrementing we re-check the generation is still current; if superseded mid-pin
// we drop the stale pin and retry (hazard-pointer style).
func (s *BaseRoSnapshots) acquireVisible() *snapshotVisible {
	for {
		v := s.visible.Load()
		v.refcnt.Add(1)
		if s.visible.Load() == v {
			return v
		}
		s.releaseVisible(v)
	}
}

// releaseVisible drops a pin taken by acquireVisible; the last reader of a superseded
// generation triggers reclamation of drained generations' retired files.
func (s *BaseRoSnapshots) releaseVisible(v *snapshotVisible) {
	if v.refcnt.Add(-1) == 0 {
		s.reclaimRetired()
	}
}

// reclaimRetiredLocked walks the oldest->newest chain from the head, collecting the
// retired files of every fully-drained generation older than the current one. Must be
// called with dirtyLock held; the returned files are deleted by the caller off-lock.
func (s *BaseRoSnapshots) reclaimRetiredLocked() (toDelete []*DirtySegment) {
	cur := s.visible.Load()
	for h := s.oldestVisible; h != cur && h.refcnt.Load() == 0; h = h.next {
		toDelete = append(toDelete, h.retired...)
		h.retired = nil
		s.oldestVisible = h.next
	}
	return toDelete
}

func (s *BaseRoSnapshots) reclaimRetired() {
	s.dirtyLock.Lock()
	toDelete := s.reclaimRetiredLocked()
	s.dirtyLock.Unlock()
	closeAndRemoveSegments(toDelete)
}

func closeAndRemoveSegments(segs []*DirtySegment) {
	for _, sn := range segs {
		sn.closeAndRemoveFiles()
	}
}

// minimax of existing indices
func (s *BaseRoSnapshots) idxAvailability() uint64 {
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
	visible := s.visible.Load().segments[s.enums[0]]
	if len(visible) > 0 {
		maxIdx = visible[len(visible)-1].to - 1
	}

	return maxIdx
}

func (s *BaseRoSnapshots) dirtyIdxAvailability(segtype snaptype.Enum) uint64 {
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

func (s *BaseRoSnapshots) visibleIdxAvailability(segtype snaptype.Enum) (maxVisibleIdx uint64) {
	visibleFiles := s.visible.Load().segments[segtype]
	if len(visibleFiles) > 0 {
		maxVisibleIdx = visibleFiles[len(visibleFiles)-1].to - 1
	}

	return
}

func (s *BaseRoSnapshots) Ls() {
	view := s.View()
	defer view.Close()

	var stats seg.Stats
	for _, t := range s.enums {
		for _, sn := range view.segments[t].Segments {
			if sn.src == nil || sn.src.Decompressor == nil {
				continue
			}
			d := sn.src.Decompressor
			log.Info("[snapshots] ", "f", d.FileName(), "words", d.Count(), "dictOnDisk", common.ByteCount(d.SerializedTotalDictSize()), "dictMem", common.ByteCount(d.DictMemSize()))
			stats.Add(d)
		}
	}
	log.Info("[snapshots] total", "words", stats.Words, "dictOnDisk", common.ByteCount(stats.Dict), "dictMem", common.ByteCount(stats.DictMem))
}

func (s *BaseRoSnapshots) Files() (list []string) {
	view := s.View()
	defer view.Close()
	for _, t := range s.enums {
		for _, seg := range view.segments[t].Segments {
			list = append(list, seg.src.FileName())
		}
	}

	return
}

// AllTypedSegments returns the raw, unfiltered list of segment files on disk
// that match the given types. No overlap or gap removal is applied.
func AllTypedSegments(dir string, types []snaptype.Type) (res []snaptype.FileInfo, err error) {
	list, err := snaptype.Segments(dir)
	if err != nil {
		return nil, err
	}

	for _, segType := range types {
		for _, f := range list {
			if f.Type.Enum() != segType.Enum() {
				continue
			}
			res = append(res, f)
		}
	}
	return res, nil
}

func (s *BaseRoSnapshots) openSegments(fileNames []string, open bool, optimistic bool) error {
	wg := &errgroup.Group{}
	wg.SetLimit(estimate.HalfCPUs())
	//fmt.Println("RS", s)
	//defer fmt.Println("Done RS", s)

	// Read full directory listing once for efficient index file lookups
	var dirEntries []string
	if open {
		entries, err := os.ReadDir(s.dir)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("read dir %s: %w", s.dir, err)
		}
		dirEntries = make([]string, 0, len(entries))
		for _, e := range entries {
			if !e.IsDir() {
				dirEntries = append(dirEntries, e.Name())
			}
		}
	}

	for _, fName := range fileNames {
		f, isState, ok := snaptype.ParseFileName(s.dir, fName)
		if !ok || isState || snaptype.IsTorrentPartial(f.Ext) {
			continue
		}
		if !s.HasType(f.Type) {
			continue
		}
		if s.isInProgress(f.Type.Enum(), f.From, f.To) {
			continue
		}

		segtype := s.dirty[f.Type.Enum()]
		if segtype == nil {
			log.Debug("[snapshot] rebuildSegments: unknown type", "t", f.Type.Enum().String())
			continue
		}

		sn, exists := FindOpenSegment(segtype, fName)
		if !exists {
			sn = &DirtySegment{segType: f.Type, version: f.Version, Range: Range{f.From, f.To}, frozen: s.snCfg.IsFrozen(f)}
		}

		if open {
			if err := sn.Open(s.dir); err != nil {
				stop, failErr := ClassifyOpenErr(err, optimistic)
				if failErr != nil {
					return failErr
				}
				if stop {
					break
				}
				continue
			}
		}

		if !exists {
			// it's possible to iterate over .seg file even if you don't have index
			// then make segment available even if index open may fail
			segtype.Set(sn)
		}

		if open {
			wg.Go(func() error {
				if err := sn.OpenIdxIfNeed(s.dir, optimistic, dirEntries); err != nil {
					return err
				}
				return nil
			})
		}

	}

	if err := wg.Wait(); err != nil {
		return err
	}

	return nil
}

func (s *BaseRoSnapshots) Ranges(align bool) []Range {
	view := s.View()
	defer view.Close()
	return view.Ranges(align)
}

func (s *BaseRoSnapshots) OptimisticalyOpenFolder() { _ = s.OpenFolder() }
func (s *BaseRoSnapshots) OpenFolder() error {
	var retired []*DirtySegment
	err := func() error {
		s.dirtyLock.Lock()
		defer s.dirtyLock.Unlock()
		// Publish under the same lock (and even on error), else the detached segments leak:
		// removed from dirty, never closed, never reclaimed.
		defer func() { s.recalcVisibleFiles(s.alignMin, retired) }()

		files, err := AllTypedSegments(s.dir, s.Types())
		if err != nil {
			return err
		}

		list := make([]string, 0, len(files))
		for _, f := range files {
			_, fName := filepath.Split(f.Path)
			list = append(list, fName)
		}
		// Segments whose file vanished from disk leave the visible set; retire them so
		// their fds close only after readers of the current generation drain.
		retired = s.detachNotInList(list)
		return s.openSegments(list, true, false)
	}()
	if err != nil {
		return fmt.Errorf("OpenFolder: %w", err)
	}

	wasReady := s.segmentsReady.Swap(true)
	if !wasReady {
		if s.downloadReady.Load() {
			s.ready.Set()
		}
	}
	return nil
}

func (s *BaseRoSnapshots) OpenSegments(types []snaptype.Type, alignMin bool) error {
	s.dirtyLock.Lock()
	defer s.dirtyLock.Unlock()
	defer s.recalcVisibleFiles(alignMin, nil)

	files, err := AllTypedSegments(s.dir, types)

	if err != nil {
		return err
	}
	list := make([]string, 0, len(files))
	for _, f := range files {
		_, fName := filepath.Split(f.Path)
		list = append(list, fName)
	}

	// Do not call detachNotInList(list) here. list only contains segments of the
	// requested types; calling it would drop all other types (e.g. Transactions) from dirty.
	// Stale entries for the requested types are cleaned by the next OpenFolder call.
	if err := s.openSegments(list, true, false); err != nil {
		return err
	}
	return nil
}

func (s *BaseRoSnapshots) Close() {
	if s == nil {
		return
	}
	s.dirtyLock.Lock()
	defer s.dirtyLock.Unlock()

	detached := s.detachNotInList(nil)

	// Publish the empty generation before reading the outgoing one's refcnt, so a concurrent
	// lock-free View() re-check fails its pin on it and retries onto the empty generation.
	prev := s.visible.Load()
	s.recalcVisibleFiles(s.alignMin, nil)

	// Close fds only when no reader still pins the outgoing generation; closing a segment
	// a live View holds would nil its Decompressor out from under that reader. At shutdown
	// leaking the fds is preferable to that use-after-close.
	if prev != nil && prev.refcnt.Load() != 0 {
		s.logger.Warn("[snapshots] Close called with live readers; leaving fds open", "refcnt", prev.refcnt.Load())
	} else {
		for _, sn := range detached {
			sn.close()
		}
	}
}

// detachNotInList removes from dirty every segment whose file name is not in `protect`
// and returns them without closing; the caller owns closing or retiring them. Must be
// called with dirtyLock held.
func (s *BaseRoSnapshots) detachNotInList(protect []string) []*DirtySegment {
	protectFiles := make(map[string]struct{}, len(protect))
	for _, f := range protect {
		protectFiles[f] = struct{}{}
	}
	total := 0
	for _, t := range s.enums {
		total += s.dirty[t].Len()
	}
	detached := make([]*DirtySegment, 0, total)
	for _, t := range s.enums {
		toDelete := make([]*DirtySegment, 0, s.dirty[t].Len())
		s.dirty[t].Walk(func(segs []*DirtySegment) bool {
			for _, seg := range segs {
				if _, ok := protectFiles[seg.FileName()]; ok {
					continue
				}
				toDelete = append(toDelete, seg)
			}
			return true
		})
		for _, seg := range toDelete {
			s.dirty[t].Delete(seg)
		}
		detached = append(detached, toDelete...)
	}
	return detached
}

// CloseSegmentsNotInList closes and drops tree segments whose file name is not
// in protectFiles.
func CloseSegmentsNotInList(tree *btree.BTreeG[*DirtySegment], protectFiles map[string]struct{}) {
	closeAndDropNotProtected(tree, protectFiles, (*DirtySegment).FileName)
}

func closeAndDropNotProtected(tree *btree.BTreeG[*DirtySegment], protectFiles map[string]struct{}, nameOf func(*DirtySegment) string) {
	var toClose []*DirtySegment
	tree.Walk(func(segs []*DirtySegment) bool {
		for _, seg := range segs {
			if _, ok := protectFiles[nameOf(seg)]; ok {
				continue
			}
			toClose = append(toClose, seg)
		}
		return true
	})
	for _, delSeg := range toClose {
		delSeg.close()
		tree.Delete(delSeg)
	}
}

// FindOpenSegment returns tree's already-open segment named fName, if any.
func FindOpenSegment(tree *btree.BTreeG[*DirtySegment], fName string) (*DirtySegment, bool) {
	return findOpenSegment(tree, func(sn *DirtySegment) bool { return sn.FileName() == fName })
}

func findOpenSegment(tree *btree.BTreeG[*DirtySegment], match func(*DirtySegment) bool) (sn *DirtySegment, ok bool) {
	tree.Walk(func(segs []*DirtySegment) bool {
		for _, sn2 := range segs {
			if sn2.Decompressor == nil { // it's ok if some segment was not able to open
				continue
			}
			if match(sn2) {
				sn, ok = sn2, true
				return false
			}
		}
		return true
	})
	return sn, ok
}

// ClassifyOpenErr says how a snapshot-listing loop proceeds after a segment
// open error: stop the whole listing (stop), fail hard (failErr), or, when
// neither, skip just this file.
func ClassifyOpenErr(err error, optimistic bool) (stop bool, failErr error) {
	if errors.Is(err, os.ErrNotExist) {
		return !optimistic, nil
	}
	if optimistic {
		return false, nil
	}
	return false, err
}

func (s *BaseRoSnapshots) RemoveOverlaps(onDelete func(l []string) error) error {
	list, err := snaptype.Segments(s.dir)
	if err != nil {
		return err
	}
	keepSegments, segmentsToRemove := findOverlaps(list)

	keepNames := make([]string, 0, len(keepSegments))
	for _, info := range keepSegments {
		keepNames = append(keepNames, info.Name())
	}

	// Notify the seeder before deletion. Includes idx overlaps whose .seg is already gone
	// (kill between deletes): those have no DirtySegment, so reclamation can't reach them.
	if onDelete != nil {
		idxList, err := snaptype.IdxFiles(s.dir)
		if err != nil {
			return err
		}
		_, accessorsToRemove := findOverlaps(idxList)
		toRemove := make([]string, 0, len(segmentsToRemove)+len(accessorsToRemove))
		for _, info := range segmentsToRemove {
			toRemove = append(toRemove, info.Path)
		}
		for _, info := range accessorsToRemove {
			toRemove = append(toRemove, info.Path)
		}
		relativePaths, err := toRelativePaths(s.dir, toRemove)
		if err != nil {
			return err
		}
		if err := onDelete(relativePaths); err != nil {
			return fmt.Errorf("onDelete: %w", err)
		}
	}

	// Retire the subsumed segments. The pin taken here mirrors the Aggregator's
	// cleanAfterMerge: recalc's reclaim skips the generation it holds, and v.Close drops
	// the pin so the actual unlink (of each segment's .seg + indexes) happens off-lock —
	// or defers to the true watermark if another reader still holds the generation.
	v := s.View()
	defer v.Close()
	func() {
		s.dirtyLock.Lock()
		defer s.dirtyLock.Unlock()
		retired := s.detachNotInList(keepNames)
		s.recalcVisibleFiles(s.alignMin, retired)
	}()

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

func (s *BaseRoSnapshots) BuildMissedIndices(ctx context.Context, logPrefix string, notifier snapshotNotifier, dirs datadir.Dirs, cc *chain.Config, logger log.Logger) error {
	if !s.Cfg().ProduceE2 && s.IndicesMax() == 0 {
		if s.SegmentsMax() == 0 {
			return nil
		}
		return errors.New("please remove --snap.stop, erigon can't work without creating basic indices")
	}
	if !s.Cfg().ProduceE2 {
		return nil
	}
	if !s.SegmentsReady() {
		return fmt.Errorf("not all snapshot segments are available: segments max=%d, indices max=%d, download ready=%t",
			s.SegmentsMax(), s.IndicesMax(), s.DownloadReady())
	}

	// wait for Downloader service to download all expected snapshots
	indexWorkers := estimate.IndexSnapshot.Workers()
	newIdxBuilt, err := s.buildMissedIndices(logPrefix, ctx, dirs, cc, indexWorkers, logger)
	if err != nil {
		return fmt.Errorf("can't build missed indices: %w", err)
	}

	if newIdxBuilt {
		if err := s.OpenFolder(); err != nil {
			return err
		}
		s.LogStat("missed-idx:open")
	}
	if notifier != nil {
		notifier.OnNewSnapshot()
	}
	return nil
}

// delete removes the named segment from dirty and returns it (nil if not found) so
// the caller can retire it. Physical unlink happens later, at the reader watermark. Must
// be called with dirtyLock held.
func (s *BaseRoSnapshots) delete(fileName string) *DirtySegment {
	var delSeg *DirtySegment
	var dirtySegments *btree.BTreeG[*DirtySegment]

	_, fName := filepath.Split(fileName)
	for _, t := range s.enums {
		found := false
		s.dirty[t].Walk(func(segs []*DirtySegment) bool {
			for _, sn := range segs {
				if sn.Decompressor == nil {
					continue
				}
				if sn.FileName() != fName {
					continue
				}
				delSeg = sn
				dirtySegments = s.dirty[t]
				found = true
				return false
			}
			return true
		})
		if found {
			break
		}
	}
	if delSeg == nil || dirtySegments == nil {
		// Deletion is intentionally idempotent because a snapshot may already
		// have been removed from the in-memory set by another pruning path.
		return nil
	}
	dirtySegments.Delete(delSeg)
	return delSeg
}

// prune visible segments
func (s *BaseRoSnapshots) Delete(fileNames ...string) error {
	if s == nil {
		return nil
	}

	s.dirtyLock.Lock()
	defer s.dirtyLock.Unlock()

	var retired []*DirtySegment
	for _, fileName := range fileNames {
		if sn := s.delete(fileName); sn != nil {
			retired = append(retired, sn)
		}
	}
	s.recalcVisibleFiles(s.alignMin, retired)
	return nil
}

func (s *BaseRoSnapshots) buildMissedIndices(logPrefix string, ctx context.Context, dirs datadir.Dirs, chainConfig *chain.Config, workers int, logger log.Logger) (newIdxBuilt bool, err error) {
	if s == nil {
		return
	}

	if _, err = snaptype.GetIndexSalt(dirs.Snap, logger); err != nil {
		return
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
				if segment.IsIndexed() {
					continue
				}
				// Skip if a merge/dump/another recovery is already building this
				// (type, range); otherwise claim it so we don't double-build.
				from, to := segment.From(), segment.To()
				if !s.TryAcquireRange(t, from, to) {
					continue
				}
				info := segment.FileInfo(dir)

				newIdxBuilt = true

				segment.closeIdx()

				indexBuilder := s.IndexBuilder(t.Type())

				g.Go(func() error {
					defer s.ReleaseRange(t, from, to)
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
		if err = g.Wait(); err != nil {
			return
		}
		return newIdxBuilt, ie
	case <-ctx.Done():
		return newIdxBuilt, ctx.Err()
	}
}

type View struct {
	s           *BaseRoSnapshots
	visible     *snapshotVisible // the pinned generation; released once by Close
	segments    []*RoTx
	baseSegType snaptype.Type
}

func (s *BaseRoSnapshots) View() *View {
	v := s.acquireVisible()
	sgs := make([]*RoTx, snaptype.MaxEnum)
	for _, t := range s.enums {
		sgs[t] = v.segments[t].BeginRo() // non-owning children; the View owns the single pin
	}
	return &View{s: s, visible: v, segments: sgs, baseSegType: snaptype2.Transactions} // Transactions is the last segment to be processed, so it's the most reliable.
}

func (v *View) Close() {
	if v == nil || v.s == nil {
		return
	}
	v.s.releaseVisible(v.visible)
	v.s = nil
}

// WithBaseSegType returns a shallow copy sharing the same single pin; only the copy is
// Closed, so the pin stays balanced. It must not re-acquire — that would leak a pin.
func (s *View) WithBaseSegType(t snaptype.Type) *View {
	v := *s
	v.baseSegType = t
	return &v
}

var noop = func() {}

func (s *BaseRoSnapshots) ViewType(t snaptype.Type) *RoTx {
	v := s.acquireVisible()
	rotx := v.segments[t.Enum()].BeginRo()
	rotx.release = func() { s.releaseVisible(v) }
	return rotx
}

func (s *BaseRoSnapshots) ViewSingleFile(t snaptype.Type, blockNum uint64) (segment *VisibleSegment, ok bool, close func()) {
	segmentRotx := s.ViewType(t)

	for _, seg := range segmentRotx.Segments {
		if !(blockNum >= seg.from && blockNum < seg.to) {
			continue
		}
		return seg, true, segmentRotx.Close
	}
	segmentRotx.Close()
	return nil, false, noop
}

func (v *View) Segments(t snaptype.Type) VisibleSegments {
	return v.segments[t.Enum()].Segments
}

func (v *View) Segment(t snaptype.Type, blockNum uint64) (*VisibleSegment, bool) {
	for _, seg := range v.segments[t.Enum()].Segments {
		if !(blockNum >= seg.from && blockNum < seg.to) {
			continue
		}
		return seg, true
	}
	return nil, false
}

func (v *View) Ranges(align bool) (ranges []Range) {
	if !align {
		for _, sn := range v.Segments(v.baseSegType) {
			ranges = append(ranges, sn.Range)
		}

		return ranges
	}

	var alignedRangeTo *uint64

	for _, t := range v.s.types {
		maxRangeTo := uint64(0)

		for _, sn := range v.Segments(t) {
			if sn.Range.to > maxRangeTo {
				maxRangeTo = sn.Range.to
			}
		}

		if alignedRangeTo == nil {
			alignedRangeTo = &maxRangeTo
			continue
		}

		if maxRangeTo < *alignedRangeTo {
			alignedRangeTo = &maxRangeTo
		}
	}

	for _, sn := range v.Segments(v.baseSegType) {
		if alignedRangeTo != nil && sn.Range.to > *alignedRangeTo {
			continue
		}

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
			if f.Type != nil && f.Type.Enum() != snaptype.CaplinEnums.BeaconBlocks && f.Type.Enum() != snaptype.CaplinEnums.BlobSidecars {
				continue
			}
			if f.Type != nil && f.Type.Enum() == snaptype.CaplinEnums.BlobSidecars {
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
