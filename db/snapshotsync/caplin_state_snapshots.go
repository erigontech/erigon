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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/tidwall/btree"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/node/ethconfig"
)

func BeaconSimpleIdx(ctx context.Context, sn snaptype.FileInfo, salt uint32, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
	num := make([]byte, binary.MaxVarintLen64)
	cfg := recsplit.RecSplitArgs{
		Enums:      true,
		BucketSize: recsplit.DefaultBucketSize,
		LeafSize:   recsplit.DefaultLeafSize,
		TmpDir:     tmpDir,
		Salt:       &salt,
		BaseDataID: sn.From,
	}
	if err := snaptype.BuildIndex(ctx, sn, version.Versions{
		Current:      sn.Version,
		MinSupported: sn.Version,
	}, cfg, log.LvlDebug, p, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
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

func getKvGetterForStateTable(db kv.RoDB, tableName string) KeyValueGetter {
	return func(numId uint64) ([]byte, []byte, error) {
		var key, value []byte
		var err error
		if err := db.View(context.TODO(), func(tx kv.Tx) error {
			key = base_encoding.Encode64ToBytes4(numId)
			value, err = tx.GetOne(tableName, key)
			value = common.Copy(value)
			return err
		}); err != nil {
			return nil, nil, err
		}
		return key, value, nil
	}
}

func MakeCaplinStateSnapshotsTypes(db kv.RoDB) SnapshotTypes {
	getters := make(map[CaplinStateType]KeyValueGetter, caplinStateTypeCount)
	for t := CaplinStateType(0); t < caplinStateTypeCount; t++ {
		getters[t] = getKvGetterForStateTable(db, t.String())
	}
	return SnapshotTypes{
		KeyValueGetters: getters,
		Compression:     map[CaplinStateType]bool{},
	}
}

// value: chunked(ssz(SignedBeaconBlocks))
// slot       -> beacon_slot_segment_offset

type CaplinStateSnapshots struct {
	indicesReady  atomic.Bool
	segmentsReady atomic.Bool

	Salt uint32

	// dirtyLock guards the dirty tree and the generation chain (publish/reclaim); the
	// generation core shares it so a dirty mutation and its publish are one atomic step.
	dirtyLock     sync.RWMutex
	dirty         map[CaplinStateType]*btree.BTreeG[*DirtySegment] // ordered map type -> DirtySegments
	_visibleFiles visibleGenerations[caplinStateVisible]
	types         []CaplinStateType // configured types (sorted), immutable after construction

	snapshotTypes SnapshotTypes

	dir         string
	tmpdir      string
	segmentsMax atomic.Uint64 // all types of .seg files are available - up to this number; snapshotted into the published payload
	cfg         ethconfig.BlocksFreezing
	logger      log.Logger
	// chain cfg
	beaconCfg *clparams.BeaconChainConfig
}

type caplinStateVisible struct {
	segments    []VisibleSegments // enum-indexed: CaplinStateType -> VisibleSegments
	segmentsMax uint64            // max .seg height across all types
	idxMax      uint64            // min visible .idx height across configured types
}

type KeyValueGetter func(numId uint64) ([]byte, []byte, error)

type SnapshotTypes struct {
	KeyValueGetters map[CaplinStateType]KeyValueGetter
	Compression     map[CaplinStateType]bool
}

// NewCaplinStateSnapshots - opens all snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewCaplinStateSnapshots(cfg ethconfig.BlocksFreezing, beaconCfg *clparams.BeaconChainConfig, dirs datadir.Dirs, snapshotTypes SnapshotTypes, logger log.Logger) *CaplinStateSnapshots {
	if cfg.ChainName == "" {
		log.Debug("[dbg] NewCaplinSnapshots created with empty ChainName", "stack", dbg.Stack())
	}

	dirty := make(map[CaplinStateType]*btree.BTreeG[*DirtySegment])
	types := make([]CaplinStateType, 0, len(snapshotTypes.KeyValueGetters))
	for k := range snapshotTypes.KeyValueGetters {
		dirty[k] = btree.NewBTreeGOptions[*DirtySegment](DirtySegmentLess, btree.Options{Degree: 128, NoLocks: false})
		types = append(types, k)
	}
	sort.Slice(types, func(i, j int) bool { return types[i] < types[j] })

	c := &CaplinStateSnapshots{snapshotTypes: snapshotTypes, dir: dirs.SnapCaplin, tmpdir: dirs.Tmp, cfg: cfg, dirty: dirty, types: types, logger: logger, beaconCfg: beaconCfg}
	c._visibleFiles.init(&c.dirtyLock, caplinStateVisible{segments: make([]VisibleSegments, caplinStateTypeCount)})

	c.dirtyLock.Lock()
	c.recalcVisibleFiles(nil)
	c.dirtyLock.Unlock()
	return c
}

func (s *CaplinStateSnapshots) IndicesMax() uint64  { return s._visibleFiles.current().idxMax }
func (s *CaplinStateSnapshots) SegmentsMax() uint64 { return s._visibleFiles.current().segmentsMax }

func (s *CaplinStateSnapshots) LogStat(str string) {
	s.logger.Info(fmt.Sprintf("[snapshots:%s] Stat", str),
		"blocks", common.PrettyCounter(s.SegmentsMax()+1), "indices", common.PrettyCounter(s.IndicesMax()+1))
}

func (s *CaplinStateSnapshots) LS() {
	if s == nil {
		return
	}
	view := s.View()
	defer view.Close()

	var stats seg.Stats
	for _, segs := range view.visible.payload.segments {
		for _, sn := range segs {
			d := sn.src.Decompressor
			s.logger.Info("[agg] ", "f", d.FileName(), "words", d.Count(), "dictOnDisk", common.ByteCount(d.SerializedTotalDictSize()), "dictMem", common.ByteCount(d.DictMemSize()))
			stats.Add(d)
		}
	}
	s.logger.Info("[agg] total", "words", stats.Words, "dictOnDisk", common.ByteCount(stats.Dict), "dictMem", common.ByteCount(stats.DictMem))
}

func (s *CaplinStateSnapshots) SegFileNames(from, to uint64) []string {
	view := s.View()
	defer view.Close()

	var res []string

	for _, segs := range view.visible.payload.segments {
		for _, seg := range segs {
			if seg.from >= to || seg.to <= from {
				continue
			}
			res = append(res, seg.src.filePath)
		}
	}
	return res
}

func (s *CaplinStateSnapshots) BlocksAvailable() uint64 {
	p := s._visibleFiles.current()
	return min(p.segmentsMax, p.idxMax)
}

func (s *CaplinStateSnapshots) coveredRangesForType(name CaplinStateType) []Range {
	segments := s._visibleFiles.current().segments
	if int(name) < 0 || int(name) >= len(segments) {
		return nil
	}
	segs := segments[name]
	ranges := make([]Range, 0, len(segs))
	for _, seg := range segs {
		ranges = append(ranges, seg.Range)
	}
	return ranges
}

// Close tears down the snapshot set on shutdown: it detaches every segment from dirty,
// publishes an empty generation, then closes their fds directly — but only if no reader still
// pins the outgoing generation. It must never unlink, or a normal shutdown would delete the
// whole on-disk state snapshot set.
func (s *CaplinStateSnapshots) Close() {
	if s == nil {
		return
	}
	s.dirtyLock.Lock()
	defer s.dirtyLock.Unlock()

	detached := s.detachNotInList(nil)
	prev := s._visibleFiles.visible.Load()
	s.recalcVisibleFiles(nil)

	if prev != nil && prev.refcnt.Load() != 0 {
		s.logger.Warn("[caplin-state] Close called with live readers; leaving fds open", "refcnt", prev.refcnt.Load())
	} else {
		for _, sn := range detached {
			sn.close()
		}
	}
}

func (s *CaplinStateSnapshots) openSegIfNeed(sn *DirtySegment, filepath string) error {
	if sn.Decompressor != nil {
		return nil
	}
	var err error
	sn.Decompressor, err = seg.NewDecompressor(filepath)
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, filepath)
	}
	return nil
}

// OpenList stops on optimistic=false, continue opening files on optimistic=true
func (s *CaplinStateSnapshots) OpenList(fileNames []string, optimistic bool) error {
	s.dirtyLock.Lock()
	defer s.dirtyLock.Unlock()

	// Detach segments that are in dirty but no longer in the list (in production the list is the
	// full dir scan, so these are files already gone from disk) and retire them: their fds close
	// and the already-absent file is unlink-no-op'd once readers of the current generation drain.
	// One publish under the lock, after all new files are opened, so no reader ever sees a
	// transient set with stale gone but new files not yet visible.
	retired := s.detachNotInList(fileNames)
	defer s.recalcVisibleFiles(retired) // LIFO: runs before Unlock, so publish holds the lock

	var segmentsMax uint64
	var segmentsMaxSet bool
	for _, fName := range fileNames {
		f, _, _ := snaptype.ParseFileName(s.dir, fName)

		typ, ok := ParseCaplinStateType(f.CaplinTypeString)
		if !ok {
			continue
		}
		dirtySegments, ok := s.dirty[typ]
		if !ok {
			continue
		}
		filePath := filepath.Join(s.dir, fName)
		sn, exists := findOpenSegment(dirtySegments, func(sn2 *DirtySegment) bool { return sn2.filePath == filePath })
		if !exists {
			sn = &DirtySegment{
				// segType: f.Type, Unsupported
				version:  f.Version,
				Range:    Range{f.From, f.To},
				frozen:   true,
				filePath: filePath,
			}
		}
		if err := s.openSegIfNeed(sn, filePath); err != nil {
			stop, failErr := ClassifyOpenErr(err, optimistic)
			if failErr != nil {
				return failErr
			}
			if stop {
				break
			}
			if !errors.Is(err, os.ErrNotExist) {
				s.logger.Warn("[snapshots] open segment", "err", err)
			}
			continue
		}

		if !exists {
			// it's possible to iterate over .seg file even if you don't have index
			// then make segment available even if index open may fail
			dirtySegments.Set(sn)
		}
		if err := openIdxForCaplinStateIfNeeded(sn, filePath, optimistic); err != nil {
			return err
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

func openIdxForCaplinStateIfNeeded(s *DirtySegment, filePath string, optimistic bool) error {
	if s.Decompressor == nil {
		return nil
	}
	err := openIdxIfNeedForCaplinState(s, filePath)
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

func openIdxIfNeedForCaplinState(s *DirtySegment, filePath string) (err error) {
	if len(s.indexes) > 0 && s.indexes[0] != nil {
		return nil
	}

	s.indexes = make([]*recsplit.Index, 1)

	// Swap only the trailing extension — the datadir path itself may contain ".seg".
	filePath = strings.TrimSuffix(filePath, ".seg") + ".idx"
	index, err := recsplit.OpenIndex(filePath)
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, filePath)
	}

	s.indexes[0] = index

	return nil
}

func isIndexed(s *DirtySegment) bool {
	if s.Decompressor == nil {
		return false
	}

	for _, idx := range s.indexes {
		if idx == nil {
			return false
		}
	}
	return true
}

// recalcVisibleFiles builds a fresh enum-indexed visible bundle from dirty and publishes it
// as the new generation. Must be called with dirtyLock held, so the caller's dirty mutation
// and this publish are one atomic step. `retired` are files removed from dirty during the
// outgoing generation's tenure; they are closed and unlinked once no reader pins that
// generation. Ordinary visibility replacement passes nil (it never removes files).
func (s *CaplinStateSnapshots) recalcVisibleFiles(retired []*DirtySegment) {
	getNewVisibleSegments := func(dirtySegments *btree.BTreeG[*DirtySegment]) VisibleSegments {
		newVisibleSegments := make(VisibleSegments, 0, dirtySegments.Len())
		dirtySegments.Walk(func(segments []*DirtySegment) bool {
			for _, sn := range segments {
				// An un-indexed segment (a .seg published before its .idx) must never
				// become visible: it has no index to serve a read yet would shadow the DB
				// for its range. This gate is what keeps the publish-before-index window safe.
				if !isIndexed(sn) {
					continue
				}
				if n := len(newVisibleSegments); n > 0 && sn.isSubSetOf(newVisibleSegments[n-1].src) {
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

	segments := make([]VisibleSegments, caplinStateTypeCount)
	for _, t := range s.types {
		segments[t] = getNewVisibleSegments(s.dirty[t])
	}

	s._visibleFiles.publish(caplinStateVisible{
		segments:    segments,
		segmentsMax: s.segmentsMax.Load(),
		idxMax:      idxAvailabilityFrom(segments, s.types),
	}, retired)
	s.indicesReady.Store(true)
}

// RemoveOverlaps retires state segment files that are fully covered by a larger indexed
// segment of the same type, so the on-disk publishable check stops flagging them as
// overlapping. It hands the covered subsets to publish as retired; the drain-gated reclaim does
// the close+unlink once no reader pins the outgoing generation — it never closes or removes
// anything itself. The temp View pin taken here forces the drain, so with no other reader the
// covered files are unlinked by the time this returns.
func (s *CaplinStateSnapshots) RemoveOverlaps() error {
	if s == nil {
		return nil
	}

	v := s.View()
	defer v.Close()
	func() {
		s.dirtyLock.Lock()
		defer s.dirtyLock.Unlock()
		s.recalcVisibleFiles(s.detachOverlappedSubsets())
	}()
	return nil
}

// detachOverlappedSubsets removes from dirty and returns for retirement every segment fully
// covered by a larger indexed segment of the same type. Coverage is index-aware: an un-indexed
// superset can't serve reads, so a subset it covers is kept until the superset is indexed,
// while a covered subset is dropped regardless of its own index state. Must be called with
// dirtyLock held.
func (s *CaplinStateSnapshots) detachOverlappedSubsets() []*DirtySegment {
	var retired []*DirtySegment //nolint:prealloc // sparse subset of dirty; full-size prealloc would over-allocate
	for _, dirtySegments := range s.dirty {
		var indexed []*DirtySegment
		dirtySegments.Walk(func(segments []*DirtySegment) bool {
			for _, sn := range segments {
				if isIndexed(sn) {
					indexed = append(indexed, sn)
				}
			}
			return true
		})

		var rm []*DirtySegment
		dirtySegments.Walk(func(segments []*DirtySegment) bool {
			for _, sn := range segments {
				for _, sup := range indexed {
					if sn != sup && sn.isSubSetOf(sup) {
						rm = append(rm, sn)
						break
					}
				}
			}
			return true
		})

		for _, sn := range rm {
			dirtySegments.Delete(sn)
		}
		retired = append(retired, rm...)
	}
	return retired
}

// idxAvailabilityFrom computes the min visible .idx height across the configured types of a
// candidate segment bundle (I4: read from the candidate about to be published, never from the
// already-published generation). A configured type with no visible segment caps availability at 0.
func idxAvailabilityFrom(segments []VisibleSegments, types []CaplinStateType) uint64 {
	min := uint64(math.MaxUint64)
	for _, t := range types {
		segs := segments[t]
		if len(segs) == 0 {
			return 0
		}
		if segs[len(segs)-1].to < min {
			min = segs[len(segs)-1].to
		}
	}
	if min == math.MaxUint64 {
		return 0
	}
	return min
}

func listAllSegFilesInDir(dir string) []string {
	files, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	list := make([]string, 0, len(files))
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		// check if it's a .seg file
		if filepath.Ext(f.Name()) != ".seg" {
			continue
		}
		list = append(list, f.Name())
	}
	return list
}

func (s *CaplinStateSnapshots) OpenFolder() error {
	return s.OpenList(listAllSegFilesInDir(s.dir), false)
}

// detachNotInList removes from dirty every segment whose base file name is not in `protect`
// and returns them WITHOUT closing; the caller decides their disposition (unlink or close) via
// publish, so a segment a live view still pins is not torn down under the reader. Must be
// called with dirtyLock held.
func (s *CaplinStateSnapshots) detachNotInList(protect []string) []*DirtySegment {
	protectFiles := make(map[string]struct{}, len(protect))
	for _, fName := range protect {
		protectFiles[fName] = struct{}{}
	}

	total := 0
	for _, dirtySegments := range s.dirty {
		total += dirtySegments.Len()
	}
	detached := make([]*DirtySegment, 0, total)
	for _, dirtySegments := range s.dirty {
		var toDelete []*DirtySegment
		dirtySegments.Walk(func(segments []*DirtySegment) bool {
			for _, sn := range segments {
				// sn.filePath, not the promoted Decompressor.FilePath(): the field is
				// set for every tree member, incl. stubs whose Decompressor is nil.
				_, name := filepath.Split(sn.filePath)
				if _, ok := protectFiles[name]; ok {
					continue
				}
				toDelete = append(toDelete, sn)
			}
			return true
		})
		for _, sn := range toDelete {
			dirtySegments.Delete(sn)
		}
		detached = append(detached, toDelete...)
	}
	return detached
}

type CaplinStateView struct {
	s       *CaplinStateSnapshots
	visible *generation[caplinStateVisible] // the pinned generation; released once by Close
	closed  bool
}

func (s *CaplinStateSnapshots) View() *CaplinStateView {
	if s == nil {
		return nil
	}
	return &CaplinStateView{s: s, visible: s._visibleFiles.acquire()}
}

func (v *CaplinStateView) Close() {
	if v == nil {
		return
	}
	if v.closed {
		return
	}
	v.s._visibleFiles.release(v.visible)
	v.s = nil
	v.visible = nil
	v.closed = true
}

func (v *CaplinStateView) VisibleSegments(tbl CaplinStateType) VisibleSegments {
	if v == nil || v.visible == nil {
		return nil
	}
	segs := v.visible.payload.segments
	if int(tbl) < 0 || int(tbl) >= len(segs) {
		return nil
	}
	return segs[tbl]
}

func (v *CaplinStateView) VisibleSegment(slot uint64, tbl CaplinStateType) (*VisibleSegment, bool) {
	for _, seg := range v.VisibleSegments(tbl) {
		if !(slot >= seg.from && slot < seg.to) {
			continue
		}
		return seg, true
	}
	return nil, false
}

// errIncompleteStateRange signals that a mandatory-dense state table (block/state
// roots) has a missing entry in the range being dumped, so the range must not be
// frozen yet.
var errIncompleteStateRange = errors.New("state range not fully reconstructed")

func dumpCaplinState(ctx context.Context, snapName string, kvGetter KeyValueGetter, fromSlot uint64, toSlot, blocksPerFile uint64, salt uint32, dirs datadir.Dirs, workers int, lvl log.Lvl, logger log.Logger, compress bool) error {
	tmpDir, snapDir := dirs.Tmp, dirs.SnapCaplin

	segName := snaptype.BeaconBlocks.FileName(version.ZeroVersion, fromSlot, toSlot)
	// a little bit ugly.
	segName = strings.ReplaceAll(segName, "beaconblocks", snapName)
	f, _, _ := snaptype.ParseFileName(snapDir, segName)

	compressCfg := seg.DefaultCfg
	compressCfg.Workers = workers
	sn, err := seg.NewCompressor(ctx, "Snapshots "+snapName, f.Path, tmpDir, compressCfg, lvl, logger)
	if err != nil {
		return err
	}
	defer sn.Close()

	// block_roots/state_roots are written every slot; an empty entry means the DB
	// range isn't fully reconstructed. Freezing it writes a blank word that then
	// permanently shadows the DB (snapshots take read precedence), so refuse.
	mustBeDense := snapName == kv.BlockRoot || snapName == kv.StateRoot

	// Generate .seg file, which is just the list of beacon blocks.
	for i := fromSlot; i < toSlot; i++ {
		// read root.
		_, dump, err := kvGetter(i)
		if err != nil {
			return err
		}
		if mustBeDense && len(dump) != length.Hash {
			// An empty entry is a not-yet-reconstructed slot (retry later); a
			// non-empty entry of the wrong length is corruption (surface it).
			if len(dump) != 0 {
				return fmt.Errorf("%s slot %d: corrupt root, %d bytes (want %d)", snapName, i, len(dump), length.Hash)
			}
			return fmt.Errorf("%w: %s slot %d", errIncompleteStateRange, snapName, i)
		}
		if i%20_000 == 0 {
			logger.Log(lvl, "Dumping "+snapName, "progress", i)
		}
		if compress {
			if err := sn.AddWord(dump); err != nil {
				return err
			}
		} else {
			if err := sn.AddUncompressedWord(dump); err != nil {
				return err
			}
		}
	}
	if sn.Count() != int(blocksPerFile) {
		return fmt.Errorf("expected %d blocks, got %d", blocksPerFile, sn.Count())
	}
	if err := sn.Compress(); err != nil {
		return err
	}
	// Generate .idx file, which is the slot => offset mapping.
	p := &background.Progress{}

	return simpleIdx(ctx, f, salt, tmpDir, p, lvl, logger)
}

func simpleIdx(ctx context.Context, sn snaptype.FileInfo, salt uint32, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
	num := make([]byte, binary.MaxVarintLen64)
	cfg := recsplit.RecSplitArgs{
		Enums:      true,
		BucketSize: recsplit.DefaultBucketSize,
		LeafSize:   recsplit.DefaultLeafSize,
		TmpDir:     tmpDir,
		Salt:       &salt,
		BaseDataID: sn.From,
	}
	if err := snaptype.BuildIndexWithSnapName(ctx, sn, cfg, log.LvlDebug, p, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		if i%20_000 == 0 {
			logger.Log(lvl, "Generating idx for "+sn.Name(), "progress", i)
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

type caplinStateDumpJob struct {
	name     string
	from, to uint64
}

// missingRanges returns the sub-ranges of [0, toSlot) not covered by `covered`
// (the type's existing segment ranges, sorted by `from`).
func missingRanges(covered []Range, toSlot uint64) []Range {
	var missing []Range
	var cur uint64
	for _, r := range covered {
		if r.from > cur {
			gapEnd := min(r.from, toSlot)
			missing = append(missing, Range{from: cur, to: gapEnd})
		}
		cur = max(cur, r.to)
		if cur >= toSlot {
			return missing
		}
	}
	if cur < toSlot {
		missing = append(missing, Range{from: cur, to: toSlot})
	}
	return missing
}

// planStateDump schedules only the ranges each type is missing within
// [0, toSlot), starting every full file at a gap boundary so it fills holes and
// the trailing tail without overlapping an existing segment.
func planStateDump(coverage map[string][]Range, toSlot, blocksPerFile uint64) []caplinStateDumpJob {
	toSlot = (toSlot / blocksPerFile) * blocksPerFile

	names := make([]string, 0, len(coverage))
	for name := range coverage {
		names = append(names, name)
	}
	sort.Strings(names)

	jobs := make([]caplinStateDumpJob, 0)
	for _, name := range names {
		for _, gap := range missingRanges(coverage[name], toSlot) {
			for i := gap.from; i+blocksPerFile <= gap.to; i += blocksPerFile {
				jobs = append(jobs, caplinStateDumpJob{name: name, from: i, to: i + blocksPerFile})
			}
		}
	}
	return jobs
}

func (s *CaplinStateSnapshots) DumpCaplinState(ctx context.Context, toSlot, blocksPerFile uint64, salt uint32, dirs datadir.Dirs, workers int, lvl log.Lvl, logger log.Logger) error {
	coverage := make(map[string][]Range, len(s.snapshotTypes.KeyValueGetters))
	for typ := range s.snapshotTypes.KeyValueGetters {
		coverage[typ.String()] = s.coveredRangesForType(typ)
	}

	for _, job := range planStateDump(coverage, toSlot, blocksPerFile) {
		typ, ok := ParseCaplinStateType(job.name)
		if !ok {
			continue
		}
		logger.Log(lvl, "Dumping "+job.name, "from", job.from, "to", job.to)
		if err := dumpCaplinState(ctx, job.name, s.snapshotTypes.KeyValueGetters[typ], job.from, job.to, blocksPerFile, salt, dirs, workers, lvl, logger, s.snapshotTypes.Compression[typ]); err != nil {
			if errors.Is(err, errIncompleteStateRange) {
				logger.Warn("[Caplin] skipping incomplete state range, will retry after reconstruction", "type", job.name, "from", job.from, "to", job.to, "err", err)
				continue
			}
			return err
		}
	}
	return nil
}

func (s *CaplinStateSnapshots) BuildMissingIndices(ctx context.Context, logger log.Logger) error {
	if s == nil {
		return nil
	}
	// if !s.segmentsReady.Load() {
	// 	return fmt.Errorf("not all snapshot segments are available")
	// }

	// wait for Downloader service to download all expected snapshots

	noneDone := true

	for caplinType, filesTree := range s.dirty {
		files := filesTree.Items()
		_, ok := s.snapshotTypes.KeyValueGetters[caplinType]
		if !ok {
			s.logger.Warn("no kv getter for caplin state snapshot type", "type", caplinType.String())
			continue
		}
		for _, df := range files {
			if df.Decompressor == nil {
				return fmt.Errorf("segment %s is not opened", df.FilePath())
			}
			if isIndexed(df) {
				continue
			}
			sn, _, _ := snaptype.ParseFileName(s.dir, filepath.Base(df.FilePath()))

			indexFile := filepath.Join(sn.Dir(), snaptype.IdxFileName(sn.Version, sn.From, sn.To, sn.CaplinTypeString))
			if _, err := os.Stat(indexFile); err == nil {
				logger.Info("index file already exists, yet dirtyFile didn't have it opened", "seg", sn.Name())
				continue
			}
			logger.Info("building index file", "seg", sn.Name())
			p := &background.Progress{}
			noneDone = false

			if err := simpleIdx(ctx, sn, s.Salt, s.tmpdir, p, log.LvlDebug, logger); err != nil {
				return err
			}
		}
	}
	if noneDone {
		return nil
	}

	return s.OpenFolder()
}

func (s *CaplinStateSnapshots) Get(tbl CaplinStateType, slot uint64) ([]byte, error) {
	defer func() {
		if rec := recover(); rec != nil {
			panic(fmt.Sprintf("Get(%s, %d), %s, %s\n", tbl.String(), slot, rec, debug.Stack()))
		}
	}()

	view := s.View()
	defer view.Close()

	seg, ok := view.VisibleSegment(slot, tbl)
	if !ok {
		return nil, nil
	}

	return seg.Get(slot)
}
