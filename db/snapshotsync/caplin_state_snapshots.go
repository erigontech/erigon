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
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
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
	return SnapshotTypes{
		KeyValueGetters: map[string]KeyValueGetter{
			kv.ValidatorEffectiveBalance:     getKvGetterForStateTable(db, kv.ValidatorEffectiveBalance),
			kv.ValidatorSlashings:            getKvGetterForStateTable(db, kv.ValidatorSlashings),
			kv.ValidatorBalance:              getKvGetterForStateTable(db, kv.ValidatorBalance),
			kv.StateEvents:                   getKvGetterForStateTable(db, kv.StateEvents),
			kv.ActiveValidatorIndicies:       getKvGetterForStateTable(db, kv.ActiveValidatorIndicies),
			kv.StateRoot:                     getKvGetterForStateTable(db, kv.StateRoot),
			kv.BlockRoot:                     getKvGetterForStateTable(db, kv.BlockRoot),
			kv.SlotData:                      getKvGetterForStateTable(db, kv.SlotData),
			kv.EpochData:                     getKvGetterForStateTable(db, kv.EpochData),
			kv.InactivityScores:              getKvGetterForStateTable(db, kv.InactivityScores),
			kv.NextSyncCommittee:             getKvGetterForStateTable(db, kv.NextSyncCommittee),
			kv.CurrentSyncCommittee:          getKvGetterForStateTable(db, kv.CurrentSyncCommittee),
			kv.Eth1DataVotes:                 getKvGetterForStateTable(db, kv.Eth1DataVotes),
			kv.IntraRandaoMixes:              getKvGetterForStateTable(db, kv.IntraRandaoMixes),
			kv.RandaoMixes:                   getKvGetterForStateTable(db, kv.RandaoMixes),
			kv.BalancesDump:                  getKvGetterForStateTable(db, kv.BalancesDump),
			kv.EffectiveBalancesDump:         getKvGetterForStateTable(db, kv.EffectiveBalancesDump),
			kv.PendingConsolidations:         getKvGetterForStateTable(db, kv.PendingConsolidations),
			kv.PendingPartialWithdrawals:     getKvGetterForStateTable(db, kv.PendingPartialWithdrawals),
			kv.PendingDeposits:               getKvGetterForStateTable(db, kv.PendingDeposits),
			kv.PendingConsolidationsDump:     getKvGetterForStateTable(db, kv.PendingConsolidationsDump),
			kv.PendingPartialWithdrawalsDump: getKvGetterForStateTable(db, kv.PendingPartialWithdrawalsDump),
			kv.PendingDepositsDump:           getKvGetterForStateTable(db, kv.PendingDepositsDump),
			// GLOAS (EIP-7732)
			kv.Builders:                          getKvGetterForStateTable(db, kv.Builders),
			kv.BuildersDump:                      getKvGetterForStateTable(db, kv.BuildersDump),
			kv.BuilderPendingWithdrawals:         getKvGetterForStateTable(db, kv.BuilderPendingWithdrawals),
			kv.BuilderPendingWithdrawalsDump:     getKvGetterForStateTable(db, kv.BuilderPendingWithdrawalsDump),
			kv.PayloadExpectedWithdrawals:        getKvGetterForStateTable(db, kv.PayloadExpectedWithdrawals),
			kv.PayloadExpectedWithdrawalsDump:    getKvGetterForStateTable(db, kv.PayloadExpectedWithdrawalsDump),
			kv.ExecutionPayloadAvailabilityTable: getKvGetterForStateTable(db, kv.ExecutionPayloadAvailabilityTable),
			kv.BuilderPendingPaymentsTable:       getKvGetterForStateTable(db, kv.BuilderPendingPaymentsTable),
			kv.PtcWindowTable:                    getKvGetterForStateTable(db, kv.PtcWindowTable),
			kv.LatestExecutionPayloadBidTable:    getKvGetterForStateTable(db, kv.LatestExecutionPayloadBidTable),
		},
		Compression: map[string]bool{},
	}
}

// value: chunked(ssz(SignedBeaconBlocks))
// slot       -> beacon_slot_segment_offset

type CaplinStateSnapshots struct {
	indicesReady  atomic.Bool
	segmentsReady atomic.Bool

	Salt uint32

	// dirtySegmentsLock guards the dirty tree and the generation chain (publish/reclaim);
	// sharing one lock makes a dirty mutation and its publish one atomic step.
	dirtySegmentsLock sync.RWMutex
	dirty             map[string]*btree.BTreeG[*DirtySegment] // ordered map type name -> DirtySegments

	// visible is the current published generation, read lock-free by readers; oldestVisible is the
	// chain head reclamation walks from. A generation's retired files are closed and unlinked once
	// its refcnt drains to 0. Mirrors the EL BaseRoSnapshots refcount model.
	visible       atomic.Pointer[caplinStateVisible]
	oldestVisible *caplinStateVisible // guarded by dirtySegmentsLock

	snapshotTypes SnapshotTypes

	dir         string
	tmpdir      string
	segmentsMax atomic.Uint64 // all types of .seg files are available - up to this number
	idxMax      atomic.Uint64 // all types of .idx files are available - up to this number
	cfg         ethconfig.BlocksFreezing
	logger      log.Logger
	// chain cfg
	beaconCfg *clparams.BeaconChainConfig
}

// caplinStateVisible is one published, immutable generation of the visible file set across all
// caplin state types. Readers pin it via acquireVisible/releaseVisible; its retired files are
// closed and unlinked only once every reader that pinned it drains (refcnt hits 0).
type caplinStateVisible struct {
	segments map[string]VisibleSegments // type name -> visible segments
	refcnt   atomic.Int32               // live readers pinning this generation
	retired  []*DirtySegment            // files this generation was the last to reference; unlinked on drain
	next     *caplinStateVisible        // oldest->newest chain link (set under dirtySegmentsLock)
}

// acquireVisible pins the current generation. Load and increment are not atomic together, so after
// incrementing we re-check the generation is still current; if superseded mid-pin we drop the stale
// pin and retry (hazard-pointer style).
func (s *CaplinStateSnapshots) acquireVisible() *caplinStateVisible {
	for {
		v := s.visible.Load()
		v.refcnt.Add(1)
		if s.visible.Load() == v {
			return v
		}
		s.releaseVisible(v)
	}
}

// releaseVisible drops a pin taken by acquireVisible; the last reader of a superseded generation
// triggers reclamation of drained generations' retired files.
func (s *CaplinStateSnapshots) releaseVisible(v *caplinStateVisible) {
	if v.refcnt.Add(-1) == 0 {
		s.reclaimRetired()
	}
}

// reclaimRetiredLocked walks the oldest->newest chain from the head, collecting the retired files of
// every fully-drained generation older than the current one. Caller must hold dirtySegmentsLock; the
// returned files are closed and unlinked by the caller off-lock.
func (s *CaplinStateSnapshots) reclaimRetiredLocked() (toDelete []*DirtySegment) {
	cur := s.visible.Load()
	for h := s.oldestVisible; h != cur && h.refcnt.Load() == 0; h = h.next {
		toDelete = append(toDelete, h.retired...)
		h.retired = nil
		s.oldestVisible = h.next
	}
	return toDelete
}

func (s *CaplinStateSnapshots) reclaimRetired() {
	s.dirtySegmentsLock.Lock()
	toDelete := s.reclaimRetiredLocked()
	s.dirtySegmentsLock.Unlock()
	closeAndRemoveSegments(toDelete)
}

// drained reports whether no generation older than the current one is still pinned. Caller must
// hold dirtySegmentsLock (oldestVisible is lock-guarded).
func (s *CaplinStateSnapshots) drained() bool {
	return s.oldestVisible == s.visible.Load()
}

type KeyValueGetter func(numId uint64) ([]byte, []byte, error)

type SnapshotTypes struct {
	KeyValueGetters map[string]KeyValueGetter
	Compression     map[string]bool
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

	// BeaconBlocks := &segments{
	// 	DirtySegments: btree.NewBTreeGOptions[*DirtySegment](DirtySegmentLess, btree.Options{Degree: 128, NoLocks: false}),
	// }
	// BlobSidecars := &segments{
	// 	DirtySegments: btree.NewBTreeGOptions[*DirtySegment](DirtySegmentLess, btree.Options{Degree: 128, NoLocks: false}),
	// }
	// Segments := make(map[string]*segments)
	// for k := range snapshotTypes.KeyValueGetters {
	// 	Segments[k] = &segments{
	// 		DirtySegments: btree.NewBTreeGOptions[*DirtySegment](DirtySegmentLess, btree.Options{Degree: 128, NoLocks: false}),
	// 	}
	// }
	dirty := make(map[string]*btree.BTreeG[*DirtySegment])
	for k := range snapshotTypes.KeyValueGetters {
		dirty[k] = btree.NewBTreeGOptions[*DirtySegment](DirtySegmentLess, btree.Options{Degree: 128, NoLocks: false})
	}

	c := &CaplinStateSnapshots{snapshotTypes: snapshotTypes, dir: dirs.SnapCaplin, tmpdir: dirs.Tmp, cfg: cfg, dirty: dirty, logger: logger, beaconCfg: beaconCfg}
	empty := &caplinStateVisible{segments: make(map[string]VisibleSegments, len(dirty))}
	for k := range dirty {
		empty.segments[k] = make(VisibleSegments, 0)
	}
	c.visible.Store(empty)
	c.oldestVisible = empty

	c.dirtySegmentsLock.Lock()
	c.recalcVisibleFiles(nil)
	c.dirtySegmentsLock.Unlock()
	return c
}

func (s *CaplinStateSnapshots) IndicesMax() uint64  { return s.idxMax.Load() }
func (s *CaplinStateSnapshots) SegmentsMax() uint64 { return s.segmentsMax.Load() }

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
	for _, segs := range view.visible.segments {
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
	for _, segs := range view.visible.segments {
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
	return min(s.segmentsMax.Load(), s.idxMax.Load())
}

func (s *CaplinStateSnapshots) TypeNames() []string {
	names := make([]string, 0, len(s.snapshotTypes.KeyValueGetters))
	for name := range s.snapshotTypes.KeyValueGetters {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (s *CaplinStateSnapshots) coveredRangesForType(name string) []Range {
	segs := s.visible.Load().segments[name]
	ranges := make([]Range, 0, len(segs))
	for _, seg := range segs {
		ranges = append(ranges, seg.Range)
	}
	return ranges
}

// ContiguousCoverageEnd returns the end of the unbroken visible-segment run that
// starts at slot 0 for the given type, or 0 when coverage is not rooted at genesis.
func (s *CaplinStateSnapshots) ContiguousCoverageEnd(typeName string) uint64 {
	ranges := s.coveredRangesForType(typeName)
	slices.SortFunc(ranges, func(a, b Range) int { return cmp.Compare(a.from, b.from) })
	var end uint64
	for _, r := range ranges {
		if r.from > end {
			break
		}
		if r.to > end {
			end = r.to
		}
	}
	return end
}

func (s *CaplinStateSnapshots) Close() {
	if s == nil {
		return
	}
	s.dirtySegmentsLock.Lock()
	defer s.dirtySegmentsLock.Unlock()

	detached := s.detachNotInList(nil)
	s.recalcVisibleFiles(nil)

	if s.drained() {
		for _, sn := range detached {
			sn.close()
		}
	} else {
		s.logger.Warn("[caplin-state] Close called with live readers; leaving fds open")
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
	s.dirtySegmentsLock.Lock()
	defer s.dirtySegmentsLock.Unlock()

	// Detach segments no longer in the list and retire them: fds close and the (already-absent)
	// file is unlink-no-op'd once readers of the outgoing generation drain. Publish once, after all
	// new files open, so no reader sees a transient set with stale gone but new not yet visible.
	retired := s.detachNotInList(fileNames)
	defer s.recalcVisibleFiles(retired)

	var segmentsMax uint64
	var segmentsMaxSet bool
	for _, fName := range fileNames {
		f, _, _ := snaptype.ParseFileName(s.dir, fName)

		dirtySegments, ok := s.dirty[f.CaplinTypeString]
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

// recalcVisibleFiles builds a fresh visible bundle from dirty and publishes it as the new
// generation. Must be called with dirtySegmentsLock held so the caller's dirty mutation and this
// publish are one atomic step. retired are files removed from dirty during the outgoing generation's
// tenure; they are closed and unlinked once no reader pins that generation. Ordinary visibility
// replacement passes nil.
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

	segments := make(map[string]VisibleSegments, len(s.dirty))
	for k, dirtySegments := range s.dirty {
		segments[k] = getNewVisibleSegments(dirtySegments)
	}

	next := &caplinStateVisible{segments: segments}
	old := s.visible.Load()
	old.retired = retired
	old.next = next
	s.visible.Store(next)
	closeAndRemoveSegments(s.reclaimRetiredLocked())

	s.idxMax.Store(idxAvailabilityFrom(segments))
	s.indicesReady.Store(true)
}

// RemoveOverlaps retires state segment files fully covered by a larger indexed segment of the same
// type, so the on-disk publishable check stops flagging them as overlapping. The covered subsets are
// handed to publish as retired; the drain-gated reclaim closes and unlinks them once no reader pins
// the outgoing generation — nothing is closed or removed inline. The temp View pin taken here forces
// the drain, so with no other reader the covered files are gone by the time this returns.
func (s *CaplinStateSnapshots) RemoveOverlaps() error {
	if s == nil {
		return nil
	}
	v := s.View()
	defer v.Close()

	s.dirtySegmentsLock.Lock()
	s.recalcVisibleFiles(s.detachOverlappedSubsets())
	s.dirtySegmentsLock.Unlock()
	return nil
}

// detachOverlappedSubsets removes from dirty and returns for retirement every segment fully covered
// by a larger indexed segment of the same type. Coverage is index-aware: an un-indexed superset
// can't serve reads, so a subset it covers is kept until the superset is indexed, while a covered
// subset is dropped regardless of its own index state. Caller must hold dirtySegmentsLock.
func (s *CaplinStateSnapshots) detachOverlappedSubsets() []*DirtySegment {
	var retired []*DirtySegment //nolint:prealloc // sparse subset of dirty; full prealloc over-allocates
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

// detachNotInList removes from dirty and returns (without closing) every segment whose file is not
// in protect. Caller must hold dirtySegmentsLock; the caller owns closing or retiring the result.
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
				// sn.filePath, not the promoted Decompressor.FilePath(): the field is set
				// for every tree member, incl. stubs whose Decompressor is nil.
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

// idxAvailabilityFrom is the min visible .idx height across the candidate bundle about to be
// published (read from the candidate, never the already-published generation). A configured type
// with no visible segment caps availability at 0.
func idxAvailabilityFrom(segments map[string]VisibleSegments) uint64 {
	min := uint64(math.MaxUint64)
	for _, segs := range segments {
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

type CaplinStateView struct {
	s       *CaplinStateSnapshots
	visible *caplinStateVisible // the pinned generation; released once by Close
	closed  bool
}

func (s *CaplinStateSnapshots) View() *CaplinStateView {
	if s == nil {
		return nil
	}
	return &CaplinStateView{s: s, visible: s.acquireVisible()}
}

func (v *CaplinStateView) Close() {
	if v == nil || v.closed {
		return
	}
	v.s.releaseVisible(v.visible)
	v.s = nil
	v.visible = nil
	v.closed = true
}

func (v *CaplinStateView) VisibleSegments(tbl string) VisibleSegments {
	if v == nil || v.visible == nil {
		return nil
	}
	return v.visible.segments[tbl]
}

func (v *CaplinStateView) VisibleSegment(slot uint64, tbl string) (*VisibleSegment, bool) {
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
	for name := range s.snapshotTypes.KeyValueGetters {
		coverage[name] = s.coveredRangesForType(name)
	}

	for _, job := range planStateDump(coverage, toSlot, blocksPerFile) {
		logger.Log(lvl, "Dumping "+job.name, "from", job.from, "to", job.to)
		if err := dumpCaplinState(ctx, job.name, s.snapshotTypes.KeyValueGetters[job.name], job.from, job.to, blocksPerFile, salt, dirs, workers, lvl, logger, s.snapshotTypes.Compression[job.name]); err != nil {
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
			s.logger.Warn("no kv getter for caplin state snapshot type", "type", caplinType)
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

func (s *CaplinStateSnapshots) Get(tbl string, slot uint64) ([]byte, error) {
	defer func() {
		if rec := recover(); rec != nil {
			panic(fmt.Sprintf("Get(%s, %d), %s, %s\n", tbl, slot, rec, debug.Stack()))
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
