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
	"sync/atomic"
	"time"

	"github.com/tidwall/btree"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dbg"
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

	FileSet                   // dirty segments + published visible generations, under one lock
	tableIndex map[string]int // table name -> index into dirty/segments; immutable after init

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
	// Assign each table a stable index into dirty/segments (caplin has no
	// snaptype.Enum; sorted names give a deterministic layout).
	names := make([]string, 0, len(snapshotTypes.KeyValueGetters))
	for k := range snapshotTypes.KeyValueGetters {
		names = append(names, k)
	}
	sort.Strings(names)
	tableIndex := make(map[string]int, len(names))
	dirty := make([]*btree.BTreeG[*DirtySegment], len(names))
	for i, name := range names {
		tableIndex[name] = i
		dirty[i] = btree.NewBTreeGOptions[*DirtySegment](DirtySegmentLess, btree.Options{Degree: 128, NoLocks: false})
	}

	c := &CaplinStateSnapshots{snapshotTypes: snapshotTypes, dir: dirs.SnapCaplin, tmpdir: dirs.Tmp, cfg: cfg, tableIndex: tableIndex, logger: logger, beaconCfg: beaconCfg}
	c.dirty = dirty
	empty := &VisibleFiles{}
	c.visible.Store(empty)
	c.oldestVisible = empty
	c.recalcVisibleFiles(nil)
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

func (s *CaplinStateSnapshots) coveredRangesForType(name string) []Range {
	idx, ok := s.tableIndex[name]
	if !ok {
		return nil
	}
	segs := s.visible.Load().segments[idx]
	ranges := make([]Range, 0, len(segs))
	for _, seg := range segs {
		ranges = append(ranges, seg.Range)
	}
	return ranges
}

func (s *CaplinStateSnapshots) Close() {
	if s == nil {
		return
	}
	var retired []RetiredSegment
	defer func() { s.recalcVisibleFiles(retired) }()
	s.dirtyLock.Lock()
	defer s.dirtyLock.Unlock()

	retired = s.CloseWhatNotInList(nil)
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
	var retired []RetiredSegment
	defer func() { s.recalcVisibleFiles(retired) }()

	s.dirtyLock.Lock()
	defer s.dirtyLock.Unlock()

	retired = s.CloseWhatNotInList(fileNames)
	var segmentsMax uint64
	var segmentsMaxSet bool
Loop:
	for _, fName := range fileNames {
		f, _, _ := snaptype.ParseFileName(s.dir, fName)

		var processed bool = true
		var exists bool
		var sn *DirtySegment

		idx, ok := s.tableIndex[f.CaplinTypeString]
		if !ok {
			continue
		}
		dirtySegments := s.dirty[idx]
		filePath := filepath.Join(s.dir, fName)
		dirtySegments.Walk(func(segments []*DirtySegment) bool {
			for _, sn2 := range segments {
				if sn2.Decompressor == nil { // it's ok if some segment was not able to open
					continue
				}
				if filePath == sn2.filePath {
					sn = sn2
					exists = true
					break
				}
			}
			return true
		})
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
			dirtySegments.Set(sn)
		}
		if err := openIdxForCaplinStateIfNeeded(sn, filePath, optimistic); err != nil {
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

	filePath = strings.ReplaceAll(filePath, ".seg", ".idx")
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

func (s *CaplinStateSnapshots) recalcVisibleFiles(retired []RetiredSegment) {
	defer func() {
		s.idxMax.Store(s.idxAvailability())
		s.indicesReady.Store(true)
	}()

	getNewVisibleSegments := func(dirtySegments *btree.BTreeG[*DirtySegment]) []*VisibleSegment {
		newVisibleSegments := make([]*VisibleSegment, 0, dirtySegments.Len())
		dirtySegments.Walk(func(segments []*DirtySegment) bool {
			for _, sn := range segments {
				if !isIndexed(sn) {
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

	s.Publish(retired, func() *VisibleFiles {
		segments := make([]VisibleSegments, len(s.dirty))
		for idx, dirtySegments := range s.dirty {
			segments[idx] = getNewVisibleSegments(dirtySegments)
		}
		return &VisibleFiles{segments: segments}
	})
}

func (s *CaplinStateSnapshots) idxAvailability() uint64 {
	min := uint64(math.MaxUint64)
	for _, segs := range s.visible.Load().segments {
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
	visible *VisibleFiles // pinned generation; released in Close
}

func (s *CaplinStateSnapshots) View() *CaplinStateView {
	if s == nil {
		return nil
	}
	return &CaplinStateView{s: s, visible: s.AcquireVisible()}
}

func (v *CaplinStateView) Close() {
	if v == nil || v.s == nil {
		return
	}
	v.s.ReleaseVisible(v.visible)
	v.s, v.visible = nil, nil
}

func (v *CaplinStateView) VisibleSegments(tbl string) []*VisibleSegment {
	if v.s == nil {
		return nil
	}
	idx, ok := v.s.tableIndex[tbl]
	if !ok {
		return nil
	}
	return v.visible.segments[idx]
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

	// Generate .seg file, which is just the list of beacon blocks.
	for i := fromSlot; i < toSlot; i++ {
		// read root.
		_, dump, err := kvGetter(i)
		if err != nil {
			return err
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

	// Ugly hack to wait for fsync
	time.Sleep(15 * time.Second)

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

	for _, filesTree := range s.dirty {
		files := filesTree.Items()
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
