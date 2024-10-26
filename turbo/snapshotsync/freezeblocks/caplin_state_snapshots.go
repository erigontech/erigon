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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"

	"github.com/erigontech/erigon-lib/chain/snapcfg"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/seg"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/eth/ethconfig"
)

func getKvGetterForStateTable(db kv.RoDB, tableName string) KeyValueGetter {
	return func(numId uint64) ([]byte, []byte, error) {
		var key, value []byte
		var err error
		if err := db.View(context.TODO(), func(tx kv.Tx) error {
			key = base_encoding.Encode64ToBytes4(numId)
			value, err = tx.GetOne(tableName, base_encoding.Encode64ToBytes4(numId))
			return err
		}); err != nil {
			return nil, nil, err
		}
		return key, value, nil
	}
}

func MakeCaplinStateSnapshotsTypes(db kv.RoDB) SnapshotTypes {
	return SnapshotTypes{
		Types: map[string]KeyValueGetter{
			kv.ValidatorEffectiveBalance: getKvGetterForStateTable(db, kv.ValidatorEffectiveBalance),
			kv.ValidatorSlashings:        getKvGetterForStateTable(db, kv.ValidatorSlashings),
			kv.ValidatorBalance:          getKvGetterForStateTable(db, kv.ValidatorBalance),
			kv.StateEvents:               getKvGetterForStateTable(db, kv.StateEvents),
			kv.ActiveValidatorIndicies:   getKvGetterForStateTable(db, kv.ActiveValidatorIndicies),
			kv.StateRoot:                 getKvGetterForStateTable(db, kv.StateRoot),
			kv.BlockRoot:                 getKvGetterForStateTable(db, kv.BlockRoot),
			kv.SlotData:                  getKvGetterForStateTable(db, kv.SlotData),
			kv.EpochData:                 getKvGetterForStateTable(db, kv.EpochData),
			kv.InactivityScores:          getKvGetterForStateTable(db, kv.InactivityScores),
			kv.NextSyncCommittee:         getKvGetterForStateTable(db, kv.NextSyncCommittee),
			kv.CurrentSyncCommittee:      getKvGetterForStateTable(db, kv.CurrentSyncCommittee),
			kv.Eth1DataVotes:             getKvGetterForStateTable(db, kv.Eth1DataVotes),
			kv.IntraRandaoMixes:          getKvGetterForStateTable(db, kv.IntraRandaoMixes),
			kv.RandaoMixes:               getKvGetterForStateTable(db, kv.RandaoMixes),
			kv.Proposers:                 getKvGetterForStateTable(db, kv.Proposers),
		},
	}
}

// value: chunked(ssz(SignedBeaconBlocks))
// slot       -> beacon_slot_segment_offset

type CaplinStateSnapshots struct {
	indicesReady  atomic.Bool
	segmentsReady atomic.Bool

	Salt uint32

	dirtySegmentsLock   sync.RWMutex
	visibleSegmentsLock sync.RWMutex

	// BeaconBlocks *segments
	// BlobSidecars *segments
	Segments      map[string]*segments
	snapshotTypes SnapshotTypes

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

type KeyValueGetter func(numId uint64) ([]byte, []byte, error)

type SnapshotTypes struct {
	Types map[string]KeyValueGetter
}

// NewCaplinStateSnapshots - opens all snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewCaplinStateSnapshots(cfg ethconfig.BlocksFreezing, beaconCfg *clparams.BeaconChainConfig, dirs datadir.Dirs, snapshotTypes SnapshotTypes, logger log.Logger) *CaplinStateSnapshots {
	// BeaconBlocks := &segments{
	// 	DirtySegments: btree.NewBTreeGOptions[*DirtySegment](DirtySegmentLess, btree.Options{Degree: 128, NoLocks: false}),
	// }
	// BlobSidecars := &segments{
	// 	DirtySegments: btree.NewBTreeGOptions[*DirtySegment](DirtySegmentLess, btree.Options{Degree: 128, NoLocks: false}),
	// }
	Segments := make(map[string]*segments)
	for k := range snapshotTypes.Types {
		Segments[strings.ToLower(k)] = &segments{
			DirtySegments: btree.NewBTreeGOptions[*DirtySegment](DirtySegmentLess, btree.Options{Degree: 128, NoLocks: false}),
		}
	}
	c := &CaplinStateSnapshots{snapshotTypes: snapshotTypes, dir: dirs.SnapCaplin, tmpdir: dirs.Tmp, cfg: cfg, Segments: Segments, logger: logger, beaconCfg: beaconCfg}
	c.recalcVisibleFiles()
	return c
}

func (s *CaplinStateSnapshots) IndicesMax() uint64  { return s.idxMax.Load() }
func (s *CaplinStateSnapshots) SegmentsMax() uint64 { return s.segmentsMax.Load() }

func (s *CaplinStateSnapshots) LogStat(str string) {
	s.logger.Info(fmt.Sprintf("[snapshots:%s] Stat", str),
		"blocks", libcommon.PrettyCounter(s.SegmentsMax()+1), "indices", libcommon.PrettyCounter(s.IndicesMax()+1))
}

func (s *CaplinStateSnapshots) LS() {
	if s == nil {
		return
	}
	view := s.View()
	defer view.Close()

	for _, roTx := range view.roTxs {
		if roTx != nil {
			for _, seg := range roTx.VisibleSegments {
				s.logger.Info("[agg] ", "f", seg.src.Decompressor.FileName(), "words", seg.src.Decompressor.Count())
			}
		}
	}
}

func (s *CaplinStateSnapshots) SegFileNames(from, to uint64) []string {
	view := s.View()
	defer view.Close()

	var res []string

	for _, roTx := range view.roTxs {
		if roTx == nil {
			continue
		}
		for _, seg := range roTx.VisibleSegments {
			if seg.from >= from && seg.to <= to {
				res = append(res, seg.src.FileName())
			}
		}
	}
	return res
}

func (s *CaplinStateSnapshots) BlocksAvailable() uint64 {
	return min(s.segmentsMax.Load(), s.idxMax.Load())
}

func (s *CaplinStateSnapshots) Close() {
	if s == nil {
		return
	}
	s.dirtySegmentsLock.Lock()
	defer s.dirtySegmentsLock.Unlock()

	s.closeWhatNotInList(nil)
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
	defer s.recalcVisibleFiles()
	fmt.Println(fileNames)

	s.dirtySegmentsLock.Lock()
	defer s.dirtySegmentsLock.Unlock()

	s.closeWhatNotInList(fileNames)
	var segmentsMax uint64
	var segmentsMaxSet bool
Loop:
	for _, fName := range fileNames {
		f, _, _ := snaptype.ParseFileName(s.dir, fName)

		var processed bool = true
		var exists bool
		var sn *DirtySegment

		segments, ok := s.Segments[f.TypeString]
		if !ok {
			continue
		}
		segments.DirtySegments.Walk(func(segments []*DirtySegment) bool {
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
				// segType: f.Type, Unsupported
				version: f.Version,
				Range:   Range{f.From, f.To},
				frozen:  snapcfg.IsFrozen(s.cfg.ChainName, f),
			}
		}
		filePath := filepath.Join(s.dir, fName)
		if err := s.openSegIfNeed(sn, filePath); err != nil {
			fmt.Println(err)
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
			segments.DirtySegments.Set(sn)
		}
		if err := openIdxForCaplinStateIfNeeded(sn, s.dir, optimistic); err != nil {
			fmt.Println(err)
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

func openIdxForCaplinStateIfNeeded(s *DirtySegment, dir string, optimistic bool) error {
	if s.Decompressor == nil {
		return nil
	}
	err := openIdxIfNeedForCaplinState(s, dir)
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

func openIdxIfNeedForCaplinState(s *DirtySegment, dir string) (err error) {
	s.indexes = make([]*recsplit.Index, 1)
	if s.indexes[0] != nil {
		return nil
	}

	fileName := strings.ReplaceAll(s.FileName(), ".seg", ".idx")
	index, err := recsplit.OpenIndex(filepath.Join(dir, fileName))
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, fileName)
	}

	s.indexes[0] = index

	return nil
}
func (s *CaplinStateSnapshots) recalcVisibleFiles() {
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
				if !sn.Indexed() {
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

	for _, segments := range s.Segments {
		segments.VisibleSegments = getNewVisibleSegments(segments.DirtySegments)
		var maxIdx uint64
		if len(segments.VisibleSegments) > 0 {
			maxIdx = segments.VisibleSegments[len(segments.VisibleSegments)-1].to - 1
		}
		segments.maxVisibleBlock.Store(maxIdx)
	}
}

func (s *CaplinStateSnapshots) idxAvailability() uint64 {
	minVisible := uint64(math.MaxUint64)
	for _, segments := range s.Segments {
		if segments.maxVisibleBlock.Load() < minVisible {
			minVisible = segments.maxVisibleBlock.Load()
		}
	}
	if minVisible == math.MaxUint64 {
		return 0
	}
	return minVisible
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

func (s *CaplinStateSnapshots) closeWhatNotInList(l []string) {
	protectFiles := make(map[string]struct{}, len(l))
	for _, fName := range l {
		protectFiles[fName] = struct{}{}
	}

	for _, segments := range s.Segments {
		toClose := make([]*DirtySegment, 0)
		segments.DirtySegments.Walk(func(segments []*DirtySegment) bool {
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
			sn.close()
			segments.DirtySegments.Delete(sn)
		}
	}
}

type CaplinStateView struct {
	s      *CaplinStateSnapshots
	roTxs  map[string]*segmentsRotx
	closed bool
}

func (s *CaplinStateSnapshots) View() *CaplinStateView {
	s.visibleSegmentsLock.RLock()
	defer s.visibleSegmentsLock.RUnlock()

	v := &CaplinStateView{s: s}
	// BeginRo increments refcount - which is contended
	s.dirtySegmentsLock.RLock()
	defer s.dirtySegmentsLock.RUnlock()

	for k, segments := range s.Segments {
		v.roTxs[k] = segments.BeginRotx()
	}
	return v
}

func (v *CaplinStateView) Close() {
	if v.closed {
		return
	}
	for _, segments := range v.roTxs {
		segments.Close()
	}
	v.s = nil
	v.closed = true
}

func (v *CaplinStateView) VisibleSegments(tbl string) []*VisibleSegment {
	return v.s.Segments[tbl].VisibleSegments
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

func dumpCaplinState(ctx context.Context, snapName string, kvGetter KeyValueGetter, fromSlot uint64, toSlot, blocksPerFile uint64, salt uint32, dirs datadir.Dirs, workers int, lvl log.Lvl, logger log.Logger) error {
	tmpDir, snapDir := dirs.Tmp, dirs.SnapCaplin

	segName := snaptype.BeaconBlocks.FileName(0, fromSlot, toSlot)
	// a little bit ugly.
	segName = strings.ReplaceAll(segName, "beaconblocks", strings.ToLower(snapName))
	f, _, _ := snaptype.ParseFileName(snapDir, segName)

	compressCfg := seg.DefaultCfg
	compressCfg.Workers = workers
	sn, err := seg.NewCompressor(ctx, fmt.Sprintf("Snapshots %s", snapName), f.Path, tmpDir, compressCfg, lvl, logger)
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
			logger.Log(lvl, fmt.Sprintf("Dumping %s", snapName), "progress", i)
		}
		if err := sn.AddUncompressedWord(dump); err != nil {
			return err
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
		BucketSize: 2000,
		LeafSize:   8,
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

func (s *CaplinStateSnapshots) DumpCaplinState(ctx context.Context, fromSlot, toSlot, blocksPerFile uint64, salt uint32, dirs datadir.Dirs, workers int, lvl log.Lvl, logger log.Logger) error {
	fromSlot = (fromSlot / blocksPerFile) * blocksPerFile
	toSlot = (toSlot / blocksPerFile) * blocksPerFile
	fmt.Println("DumpCaplinState", fromSlot, toSlot)
	for snapName, kvGetter := range s.snapshotTypes.Types {
		for i := fromSlot; i < toSlot; i += blocksPerFile {
			if toSlot-i < blocksPerFile {
				break
			}
			// keep beaconblocks here but whatever....
			to := i + blocksPerFile
			logger.Log(lvl, fmt.Sprintf("Dumping %s", snapName), "from", i, "to", to)
			if err := dumpCaplinState(ctx, snapName, kvGetter, i, to, blocksPerFile, salt, dirs, workers, lvl, logger); err != nil {
				return err
			}
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

	return s.OpenFolder()
}

func (s *CaplinStateSnapshots) Get(tbl string, slot uint64) ([]byte, error) {
	defer func() {
		if rec := recover(); rec != nil {
			panic(fmt.Sprintf("ReadHeader(%d), %s, %s\n", slot, rec, dbg.Stack()))
		}
	}()

	view := s.View()
	defer view.Close()

	var buf []byte

	seg, ok := view.VisibleSegment(slot, tbl)
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

	return buf, nil
}
