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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/eth/ethconfig"
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

	dirtySegmentsLock   sync.RWMutex
	visibleSegmentsLock sync.RWMutex

	// BeaconBlocks *segments
	// BlobSidecars *segments
	// Segments      map[string]*segments
	dirtyLock sync.RWMutex                            // guards `dirty` field
	dirty     map[string]*btree.BTreeG[*DirtySegment] // ordered map `type.Enum()` -> DirtySegments

	visibleLock sync.RWMutex // guards  `visible` field
	visible     sync.Map
	//visible     map[string]VisibleSegments // ordered map `type.Enum()` -> VisbileSegments

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
	dirty := make(map[string]*btree.BTreeG[*DirtySegment])
	for k := range snapshotTypes.KeyValueGetters {
		dirty[k] = btree.NewBTreeGOptions[*DirtySegment](DirtySegmentLess, btree.Options{Degree: 128, NoLocks: false})
	}

	c := &CaplinStateSnapshots{snapshotTypes: snapshotTypes, dir: dirs.SnapCaplin, tmpdir: dirs.Tmp, cfg: cfg, dirty: dirty, logger: logger, beaconCfg: beaconCfg}
	for k := range snapshotTypes.KeyValueGetters {
		c.visible.Store(k, make(VisibleSegments, 0))
	}
	c.recalcVisibleFiles()
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

	for _, roTx := range view.roTxs {
		if roTx != nil {
			for _, seg := range roTx.Segments {
				s.logger.Info("[agg] ", "f", seg.src.filePath, "words", seg.src.Decompressor.Count())
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
		for _, seg := range roTx.Segments {
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

		dirtySegments, ok := s.dirty[f.CaplinTypeString]
		if !ok {
			continue
		}
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
	s.indexes = make([]*recsplit.Index, 1)
	if s.indexes[0] != nil {
		return nil
	}

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

func (s *CaplinStateSnapshots) recalcVisibleFiles() {
	defer func() {
		s.idxMax.Store(s.idxAvailability())
		s.indicesReady.Store(true)
	}()

	s.visibleLock.Lock()
	defer s.visibleLock.Unlock()

	getNewVisibleSegments := func(dirtySegments *btree.BTreeG[*DirtySegment]) []*VisibleSegment {
		newVisibleSegments := make([]*VisibleSegment, 0, dirtySegments.Len())
		dirtySegments.Walk(func(segments []*DirtySegment) bool {
			for _, sn := range segments {
				if sn.canDelete.Load() {
					continue
				}
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

	// for k := range s.visible {
	// 	s.visible[k] = getNewVisibleSegments(s.dirty[k])
	// }
	s.visible.Range(func(k, v interface{}) bool {
		s.visible.Store(k, getNewVisibleSegments(s.dirty[k.(string)]))
		return true
	})
}

func (s *CaplinStateSnapshots) idxAvailability() uint64 {
	s.visibleLock.RLock()
	defer s.visibleLock.RUnlock()

	min := uint64(math.MaxUint64)
	// for _, segs := range s.visible {
	// 	if len(segs) == 0 {
	// 		return 0
	// 	}
	// 	if segs[len(segs)-1].to < min {
	// 		min = segs[len(segs)-1].to
	// 	}
	// }
	s.visible.Range(func(_, v interface{}) bool {
		segs := v.([]*VisibleSegment)
		if len(segs) == 0 {
			min = 0
			return false
		}
		if segs[len(segs)-1].to < min {
			min = segs[len(segs)-1].to
		}
		return true
	})
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

func (s *CaplinStateSnapshots) closeWhatNotInList(l []string) {
	protectFiles := make(map[string]struct{}, len(l))
	for _, fName := range l {
		protectFiles[fName] = struct{}{}
	}

	for _, dirtySegments := range s.dirty {
		toClose := make([]*DirtySegment, 0)
		dirtySegments.Walk(func(segments []*DirtySegment) bool {
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
			dirtySegments.Delete(sn)
		}
	}
}

type CaplinStateView struct {
	s      *CaplinStateSnapshots
	roTxs  map[string]*RoTx
	closed bool
}

func (s *CaplinStateSnapshots) View() *CaplinStateView {
	if s == nil {
		return nil
	}
	s.visibleSegmentsLock.RLock()
	defer s.visibleSegmentsLock.RUnlock()

	v := &CaplinStateView{s: s, roTxs: make(map[string]*RoTx)}
	// BeginRo increments refcount - which is contended
	s.dirtySegmentsLock.RLock()
	defer s.dirtySegmentsLock.RUnlock()

	// for k, segments := range s.visible {
	// 	v.roTxs[k] = segments.BeginRo()
	// }
	s.visible.Range(func(k, val interface{}) bool {
		v.roTxs[k.(string)] = VisibleSegments(val.([]*VisibleSegment)).BeginRo()
		return true
	})
	return v
}

func (v *CaplinStateView) Close() {
	if v == nil {
		return
	}
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
	// if v.s == nil || v.s.visible[tbl] == nil {
	// 	return nil
	// }
	// return v.s.visible[tbl]
	if v.s == nil {
		return nil
	}
	if val, ok := v.s.visible.Load(tbl); ok {
		return val.([]*VisibleSegment)
	}
	return nil
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

func (s *CaplinStateSnapshots) DumpCaplinState(ctx context.Context, fromSlot, toSlot, blocksPerFile uint64, salt uint32, dirs datadir.Dirs, workers int, lvl log.Lvl, logger log.Logger) error {
	fromSlot = (fromSlot / blocksPerFile) * blocksPerFile
	toSlot = (toSlot / blocksPerFile) * blocksPerFile
	for snapName, kvGetter := range s.snapshotTypes.KeyValueGetters {
		for i := fromSlot; i < toSlot; i += blocksPerFile {
			if toSlot-i < blocksPerFile {
				break
			}
			// keep beaconblocks here but whatever....
			to := i + blocksPerFile
			logger.Log(lvl, "Dumping "+snapName, "from", i, "to", to)
			if err := dumpCaplinState(ctx, snapName, kvGetter, i, to, blocksPerFile, salt, dirs, workers, lvl, logger, s.snapshotTypes.Compression[snapName]); err != nil {
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
	segments, _, err := SegmentsCaplin(s.dir)
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
