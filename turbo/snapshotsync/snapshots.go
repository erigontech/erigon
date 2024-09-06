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

type Segment struct {
	Range
	*seg.Decompressor
	indexes []*recsplit.Index
	segType snaptype.Type
	version snaptype.Version
}

func NewSegment(segType snaptype.Type, version snaptype.Version, r Range) *Segment {
	return &Segment{segType: segType, version: version, Range: r}
}

func (s Segment) Type() snaptype.Type {
	return s.segType
}

func (s Segment) Version() snaptype.Version {
	return s.version
}

func (s Segment) Index(index ...snaptype.Index) *recsplit.Index {
	if len(index) == 0 {
		index = []snaptype.Index{{}}
	}

	if len(s.indexes) <= index[0].Offset {
		return nil
	}

	return s.indexes[index[0].Offset]
}

func (s Segment) IsIndexed() bool {
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

func (s Segment) FileName() string {
	return s.Type().FileName(s.version, s.from, s.to)
}

func (s Segment) FileInfo(dir string) snaptype.FileInfo {
	return s.Type().FileInfo(dir, s.from, s.to)
}

func (s *Segment) Reopen(dir string) (err error) {
	s.closeSeg()
	s.Decompressor, err = seg.NewDecompressor(filepath.Join(dir, s.FileName()))
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, s.FileName())
	}
	return nil
}

func (s *Segment) closeSeg() {
	if s.Decompressor != nil {
		s.Close()
		s.Decompressor = nil
	}
}

func (s *Segment) closeIdx() {
	for _, index := range s.indexes {
		index.Close()
	}

	s.indexes = nil
}

func CloseSegment(s *Segment) {
	s.close()
}

func (s *Segment) close() {
	if s != nil {
		s.closeSeg()
		s.closeIdx()
	}
}

func (s *Segment) openFiles() []string {
	files := make([]string, 0, len(s.indexes)+1)

	if s.IsOpen() {
		files = append(files, s.FilePath())
	}

	for _, index := range s.indexes {
		files = append(files, index.FilePath())
	}

	return files
}

func (s *Segment) ReopenIdxIfNeed(dir string, optimistic bool) (err error) {
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

func (s *Segment) reopenIdx(dir string) (err error) {
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

type Segments struct {
	sync.RWMutex
	Segments []*Segment
}

func (s *Segments) View(f func(segments []*Segment) error) error {
	s.RLock()
	defer s.RUnlock()
	return f(s.Segments)
}

func (s *Segments) Segment(blockNum uint64, f func(*Segment) error) (found bool, err error) {
	s.RLock()
	defer s.RUnlock()
	for _, seg := range s.Segments {
		if !(blockNum >= seg.from && blockNum < seg.to) {
			continue
		}
		return true, f(seg)
	}
	return false, nil
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
}

type RoSnapshots struct {
	downloadReady atomic.Bool
	indicesReady  atomic.Bool
	segmentsReady atomic.Bool

	types    []snaptype.Type
	segments btree.Map[snaptype.Enum, *Segments]

	dir         string
	segmentsMax atomic.Uint64 // all types of .seg files are available - up to this number
	idxMax      atomic.Uint64 // all types of .idx files are available - up to this number
	cfg         ethconfig.BlocksFreezing
	logger      log.Logger

	// allows for pruning segments - this is the min availible segment
	segmentsMin atomic.Uint64
	ready       ready
}

func NewRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, types []snaptype.Type, segmentsMin uint64, logger log.Logger) *RoSnapshots {
	var segs btree.Map[snaptype.Enum, *Segments]
	for _, snapType := range types {
		segs.Set(snapType.Enum(), &Segments{})
	}

	s := &RoSnapshots{dir: snapDir, cfg: cfg, segments: segs, logger: logger, types: types}
	s.segmentsMin.Store(segmentsMin)
	s.ready.init()

	return s
}

func (s *RoSnapshots) TypedSegments() *btree.Map[snaptype.Enum, *Segments] {
	return &s.segments
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

func (s *RoSnapshots) DownloadComplete() {
	wasReady := s.downloadReady.Swap(true)

	if !wasReady && s.SegmentsReady() && s.IndicesReady() {
		s.ready.set()
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
	s.segments.Scan(func(segtype snaptype.Enum, value *Segments) bool {
		value.RLock()
		defer value.RUnlock()
		for _, sn := range value.Segments {
			sn.DisableReadAhead()
		}
		return true
	})

	return s
}

func (s *RoSnapshots) EnableReadAhead() *RoSnapshots {
	s.segments.Scan(func(segtype snaptype.Enum, value *Segments) bool {
		value.RLock()
		defer value.RUnlock()
		for _, sn := range value.Segments {
			sn.EnableReadAhead()
		}
		return true
	})

	return s
}

func (s *RoSnapshots) EnableMadvWillNeed() *RoSnapshots {
	s.segments.Scan(func(segtype snaptype.Enum, value *Segments) bool {
		value.RLock()
		defer value.RUnlock()
		for _, sn := range value.Segments {
			sn.EnableMadvWillNeed()
		}
		return true
	})
	return s
}

// minimax of existing indices
func (s *RoSnapshots) idxAvailability() uint64 {
	// Use-Cases:
	//   1. developers can add new types in future. and users will not have files of this type
	//   2. some types are network-specific. example: borevents exists only on Bor-consensus networks
	//   3. user can manually remove 1 .idx file: `rm snapshots/v1-type1-0000-1000.idx`
	//   4. user can manually remove all .idx files of given type: `rm snapshots/*type1*.idx`
	//   5. file-types may have different height: 10 headers, 10 bodies, 9 trancasctions (for example if `kill -9` came during files building/merge). still need index all 3 types.
	amount := 0
	s.segments.Scan(func(segtype snaptype.Enum, value *Segments) bool {
		if len(value.Segments) == 0 || !s.HasType(segtype.Type()) {
			return true
		}
		amount++
		return true
	})

	maximums := make([]uint64, amount)
	var i int
	s.segments.Scan(func(segtype snaptype.Enum, value *Segments) bool {
		if len(value.Segments) == 0 || !s.HasType(segtype.Type()) {
			return true
		}

		for _, seg := range value.Segments {
			if !seg.IsIndexed() {
				break
			}

			maximums[i] = seg.to - 1
		}

		i++
		return true
	})

	if len(maximums) == 0 {
		return 0
	}

	if len(maximums) != len(s.types) {
		return 0
	}

	return slices.Min(maximums)
}

// OptimisticReopenWithDB - optimistically open snapshots (ignoring error), useful at App startup because:
// - user must be able: delete any snapshot file and Erigon will self-heal by re-downloading
// - RPC return Nil for historical blocks if snapshots are not open
func (s *RoSnapshots) OptimisticReopenWithDB(db kv.RoDB) {
	var snList []string
	_ = db.View(context.Background(), func(tx kv.Tx) (err error) {
		snList, _, err = rawdb.ReadSnapshots(tx)
		if err != nil {
			return err
		}
		return nil
	})
	_ = s.ReopenList(snList, true)
}

func (s *RoSnapshots) Ls() {
	s.segments.Scan(func(segtype snaptype.Enum, value *Segments) bool {
		value.RLock()
		defer value.RUnlock()

		for _, seg := range value.Segments {
			if seg.Decompressor == nil {
				continue
			}
			log.Info("[agg] ", "f", seg.Decompressor.FileName(), "words", seg.Decompressor.Count())
		}
		return true
	})
}

func (s *RoSnapshots) Files() (list []string) {
	maxBlockNumInFiles := s.BlocksAvailable()

	s.segments.Scan(func(segtype snaptype.Enum, value *Segments) bool {
		value.RLock()
		defer value.RUnlock()

		for _, seg := range value.Segments {
			if seg.Decompressor == nil {
				continue
			}
			if seg.from > maxBlockNumInFiles {
				continue
			}
			list = append(list, seg.FileName())
		}
		return true
	})

	slices.Sort(list)
	return list
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

// ReopenList stops on optimistic=false, continue opening files on optimistic=true
func (s *RoSnapshots) ReopenList(fileNames []string, optimistic bool) error {
	s.lockSegments()
	defer s.unlockSegments()
	s.closeWhatNotInList(fileNames)
	if err := s.rebuildSegments(fileNames, true, optimistic); err != nil {
		return err
	}
	return nil
}

func (s *RoSnapshots) InitSegments(fileNames []string) error {
	s.lockSegments()
	defer s.unlockSegments()
	s.closeWhatNotInList(fileNames)
	if err := s.rebuildSegments(fileNames, false, true); err != nil {
		return err
	}
	return nil
}

func (s *RoSnapshots) lockSegments() {
	s.segments.Scan(func(segtype snaptype.Enum, value *Segments) bool {
		value.Lock()
		return true
	})
}

func (s *RoSnapshots) unlockSegments() {
	s.segments.Scan(func(segtype snaptype.Enum, value *Segments) bool {
		value.Unlock()
		return true
	})
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
			segtype = &Segments{}
			s.segments.Set(f.Type.Enum(), segtype)
			segtype.Lock() // this will be unlocked by defer s.unlockSegments() above
		}

		var sn *Segment
		var exists bool
		for _, sn2 := range segtype.Segments {
			if sn2.Decompressor == nil { // it's ok if some segment was not able to open
				continue
			}

			if fName == sn2.FileName() {
				sn = sn2
				exists = true
				break
			}
		}

		if !exists {
			sn = &Segment{segType: f.Type, version: f.Version, Range: Range{f.From, f.To}}
		}

		if open {
			if err := sn.Reopen(s.dir); err != nil {
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
			segtype.Segments = append(segtype.Segments, sn)
		}

		if open {
			if err := sn.ReopenIdxIfNeed(s.dir, optimistic); err != nil {
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
	wasReady := s.segmentsReady.Swap(true)
	s.idxMax.Store(s.idxAvailability())
	wasReady = wasReady && s.indicesReady.Swap(true)

	if !wasReady && s.DownloadReady() {
		s.ready.set()
	}

	return nil
}

func (s *RoSnapshots) Ranges() []Range {
	view := s.View()
	defer view.Close()
	return view.Ranges()
}

func (s *RoSnapshots) OptimisticalyReopenFolder()           { _ = s.ReopenFolder() }
func (s *RoSnapshots) OptimisticalyReopenWithDB(db kv.RoDB) { _ = s.ReopenWithDB(db) }
func (s *RoSnapshots) ReopenFolder() error {
	files, _, err := TypedSegments(s.dir, s.segmentsMin.Load(), s.Types(), false)
	if err != nil {
		return err
	}

	list := make([]string, 0, len(files))
	for _, f := range files {
		_, fName := filepath.Split(f.Path)
		list = append(list, fName)
	}
	return s.ReopenList(list, false)
}

func (s *RoSnapshots) ReopenSegments(types []snaptype.Type, allowGaps bool) error {
	files, _, err := TypedSegments(s.dir, s.segmentsMin.Load(), types, allowGaps)

	if err != nil {
		return err
	}
	list := make([]string, 0, len(files))
	for _, f := range files {
		_, fName := filepath.Split(f.Path)
		list = append(list, fName)
	}

	s.lockSegments()
	defer s.unlockSegments()
	// don't need close already opened files
	if err := s.rebuildSegments(list, true, false); err != nil {
		return err
	}
	return nil
}

func (s *RoSnapshots) ReopenWithDB(db kv.RoDB) error {
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		snList, _, err := rawdb.ReadSnapshots(tx)
		if err != nil {
			return err
		}
		return s.ReopenList(snList, true)
	}); err != nil {
		return fmt.Errorf("ReopenWithDB: %w", err)
	}
	return nil
}

func (s *RoSnapshots) Close() {
	if s == nil {
		return
	}
	s.lockSegments()
	defer s.unlockSegments()
	s.closeWhatNotInList(nil)
}

func (s *RoSnapshots) closeWhatNotInList(l []string) {
	s.segments.Scan(func(segtype snaptype.Enum, value *Segments) bool {
	Segments:
		for i, sn := range value.Segments {
			if sn.Decompressor == nil {
				continue Segments
			}
			_, name := filepath.Split(sn.FilePath())
			for _, fName := range l {
				if fName == name {
					continue Segments
				}
			}
			sn.close()
			value.Segments[i] = nil
		}
		return true
	})

	s.segments.Scan(func(segtype snaptype.Enum, value *Segments) bool {
		var i int
		for i = 0; i < len(value.Segments) && value.Segments[i] != nil && value.Segments[i].Decompressor != nil; i++ {
		}
		tail := value.Segments[i:]
		value.Segments = value.Segments[:i]
		for i = 0; i < len(tail); i++ {
			if tail[i] != nil {
				tail[i].close()
				tail[i] = nil
			}
		}
		return true
	})
}

func (s *RoSnapshots) RemoveOverlaps() error {
	s.lockSegments()
	defer s.unlockSegments()

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
	v := s.View()
	defer v.Close()

	_, fName := filepath.Split(fileName)
	var err error
	s.segments.Scan(func(segtype snaptype.Enum, value *Segments) bool {
		idxsToRemove := []int{}
		for i, sn := range value.Segments {
			if sn.Decompressor == nil {
				continue
			}
			if sn.segType.FileName(sn.version, sn.from, sn.to) != fName {
				continue
			}
			files := sn.openFiles()
			sn.close()
			idxsToRemove = append(idxsToRemove, i)
			for _, f := range files {
				_ = os.Remove(f)
			}
		}
		for i := len(idxsToRemove) - 1; i >= 0; i-- {
			value.Segments = append(value.Segments[:idxsToRemove[i]], value.Segments[idxsToRemove[i]+1:]...)
		}
		return true
	})
	return err
}

func (s *RoSnapshots) Delete(fileName string) error {
	if s == nil {
		return nil
	}
	if err := s.delete(fileName); err != nil {
		return fmt.Errorf("can't delete file: %w", err)
	}
	return s.ReopenFolder()

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

	s.segments.Scan(func(segtype snaptype.Enum, value *Segments) bool {
		for _, segment := range value.Segments {
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
	s.segments.Scan(func(key snaptype.Enum, value *Segments) bool {
		fmt.Println("    == [dbg] Snapshots,", key.String())
		for _, sn := range value.Segments {
			args := make([]any, 0, len(sn.Type().Indexes())+1)
			args = append(args, sn.from)
			for _, index := range sn.Type().Indexes() {
				args = append(args, sn.Index(index) != nil)
			}
			fmt.Println(args...)
		}
		return true
	})
}

func mappedHeaderSnapshot(sn *Segment) *silkworm.MappedHeaderSnapshot {
	segmentRegion := silkworm.NewMemoryMappedRegion(sn.FilePath(), sn.DataHandle(), sn.Size())
	idxRegion := silkworm.NewMemoryMappedRegion(sn.Index().FilePath(), sn.Index().DataHandle(), sn.Index().Size())
	return silkworm.NewMappedHeaderSnapshot(segmentRegion, idxRegion)
}

func mappedBodySnapshot(sn *Segment) *silkworm.MappedBodySnapshot {
	segmentRegion := silkworm.NewMemoryMappedRegion(sn.FilePath(), sn.DataHandle(), sn.Size())
	idxRegion := silkworm.NewMemoryMappedRegion(sn.Index().FilePath(), sn.Index().DataHandle(), sn.Index().Size())
	return silkworm.NewMappedBodySnapshot(segmentRegion, idxRegion)
}

func mappedTxnSnapshot(sn *Segment) *silkworm.MappedTxnSnapshot {
	segmentRegion := silkworm.NewMemoryMappedRegion(sn.FilePath(), sn.DataHandle(), sn.Size())
	idxTxnHash := sn.Index(coresnaptype.Indexes.TxnHash)
	idxTxnHashRegion := silkworm.NewMemoryMappedRegion(idxTxnHash.FilePath(), idxTxnHash.DataHandle(), idxTxnHash.Size())
	idxTxnHash2BlockNum := sn.Index(coresnaptype.Indexes.TxnHash2BlockNum)
	idxTxnHash2BlockRegion := silkworm.NewMemoryMappedRegion(idxTxnHash2BlockNum.FilePath(), idxTxnHash2BlockNum.DataHandle(), idxTxnHash2BlockNum.Size())
	return silkworm.NewMappedTxnSnapshot(segmentRegion, idxTxnHashRegion, idxTxnHash2BlockRegion)
}

func (s *RoSnapshots) AddSnapshotsToSilkworm(silkwormInstance *silkworm.Silkworm) error {
	mappedHeaderSnapshots := make([]*silkworm.MappedHeaderSnapshot, 0)
	if headers, ok := s.segments.Get(coresnaptype.Enums.Headers); ok {
		err := headers.View(func(segments []*Segment) error {
			for _, headerSegment := range segments {
				mappedHeaderSnapshots = append(mappedHeaderSnapshots, mappedHeaderSnapshot(headerSegment))
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	mappedBodySnapshots := make([]*silkworm.MappedBodySnapshot, 0)
	if bodies, ok := s.segments.Get(coresnaptype.Enums.Bodies); ok {
		err := bodies.View(func(segments []*Segment) error {
			for _, bodySegment := range segments {
				mappedBodySnapshots = append(mappedBodySnapshots, mappedBodySnapshot(bodySegment))
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	mappedTxnSnapshots := make([]*silkworm.MappedTxnSnapshot, 0)
	if txs, ok := s.segments.Get(coresnaptype.Enums.Transactions); ok {
		err := txs.View(func(segments []*Segment) error {
			for _, txnSegment := range segments {
				mappedTxnSnapshots = append(mappedTxnSnapshots, mappedTxnSnapshot(txnSegment))
			}
			return nil
		})
		if err != nil {
			return err
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
	s           *RoSnapshots
	baseSegType snaptype.Type
	closed      bool
}

func (s *View) WithBaseSegType(t snaptype.Type) *View {
	v := *s
	s.baseSegType = t
	return &v
}

func (s *RoSnapshots) View() *View {
	v := &View{s: s, baseSegType: coresnaptype.Headers}
	s.segments.Scan(func(segtype snaptype.Enum, value *Segments) bool {
		value.RLock()
		return true
	})
	return v
}

func (v *View) Close() {
	if v.closed {
		return
	}
	v.closed = true
	v.s.segments.Scan(func(segtype snaptype.Enum, value *Segments) bool {
		value.RUnlock()
		return true
	})
}

var noop = func() {}

func (s *RoSnapshots) ViewType(t snaptype.Type) (segments []*Segment, release func()) {
	segs, ok := s.segments.Get(t.Enum())
	if !ok {
		return nil, noop
	}

	segs.RLock()
	var released = false
	return segs.Segments, func() {
		if released {
			return
		}
		segs.RUnlock()
		released = true
	}
}

func (s *RoSnapshots) ViewSingleFile(t snaptype.Type, blockNum uint64) (segment *Segment, ok bool, release func()) {
	segs, ok := s.segments.Get(t.Enum())
	if !ok {
		return nil, false, noop
	}

	segs.RLock()
	var released = false
	for _, seg := range segs.Segments {
		if !(blockNum >= seg.from && blockNum < seg.to) {
			continue
		}
		return seg, true, func() {
			if released {
				return
			}
			segs.RUnlock()
			released = true
		}
	}
	segs.RUnlock()
	return nil, false, noop
}

func (v *View) Segments(t snaptype.Type) []*Segment {
	if s, ok := v.s.segments.Get(t.Enum()); ok {
		return s.Segments
	}
	return nil
}

func (v *View) Segment(t snaptype.Type, blockNum uint64) (*Segment, bool) {
	if s, ok := v.s.segments.Get(t.Enum()); ok {
		for _, seg := range s.Segments {
			if !(blockNum >= seg.from && blockNum < seg.to) {
				continue
			}
			return seg, true
		}
	}
	return nil, false
}

func (v *View) Ranges() (ranges []Range) {
	for _, sn := range v.Segments(v.baseSegType) {
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
