// Copyright 2022 The Erigon Authors
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

package state

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rand2 "golang.org/x/exp/rand"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/bitmapdb"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/diagnostics/diaglib"
	"github.com/erigontech/erigon/execution/commitment"
)

type Aggregator struct {
	db       kv.RoDB
	d        [kv.DomainLen]*Domain
	iis      []*InvertedIndex
	dirs     datadir.Dirs
	stepSize uint64

	dirtyFilesLock           sync.Mutex
	visibleFilesLock         sync.RWMutex
	visibleFilesMinimaxTxNum atomic.Uint64
	snapshotBuildSema        *semaphore.Weighted

	collateAndBuildWorkers int // minimize amount of background workers by default
	mergeWorkers           int // usually 1

	// To keep DB small - need move data to small files ASAP.
	// It means goroutine which creating small files - can't be locked by merge or indexing.
	buildingFiles atomic.Bool
	mergingFiles  atomic.Bool

	//warmupWorking          atomic.Bool
	ctx       context.Context
	ctxCancel context.CancelFunc

	wg sync.WaitGroup // goroutines spawned by Aggregator, to ensure all of them are finish at agg.Close

	onFilesChange kv.OnFilesChange
	onFilesDelete kv.OnFilesChange

	ps *background.ProgressSet

	// next fields are set only if agg.doTraceCtx is true. can enable by env: TRACE_AGG=true
	leakDetector *dbg.LeakDetector
	logger       log.Logger

	produce bool

	checker *DependencyIntegrityChecker
}

func newAggregatorOld(ctx context.Context, dirs datadir.Dirs, stepSize uint64, db kv.RoDB, logger log.Logger) (*Aggregator, error) {
	ctx, ctxCancel := context.WithCancel(ctx)
	return &Aggregator{
		ctx:                    ctx,
		ctxCancel:              ctxCancel,
		onFilesChange:          func(frozenFileNames []string) {},
		onFilesDelete:          func(frozenFileNames []string) {},
		dirs:                   dirs,
		stepSize:               stepSize,
		db:                     db,
		leakDetector:           dbg.NewLeakDetector("agg", dbg.SlowTx()),
		ps:                     background.NewProgressSet(),
		logger:                 logger,
		collateAndBuildWorkers: 1,
		mergeWorkers:           1,

		produce: true,
	}, nil
}

// GetStateIndicesSalt - try read salt for all indices from DB. Or fall-back to new salt creation.
// if db is Read-Only (for example remote RPCDaemon or utilities) - we will not create new indices -
// and existing indices have salt in metadata.
func GetStateIndicesSalt(dirs datadir.Dirs, genNew bool, logger log.Logger) (salt *uint32, err error) {
	baseDir := dirs.Snap
	saltExists, err := dir.FileExist(filepath.Join(baseDir, "salt.txt"))
	if err != nil {
		return nil, err
	}

	saltStateExists, err := dir.FileExist(filepath.Join(baseDir, "salt-state.txt"))
	if err != nil {
		return nil, err
	}

	if saltExists && !saltStateExists {
		_ = os.Rename(filepath.Join(baseDir, "salt.txt"), filepath.Join(baseDir, "salt-state.txt"))
	}

	fpath := filepath.Join(baseDir, "salt-state.txt")
	fexists, err := dir.FileExist(fpath)
	if err != nil {
		return nil, err
	}

	// Initialize salt if it doesn't exist
	if !fexists {
		if !genNew {
			logger.Debug("not generating new state-salt file as genNew=false")
			// Using nil salt for now, actual value should be injected when salt file is downloaded
			return nil, nil
		}
		logger.Info("generating new salt file")

		saltV := rand2.Uint32()
		salt = &saltV
		saltBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(saltBytes, *salt)
		if err := dir.WriteFileWithFsync(fpath, saltBytes, os.ModePerm); err != nil {
			return nil, err
		}
		return salt, nil // Return the newly created salt directly
	}

	saltBytes, err := os.ReadFile(fpath)
	if err != nil {
		return nil, err
	}
	saltV := binary.BigEndian.Uint32(saltBytes)
	salt = &saltV
	return salt, nil
}

func (a *Aggregator) RegisterDomain(cfg statecfg.DomainCfg, salt *uint32, dirs datadir.Dirs, logger log.Logger) (err error) {
	a.d[cfg.Name], err = NewDomain(cfg, a.stepSize, dirs, logger)
	if err != nil {
		return err
	}
	a.d[cfg.Name].salt.Store(salt)
	a.AddDependencyBtwnHistoryII(cfg.Name)
	return nil
}

func (a *Aggregator) RegisterII(cfg statecfg.InvIdxCfg, salt *uint32, dirs datadir.Dirs, logger log.Logger) error {
	if ii := a.searchII(cfg.Name); ii != nil {
		return fmt.Errorf("inverted index %s already registered", cfg.Name)
	}
	ii, err := NewInvertedIndex(cfg, a.stepSize, dirs, logger)
	if err != nil {
		return err
	}
	ii.salt.Store(salt)
	a.iis = append(a.iis, ii)
	return nil
}

func (a *Aggregator) OnFilesChange(onChange, onDel kv.OnFilesChange) {
	a.onFilesChange = onChange
	a.onFilesDelete = onDel
}

func (a *Aggregator) StepSize() uint64 { return a.stepSize }
func (a *Aggregator) DisableFsync() {
	for _, d := range a.d {
		d.DisableFsync()
	}
	for _, ii := range a.iis {
		ii.DisableFsync()
	}
}

func (a *Aggregator) reloadSalt() error {
	salt, err := GetStateIndicesSalt(a.dirs, false, a.logger)
	if err != nil {
		return err
	}

	if salt == nil {
		return errors.New("salt not found on ReloadSalt")
	}

	for _, d := range a.d {
		d.salt.Store(salt)
	}

	for _, ii := range a.iis {
		ii.salt.Store(salt)
	}

	return nil
}

func (a *Aggregator) AddDependencyBtwnDomains(dependency kv.Domain, dependent kv.Domain) {
	dd := a.d[dependent]
	if dd.Disable || a.d[dependency].Disable {
		a.logger.Debug("skipping dependency between disabled domains", "dependency", dependency, "dependent", dependent)
		return
	}
	// "hard alignment":
	// only corresponding files should be included. e.g. commitment + account -
	// cannot have merged account visibleFile, and unmerged commitment visibleFile for same step range.
	if a.checker == nil {
		a.checker = NewDependencyIntegrityChecker(a.dirs, a.logger)
	}

	a.checker.AddDependency(FromDomain(dependency), &DependentInfo{
		entity:      FromDomain(dependent),
		filesGetter: func() *btree.BTreeG[*FilesItem] { return dd.dirtyFiles },
		accessors:   dd.Accessors,
	})
	a.d[dependency].SetChecker(a.checker)
}

func (a *Aggregator) AddDependencyBtwnHistoryII(domain kv.Domain) {
	// ii has checker on history dirtyFiles (same domain)
	dd := a.d[domain]
	if dd.HistCfg.SnapshotsDisabled || dd.HistCfg.HistoryDisabled || dd.Disable {
		a.logger.Debug("history or ii disabled, can't register dependency", "domain", domain.String())
		return
	}

	if a.checker == nil {
		a.checker = NewDependencyIntegrityChecker(a.dirs, a.logger)
	}

	h := dd.History
	ue := FromII(dd.InvertedIndex.InvIdxCfg.Name)
	a.checker.AddDependency(ue, &DependentInfo{
		entity: ue,
		filesGetter: func() *btree.BTreeG[*FilesItem] {
			return h.dirtyFiles
		},
		accessors: h.Accessors,
	})
	h.InvertedIndex.SetChecker(a.checker)
}

func (a *Aggregator) EnableAllDependencies() {
	if a.checker == nil {
		return
	}
	a.checker.Enable()
	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()
	a.recalcVisibleFiles(a.dirtyFilesEndTxNumMinimax())
}

func (a *Aggregator) DisableAllDependencies() {
	if a.checker == nil {
		return
	}
	a.checker.Disable()
	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()
	a.recalcVisibleFiles(a.dirtyFilesEndTxNumMinimax())
}

func (a *Aggregator) OpenFolder() error {
	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()
	if err := a.reloadSalt(); err != nil {
		return err
	}
	if err := a.openFolder(); err != nil {
		return fmt.Errorf("OpenFolder: %w", err)
	}
	return nil
}

// TODO: convert this func to `map` or struct instead of 4 return params
func scanDirs(dirs datadir.Dirs) (r *ScanDirsResult, err error) {
	r = &ScanDirsResult{}
	r.iiFiles, err = filesFromDir(dirs.SnapIdx)
	if err != nil {
		return
	}
	r.historyFiles, err = filesFromDir(dirs.SnapHistory)
	if err != nil {
		return
	}
	r.domainFiles, err = filesFromDir(dirs.SnapDomain)
	if err != nil {
		return
	}
	return r, nil
}

type ScanDirsResult struct {
	domainFiles  []string
	historyFiles []string
	iiFiles      []string
}

func (a *Aggregator) openFolder() error {
	scanDirsRes, err := scanDirs(a.dirs)
	if err != nil {
		return err
	}

	eg := &errgroup.Group{}
	for _, d := range a.d {
		if d.Disable {
			continue
		}

		d := d
		eg.Go(func() error {
			select {
			case <-a.ctx.Done():
				return a.ctx.Err()
			default:
			}
			return d.openFolder(scanDirsRes)
		})
	}
	for _, ii := range a.iis {
		if ii.Disable {
			continue
		}
		ii := ii
		eg.Go(func() error { return ii.openFolder(scanDirsRes) })
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("openFolder: %w", err)
	}
	a.recalcVisibleFiles(a.dirtyFilesEndTxNumMinimax())
	return nil
}

func (a *Aggregator) ReloadFiles() error {
	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()
	a.closeDirtyFiles()
	return a.openFolder()
}

func (a *Aggregator) OpenList(files []string, readonly bool) error {
	return a.OpenFolder()
}

func (a *Aggregator) WaitForFiles() {
	for range a.WaitForBuildAndMerge(a.ctx) {
		// The loop will exit when the channel is closed
	}
}

func (a *Aggregator) Close() {
	a.WaitForFiles()
	if a.ctxCancel == nil { // invariant: it's safe to call Close multiple times
		return
	}
	a.ctxCancel()
	a.ctxCancel = nil
	a.wg.Wait()

	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()
	a.closeDirtyFiles()
	a.recalcVisibleFiles(a.dirtyFilesEndTxNumMinimax())
}

func (a *Aggregator) closeDirtyFiles() {
	wg := &sync.WaitGroup{}
	for _, d := range a.d {
		d := d
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.Close()
		}()
	}
	for _, ii := range a.iis {
		ii := ii
		wg.Add(1)
		go func() {
			defer wg.Done()
			ii.Close()
		}()
	}
	wg.Wait()
}

func (a *Aggregator) EnableDomain(domain kv.Domain)   { a.d[domain].Disable = false }
func (a *Aggregator) SetCollateAndBuildWorkers(i int) { a.collateAndBuildWorkers = i }
func (a *Aggregator) SetMergeWorkers(i int)           { a.mergeWorkers = i }
func (a *Aggregator) SetCompressWorkers(i int) {
	for _, d := range a.d {
		d.CompressCfg.Workers = i
		d.History.CompressorCfg.Workers = i
		d.History.InvertedIndex.CompressorCfg.Workers = i
	}
	for _, ii := range a.iis {
		ii.CompressorCfg.Workers = i
	}
}

func (a *Aggregator) HasBackgroundFilesBuild2() bool {
	return a.buildingFiles.Load() || a.mergingFiles.Load()
}

func (a *Aggregator) HasBackgroundFilesBuild() bool { return a.ps.Has() }
func (a *Aggregator) BackgroundProgress() string    { return a.ps.String() }

type VisibleFile = kv.VisibleFile
type VisibleFiles = kv.VisibleFiles

func (at *AggregatorRoTx) AllFiles() VisibleFiles {
	var res VisibleFiles
	if at == nil {
		return res
	}
	for _, d := range at.d {
		res = append(res, d.Files()...)
	}
	for _, ii := range at.iis {
		res = append(res, ii.Files()...)
	}
	return res
}
func (at *AggregatorRoTx) Files(domain kv.Domain) VisibleFiles { return at.d[domain].Files() }
func (at *AggregatorRoTx) StepSize() uint64                    { return at.a.StepSize() }

func (a *Aggregator) Files() []string {
	ac := a.BeginFilesRo()
	defer ac.Close()
	return ac.AllFiles().Fullpaths()
}
func (a *Aggregator) LS() {
	doLS := func(dirtyFiles *btree.BTreeG[*FilesItem]) {
		dirtyFiles.Walk(func(items []*FilesItem) bool {
			for _, item := range items {
				if item.decompressor == nil {
					continue
				}
				a.logger.Info("[agg] ", "f", item.decompressor.FileName(), "words", item.decompressor.Count())
			}
			return true
		})
	}

	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()
	for _, d := range a.d {
		doLS(d.dirtyFiles)
		doLS(d.History.dirtyFiles)
		doLS(d.History.InvertedIndex.dirtyFiles)
	}
	for _, d := range a.iis {
		doLS(d.dirtyFiles)
	}
}

func (a *Aggregator) WaitForBuildAndMerge(ctx context.Context) chan struct{} {
	res := make(chan struct{})
	go func() {
		defer close(res)

		chkEvery := time.NewTicker(3 * time.Second)
		defer chkEvery.Stop()
		for a.buildingFiles.Load() || a.mergingFiles.Load() {
			select {
			case <-ctx.Done():
				return
			case <-chkEvery.C:
				a.logger.Trace("[agg] waiting for files",
					"building files", a.buildingFiles.Load(),
					"merging files", a.mergingFiles.Load())
			}
		}
	}()
	return res
}

func (a *Aggregator) BuildMissedAccessors(ctx context.Context, workers int) error {
	startIndexingTime := time.Now()
	ps := background.NewProgressSet()

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	go func() {
		logEvery := time.NewTicker(20 * time.Second)
		defer logEvery.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-logEvery.C:
				var m runtime.MemStats
				dbg.ReadMemStats(&m)
				sendDiagnostics(startIndexingTime, ps.DiagnosticsData(), m.Alloc, m.Sys)
				a.logger.Info("[snapshots] Indexing", "progress", ps.String(), "total-indexing-time", time.Since(startIndexingTime).Round(time.Second).String(), "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
			}
		}
	}()

	rotx := a.DebugBeginDirtyFilesRo()
	defer rotx.Close()

	missedFilesItems := rotx.FilesWithMissedAccessors()
	if !missedFilesItems.IsEmpty() {
		defer a.onFilesChange(nil)
	}

	for _, d := range a.d {
		d.BuildMissedAccessors(ctx, g, ps, missedFilesItems.domain[d.Name])
	}

	for _, ii := range a.iis {
		ii.BuildMissedAccessors(ctx, g, ps, missedFilesItems.ii[ii.Name])
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	rotx.Close()

	if err := a.OpenFolder(); err != nil {
		return err
	}
	return nil
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

type AggV3Collation struct {
	logAddrs   map[string]*roaring64.Bitmap
	logTopics  map[string]*roaring64.Bitmap
	tracesFrom map[string]*roaring64.Bitmap
	tracesTo   map[string]*roaring64.Bitmap
	accounts   Collation
	storage    Collation
	code       Collation
	commitment Collation
}

func (c AggV3Collation) Close() {
	c.accounts.Close()
	c.storage.Close()
	c.code.Close()
	c.commitment.Close()

	for _, b := range c.logAddrs {
		bitmapdb.ReturnToPool64(b)
	}
	for _, b := range c.logTopics {
		bitmapdb.ReturnToPool64(b)
	}
	for _, b := range c.tracesFrom {
		bitmapdb.ReturnToPool64(b)
	}
	for _, b := range c.tracesTo {
		bitmapdb.ReturnToPool64(b)
	}
}

type AggV3StaticFiles struct {
	d    [kv.DomainLen]StaticFiles
	ivfs []InvertedFiles
}

// CleanupOnError - call it on collation fail. It's closing all files
func (sf AggV3StaticFiles) CleanupOnError() {
	for _, d := range sf.d {
		d.CleanupOnError()
	}
	for _, ivf := range sf.ivfs {
		ivf.CleanupOnError()
	}
}

func (a *Aggregator) buildFiles(ctx context.Context, step kv.Step) error {
	a.logger.Debug("[agg] collate and build", "step", step, "collate_workers", a.collateAndBuildWorkers, "merge_workers", a.mergeWorkers, "compress_workers", a.d[kv.AccountsDomain].CompressCfg.Workers)

	var (
		logEvery      = time.NewTicker(time.Second * 30)
		txFrom        = a.FirstTxNumOfStep(step)
		txTo          = a.FirstTxNumOfStep(step + 1)
		stepStartedAt = time.Now()

		static          = &AggV3StaticFiles{ivfs: make([]InvertedFiles, len(a.iis))}
		closeCollations = true
		collListMu      = sync.Mutex{}
		collations      = make([]Collation, 0)
	)

	defer logEvery.Stop()
	defer func() {
		if !closeCollations {
			return
		}
		for _, c := range collations {
			c.Close()
		}
	}()

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(a.collateAndBuildWorkers)
	for _, d := range a.d {
		if d.Disable {
			continue
		}

		d := d
		dc := d.BeginFilesRo()
		firstStepNotInFiles := dc.FirstStepNotInFiles()
		dc.Close()
		if step < firstStepNotInFiles {
			continue
		}

		a.wg.Add(1)
		g.Go(func() error {
			defer a.wg.Done()

			var collation Collation
			if err := a.db.View(ctx, func(tx kv.Tx) (err error) {
				collation, err = d.collate(ctx, step, txFrom, txTo, tx)
				return err
			}); err != nil {
				return fmt.Errorf("domain collation %q has failed: %w", d.FilenameBase, err)
			}
			collListMu.Lock()
			collations = append(collations, collation)
			collListMu.Unlock()

			sf, err := d.buildFiles(ctx, step, collation, a.ps)
			collation.Close()
			if err != nil {
				sf.CleanupOnError()
				return err
			}

			dd, err := kv.String2Domain(d.FilenameBase)
			if err != nil {
				return err
			}
			static.d[dd] = sf
			return nil
		})
	}
	closeCollations = false

	// indices are built concurrently
	for iikey, ii := range a.iis {
		if ii.Disable {
			continue
		}

		ii := ii
		dc := ii.BeginFilesRo()
		firstStepNotInFiles := dc.FirstStepNotInFiles()
		dc.Close()
		if step < firstStepNotInFiles {
			continue
		}

		a.wg.Add(1)
		g.Go(func() error {
			defer a.wg.Done()

			var collation InvertedIndexCollation
			err := a.db.View(ctx, func(tx kv.Tx) (err error) {
				collation, err = ii.collate(ctx, step, tx)
				return err
			})
			if err != nil {
				return fmt.Errorf("index collation %q has failed: %w", ii.FilenameBase, err)
			}
			sf, err := ii.buildFiles(ctx, step, collation, a.ps)
			if err != nil {
				sf.CleanupOnError()
				return err
			}

			static.ivfs[iikey] = sf
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		static.CleanupOnError()
		return fmt.Errorf("domain collate-build: %w", err)
	}
	mxStepTook.ObserveDuration(stepStartedAt)
	a.IntegrateDirtyFiles(static, txFrom, txTo)
	a.logger.Info("[snapshots] aggregated", "step", step, "took", time.Since(stepStartedAt))

	return nil
}

func (a *Aggregator) BuildFiles(toTxNum uint64) (err error) {
	finished := a.BuildFilesInBackground(toTxNum)
	if !(a.buildingFiles.Load() || a.mergingFiles.Load()) {
		return nil
	}

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
Loop:
	for {
		select {
		case <-a.ctx.Done():
			return a.ctx.Err()
		case <-finished:
			break Loop
		case <-logEvery.C:
			if !(a.buildingFiles.Load() || a.mergingFiles.Load()) {
				break Loop
			}
			if a.HasBackgroundFilesBuild() {
				a.logger.Info("[snapshots] Files build", "progress", a.BackgroundProgress())
			}
		}
	}

	return nil
}

// [from, to)
func (a *Aggregator) BuildFiles2(ctx context.Context, fromStep, toStep kv.Step) error {
	if ok := a.buildingFiles.CompareAndSwap(false, true); !ok {
		return nil
	}
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer a.buildingFiles.Store(false)
		if toStep > fromStep {
			log.Info("[agg] build", "fromStep", fromStep, "toStep", toStep)
		}
		for step := fromStep; step < toStep; step++ { //`step` must be fully-written - means `step+1` records must be visible
			if err := a.buildFiles(ctx, step); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, common.ErrStopped) {
					panic(err)
				}
				a.logger.Warn("[snapshots] buildFilesInBackground", "err", err)
				panic(err)
			}
			a.onFilesChange(nil)
		}

		go func() {
			if err := a.MergeLoop(ctx); err != nil {
				panic(err)
			}
		}()
	}()
	return nil
}

func (a *Aggregator) mergeLoopStep(ctx context.Context, toTxNum uint64) (somethingDone bool, err error) {
	a.logger.Debug("[agg] merge", "collate_workers", a.collateAndBuildWorkers, "merge_workers", a.mergeWorkers, "compress_workers", a.d[kv.AccountsDomain].CompressCfg.Workers)

	aggTx := a.BeginFilesRo()
	defer aggTx.Close()
	mxRunningMerges.Inc()
	defer mxRunningMerges.Dec()

	maxSpan := config3.StepsInFrozenFile * a.StepSize()
	r := aggTx.findMergeRange(toTxNum, maxSpan)
	if !r.any() {
		a.cleanAfterMerge(nil)
		return false, nil
	}

	outs, err := aggTx.FilesInRange(r)
	if err != nil {
		return false, err
	}

	in, err := aggTx.mergeFiles(ctx, outs, r)
	if err != nil {
		in.Close()
		return true, err
	}
	a.IntegrateMergedDirtyFiles(outs, in)
	a.cleanAfterMerge(in)
	return true, nil
}

func (a *Aggregator) RemoveOverlapsAfterMerge(ctx context.Context) (err error) {
	a.cleanAfterMerge(nil)
	return nil
}

func (a *Aggregator) MergeLoop(ctx context.Context) (err error) {
	if dbg.NoMerge() || !a.mergingFiles.CompareAndSwap(false, true) {
		return nil // currently merging or merge is prohibited
	}

	// Merge is background operation. It must not crush application.
	// Convert panic to error.
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("[snapshots] background files merge: %s, %s", rec, dbg.Stack())
		}
	}()

	a.wg.Add(1)
	defer a.wg.Done()
	defer a.mergingFiles.Store(false)

	for {
		somethingMerged, err := a.mergeLoopStep(ctx, a.visibleFilesMinimaxTxNum.Load())
		if err != nil {
			return err
		}
		if !somethingMerged {
			return nil
		}
	}
}

func (a *Aggregator) IntegrateDirtyFiles(sf *AggV3StaticFiles, txNumFrom, txNumTo uint64) {
	defer a.onFilesChange(nil) //TODO: add relative file paths

	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()

	for id, d := range a.d {
		d.integrateDirtyFiles(sf.d[id], txNumFrom, txNumTo)
	}
	for id, ii := range a.iis {
		ii.integrateDirtyFiles(sf.ivfs[id], txNumFrom, txNumTo)
	}

	a.recalcVisibleFiles(a.dirtyFilesEndTxNumMinimax())
}

func (a *Aggregator) DomainTables(names ...kv.Domain) (tables []string) {
	for _, name := range names {
		tables = append(tables, a.d[name].Tables()...)
	}
	return tables
}
func (at *AggregatorRoTx) DomainFiles(domains ...kv.Domain) (files VisibleFiles) {
	for _, domain := range domains {
		files = append(files, at.d[domain].Files()...)
	}
	return files
}
func (at *AggregatorRoTx) CurrentDomainVersion(domain kv.Domain) version.Version {
	return at.d[domain].d.Version.DataKV.Current
}
func (a *Aggregator) InvertedIdxTables(indices ...kv.InvertedIdx) (tables []string) {
	for _, idx := range indices {
		if ii := a.searchII(idx); ii != nil {
			tables = append(tables, ii.Tables()...)
		}
	}

	return
}

func (a *Aggregator) searchII(name kv.InvertedIdx) *InvertedIndex {
	for _, ii := range a.iis {
		if ii.Name == name {
			return ii
		}
	}
	return nil
}

type flusher interface {
	Flush(ctx context.Context, tx kv.RwTx) error
}

func (at *AggregatorRoTx) StepsInFiles(entitySet ...kv.Domain) uint64 {
	txNumInFiles := at.TxNumsInFiles(entitySet...)
	if txNumInFiles > 0 {
		txNumInFiles--
	}
	return txNumInFiles / at.StepSize()
}

func (at *AggregatorRoTx) TxNumsInFiles(entitySet ...kv.Domain) (minTxNum uint64) {
	if len(entitySet) == 0 {
		panic("assert: missed arguments")
	}
	minTxNum = math.MaxUint64
	for _, domain := range entitySet {
		domainEnd := at.d[domain].files.EndTxNum()
		if domainEnd < minTxNum {
			minTxNum = domainEnd
		}
	}
	return minTxNum
}

func (at *AggregatorRoTx) CanPrune(tx kv.Tx, untilTx uint64) bool {
	if dbg.NoPrune() {
		return false
	}
	for _, d := range at.d {
		if d.CanPruneUntil(tx, untilTx) {
			return true
		}
	}
	for _, ii := range at.iis {
		if ii.CanPrune(tx) {
			return true
		}
	}
	return false
}

func (at *AggregatorRoTx) CanUnwindToBlockNum(tx kv.Tx) (uint64, error) {
	minUnwindale, err := ReadLowestUnwindableBlock(tx)
	if err != nil {
		return 0, err
	}
	if minUnwindale == math.MaxUint64 { // no unwindable block found
		stateVal, _, _, err := at.d[kv.CommitmentDomain].GetLatest(keyCommitmentState, tx)
		if err != nil {
			return 0, err
		}
		if len(stateVal) == 0 {
			return 0, nil
		}
		_, minUnwindale = _decodeTxBlockNums(stateVal)
	}
	return minUnwindale, nil
}

// CanUnwindBeforeBlockNum - returns `true` if can unwind to requested `blockNum`, otherwise returns nearest `unwindableBlockNum`
func (at *AggregatorRoTx) CanUnwindBeforeBlockNum(blockNum uint64, tx kv.Tx) (unwindableBlockNum uint64, ok bool, err error) {
	_minUnwindableBlockNum, err := at.CanUnwindToBlockNum(tx)
	if err != nil {
		return 0, false, err
	}
	if blockNum < _minUnwindableBlockNum {
		return _minUnwindableBlockNum, false, nil
	}
	return blockNum, true, nil
}

// PruneSmallBatches is not cancellable, it's over when it's over or failed.
// It fills whole timeout with pruning by small batches (of 100 keys) and making some progress
func (at *AggregatorRoTx) PruneSmallBatches(ctx context.Context, timeout time.Duration, tx kv.RwTx) (haveMore bool, err error) {
	if dbg.NoPrune() {
		return false, nil
	}
	// On tip-of-chain timeout is about `3sec`
	//  On tip of chain:     must be real-time - prune by small batches and prioritize exact-`timeout`
	//  Not on tip of chain: must be aggressive (prune as much as possible) by bigger batches

	furiousPrune := timeout > 5*time.Hour
	aggressivePrune := !furiousPrune && timeout >= 1*time.Minute

	var pruneLimit uint64 = 100
	if furiousPrune {
		pruneLimit = 1_000_000
	}

	started := time.Now()
	localTimeout := time.NewTicker(timeout)
	defer localTimeout.Stop()
	logPeriod := 30 * time.Second
	logEvery := time.NewTicker(logPeriod)
	defer logEvery.Stop()
	aggLogEvery := time.NewTicker(600 * time.Second) // to hide specific domain/idx logging
	defer aggLogEvery.Stop()

	fullStat := newAggregatorPruneStat()

	for {
		if sptx, ok := tx.(kv.HasSpaceDirty); ok && !furiousPrune && !aggressivePrune {
			spaceDirty, _, err := sptx.SpaceDirty()
			if err != nil {
				return false, err
			}
			if spaceDirty > uint64(statecfg.MaxNonFuriousDirtySpacePerTx) {
				return false, nil
			}
		}
		iterationStarted := time.Now()
		// `context.Background()` is important here!
		//     it allows keep DB consistent - prune all keys-related data or noting
		//     can't interrupt by ctrl+c and leave dirt in DB
		stat, err := at.prune(context.Background(), tx, pruneLimit, aggLogEvery)
		if err != nil {
			at.a.logger.Warn("[snapshots] PruneSmallBatches failed", "err", err)
			return false, err
		}
		if stat == nil || stat.PrunedNothing() {
			if !fullStat.PrunedNothing() {
				at.a.logger.Info("[snapshots] PruneSmallBatches finished", "took", time.Since(started).String(), "stat", fullStat.String())
			}
			return false, nil
		}
		fullStat.Accumulate(stat)

		if aggressivePrune {
			took := time.Since(iterationStarted)
			if took < 2*time.Second {
				pruneLimit *= 10
			}
			if took > logPeriod {
				pruneLimit /= 10
			}
		}

		select {
		case <-localTimeout.C: //must be first to improve responsivness
			return true, nil
		case <-logEvery.C:
			if furiousPrune {
				at.a.logger.Info("[prune] state",
					//"until commit", time.Until(started.Add(timeout)).String(),
					//"pruneLimit", pruneLimit,
					//"aggregatedStep", at.StepsInFiles(kv.AccountsDomain),
					"stepsRangeInDB", at.stepsRangeInDBAsStr(tx),
					//"pruned", fullStat.String(),
				)
			} else {
				at.a.logger.Info("[prune] state",
					"until commit", time.Until(started.Add(timeout)).String(),
					//"pruneLimit", pruneLimit,
					//"aggregatedStep", at.StepsInFiles(kv.AccountsDomain),
					"stepsRangeInDB", at.stepsRangeInDBAsStr(tx),
					//"pruned", fullStat.String(),
				)
			}

		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}
	}
}

func (at *AggregatorRoTx) stepsRangeInDBAsStr(tx kv.Tx) string {
	steps := make([]string, 0, len(at.d)+len(at.iis))
	for _, dt := range at.d {
		a1, a2 := dt.stepsRangeInDB(tx)
		steps = append(steps, fmt.Sprintf("%s:%.1f", dt.d.FilenameBase, a2-a1))
	}
	for _, iit := range at.iis {
		a1, a2 := iit.stepsRangeInDB(tx)
		steps = append(steps, fmt.Sprintf("%s:%.1f", iit.ii.FilenameBase, a2-a1))
	}
	return strings.Join(steps, ", ")
}

type AggregatorPruneStat struct {
	Domains map[string]*DomainPruneStat
	Indices map[string]*InvertedIndexPruneStat
}

func (as *AggregatorPruneStat) PrunedNothing() bool {
	for _, d := range as.Domains {
		if d != nil && !d.PrunedNothing() {
			return false
		}
	}
	for _, i := range as.Indices {
		if i != nil && !i.PrunedNothing() {
			return false
		}
	}
	return true
}

func newAggregatorPruneStat() *AggregatorPruneStat {
	return &AggregatorPruneStat{Domains: make(map[string]*DomainPruneStat), Indices: make(map[string]*InvertedIndexPruneStat)}
}

func (as *AggregatorPruneStat) String() string {
	if as == nil {
		return ""
	}
	names := make([]string, 0)
	for k := range as.Domains {
		names = append(names, k)
	}

	slices.Sort(names)

	var sb strings.Builder
	for _, d := range names {
		v, ok := as.Domains[d]
		if ok && v != nil && !v.PrunedNothing() {
			sb.WriteString(fmt.Sprintf("%s| %s; ", d, v.String()))
		}
	}
	names = names[:0]
	for k := range as.Indices {
		names = append(names, k)
	}
	slices.Sort(names)

	for _, d := range names {
		v, ok := as.Indices[d]
		if ok && v != nil && !v.PrunedNothing() {
			sb.WriteString(fmt.Sprintf("%s| %s; ", d, v.String()))
		}
	}
	return strings.TrimSuffix(sb.String(), "; ")
}

func (as *AggregatorPruneStat) Accumulate(other *AggregatorPruneStat) {
	for k, v := range other.Domains {
		ds, ok := as.Domains[k]
		if !ok || ds == nil {
			ds = v
		} else {
			ds.Accumulate(v)
		}
		as.Domains[k] = ds
	}
	for k, v := range other.Indices {
		id, ok := as.Indices[k]
		if !ok || id == nil {
			id = v
		} else {
			id.Accumulate(v)
		}
		as.Indices[k] = id
	}
}

// temporal function to prune history straight after commitment is done - reduce history size in db until we build
// pruning in background. This helps on chain-tip performance (while full pruning is not available we can prune at least commit)
func (at *AggregatorRoTx) GreedyPruneHistory(ctx context.Context, domain kv.Domain, tx kv.RwTx) error {
	cd := at.d[domain]
	if cd.ht.h.HistoryDisabled {
		return nil
	}

	txFrom := uint64(0)
	canHist, txTo := cd.ht.canPruneUntil(tx, math.MaxUint64)
	if dbg.NoPrune() || !canHist {
		return nil
	}

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	defer mxPruneTookAgg.ObserveDuration(time.Now())

	stat, err := cd.ht.Prune(ctx, tx, txFrom, txTo, math.MaxUint64, false, logEvery)
	if err != nil {
		return err
	}

	at.a.logger.Info("commitment history backpressure pruning", "pruned", stat.String())
	return nil
}

func (at *AggregatorRoTx) prune(ctx context.Context, tx kv.RwTx, limit uint64, logEvery *time.Ticker) (*AggregatorPruneStat, error) {
	defer mxPruneTookAgg.ObserveDuration(time.Now())

	if limit == 0 {
		limit = uint64(math.MaxUint64)
	}

	var txFrom uint64 // txFrom is always 0 to avoid dangling keys in indices/hist
	var step kv.Step
	txTo := at.a.visibleFilesMinimaxTxNum.Load()
	if txTo > 0 {
		// txTo is first txNum in next step, has to go 1 tx behind to get correct step number
		step = kv.Step((txTo - 1) / at.StepSize())
	}

	if txFrom == txTo || !at.CanPrune(tx, txTo) {
		return nil, nil
	}

	if logEvery == nil {
		logEvery = time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
	}
	//at.a.logger.Info("aggregator prune", "step", step,
	//	"txn_range", fmt.Sprintf("[%d,%d)", txFrom, txTo), "limit", limit,
	//	/*"stepsLimit", limit/at.a.stepSize,*/ "stepsRangeInDB", at.a.stepsRangeInDBAsStr(tx))
	aggStat := newAggregatorPruneStat()
	for id, d := range at.d {
		var err error
		aggStat.Domains[at.d[id].d.FilenameBase], err = d.Prune(ctx, tx, step, txFrom, txTo, limit, logEvery)
		if err != nil {
			return aggStat, err
		}
	}

	stats := make([]*InvertedIndexPruneStat, len(at.a.iis))
	for iikey := range at.a.iis {
		stat, err := at.iis[iikey].Prune(ctx, tx, txFrom, txTo, limit, logEvery, false, nil)
		if err != nil {
			return nil, err
		}
		stats[iikey] = stat
	}
	for iikey := range at.a.iis {
		aggStat.Indices[at.iis[iikey].ii.FilenameBase] = stats[iikey]
	}

	return aggStat, nil
}

func (at *AggregatorRoTx) EndTxNumNoCommitment() uint64 {
	return min(
		at.d[kv.AccountsDomain].files.EndTxNum(),
		at.d[kv.CodeDomain].files.EndTxNum(),
		at.d[kv.StorageDomain].files.EndTxNum(),
	)
}

func (at *AggregatorRoTx) Agg() *Aggregator { return at.a }

func (at *AggregatorRoTx) MinStepInDb(tx kv.Tx, domain kv.Domain) (lstInDb uint64) {
	return at.d[domain].d.minStepInDB(tx)
}

func (a *Aggregator) EndTxNumMinimax() uint64 { return a.visibleFilesMinimaxTxNum.Load() }
func (a *Aggregator) FilesAmount() (res []int) {
	for _, d := range a.d {
		res = append(res, d.dirtyFiles.Len())
	}
	for _, ii := range a.iis {
		res = append(res, ii.dirtyFiles.Len())
	}
	return res
}

func firstTxNumOfStep(step kv.Step, stepSize uint64) uint64 {
	return uint64(step) * stepSize
}

func lastTxNumOfStep(step kv.Step, stepSize uint64) uint64 {
	return firstTxNumOfStep(step+1, stepSize) - 1
}

// firstTxNumOfStep returns txStepBeginning of given step.
// Step 0 is a range [0, stepSize).
// To prune step needed to fully Prune range [txStepBeginning, txNextStepBeginning)
func (a *Aggregator) FirstTxNumOfStep(step kv.Step) uint64 { // could have some smaller steps to prune// could have some smaller steps to prune
	return firstTxNumOfStep(step, a.StepSize())
}

func (a *Aggregator) dirtyFilesEndTxNumMinimax() uint64 {
	m := min(
		a.d[kv.AccountsDomain].dirtyFilesEndTxNumMinimax(),
		a.d[kv.StorageDomain].dirtyFilesEndTxNumMinimax(),
		a.d[kv.CodeDomain].dirtyFilesEndTxNumMinimax(),
		// a.d[kv.CommitmentDomain].dirtyFilesEndTxNumMinimax(),
	)
	// TODO(awskii) have two different functions including commitment/without it
	//  Usually its skipped because commitment either have MaxUint64 due to no history or equal to other domains

	//log.Warn("dirtyFilesEndTxNumMinimax", "min", m,
	//	"acc", a.d[kv.AccountsDomain].dirtyFilesEndTxNumMinimax(),
	//	"sto", a.d[kv.StorageDomain].dirtyFilesEndTxNumMinimax(),
	//	"cod", a.d[kv.CodeDomain].dirtyFilesEndTxNumMinimax(),
	//	"com", a.d[kv.CommitmentDomain].dirtyFilesEndTxNumMinimax(),
	//)
	return m
}

func (a *Aggregator) recalcVisibleFiles(toTxNum uint64) {
	defer a.recalcVisibleFilesMinimaxTxNum()

	a.visibleFilesLock.Lock()
	defer a.visibleFilesLock.Unlock()
	for _, d := range a.d {
		if d == nil {
			continue
		}
		d.reCalcVisibleFiles(toTxNum)
	}
	for _, ii := range a.iis {
		if ii == nil {
			continue
		}
		ii.reCalcVisibleFiles(toTxNum)
	}
}

func (a *Aggregator) recalcVisibleFilesMinimaxTxNum() {
	aggTx := a.BeginFilesRo()
	defer aggTx.Close()
	a.visibleFilesMinimaxTxNum.Store(aggTx.TxNumsInFiles(kv.StateDomains...))
}

func (at *AggregatorRoTx) findMergeRange(maxEndTxNum, maxSpan uint64) *Ranges {
	r := &Ranges{invertedIndex: make([]*MergeRange, len(at.a.iis))}
	commitmentUseReferencedBranches := at.a.d[kv.CommitmentDomain].ReplaceKeysInValues
	if commitmentUseReferencedBranches {
		lmrAcc := at.d[kv.AccountsDomain].files.LatestMergedRange()
		lmrSto := at.d[kv.StorageDomain].files.LatestMergedRange()
		lmrCom := at.d[kv.CommitmentDomain].files.LatestMergedRange()

		if !lmrCom.Equal(&lmrAcc) || !lmrCom.Equal(&lmrSto) {
			// ensure that we do not make further merge progress until ranges are not equal
			maxEndTxNum = min(maxEndTxNum, max(lmrAcc.to, lmrSto.to, lmrCom.to))
			at.a.logger.Warn("findMergeRange: hold further merge", "to", maxEndTxNum/at.StepSize(),
				"acc", lmrAcc.String("", at.StepSize()), "sto", lmrSto.String("", at.StepSize()), "com", lmrCom.String("", at.StepSize()))
		}
	}
	for id, d := range at.d {
		if d.d.Disable {
			continue
		}
		r.domain[id] = d.findMergeRange(maxEndTxNum, maxSpan)
	}

	if commitmentUseReferencedBranches && r.domain[kv.CommitmentDomain].values.needMerge {
		cr := r.domain[kv.CommitmentDomain]

		restorePrevRange := false
		for k, dr := range &r.domain {
			kd := kv.Domain(k)
			if kd == kv.CommitmentDomain || cr.values.Equal(&dr.values) {
				continue
			}
			if !(kd == kv.AccountsDomain || kd == kv.StorageDomain || kd == kv.CommitmentDomain) {
				continue
			}
			// commitment waits until storage and account are merged so it may be a bit behind (if merge was interrupted before)
			if !dr.values.needMerge || cr.values.to < dr.values.from {
				if mf := at.d[kd].lookupDirtyFileByItsRange(cr.values.from, cr.values.to); mf != nil {
					// file for required range exists, hold this domain from merge but allow to merge comitemnt
					r.domain[k].values = MergeRange{}
					at.a.logger.Debug("findMergeRange: commitment range is different but file exists in domain, hold further merge",
						at.d[k].d.FilenameBase, dr.values.String("vals", at.StepSize()),
						"commitment", cr.values.String("vals", at.StepSize()))
					continue
				}

				restorePrevRange = true
			}
		}
		if restorePrevRange {
			for k, dr := range &r.domain {
				r.domain[k].values = MergeRange{}
				at.a.logger.Debug("findMergeRange: commitment range is different than accounts or storage, cancel kv merge",
					at.d[k].d.FilenameBase, dr.values.String("", at.StepSize()))
			}
		}
	}

	for id, ii := range at.iis {
		if ii.ii.Disable {
			continue
		}
		r.invertedIndex[id] = ii.findMergeRange(maxEndTxNum, maxSpan)
	}

	//log.Info(fmt.Sprintf("findMergeRange(%d, %d)=%s\n", maxEndTxNum/at.a.stepSize, maxSpan/at.a.stepSize, r))
	return r
}

func (at *AggregatorRoTx) mergeFiles(ctx context.Context, files *SelectedStaticFiles, r *Ranges) (mf *MergedFilesV3, err error) {
	mf = &MergedFilesV3{iis: make([]*FilesItem, len(at.a.iis))}
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(at.a.mergeWorkers)
	closeFiles := true
	defer func() {
		if closeFiles {
			mf.Close()
		}

		// Merge is background operation. It must not crush application.
		// Convert panic to error.
		if rec := recover(); rec != nil {
			err = fmt.Errorf("[snapshots] background mergeFiles: %s, %s, %s", r.String(), rec, dbg.Stack())
		}
	}()

	at.a.logger.Info("[snapshots] merge state " + r.String())
	commitmentUseReferencedBranches := at.a.d[kv.CommitmentDomain].ReplaceKeysInValues

	accStorageMerged := new(sync.WaitGroup)

	for id := range at.d {
		if at.d[id].d.Disable {
			continue
		}
		if !r.domain[id].any() {
			continue
		}

		id := id
		kid := kv.Domain(id)
		if commitmentUseReferencedBranches && (kid == kv.AccountsDomain || kid == kv.StorageDomain) {
			accStorageMerged.Add(1)
		}

		g.Go(func() (err error) {
			var vt valueTransformer
			if commitmentUseReferencedBranches && kid == kv.CommitmentDomain {
				accStorageMerged.Wait()

				// prepare transformer callback to correctly dereference previously merged accounts/storage plain keys
				vt, err = at.d[kv.CommitmentDomain].commitmentValTransformDomain(r.domain[kid].values, at.d[kv.AccountsDomain], at.d[kv.StorageDomain],
					mf.d[kv.AccountsDomain], mf.d[kv.StorageDomain])

				if err != nil {
					return fmt.Errorf("failed to create commitment value transformer: %w", err)
				}
			}

			mf.d[id], mf.dIdx[id], mf.dHist[id], err = at.d[id].mergeFiles(ctx, files.d[id], files.dIdx[id], files.dHist[id], r.domain[id], vt, at.a.ps)
			if commitmentUseReferencedBranches {
				if kid == kv.AccountsDomain || kid == kv.StorageDomain {
					accStorageMerged.Done()
				}
			}
			return err
		})
	}

	for id, rng := range r.invertedIndex {
		if at.iis[id].ii.Disable {
			continue
		}

		if !rng.needMerge {
			continue
		}
		id := id
		rng := rng
		g.Go(func() error {
			var err error
			mf.iis[id], err = at.iis[id].mergeFiles(ctx, files.ii[id], rng.from, rng.to, at.a.ps)
			return err
		})
	}

	err = g.Wait()
	if err == nil {
		closeFiles = false
		at.a.logger.Info("[snapshots] state merge done " + r.String())
	} else if !errors.Is(err, context.Canceled) {
		at.a.logger.Warn(fmt.Sprintf("[snapshots] state merge failed err=%v %s", err, r.String()))
	}
	return mf, err
}

func (a *Aggregator) IntegrateMergedDirtyFiles(outs *SelectedStaticFiles, in *MergedFilesV3) {
	defer a.onFilesChange(in.FilePaths(a.dirs.Snap))

	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()

	for id, d := range a.d {
		if d.Disable {
			continue
		}
		d.integrateMergedDirtyFiles(in.d[id], in.dIdx[id], in.dHist[id])
	}

	for id, ii := range a.iis {
		if ii.Disable {
			continue
		}
		ii.integrateMergedDirtyFiles(in.iis[id])
	}

	a.recalcVisibleFiles(a.dirtyFilesEndTxNumMinimax())
}

func (a *Aggregator) cleanAfterMerge(in *MergedFilesV3) {
	var deleted []string

	at := a.BeginFilesRo()
	defer at.Close()

	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()

	// Step 1: collect file names and do Blocking-Notification of downstream (like Downloader). Only then delete files (otherwise Downloader may re-create deleted file)
	// ToDo: call only `.garbage()` and remove `dryRun` parameter from `cleanAfterMerge`. Also remove return parameter from `cleanAfterMerge`
	dryRun := true
	for id, d := range at.d {
		if d.d.Disable {
			continue
		}
		if in == nil {
			deleted = append(deleted, d.cleanAfterMerge(nil, nil, nil, dryRun)...)
		} else {
			deleted = append(deleted, d.cleanAfterMerge(in.d[id], in.dHist[id], in.dIdx[id], dryRun)...)
		}
	}
	for id, ii := range at.iis {
		if ii.ii.Disable {
			continue
		}
		if in == nil {
			deleted = append(deleted, ii.cleanAfterMerge(nil, dryRun)...)
		} else {
			deleted = append(deleted, ii.cleanAfterMerge(in.iis[id], dryRun)...)
		}
	}
	a.onFilesDelete(deleted)

	// Step 2: delete
	dryRun = false
	for id, d := range at.d {
		if d.d.Disable {
			continue
		}
		if in == nil {
			d.cleanAfterMerge(nil, nil, nil, dryRun)
		} else {
			d.cleanAfterMerge(in.d[id], in.dHist[id], in.dIdx[id], dryRun)
		}
	}
	for id, ii := range at.iis {
		if ii.ii.Disable {
			continue
		}
		if in == nil {
			ii.cleanAfterMerge(nil, dryRun)
		} else {
			ii.cleanAfterMerge(in.iis[id], dryRun)
		}
	}
}

// KeepRecentTxnsOfHistoriesWithDisabledSnapshots limits amount of recent transactions protected from prune in domains history.
// Affects only domains with dontProduceHistoryFiles=true.
// Usually equal to one a.stepSize, but could be set to step/2 or step/4 to reduce size of history tables.
// when we exec blocks from snapshots we can set it to 0, because no re-org on those blocks are possible
func (a *Aggregator) KeepRecentTxnsOfHistoriesWithDisabledSnapshots(recentTxs uint64) {
	for _, d := range a.d {
		if d != nil && d.History.SnapshotsDisabled {
			d.History.KeepRecentTxnInDB = recentTxs
		}
	}
}

func (a *Aggregator) SetSnapshotBuildSema(semaphore *semaphore.Weighted) {
	a.snapshotBuildSema = semaphore
}

// SetProduceMod allows setting produce to false in order to stop making state files (default value is true)
func (a *Aggregator) SetProduceMod(produce bool) {
	a.produce = produce
}

// Returns channel which is closed when aggregation is done
func (a *Aggregator) BuildFilesInBackground(txNum uint64) chan struct{} {
	fin := make(chan struct{})

	if !a.produce {
		close(fin)
		return fin
	}

	if (txNum + 1) <= a.visibleFilesMinimaxTxNum.Load()+a.stepSize {
		close(fin)
		return fin
	}

	if ok := a.buildingFiles.CompareAndSwap(false, true); !ok {
		close(fin)
		return fin
	}

	step := kv.Step(a.visibleFilesMinimaxTxNum.Load() / a.StepSize())

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer a.buildingFiles.Store(false)

		if a.snapshotBuildSema != nil {
			//we are inside own goroutine - it's fine to block here
			if err := a.snapshotBuildSema.Acquire(a.ctx, 1); err != nil { //TODO: not sure if this ctx is correct
				a.logger.Warn("[snapshots] buildFilesInBackground", "err", err)
				close(fin)
				return //nolint
			}
			defer a.snapshotBuildSema.Release(1)
		}

		lastInDB := max(
			lastIdInDB(a.db, a.d[kv.AccountsDomain]),
			lastIdInDB(a.db, a.d[kv.CodeDomain]),
			lastIdInDB(a.db, a.d[kv.StorageDomain]),
			lastIdInDBNoHistory(a.db, a.d[kv.CommitmentDomain]))
		a.logger.Info("BuildFilesInBackground", "step", step, "lastInDB", lastInDB)

		// check if db has enough data (maybe we didn't commit them yet or all keys are unique so history is empty)
		//lastInDB := lastIdInDB(a.db, a.d[kv.AccountsDomain])
		hasData := lastInDB > step // `step` must be fully-written - means `step+1` records must be visible
		if !hasData {
			close(fin)
			return
		}

		// trying to create as much small-step-files as possible:
		// - to reduce amount of small merges
		// - to remove old data from db as early as possible
		// - during files build, may happen commit of new data. on each loop step getting latest id in db
		for ; step < lastInDB; step++ { //`step` must be fully-written - means `step+1` records must be visible
			if err := a.buildFiles(a.ctx, step); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, common.ErrStopped) {
					close(fin)
					return
				}
				a.logger.Warn("[snapshots] buildFilesInBackground", "err", err)
				break
			}
			a.onFilesChange(nil)
		}
		go func() {
			defer close(fin)

			if err := a.MergeLoop(a.ctx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, common.ErrStopped) {
					return
				}
				a.logger.Warn("[snapshots] merge", "err", err)
			}
		}()
	}()
	return fin
}

// Returns the first known txNum found in history files of a given domain
func (at *AggregatorRoTx) HistoryStartFrom(name kv.Domain) uint64 {
	return at.d[name].HistoryStartFrom()
}

func (at *AggregatorRoTx) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int, tx kv.Tx) (timestamps stream.U64, err error) {
	// check domain iis
	for _, d := range at.d {
		if d.d.HistoryIdx == name {
			return d.ht.IdxRange(k, fromTs, toTs, asc, limit, tx)
		}
	}

	// check the ii
	if ii := at.searchII(name); ii != nil {
		return ii.IdxRange(k, fromTs, toTs, asc, limit, tx)
	}
	return nil, fmt.Errorf("unexpected history name: %s", name)
}

// -- range end

func (at *AggregatorRoTx) HistorySeek(domain kv.Domain, key []byte, ts uint64, tx kv.Tx) (v []byte, ok bool, err error) {
	return at.d[domain].ht.HistorySeek(key, ts, tx)
}

func (at *AggregatorRoTx) HistoryRange(domain kv.Domain, fromTs, toTs int, asc order.By, limit int, tx kv.Tx) (it stream.KV, err error) {
	return at.d[domain].ht.HistoryRange(fromTs, toTs, asc, limit, tx)
}

func (at *AggregatorRoTx) KeyCountInFiles(d kv.Domain, start, end uint64) (totalKeys uint64) {
	if d >= kv.DomainLen {
		return 0
	}

	for _, f := range at.d[d].visible.files {
		if f.startTxNum >= start && f.endTxNum <= end {
			totalKeys += uint64(f.src.decompressor.Count() / 2)
		}
	}
	return totalKeys
}

func (at *AggregatorRoTx) FileStream(name kv.Domain, fromTxNum, toTxNum uint64) (stream.KV, error) {
	dt := at.d[name]

	fi := -1
	for idx, f := range dt.files {
		if f.startTxNum == fromTxNum && f.endTxNum == toTxNum {
			fi = idx
			break
		}
	}
	if fi < 0 {
		return nil, fmt.Errorf("FileStream: file not found: %s, %d-%d", name, fromTxNum/at.StepSize(), toTxNum/at.StepSize())
	}
	r := dt.dataReader(dt.files[fi].src.decompressor)
	return NewSegStreamReader(r, -1), nil
}

// AggregatorRoTx guarantee consistent View of files ("snapshots isolation" level https://en.wikipedia.org/wiki/Snapshot_isolation):
//   - long-living consistent view of all files (no limitations)
//   - hiding garbage and files overlaps
//   - protecting useful files from removal
//   - user will not see "partial writes" or "new files appearance"
//   - last reader removing garbage files inside `Close` method
type AggregatorRoTx struct {
	a   *Aggregator
	d   [kv.DomainLen]*DomainRoTx
	iis []*InvertedIndexRoTx

	_leakID uint64 // set only if TRACE_AGG=true
}

func (a *Aggregator) BeginFilesRo() *AggregatorRoTx {
	ac := &AggregatorRoTx{
		a:       a,
		_leakID: a.leakDetector.Add(),
		iis:     make([]*InvertedIndexRoTx, len(a.iis)),
	}

	a.visibleFilesLock.RLock()
	for id, ii := range a.iis {
		ac.iis[id] = ii.BeginFilesRo()
	}
	for id, d := range a.d {
		if d != nil {
			ac.d[id] = d.BeginFilesRo()
		}
	}
	a.visibleFilesLock.RUnlock()

	return ac
}

// func (at *AggregatorRoTx) DomainProgress(name kv.Domain, tx kv.Tx) uint64 {
// 	return at.d[name].d.maxTxNumInDB(tx)
// }

func (at *AggregatorRoTx) DomainProgress(name kv.Domain, tx kv.Tx) uint64 {
	d := at.d[name]
	if d.d.HistoryDisabled {
		// this is not accurate, okay for reporting...
		// if historyDisabled, there's no way to get progress in
		// terms of exact txNum
		return at.d[name].d.maxStepInDBNoHistory(tx).ToTxNum(at.a.stepSize)
	}
	return at.d[name].HistoryProgress(tx)
}
func (at *AggregatorRoTx) IIProgress(name kv.InvertedIdx, tx kv.Tx) uint64 {
	return at.searchII(name).Progress(tx)
}

// --- Domain part START ---

func (at *AggregatorRoTx) RangeAsOf(ctx context.Context, tx kv.Tx, domain kv.Domain, fromKey, toKey []byte, ts uint64, asc order.By, limit int) (it stream.KV, err error) {
	return at.d[domain].RangeAsOf(ctx, tx, fromKey, toKey, ts, asc, limit)
}
func (at *AggregatorRoTx) DebugRangeLatest(tx kv.Tx, domain kv.Domain, from, to []byte, limit int) (stream.KV, error) {
	return at.d[domain].DebugRangeLatest(tx, from, to, limit)
}

func (at *AggregatorRoTx) GetAsOf(name kv.Domain, k []byte, ts uint64, tx kv.Tx) (v []byte, ok bool, err error) {
	return at.d[name].GetAsOf(k, ts, tx)
}

func (at *AggregatorRoTx) GetLatest(domain kv.Domain, k []byte, tx kv.Tx) (v []byte, step kv.Step, ok bool, err error) {
	if domain != kv.CommitmentDomain {
		return at.d[domain].GetLatest(k, tx)
	}

	v, step, ok, err = at.d[domain].getLatestFromDb(k, tx)
	if err != nil {
		return nil, kv.Step(0), false, err
	}
	if ok {
		return v, step, true, nil
	}

	v, found, fileStartTxNum, fileEndTxNum, err := at.d[domain].getLatestFromFiles(k, 0)
	if !found {
		return nil, kv.Step(0), false, err
	}

	v, err = at.replaceShortenedKeysInBranch(k, commitment.BranchData(v), fileStartTxNum, fileEndTxNum)
	return v, kv.Step(fileEndTxNum / at.StepSize()), found, err
}

func (at *AggregatorRoTx) DebugGetLatestFromDB(domain kv.Domain, key []byte, tx kv.Tx) ([]byte, kv.Step, bool, error) {
	return at.d[domain].getLatestFromDb(key, tx)
}

func (at *AggregatorRoTx) DebugGetLatestFromFiles(domain kv.Domain, k []byte, maxTxNum uint64) (v []byte, found bool, fileStartTxNum uint64, fileEndTxNum uint64, err error) {
	v, found, fileStartTxNum, fileEndTxNum, err = at.d[domain].getLatestFromFiles(k, maxTxNum)
	if domain == kv.CommitmentDomain && found {
		v, err = at.replaceShortenedKeysInBranch(k, commitment.BranchData(v), fileStartTxNum, fileEndTxNum)
	}
	return
}

func (at *AggregatorRoTx) Unwind(ctx context.Context, tx kv.RwTx, txNumUnwindTo uint64, changeset *[kv.DomainLen][]kv.DomainEntryDiff) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	step := txNumUnwindTo / at.StepSize()
	for idx, d := range at.d {
		if err := d.unwind(ctx, tx, step, txNumUnwindTo, changeset[idx]); err != nil {
			return err
		}
	}
	for _, ii := range at.iis {
		if err := ii.unwind(ctx, tx, txNumUnwindTo, math.MaxUint64, math.MaxUint64, logEvery, true, nil); err != nil {
			return err
		}
	}
	return nil
}

// --- Domain part END ---

func (at *AggregatorRoTx) MadvNormal() *AggregatorRoTx {
	for _, d := range at.d {
		for _, f := range d.files {
			f.src.MadvNormal()
		}
		for _, f := range d.ht.files {
			f.src.MadvNormal()
		}
		for _, f := range d.ht.iit.files {
			f.src.MadvNormal()
		}
	}
	for _, ii := range at.iis {
		for _, f := range ii.files {
			f.src.MadvNormal()
		}
	}
	return at
}
func (at *AggregatorRoTx) DisableReadAhead() {
	for _, d := range at.d {
		for _, f := range d.files {
			f.src.DisableReadAhead()
		}
		for _, f := range d.ht.files {
			f.src.DisableReadAhead()
		}
		for _, f := range d.ht.iit.files {
			f.src.DisableReadAhead()
		}
	}
	for _, ii := range at.iis {
		for _, f := range ii.files {
			f.src.DisableReadAhead()
		}
	}
}
func (a *Aggregator) MadvNormal() *Aggregator {
	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()
	for _, d := range a.d {
		for _, f := range d.dirtyFiles.Items() {
			f.MadvNormal()
		}
		for _, f := range d.History.dirtyFiles.Items() {
			f.MadvNormal()
		}
		for _, f := range d.History.InvertedIndex.dirtyFiles.Items() {
			f.MadvNormal()
		}
	}
	for _, ii := range a.iis {
		for _, f := range ii.dirtyFiles.Items() {
			f.MadvNormal()
		}
	}
	return a
}
func (a *Aggregator) DisableReadAhead() {
	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()
	for _, d := range a.d {
		for _, f := range d.dirtyFiles.Items() {
			f.DisableReadAhead()
		}
		for _, f := range d.History.dirtyFiles.Items() {
			f.DisableReadAhead()
		}
		for _, f := range d.History.InvertedIndex.dirtyFiles.Items() {
			f.DisableReadAhead()
		}
	}
	for _, ii := range a.iis {
		for _, f := range ii.dirtyFiles.Items() {
			f.DisableReadAhead()
		}
	}
}

func (at *AggregatorRoTx) Close() {
	if at == nil || at.a == nil { // invariant: it's safe to call Close multiple times
		return
	}
	at.a.leakDetector.Del(at._leakID)
	at.a = nil

	for _, d := range at.d {
		if d != nil {
			d.Close()
		}
	}
	for _, ii := range at.iis {
		ii.Close()
	}
}

// Inverted index tables only
func lastIdInDB(db kv.RoDB, domain *Domain) (lstInDb kv.Step) {
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		lstInDb = domain.maxStepInDB(tx)
		return nil
	}); err != nil {
		log.Warn("[snapshots] lastIdInDB", "err", err)
	}
	return lstInDb
}

func lastIdInDBNoHistory(db kv.RoDB, domain *Domain) (lstInDb kv.Step) {
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		//lstInDb = domain.maxStepInDB(tx)
		lstInDb = domain.maxStepInDBNoHistory(tx)
		return nil
	}); err != nil {
		log.Warn("[snapshots] lastIdInDB", "err", err)
	}
	return lstInDb
}
