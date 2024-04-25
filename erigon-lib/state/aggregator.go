/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	math2 "math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/log/v3"
	rand2 "golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/metrics"
	"github.com/ledgerwatch/erigon-lib/seg"
)

var (
	mxPruneTookAgg = metrics.GetOrCreateSummary(`prune_seconds{type="state"}`)
)

type Aggregator struct {
	db               kv.RoDB
	d                [kv.DomainLen]*Domain
	tracesTo         *InvertedIndex
	logAddrs         *InvertedIndex
	logTopics        *InvertedIndex
	tracesFrom       *InvertedIndex
	backgroundResult *BackgroundResult
	dirs             datadir.Dirs
	tmpdir           string
	aggregationStep  uint64
	keepInDB         uint64

	dirtyFilesLock           sync.Mutex
	visibleFilesMinimaxTxNum atomic.Uint64
	snapshotBuildSema        *semaphore.Weighted

	collateAndBuildWorkers int // minimize amount of background workers by default
	mergeWorkers           int // usually 1

	commitmentValuesTransform bool

	// To keep DB small - need move data to small files ASAP.
	// It means goroutine which creating small files - can't be locked by merge or indexing.
	buildingFiles           atomic.Bool
	mergeingFiles           atomic.Bool
	buildingOptionalIndices atomic.Bool

	//warmupWorking          atomic.Bool
	ctx       context.Context
	ctxCancel context.CancelFunc

	needSaveFilesListInDB atomic.Bool

	wg sync.WaitGroup // goroutines spawned by Aggregator, to ensure all of them are finish at agg.Close

	onFreeze OnFreezeFunc

	ps *background.ProgressSet

	// next fields are set only if agg.doTraceCtx is true. can enable by env: TRACE_AGG=true
	leakDetector *dbg.LeakDetector
	logger       log.Logger

	ctxAutoIncrement atomic.Uint64
}

type OnFreezeFunc func(frozenFileNames []string)

const AggregatorSqueezeCommitmentValues = true

func NewAggregator(ctx context.Context, dirs datadir.Dirs, aggregationStep uint64, db kv.RoDB, logger log.Logger) (*Aggregator, error) {
	tmpdir := dirs.Tmp
	salt, err := getStateIndicesSalt(dirs.Snap)
	if err != nil {
		return nil, err
	}

	ctx, ctxCancel := context.WithCancel(ctx)
	a := &Aggregator{
		ctx:                    ctx,
		ctxCancel:              ctxCancel,
		onFreeze:               func(frozenFileNames []string) {},
		dirs:                   dirs,
		tmpdir:                 tmpdir,
		aggregationStep:        aggregationStep,
		db:                     db,
		leakDetector:           dbg.NewLeakDetector("agg", dbg.SlowTx()),
		ps:                     background.NewProgressSet(),
		backgroundResult:       &BackgroundResult{},
		logger:                 logger,
		collateAndBuildWorkers: 1,
		mergeWorkers:           1,

		commitmentValuesTransform: AggregatorSqueezeCommitmentValues,
	}
	cfg := domainCfg{
		hist: histCfg{
			iiCfg:             iiCfg{salt: salt, dirs: dirs, db: db},
			withLocalityIndex: false, withExistenceIndex: false, compression: CompressNone, historyLargeValues: false,
		},
		restrictSubsetFileDeletions: a.commitmentValuesTransform,
	}
	if a.d[kv.AccountsDomain], err = NewDomain(cfg, aggregationStep, "accounts", kv.TblAccountKeys, kv.TblAccountVals, kv.TblAccountHistoryKeys, kv.TblAccountHistoryVals, kv.TblAccountIdx, logger); err != nil {
		return nil, err
	}
	cfg = domainCfg{
		hist: histCfg{
			iiCfg:             iiCfg{salt: salt, dirs: dirs, db: db},
			withLocalityIndex: false, withExistenceIndex: false, compression: CompressNone, historyLargeValues: false,
		},
		restrictSubsetFileDeletions: a.commitmentValuesTransform,
	}
	if a.d[kv.StorageDomain], err = NewDomain(cfg, aggregationStep, "storage", kv.TblStorageKeys, kv.TblStorageVals, kv.TblStorageHistoryKeys, kv.TblStorageHistoryVals, kv.TblStorageIdx, logger); err != nil {
		return nil, err
	}
	cfg = domainCfg{
		hist: histCfg{
			iiCfg:             iiCfg{salt: salt, dirs: dirs, db: db},
			withLocalityIndex: false, withExistenceIndex: false, compression: CompressKeys | CompressVals, historyLargeValues: true,
		},
	}
	if a.d[kv.CodeDomain], err = NewDomain(cfg, aggregationStep, "code", kv.TblCodeKeys, kv.TblCodeVals, kv.TblCodeHistoryKeys, kv.TblCodeHistoryVals, kv.TblCodeIdx, logger); err != nil {
		return nil, err
	}
	cfg = domainCfg{
		hist: histCfg{
			iiCfg:             iiCfg{salt: salt, dirs: dirs, db: db},
			withLocalityIndex: false, withExistenceIndex: false, compression: CompressNone, historyLargeValues: false,
			dontProduceHistoryFiles: true,
		},
		replaceKeysInValues:         a.commitmentValuesTransform,
		restrictSubsetFileDeletions: a.commitmentValuesTransform,
		compress:                    CompressNone,
	}
	if a.d[kv.CommitmentDomain], err = NewDomain(cfg, aggregationStep, "commitment", kv.TblCommitmentKeys, kv.TblCommitmentVals, kv.TblCommitmentHistoryKeys, kv.TblCommitmentHistoryVals, kv.TblCommitmentIdx, logger); err != nil {
		return nil, err
	}
	//cfg = domainCfg{
	//	hist: histCfg{
	//		iiCfg:             iiCfg{salt: salt, dirs: dirs},
	//		withLocalityIndex: false, withExistenceIndex: false, compression: CompressKeys | CompressVals, historyLargeValues: false,
	//	},
	//}
	//if a.d[kv.GasUsedDomain], err = NewDomain(cfg, aggregationStep, "gasused", kv.TblGasUsedKeys, kv.TblGasUsedVals, kv.TblGasUsedHistoryKeys, kv.TblGasUsedVals, kv.TblGasUsedIdx, logger); err != nil {
	//	return nil, err
	//}
	idxCfg := iiCfg{salt: salt, dirs: dirs, db: db}
	if a.logAddrs, err = NewInvertedIndex(idxCfg, aggregationStep, "logaddrs", kv.TblLogAddressKeys, kv.TblLogAddressIdx, false, nil, logger); err != nil {
		return nil, err
	}
	idxCfg = iiCfg{salt: salt, dirs: dirs, db: db}
	if a.logTopics, err = NewInvertedIndex(idxCfg, aggregationStep, "logtopics", kv.TblLogTopicsKeys, kv.TblLogTopicsIdx, false, nil, logger); err != nil {
		return nil, err
	}
	idxCfg = iiCfg{salt: salt, dirs: dirs, db: db}
	if a.tracesFrom, err = NewInvertedIndex(idxCfg, aggregationStep, "tracesfrom", kv.TblTracesFromKeys, kv.TblTracesFromIdx, false, nil, logger); err != nil {
		return nil, err
	}
	idxCfg = iiCfg{salt: salt, dirs: dirs, db: db}
	if a.tracesTo, err = NewInvertedIndex(idxCfg, aggregationStep, "tracesto", kv.TblTracesToKeys, kv.TblTracesToIdx, false, nil, logger); err != nil {
		return nil, err
	}
	a.KeepStepsInDB(1)
	a.recalcVisibleFilesMinimaxTxNum()

	if dbg.NoSync() {
		a.DisableFsync()
	}

	return a, nil
}

// getStateIndicesSalt - try read salt for all indices from DB. Or fall-back to new salt creation.
// if db is Read-Only (for example remote RPCDaemon or utilities) - we will not create new indices - and existing indices have salt in metadata.
func getStateIndicesSalt(baseDir string) (salt *uint32, err error) {
	if dir.FileExist(filepath.Join(baseDir, "salt.txt")) && !dir.FileExist(filepath.Join(baseDir, "salt-state.txt")) {
		_ = os.Rename(filepath.Join(baseDir, "salt.txt"), filepath.Join(baseDir, "salt-state.txt"))
	}
	fpath := filepath.Join(baseDir, "salt-state.txt")
	if !dir.FileExist(fpath) {
		if salt == nil {
			saltV := rand2.Uint32()
			salt = &saltV
		}
		saltBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(saltBytes, *salt)
		if err := dir.WriteFileWithFsync(fpath, saltBytes, os.ModePerm); err != nil {
			return nil, err
		}
	}
	saltBytes, err := os.ReadFile(fpath)
	if err != nil {
		return nil, err
	}
	saltV := binary.BigEndian.Uint32(saltBytes)
	salt = &saltV
	return salt, nil
}

func (a *Aggregator) OnFreeze(f OnFreezeFunc) { a.onFreeze = f }
func (a *Aggregator) DisableFsync() {
	for _, d := range a.d {
		d.DisableFsync()
	}
	a.logAddrs.DisableFsync()
	a.logTopics.DisableFsync()
	a.tracesFrom.DisableFsync()
	a.tracesTo.DisableFsync()
}

func (a *Aggregator) OpenFolder(readonly bool) error {
	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()
	eg := &errgroup.Group{}
	for _, d := range a.d {
		d := d
		eg.Go(func() error {
			select {
			case <-a.ctx.Done():
				return a.ctx.Err()
			default:
			}
			return d.OpenFolder(readonly)
		})
	}
	eg.Go(func() error { return a.logAddrs.OpenFolder(readonly) })
	eg.Go(func() error { return a.logTopics.OpenFolder(readonly) })
	eg.Go(func() error { return a.tracesFrom.OpenFolder(readonly) })
	eg.Go(func() error { return a.tracesTo.OpenFolder(readonly) })
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("OpenFolder: %w", err)
	}
	a.recalcVisibleFilesMinimaxTxNum()
	return nil
}

func (a *Aggregator) OpenList(files []string, readonly bool) error {
	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()
	eg := &errgroup.Group{}
	for _, d := range a.d {
		d := d
		eg.Go(func() error { return d.OpenFolder(readonly) })
	}
	eg.Go(func() error { return a.logAddrs.OpenFolder(readonly) })
	eg.Go(func() error { return a.logTopics.OpenFolder(readonly) })
	eg.Go(func() error { return a.tracesFrom.OpenFolder(readonly) })
	eg.Go(func() error { return a.tracesTo.OpenFolder(readonly) })
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("OpenList: %w", err)
	}
	a.recalcVisibleFilesMinimaxTxNum()
	return nil
}

func (a *Aggregator) Close() {
	if a.ctxCancel == nil { // invariant: it's safe to call Close multiple times
		return
	}
	a.ctxCancel()
	a.ctxCancel = nil
	a.wg.Wait()

	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()

	for _, d := range a.d {
		d.Close()
	}
	a.logAddrs.Close()
	a.logTopics.Close()
	a.tracesFrom.Close()
	a.tracesTo.Close()
}

func (a *Aggregator) SetCollateAndBuildWorkers(i int) { a.collateAndBuildWorkers = i }
func (a *Aggregator) SetMergeWorkers(i int)           { a.mergeWorkers = i }
func (a *Aggregator) SetCompressWorkers(i int) {
	for _, d := range a.d {
		d.compressWorkers = i
	}
	a.logAddrs.compressWorkers = i
	a.logTopics.compressWorkers = i
	a.tracesFrom.compressWorkers = i
	a.tracesTo.compressWorkers = i
}

func (a *Aggregator) HasBackgroundFilesBuild() bool { return a.ps.Has() }
func (a *Aggregator) BackgroundProgress() string    { return a.ps.String() }

func (ac *AggregatorRoTx) Files() []string {
	var res []string
	if ac == nil {
		return res
	}
	for _, d := range ac.d {
		res = append(res, d.Files()...)
	}
	res = append(res, ac.logAddrs.Files()...)
	res = append(res, ac.logTopics.Files()...)
	res = append(res, ac.tracesFrom.Files()...)
	res = append(res, ac.tracesTo.Files()...)
	return res
}
func (a *Aggregator) Files() []string {
	ac := a.BeginFilesRo()
	defer ac.Close()
	return ac.Files()
}

func (a *Aggregator) BuildOptionalMissedIndicesInBackground(ctx context.Context, workers int) {
	if ok := a.buildingOptionalIndices.CompareAndSwap(false, true); !ok {
		return
	}
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer a.buildingOptionalIndices.Store(false)
		aggTx := a.BeginFilesRo()
		defer aggTx.Close()
		if err := aggTx.buildOptionalMissedIndices(ctx, workers); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, common2.ErrStopped) {
				return
			}
			log.Warn("[snapshots] BuildOptionalMissedIndicesInBackground", "err", err)
		}
	}()
}

func (a *Aggregator) BuildOptionalMissedIndices(ctx context.Context, workers int) error {
	if ok := a.buildingOptionalIndices.CompareAndSwap(false, true); !ok {
		return nil
	}
	defer a.buildingOptionalIndices.Store(false)
	filesTx := a.BeginFilesRo()
	defer filesTx.Close()
	if err := filesTx.buildOptionalMissedIndices(ctx, workers); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, common2.ErrStopped) {
			return nil
		}
		return err
	}
	return nil
}

func (ac *AggregatorRoTx) buildOptionalMissedIndices(ctx context.Context, workers int) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	ps := background.NewProgressSet()
	for _, d := range ac.d {
		d := d
		if d != nil {
			g.Go(func() error { return d.BuildOptionalMissedIndices(ctx, ps) })
		}
	}
	return g.Wait()
}

func (a *Aggregator) BuildMissedIndices(ctx context.Context, workers int) error {
	startIndexingTime := time.Now()
	{
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
					log.Info("[snapshots] Indexing", "progress", ps.String(), "total-indexing-time", time.Since(startIndexingTime).Round(time.Second).String(), "alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
				}
			}
		}()
		for _, d := range a.d {
			d.BuildMissedIndices(ctx, g, ps)
		}
		a.logAddrs.BuildMissedIndices(ctx, g, ps)
		a.logTopics.BuildMissedIndices(ctx, g, ps)
		a.tracesFrom.BuildMissedIndices(ctx, g, ps)
		a.tracesTo.BuildMissedIndices(ctx, g, ps)

		if err := g.Wait(); err != nil {
			return err
		}
		if err := a.OpenFolder(true); err != nil {
			return err
		}
	}
	return nil
}

func (a *Aggregator) BuildMissedIndicesInBackground(ctx context.Context, workers int) {
	if ok := a.buildingFiles.CompareAndSwap(false, true); !ok {
		return
	}
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer a.buildingFiles.Store(false)
		aggTx := a.BeginFilesRo()
		defer aggTx.Close()
		if err := a.BuildMissedIndices(ctx, workers); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, common2.ErrStopped) {
				return
			}
			log.Warn("[snapshots] BuildOptionalMissedIndicesInBackground", "err", err)
		}
	}()
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
	d          [kv.DomainLen]StaticFiles
	logAddrs   InvertedFiles
	logTopics  InvertedFiles
	tracesFrom InvertedFiles
	tracesTo   InvertedFiles
}

// CleanupOnError - call it on collation fail. It's closing all files
func (sf AggV3StaticFiles) CleanupOnError() {
	for _, d := range sf.d {
		d.CleanupOnError()
	}
	sf.logAddrs.CleanupOnError()
	sf.logTopics.CleanupOnError()
	sf.tracesFrom.CleanupOnError()
	sf.tracesTo.CleanupOnError()
}

func (a *Aggregator) buildFiles(ctx context.Context, step uint64) error {
	a.logger.Debug("[agg] collate and build", "step", step, "collate_workers", a.collateAndBuildWorkers, "merge_workers", a.mergeWorkers, "compress_workers", a.d[kv.AccountsDomain].compressWorkers)

	var (
		logEvery      = time.NewTicker(time.Second * 30)
		txFrom        = a.FirstTxNumOfStep(step)
		txTo          = a.FirstTxNumOfStep(step + 1)
		stepStartedAt = time.Now()

		static          AggV3StaticFiles
		closeCollations = true
		collListMu      = sync.Mutex{}
		collations      = make([]Collation, 0)
	)

	defer logEvery.Stop()
	defer a.needSaveFilesListInDB.Store(true)
	defer a.recalcVisibleFilesMinimaxTxNum()
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
		d := d

		a.wg.Add(1)
		g.Go(func() error {
			defer a.wg.Done()

			var collation Collation
			if err := a.db.View(ctx, func(tx kv.Tx) (err error) {
				collation, err = d.collate(ctx, step, txFrom, txTo, tx)
				return err
			}); err != nil {
				return fmt.Errorf("domain collation %q has failed: %w", d.filenameBase, err)
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

			dd, err := kv.String2Domain(d.filenameBase)
			if err != nil {
				return err
			}
			static.d[dd] = sf
			return nil
		})
	}
	closeCollations = false

	// indices are built concurrently
	for _, d := range []*InvertedIndex{a.logTopics, a.logAddrs, a.tracesFrom, a.tracesTo} {
		d := d
		a.wg.Add(1)
		g.Go(func() error {
			defer a.wg.Done()

			var collation map[string]*roaring64.Bitmap
			err := a.db.View(ctx, func(tx kv.Tx) (err error) {
				collation, err = d.collate(ctx, step, tx)
				return err
			})
			if err != nil {
				return fmt.Errorf("index collation %q has failed: %w", d.filenameBase, err)
			}
			sf, err := d.buildFiles(ctx, step, collation, a.ps)
			if err != nil {
				sf.CleanupOnError()
				return err
			}

			switch d.indexKeysTable {
			case kv.TblLogTopicsKeys:
				static.logTopics = sf
			case kv.TblLogAddressKeys:
				static.logAddrs = sf
			case kv.TblTracesFromKeys:
				static.tracesFrom = sf
			case kv.TblTracesToKeys:
				static.tracesTo = sf
			default:
				panic("unknown index " + d.indexKeysTable)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		static.CleanupOnError()
		return fmt.Errorf("domain collate-build: %w", err)
	}
	mxStepTook.ObserveDuration(stepStartedAt)
	a.integrateFiles(static, txFrom, txTo)
	a.logger.Info("[snapshots] aggregated", "step", step, "took", time.Since(stepStartedAt))

	return nil
}

func (a *Aggregator) BuildFiles(toTxNum uint64) (err error) {
	finished := a.BuildFilesInBackground(toTxNum)
	if !(a.buildingFiles.Load() || a.mergeingFiles.Load() || a.buildingOptionalIndices.Load()) {
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
			fmt.Println("BuildFiles finished")
			break Loop
		case <-logEvery.C:
			if !(a.buildingFiles.Load() || a.mergeingFiles.Load() || a.buildingOptionalIndices.Load()) {
				break Loop
			}
			if a.HasBackgroundFilesBuild() {
				log.Info("[snapshots] Files build", "progress", a.BackgroundProgress())
			}
		}
	}

	return nil
}

func (a *Aggregator) mergeLoopStep(ctx context.Context) (somethingDone bool, err error) {
	a.logger.Debug("[agg] merge", "collate_workers", a.collateAndBuildWorkers, "merge_workers", a.mergeWorkers, "compress_workers", a.d[kv.AccountsDomain].compressWorkers)

	aggTx := a.BeginFilesRo()
	defer aggTx.Close()
	mxRunningMerges.Inc()
	defer mxRunningMerges.Dec()

	closeAll := true
	maxSpan := StepsInColdFile * a.StepSize()
	r := aggTx.findMergeRange(a.visibleFilesMinimaxTxNum.Load(), maxSpan)
	if !r.any() {
		return false, nil
	}

	outs, err := aggTx.staticFilesInRange(r)
	defer func() {
		if closeAll {
			outs.Close()
		}
	}()
	if err != nil {
		return false, err
	}

	in, err := aggTx.mergeFiles(ctx, outs, r)
	if err != nil {
		return true, err
	}
	defer func() {
		if closeAll {
			in.Close()
		}
	}()
	aggTx.integrateMergedFiles(outs, in)
	a.onFreeze(in.FrozenList())
	closeAll = false
	return true, nil
}

func (a *Aggregator) MergeLoop(ctx context.Context) error {
	for {
		somethingMerged, err := a.mergeLoopStep(ctx)
		if err != nil {
			return err
		}
		if !somethingMerged {
			return nil
		}
	}
}

func (a *Aggregator) integrateFiles(sf AggV3StaticFiles, txNumFrom, txNumTo uint64) {
	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()
	defer a.needSaveFilesListInDB.Store(true)
	defer a.recalcVisibleFilesMinimaxTxNum()

	for id, d := range a.d {
		d.integrateFiles(sf.d[id], txNumFrom, txNumTo)
	}
	a.logAddrs.integrateFiles(sf.logAddrs, txNumFrom, txNumTo)
	a.logTopics.integrateFiles(sf.logTopics, txNumFrom, txNumTo)
	a.tracesFrom.integrateFiles(sf.tracesFrom, txNumFrom, txNumTo)
	a.tracesTo.integrateFiles(sf.tracesTo, txNumFrom, txNumTo)
}

func (a *Aggregator) HasNewFrozenFiles() bool {
	if a == nil {
		return false
	}
	return a.needSaveFilesListInDB.CompareAndSwap(true, false)
}

type flusher interface {
	Flush(ctx context.Context, tx kv.RwTx) error
}

func (ac *AggregatorRoTx) minimaxTxNumInDomainFiles(cold bool) uint64 {
	return min(
		ac.d[kv.AccountsDomain].maxTxNumInDomainFiles(cold),
		ac.d[kv.CodeDomain].maxTxNumInDomainFiles(cold),
		ac.d[kv.StorageDomain].maxTxNumInDomainFiles(cold),
		ac.d[kv.CommitmentDomain].maxTxNumInDomainFiles(cold),
	)
}

func (ac *AggregatorRoTx) CanPrune(tx kv.Tx, untilTx uint64) bool {
	if dbg.NoPrune() {
		return false
	}
	for _, d := range ac.d {
		if d.CanPruneUntil(tx, untilTx) {
			return true
		}
	}
	return ac.logAddrs.CanPrune(tx) ||
		ac.logTopics.CanPrune(tx) ||
		ac.tracesFrom.CanPrune(tx) ||
		ac.tracesTo.CanPrune(tx)
}

func (ac *AggregatorRoTx) CanUnwindDomainsToBlockNum(tx kv.Tx) (uint64, error) {
	_, histBlockNumProgress, err := rawdbv3.TxNums.FindBlockNum(tx, ac.CanUnwindDomainsToTxNum())
	return histBlockNumProgress, err
}
func (ac *AggregatorRoTx) CanUnwindDomainsToTxNum() uint64 {
	return ac.minimaxTxNumInDomainFiles(false)
}
func (ac *AggregatorRoTx) MinUnwindDomainsBlockNum(tx kv.Tx) (uint64, error) {
	_, blockNum, err := rawdbv3.TxNums.FindBlockNum(tx, ac.CanUnwindDomainsToTxNum())
	return blockNum, err
}

func (ac *AggregatorRoTx) CanUnwindBeforeBlockNum(blockNum uint64, tx kv.Tx) (uint64, bool, error) {
	unwindToTxNum, err := rawdbv3.TxNums.Max(tx, blockNum)
	if err != nil {
		return 0, false, err
	}

	// not all blocks have commitment
	//fmt.Printf("CanUnwindBeforeBlockNum: blockNum=%d unwindTo=%d\n", blockNum, unwindToTxNum)
	domains, err := NewSharedDomains(tx, ac.a.logger)
	if err != nil {
		return 0, false, err
	}
	defer domains.Close()

	blockNumWithCommitment, _, _, err := domains.LatestCommitmentState(tx, ac.CanUnwindDomainsToTxNum(), unwindToTxNum)
	if err != nil {
		_minBlockNum, _ := ac.MinUnwindDomainsBlockNum(tx)
		return _minBlockNum, false, nil //nolint
	}
	if blockNumWithCommitment < blockNum {
		return blockNum, true, nil
	}
	return blockNumWithCommitment, true, nil
}

// PruneSmallBatches is not cancellable, it's over when it's over or failed.
// It fills whole timeout with pruning by small batches (of 100 keys) and making some progress
func (ac *AggregatorRoTx) PruneSmallBatches(ctx context.Context, timeout time.Duration, tx kv.RwTx) (haveMore bool, err error) {
	// On tip-of-chain timeout is about `3sec`
	//  On tip of chain:     must be real-time - prune by small batches and prioritize exact-`timeout`
	//  Not on tip of chain: must be aggressive (prune as much as possible) by bigger batches
	aggressivePrune := timeout >= 1*time.Minute

	var pruneLimit uint64 = 1_000
	var withWarmup bool = false //nolint
	/* disabling this feature for now - seems it doesn't cancel even after prune finished
	if timeout >= 1*time.Minute {
		// start from a bit high limit to give time for warmup
		// will disable warmup after first iteration and will adjust pruneLimit based on `time`
		pruneLimit = 100_000
		withWarmup = true
	}
	withWarmup = false // disabling this feature for now - seems it doesn't cancel even after prune finished
	*/

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
		iterationStarted := time.Now()
		// `context.Background()` is important here!
		//     it allows keep DB consistent - prune all keys-related data or noting
		//     can't interrupt by ctrl+c and leave dirt in DB
		stat, err := ac.Prune(context.Background(), tx, pruneLimit, withWarmup, aggLogEvery)
		if err != nil {
			ac.a.logger.Warn("[snapshots] PruneSmallBatches failed", "err", err)
			return false, err
		}
		if stat == nil {
			if fstat := fullStat.String(); fstat != "" {
				ac.a.logger.Info("[snapshots] PruneSmallBatches finished", "took", time.Since(started).String(), "stat", fstat)
			}
			return false, nil
		}
		fullStat.Accumulate(stat)

		withWarmup = false // warmup once is enough

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
			ac.a.logger.Info("[snapshots] pruning state",
				"until commit", time.Until(started.Add(timeout)).String(),
				"pruneLimit", pruneLimit,
				"aggregatedStep", (ac.minimaxTxNumInDomainFiles(false)-1)/ac.a.StepSize(),
				"stepsRangeInDB", ac.a.StepsRangeInDBAsStr(tx),
				"pruned", fullStat.String(),
			)
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}
	}
}

func (a *Aggregator) StepsRangeInDBAsStr(tx kv.Tx) string {
	steps := make([]string, 0, kv.DomainLen+4)
	for _, d := range a.d {
		steps = append(steps, d.stepsRangeInDBAsStr(tx))
	}
	steps = append(steps,
		a.logAddrs.stepsRangeInDBAsStr(tx),
		a.logTopics.stepsRangeInDBAsStr(tx),
		a.tracesFrom.stepsRangeInDBAsStr(tx),
		a.tracesTo.stepsRangeInDBAsStr(tx),
	)
	return strings.Join(steps, ", ")
}

type AggregatorPruneStat struct {
	Domains map[string]*DomainPruneStat
	Indices map[string]*InvertedIndexPruneStat
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

	sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })

	var sb strings.Builder
	for _, d := range names {
		v, ok := as.Domains[d]
		if ok && v != nil {
			sb.WriteString(fmt.Sprintf("%s| %s; ", d, v.String()))
		}
	}
	names = names[:0]
	for k := range as.Indices {
		names = append(names, k)
	}
	sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })

	for _, d := range names {
		v, ok := as.Indices[d]
		if ok && v != nil {
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

func (ac *AggregatorRoTx) Prune(ctx context.Context, tx kv.RwTx, limit uint64, withWarmup bool, logEvery *time.Ticker) (*AggregatorPruneStat, error) {
	defer mxPruneTookAgg.ObserveDuration(time.Now())

	if limit == 0 {
		limit = uint64(math2.MaxUint64)
	}

	var txFrom, step uint64 // txFrom is always 0 to avoid dangling keys in indices/hist
	txTo := ac.a.visibleFilesMinimaxTxNum.Load()
	if txTo > 0 {
		// txTo is first txNum in next step, has to go 1 tx behind to get correct step number
		step = (txTo - 1) / ac.a.StepSize()
	}

	if txFrom == txTo || !ac.CanPrune(tx, txTo) {
		return nil, nil
	}

	if logEvery == nil {
		logEvery = time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
	}
	//ac.a.logger.Info("aggregator prune", "step", step,
	//	"txn_range", fmt.Sprintf("[%d,%d)", txFrom, txTo), "limit", limit,
	//	/*"stepsLimit", limit/ac.a.aggregationStep,*/ "stepsRangeInDB", ac.a.StepsRangeInDBAsStr(tx))
	aggStat := newAggregatorPruneStat()
	for id, d := range ac.d {
		var err error
		aggStat.Domains[ac.d[id].d.filenameBase], err = d.Prune(ctx, tx, step, txFrom, txTo, limit, withWarmup, logEvery)
		if err != nil {
			return aggStat, err
		}
	}
	lap, err := ac.logAddrs.Prune(ctx, tx, txFrom, txTo, limit, logEvery, false, withWarmup, nil)
	if err != nil {
		return nil, err
	}
	ltp, err := ac.logTopics.Prune(ctx, tx, txFrom, txTo, limit, logEvery, false, withWarmup, nil)
	if err != nil {
		return nil, err
	}
	tfp, err := ac.tracesFrom.Prune(ctx, tx, txFrom, txTo, limit, logEvery, false, withWarmup, nil)
	if err != nil {
		return nil, err
	}
	ttp, err := ac.tracesTo.Prune(ctx, tx, txFrom, txTo, limit, logEvery, false, withWarmup, nil)
	if err != nil {
		return nil, err
	}
	aggStat.Indices[ac.logAddrs.ii.filenameBase] = lap
	aggStat.Indices[ac.logTopics.ii.filenameBase] = ltp
	aggStat.Indices[ac.tracesFrom.ii.filenameBase] = tfp
	aggStat.Indices[ac.tracesTo.ii.filenameBase] = ttp

	return aggStat, nil
}

func (ac *AggregatorRoTx) LogStats(tx kv.Tx, tx2block func(endTxNumMinimax uint64) uint64) {
	maxTxNum := ac.minimaxTxNumInDomainFiles(false)
	if maxTxNum == 0 {
		return
	}

	domainBlockNumProgress := tx2block(maxTxNum)
	str := make([]string, 0, len(ac.d[kv.AccountsDomain].files))
	for _, item := range ac.d[kv.AccountsDomain].files {
		bn := tx2block(item.endTxNum)
		str = append(str, fmt.Sprintf("%d=%dK", item.endTxNum/ac.a.StepSize(), bn/1_000))
	}
	//str2 := make([]string, 0, len(ac.storage.files))
	//for _, item := range ac.storage.files {
	//	str2 = append(str2, fmt.Sprintf("%s:%dm", item.src.decompressor.FileName(), item.src.decompressor.Count()/1_000_000))
	//}
	//for _, item := range ac.commitment.files {
	//	bn := tx2block(item.endTxNum) / 1_000
	//	str2 = append(str2, fmt.Sprintf("%s:%dK", item.src.decompressor.FileName(), bn))
	//}
	var lastCommitmentBlockNum, lastCommitmentTxNum uint64
	if len(ac.d[kv.CommitmentDomain].files) > 0 {
		lastCommitmentTxNum = ac.d[kv.CommitmentDomain].files[len(ac.d[kv.CommitmentDomain].files)-1].endTxNum
		lastCommitmentBlockNum = tx2block(lastCommitmentTxNum)
	}
	firstHistoryIndexBlockInDB := tx2block(ac.d[kv.AccountsDomain].d.FirstStepInDB(tx) * ac.a.StepSize())
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	log.Info("[snapshots] History Stat",
		"blocks", fmt.Sprintf("%dk", (domainBlockNumProgress+1)/1000),
		"txs", fmt.Sprintf("%dm", ac.a.visibleFilesMinimaxTxNum.Load()/1_000_000),
		"txNum2blockNum", strings.Join(str, ","),
		"first_history_idx_in_db", firstHistoryIndexBlockInDB,
		"last_comitment_block", lastCommitmentBlockNum,
		"last_comitment_tx_num", lastCommitmentTxNum,
		//"cnt_in_files", strings.Join(str2, ","),
		//"used_files", strings.Join(ac.Files(), ","),
		"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))

}

func (ac *AggregatorRoTx) EndTxNumNoCommitment() uint64 {
	return min(
		ac.d[kv.AccountsDomain].maxTxNumInDomainFiles(false),
		ac.d[kv.CodeDomain].maxTxNumInDomainFiles(false),
		ac.d[kv.StorageDomain].maxTxNumInDomainFiles(false),
	)
}

func (a *Aggregator) EndTxNumMinimax() uint64 { return a.visibleFilesMinimaxTxNum.Load() }
func (a *Aggregator) FilesAmount() (res []int) {
	for _, d := range a.d {
		res = append(res, d.dirtyFiles.Len())
	}
	return append(res,
		a.tracesFrom.dirtyFiles.Len(),
		a.tracesTo.dirtyFiles.Len(),
		a.logAddrs.dirtyFiles.Len(),
		a.logTopics.dirtyFiles.Len(),
	)
}

func FirstTxNumOfStep(step, size uint64) uint64 {
	return step * size
}

func LastTxNumOfStep(step, size uint64) uint64 {
	return FirstTxNumOfStep(step+1, size) - 1
}

// FirstTxNumOfStep returns txStepBeginning of given step.
// Step 0 is a range [0, stepSize).
// To prune step needed to fully Prune range [txStepBeginning, txNextStepBeginning)
func (a *Aggregator) FirstTxNumOfStep(step uint64) uint64 { // could have some smaller steps to prune// could have some smaller steps to prune
	return FirstTxNumOfStep(step, a.StepSize())
}

func (a *Aggregator) EndTxNumDomainsFrozen() uint64 {
	return min(
		a.d[kv.AccountsDomain].endIndexedTxNumMinimax(true),
		a.d[kv.StorageDomain].endIndexedTxNumMinimax(true),
		a.d[kv.CodeDomain].endIndexedTxNumMinimax(true),
		a.d[kv.CommitmentDomain].endIndexedTxNumMinimax(true),
	)
}

func (a *Aggregator) recalcVisibleFilesMinimaxTxNum() {
	aggTx := a.BeginFilesRo()
	defer aggTx.Close()
	a.visibleFilesMinimaxTxNum.Store(aggTx.minimaxTxNumInDomainFiles(false))
}

type RangesV3 struct {
	d                    [kv.DomainLen]DomainRanges
	logTopicsStartTxNum  uint64
	logAddrsEndTxNum     uint64
	logAddrsStartTxNum   uint64
	logTopicsEndTxNum    uint64
	tracesFromStartTxNum uint64
	tracesFromEndTxNum   uint64
	tracesToStartTxNum   uint64
	tracesToEndTxNum     uint64
	logAddrs             bool
	logTopics            bool
	tracesFrom           bool
	tracesTo             bool
}

func (r RangesV3) String() string {
	ss := []string{}
	for _, d := range r.d {
		if d.any() {
			ss = append(ss, fmt.Sprintf("%s(%s)", d.name, d.String()))
		}
	}
	if r.logAddrs {
		ss = append(ss, fmt.Sprintf("logAddr=%d-%d", r.logAddrsStartTxNum/r.d[kv.AccountsDomain].aggStep, r.logAddrsEndTxNum/r.d[kv.AccountsDomain].aggStep))
	}
	if r.logTopics {
		ss = append(ss, fmt.Sprintf("logTopic=%d-%d", r.logTopicsStartTxNum/r.d[kv.AccountsDomain].aggStep, r.logTopicsEndTxNum/r.d[kv.AccountsDomain].aggStep))
	}
	if r.tracesFrom {
		ss = append(ss, fmt.Sprintf("traceFrom=%d-%d", r.tracesFromStartTxNum/r.d[kv.AccountsDomain].aggStep, r.tracesFromEndTxNum/r.d[kv.AccountsDomain].aggStep))
	}
	if r.tracesTo {
		ss = append(ss, fmt.Sprintf("traceTo=%d-%d", r.tracesToStartTxNum/r.d[kv.AccountsDomain].aggStep, r.tracesToEndTxNum/r.d[kv.AccountsDomain].aggStep))
	}
	return strings.Join(ss, ", ")
}
func (r RangesV3) any() bool {
	for _, d := range r.d {
		if d.any() {
			return true
		}
	}
	return r.logAddrs || r.logTopics || r.tracesFrom || r.tracesTo
}

func (ac *AggregatorRoTx) findMergeRange(maxEndTxNum, maxSpan uint64) RangesV3 {
	var r RangesV3
	for id, d := range ac.d {
		r.d[id] = d.findMergeRange(maxEndTxNum, maxSpan)
	}
	r.logAddrs, r.logAddrsStartTxNum, r.logAddrsEndTxNum = ac.logAddrs.findMergeRange(maxEndTxNum, maxSpan)
	r.logTopics, r.logTopicsStartTxNum, r.logTopicsEndTxNum = ac.logTopics.findMergeRange(maxEndTxNum, maxSpan)
	r.tracesFrom, r.tracesFromStartTxNum, r.tracesFromEndTxNum = ac.tracesFrom.findMergeRange(maxEndTxNum, maxSpan)
	r.tracesTo, r.tracesToStartTxNum, r.tracesToEndTxNum = ac.tracesTo.findMergeRange(maxEndTxNum, maxSpan)
	//log.Info(fmt.Sprintf("findMergeRange(%d, %d)=%s\n", maxEndTxNum/ac.a.aggregationStep, maxSpan/ac.a.aggregationStep, r))
	return r
}

// SqueezeCommitmentFiles should be called only when NO EXECUTION is running.
// Removes commitment files and suppose following aggregator shutdown and restart  (to integrate new files and rebuild indexes)
func (ac *AggregatorRoTx) SqueezeCommitmentFiles() error {
	if !ac.a.commitmentValuesTransform {
		return nil
	}

	commitment := ac.d[kv.CommitmentDomain]
	accounts := ac.d[kv.AccountsDomain]
	storage := ac.d[kv.StorageDomain]

	// oh, again accessing domain.files directly, again and again..
	accountFiles := accounts.d.dirtyFiles.Items()
	storageFiles := storage.d.dirtyFiles.Items()
	commitFiles := commitment.d.dirtyFiles.Items()

	getSizeDelta := func(a, b string) (datasize.ByteSize, float32, error) {
		ai, err := os.Stat(a)
		if err != nil {
			return 0, 0, err
		}
		bi, err := os.Stat(b)
		if err != nil {
			return 0, 0, err
		}
		return datasize.ByteSize(ai.Size()) - datasize.ByteSize(bi.Size()), 100.0 * (float32(ai.Size()-bi.Size()) / float32(ai.Size())), nil
	}

	var (
		obsoleteFiles  []string
		temporalFiles  []string
		processedFiles int
		ai, si         int
		sizeDelta      = datasize.B
		sqExt          = ".squeezed"
	)
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	for ci := 0; ci < len(commitFiles); ci++ {
		cf := commitFiles[ci]
		for ai = 0; ai < len(accountFiles); ai++ {
			if accountFiles[ai].startTxNum == cf.startTxNum && accountFiles[ai].endTxNum == cf.endTxNum {
				break
			}
		}
		for si = 0; si < len(storageFiles); si++ {
			if storageFiles[si].startTxNum == cf.startTxNum && storageFiles[si].endTxNum == cf.endTxNum {
				break
			}
		}
		if ai == len(accountFiles) || si == len(storageFiles) {
			log.Info("SqueezeCommitmentFiles: commitment file has no corresponding account or storage file", "commitment", cf.decompressor.FileName())
			continue
		}
		af, sf := accountFiles[ai], storageFiles[si]

		err := func() error {
			log.Info("SqueezeCommitmentFiles: file start", "original", cf.decompressor.FileName(),
				"progress", fmt.Sprintf("%d/%d", ci+1, len(accountFiles)))

			originalPath := cf.decompressor.FilePath()
			squeezedTmpPath := originalPath + sqExt + ".tmp"
			squeezedCompr, err := seg.NewCompressor(context.Background(), "squeeze", squeezedTmpPath, ac.a.dirs.Tmp,
				seg.MinPatternScore, commitment.d.compressWorkers, log.LvlTrace, commitment.d.logger)

			if err != nil {
				return err
			}
			defer squeezedCompr.Close()

			cf.decompressor.EnableReadAhead()
			defer cf.decompressor.DisableReadAhead()
			reader := NewArchiveGetter(cf.decompressor.MakeGetter(), commitment.d.compression)
			reader.Reset(0)

			writer := NewArchiveWriter(squeezedCompr, commitment.d.compression)
			vt := commitment.commitmentValTransformDomain(accounts, storage, af, sf)

			i := 0
			for reader.HasNext() {
				k, _ := reader.Next(nil)
				v, _ := reader.Next(nil)
				i += 2

				if k == nil {
					// nil keys are not supported for domains
					continue
				}

				if !bytes.Equal(k, keyCommitmentState) {
					v, err = vt(v, af.startTxNum, af.endTxNum)
					if err != nil {
						return fmt.Errorf("failed to transform commitment value: %w", err)
					}
				}
				if err = writer.AddWord(k); err != nil {
					return fmt.Errorf("write key word: %w", err)
				}
				if err = writer.AddWord(v); err != nil {
					return fmt.Errorf("write value word: %w", err)
				}

				select {
				case <-logEvery.C:
					log.Info("SqueezeCommitmentFiles", "file", cf.decompressor.FileName(), "k", fmt.Sprintf("%x", k),
						"progress", fmt.Sprintf("%d/%d", i, cf.decompressor.Count()))
				default:
				}
			}

			if err = writer.Compress(); err != nil {
				return err
			}
			writer.Close()

			squeezedPath := originalPath + sqExt
			if err = os.Rename(squeezedTmpPath, squeezedPath); err != nil {
				return err
			}
			temporalFiles = append(temporalFiles, squeezedPath)

			delta, deltaP, err := getSizeDelta(originalPath, squeezedPath)
			if err != nil {
				return err
			}
			sizeDelta += delta

			log.Info("SqueezeCommitmentFiles: file done", "original", filepath.Base(originalPath),
				"sizeDelta", fmt.Sprintf("%s (%.1f%%)", delta.HR(), deltaP))

			fromStep, toStep := af.startTxNum/ac.a.StepSize(), af.endTxNum/ac.a.StepSize()

			// need to remove all indexes for commitment file as well
			obsoleteFiles = append(obsoleteFiles,
				originalPath,
				commitment.d.kvBtFilePath(fromStep, toStep),
				commitment.d.kvAccessorFilePath(fromStep, toStep),
				commitment.d.kvExistenceIdxFilePath(fromStep, toStep),
			)
			processedFiles++
			return nil
		}()
		if err != nil {
			return fmt.Errorf("failed to squeeze commitment file %q: %w", cf.decompressor.FileName(), err)
		}
	}

	log.Info("SqueezeCommitmentFiles: squeezed files has been produced, removing obsolete files",
		"toRemove", len(obsoleteFiles), "processed", fmt.Sprintf("%d/%d", processedFiles, len(commitFiles)))
	for _, path := range obsoleteFiles {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		log.Debug("SqueezeCommitmentFiles: obsolete file removal", "path", path)
	}
	log.Info("SqueezeCommitmentFiles: indices removed, renaming temporal files ")

	for _, path := range temporalFiles {
		if err := os.Rename(path, strings.TrimSuffix(path, sqExt)); err != nil {
			return err
		}
		log.Debug("SqueezeCommitmentFiles: temporal file renaming", "path", path)
	}
	log.Info("SqueezeCommitmentFiles: done", "sizeDelta", sizeDelta.HR(), "files", len(accountFiles))

	return nil
}

func (ac *AggregatorRoTx) mergeFiles(ctx context.Context, files SelectedStaticFilesV3, r RangesV3) (MergedFilesV3, error) {
	var mf MergedFilesV3
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(ac.a.mergeWorkers)
	closeFiles := true
	defer func() {
		if closeFiles {
			mf.Close()
		}
	}()

	ac.a.logger.Info(fmt.Sprintf("[snapshots] merge state %s", r.String()))

	accStorageMerged := new(sync.WaitGroup)

	for id := range ac.d {
		id := id
		if r.d[id].any() {
			kid := kv.Domain(id)
			if ac.a.commitmentValuesTransform && (kid == kv.AccountsDomain || kid == kv.StorageDomain) {
				accStorageMerged.Add(1)
			}

			g.Go(func() (err error) {
				var vt valueTransformer
				if ac.a.commitmentValuesTransform && kid == kv.CommitmentDomain {
					ac.a.d[kv.AccountsDomain].restrictSubsetFileDeletions = true
					ac.a.d[kv.StorageDomain].restrictSubsetFileDeletions = true
					ac.a.d[kv.CommitmentDomain].restrictSubsetFileDeletions = true

					accStorageMerged.Wait()

					vt = ac.d[kv.CommitmentDomain].commitmentValTransformDomain(ac.d[kv.AccountsDomain], ac.d[kv.StorageDomain],
						mf.d[kv.AccountsDomain], mf.d[kv.StorageDomain])
				}

				mf.d[id], mf.dIdx[id], mf.dHist[id], err = ac.d[id].mergeFiles(ctx, files.d[id], files.dIdx[id], files.dHist[id], r.d[id], vt, ac.a.ps)
				if ac.a.commitmentValuesTransform {
					if kid == kv.AccountsDomain || kid == kv.StorageDomain {
						accStorageMerged.Done()
					}
					if err == nil && kid == kv.CommitmentDomain {
						ac.a.d[kv.AccountsDomain].restrictSubsetFileDeletions = false
						ac.a.d[kv.StorageDomain].restrictSubsetFileDeletions = false
						ac.a.d[kv.CommitmentDomain].restrictSubsetFileDeletions = false
					}
				}
				return err
			})
		}
	}

	if r.logAddrs {
		g.Go(func() error {
			var err error
			mf.logAddrs, err = ac.logAddrs.mergeFiles(ctx, files.logAddrs, r.logAddrsStartTxNum, r.logAddrsEndTxNum, ac.a.ps)
			return err
		})
	}
	if r.logTopics {
		g.Go(func() error {
			var err error
			mf.logTopics, err = ac.logTopics.mergeFiles(ctx, files.logTopics, r.logTopicsStartTxNum, r.logTopicsEndTxNum, ac.a.ps)
			return err
		})
	}
	if r.tracesFrom {
		g.Go(func() error {
			var err error
			mf.tracesFrom, err = ac.tracesFrom.mergeFiles(ctx, files.tracesFrom, r.tracesFromStartTxNum, r.tracesFromEndTxNum, ac.a.ps)
			return err
		})
	}
	if r.tracesTo {
		g.Go(func() error {
			var err error
			mf.tracesTo, err = ac.tracesTo.mergeFiles(ctx, files.tracesTo, r.tracesToStartTxNum, r.tracesToEndTxNum, ac.a.ps)
			return err
		})
	}
	err := g.Wait()
	if err == nil {
		closeFiles = false
		ac.a.logger.Info(fmt.Sprintf("[snapshots] state merge done %s", r.String()))
	} else {
		ac.a.logger.Warn(fmt.Sprintf("[snapshots] state merge failed err=%v %s", err, r.String()))
	}
	return mf, err
}

func (ac *AggregatorRoTx) integrateMergedFiles(outs SelectedStaticFilesV3, in MergedFilesV3) (frozen []string) {
	ac.a.dirtyFilesLock.Lock()
	defer ac.a.dirtyFilesLock.Unlock()
	defer ac.a.needSaveFilesListInDB.Store(true)
	defer ac.a.recalcVisibleFilesMinimaxTxNum()

	for id, d := range ac.a.d {
		d.integrateMergedFiles(outs.d[id], outs.dIdx[id], outs.dHist[id], in.d[id], in.dIdx[id], in.dHist[id])
	}

	ac.a.logAddrs.integrateMergedFiles(outs.logAddrs, in.logAddrs)
	ac.a.logTopics.integrateMergedFiles(outs.logTopics, in.logTopics)
	ac.a.tracesFrom.integrateMergedFiles(outs.tracesFrom, in.tracesFrom)
	ac.a.tracesTo.integrateMergedFiles(outs.tracesTo, in.tracesTo)
	ac.cleanAfterMerge(in)
	return frozen
}
func (ac *AggregatorRoTx) cleanAfterMerge(in MergedFilesV3) {
	for id, d := range ac.d {
		d.cleanAfterMerge(in.d[id], in.dHist[id], in.dIdx[id])
	}
	ac.logAddrs.cleanAfterMerge(in.logAddrs)
	ac.logTopics.cleanAfterMerge(in.logTopics)
	ac.tracesFrom.cleanAfterMerge(in.tracesFrom)
	ac.tracesTo.cleanAfterMerge(in.tracesTo)
}

// KeepStepsInDB - usually equal to one a.aggregationStep, but when we exec blocks from snapshots
// we can set it to 0, because no re-org on this blocks are possible
func (a *Aggregator) KeepStepsInDB(steps uint64) *Aggregator {
	a.keepInDB = a.FirstTxNumOfStep(steps)
	for _, d := range a.d {
		if d == nil {
			continue
		}
		if d.History.dontProduceFiles {
			d.History.keepTxInDB = a.keepInDB
		}
	}

	return a
}

func (a *Aggregator) SetSnapshotBuildSema(semaphore *semaphore.Weighted) {
	a.snapshotBuildSema = semaphore
}

// Returns channel which is closed when aggregation is done
func (a *Aggregator) BuildFilesInBackground(txNum uint64) chan struct{} {
	fin := make(chan struct{})

	if (txNum + 1) <= a.visibleFilesMinimaxTxNum.Load()+a.keepInDB {
		close(fin)
		return fin
	}

	if ok := a.buildingFiles.CompareAndSwap(false, true); !ok {
		close(fin)
		return fin
	}

	step := a.visibleFilesMinimaxTxNum.Load() / a.StepSize()
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer a.buildingFiles.Store(false)

		if a.snapshotBuildSema != nil {
			//we are inside own goroutine - it's fine to block here
			if err := a.snapshotBuildSema.Acquire(a.ctx, 1); err != nil {
				log.Warn("[snapshots] buildFilesInBackground", "err", err)
				return //nolint
			}
			defer a.snapshotBuildSema.Release(1)
		}

		// check if db has enough data (maybe we didn't commit them yet or all keys are unique so history is empty)
		lastInDB := lastIdInDB(a.db, a.d[kv.AccountsDomain])
		hasData := lastInDB > step // `step` must be fully-written - means `step+1` records must be visible
		if !hasData {
			close(fin)
			return
		}

		// trying to create as much small-step-files as possible:
		// - to reduce amount of small merges
		// - to remove old data from db as early as possible
		// - during files build, may happen commit of new data. on each loop step getting latest id in db
		for ; step < lastIdInDB(a.db, a.d[kv.AccountsDomain]); step++ { //`step` must be fully-written - means `step+1` records must be visible
			if err := a.buildFiles(a.ctx, step); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, common2.ErrStopped) {
					close(fin)
					return
				}
				log.Warn("[snapshots] buildFilesInBackground", "err", err)
				break
			}
		}
		a.BuildOptionalMissedIndicesInBackground(a.ctx, 1)

		if dbg.NoMerge() {
			close(fin)
			return
		}
		if ok := a.mergeingFiles.CompareAndSwap(false, true); !ok {
			close(fin)
			return
		}
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			defer a.mergeingFiles.Store(false)

			//TODO: merge must have own semphore

			defer func() { close(fin) }()
			if err := a.MergeLoop(a.ctx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, common2.ErrStopped) {
					return
				}
				log.Warn("[snapshots] merge", "err", err)
			}

			a.BuildOptionalMissedIndicesInBackground(a.ctx, 1)
		}()
	}()
	return fin
}

func (ac *AggregatorRoTx) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int, tx kv.Tx) (timestamps iter.U64, err error) {
	switch name {
	case kv.AccountsHistoryIdx:
		return ac.d[kv.AccountsDomain].ht.IdxRange(k, fromTs, toTs, asc, limit, tx)
	case kv.StorageHistoryIdx:
		return ac.d[kv.StorageDomain].ht.IdxRange(k, fromTs, toTs, asc, limit, tx)
	case kv.CodeHistoryIdx:
		return ac.d[kv.CodeDomain].ht.IdxRange(k, fromTs, toTs, asc, limit, tx)
	case kv.CommitmentHistoryIdx:
		return ac.d[kv.StorageDomain].ht.IdxRange(k, fromTs, toTs, asc, limit, tx)
	//case kv.GasusedHistoryIdx:
	//	return ac.d[kv.GasUsedDomain].ht.IdxRange(k, fromTs, toTs, asc, limit, tx)
	case kv.LogTopicIdx:
		return ac.logTopics.IdxRange(k, fromTs, toTs, asc, limit, tx)
	case kv.LogAddrIdx:
		return ac.logAddrs.IdxRange(k, fromTs, toTs, asc, limit, tx)
	case kv.TracesFromIdx:
		return ac.tracesFrom.IdxRange(k, fromTs, toTs, asc, limit, tx)
	case kv.TracesToIdx:
		return ac.tracesTo.IdxRange(k, fromTs, toTs, asc, limit, tx)
	default:
		return nil, fmt.Errorf("unexpected history name: %s", name)
	}
}

// -- range end

func (ac *AggregatorRoTx) HistoryGet(name kv.History, key []byte, ts uint64, tx kv.Tx) (v []byte, ok bool, err error) {
	switch name {
	case kv.AccountsHistory:
		v, ok, err = ac.d[kv.AccountsDomain].ht.GetNoStateWithRecent(key, ts, tx)
		if err != nil {
			return nil, false, err
		}
		if !ok || len(v) == 0 {
			return v, ok, nil
		}
		return v, true, nil
	case kv.StorageHistory:
		return ac.d[kv.StorageDomain].ht.GetNoStateWithRecent(key, ts, tx)
	case kv.CodeHistory:
		return ac.d[kv.CodeDomain].ht.GetNoStateWithRecent(key, ts, tx)
	case kv.CommitmentHistory:
		return ac.d[kv.CommitmentDomain].ht.GetNoStateWithRecent(key, ts, tx)
	//case kv.GasUsedHistory:
	//	return ac.d[kv.GasUsedDomain].ht.GetNoStateWithRecent(key, ts, tx)
	default:
		panic(fmt.Sprintf("unexpected: %s", name))
	}
}

func (ac *AggregatorRoTx) AccountHistoryRange(startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.KV, error) {
	hr, err := ac.d[kv.AccountsDomain].ht.HistoryRange(startTxNum, endTxNum, asc, limit, tx)
	if err != nil {
		return nil, err
	}
	return iter.WrapKV(hr), nil
}

func (ac *AggregatorRoTx) StorageHistoryRange(startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.KV, error) {
	hr, err := ac.d[kv.StorageDomain].ht.HistoryRange(startTxNum, endTxNum, asc, limit, tx)
	if err != nil {
		return nil, err
	}
	return iter.WrapKV(hr), nil
}

func (ac *AggregatorRoTx) CodeHistoryRange(startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.KV, error) {
	hr, err := ac.d[kv.CodeDomain].ht.HistoryRange(startTxNum, endTxNum, asc, limit, tx)
	if err != nil {
		return nil, err
	}
	return iter.WrapKV(hr), nil
}

type FilesStats22 struct{}

func (a *Aggregator) Stats() FilesStats22 {
	var fs FilesStats22
	return fs
}

// AggregatorRoTx guarantee consistent View of files ("snapshots isolation" level https://en.wikipedia.org/wiki/Snapshot_isolation):
//   - long-living consistent view of all files (no limitations)
//   - hiding garbage and files overlaps
//   - protecting useful files from removal
//   - user will not see "partial writes" or "new files appearance"
//   - last reader removing garbage files inside `Close` method
type AggregatorRoTx struct {
	a          *Aggregator
	d          [kv.DomainLen]*DomainRoTx
	logAddrs   *InvertedIndexRoTx
	logTopics  *InvertedIndexRoTx
	tracesFrom *InvertedIndexRoTx
	tracesTo   *InvertedIndexRoTx

	id      uint64 // auto-increment id of ctx for logs
	_leakID uint64 // set only if TRACE_AGG=true
}

func (a *Aggregator) BeginFilesRo() *AggregatorRoTx {
	ac := &AggregatorRoTx{
		a:          a,
		logAddrs:   a.logAddrs.BeginFilesRo(),
		logTopics:  a.logTopics.BeginFilesRo(),
		tracesFrom: a.tracesFrom.BeginFilesRo(),
		tracesTo:   a.tracesTo.BeginFilesRo(),

		id:      a.ctxAutoIncrement.Add(1),
		_leakID: a.leakDetector.Add(),
	}
	for id, d := range a.d {
		ac.d[id] = d.BeginFilesRo()
	}

	return ac
}
func (ac *AggregatorRoTx) ViewID() uint64 { return ac.id }

// --- Domain part START ---

func (ac *AggregatorRoTx) DomainRange(tx kv.Tx, domain kv.Domain, fromKey, toKey []byte, ts uint64, asc order.By, limit int) (it iter.KV, err error) {
	return ac.d[domain].DomainRange(tx, fromKey, toKey, ts, asc, limit)
}
func (ac *AggregatorRoTx) DomainRangeLatest(tx kv.Tx, domain kv.Domain, from, to []byte, limit int) (iter.KV, error) {
	return ac.d[domain].DomainRangeLatest(tx, from, to, limit)
}

func (ac *AggregatorRoTx) DomainGetAsOf(tx kv.Tx, name kv.Domain, key []byte, ts uint64) (v []byte, ok bool, err error) {
	v, err = ac.d[name].GetAsOf(key, ts, tx)
	return v, v != nil, err
}
func (ac *AggregatorRoTx) GetLatest(domain kv.Domain, k, k2 []byte, tx kv.Tx) (v []byte, step uint64, ok bool, err error) {
	return ac.d[domain].GetLatest(k, k2, tx)
}

// search key in all files of all domains and print file names
func (ac *AggregatorRoTx) DebugKey(domain kv.Domain, k []byte) error {
	l, err := ac.d[domain].DebugKVFilesWithKey(k)
	if err != nil {
		return err
	}
	if len(l) > 0 {
		log.Info("[dbg] found in", "files", l)
	}
	return nil
}
func (ac *AggregatorRoTx) DebugEFKey(domain kv.Domain, k []byte) error {
	return ac.d[domain].DebugEFKey(k)
}

func (ac *AggregatorRoTx) DebugEFAllValuesAreInRange(ctx context.Context, name kv.InvertedIdx) error {
	switch name {
	case kv.AccountsHistoryIdx:
		err := ac.d[kv.AccountsDomain].ht.iit.DebugEFAllValuesAreInRange(ctx)
		if err != nil {
			return err
		}
	case kv.StorageHistoryIdx:
		err := ac.d[kv.CodeDomain].ht.iit.DebugEFAllValuesAreInRange(ctx)
		if err != nil {
			return err
		}
	case kv.CodeHistoryIdx:
		err := ac.d[kv.StorageDomain].ht.iit.DebugEFAllValuesAreInRange(ctx)
		if err != nil {
			return err
		}
	case kv.CommitmentHistoryIdx:
		err := ac.d[kv.CommitmentDomain].ht.iit.DebugEFAllValuesAreInRange(ctx)
		if err != nil {
			return err
		}
	//case kv.GasusedHistoryIdx:
	//	err := ac.d[kv.GasUsedDomain].ht.iit.DebugEFAllValuesAreInRange(ctx)
	//	if err != nil {
	//		return err
	//	}
	case kv.TracesFromIdx:
		err := ac.tracesFrom.DebugEFAllValuesAreInRange(ctx)
		if err != nil {
			return err
		}
	case kv.TracesToIdx:
		err := ac.tracesTo.DebugEFAllValuesAreInRange(ctx)
		if err != nil {
			return err
		}
	case kv.LogAddrIdx:
		err := ac.logAddrs.DebugEFAllValuesAreInRange(ctx)
		if err != nil {
			return err
		}
	case kv.LogTopicIdx:
		err := ac.logTopics.DebugEFAllValuesAreInRange(ctx)
		if err != nil {
			return err
		}
	default:
		panic(fmt.Sprintf("unexpected: %s", name))
	}
	return nil
}

// --- Domain part END ---

func (ac *AggregatorRoTx) Close() {
	if ac == nil || ac.a == nil { // invariant: it's safe to call Close multiple times
		return
	}
	ac.a.leakDetector.Del(ac._leakID)
	ac.a = nil

	for _, d := range ac.d {
		if d != nil {
			d.Close()
		}
	}
	ac.logAddrs.Close()
	ac.logTopics.Close()
	ac.tracesFrom.Close()
	ac.tracesTo.Close()
}

// BackgroundResult - used only indicate that some work is done
// no much reason to pass exact results by this object, just get latest state when need
type BackgroundResult struct {
	err error
	has bool
}

func (br *BackgroundResult) Has() bool     { return br.has }
func (br *BackgroundResult) Set(err error) { br.has, br.err = true, err }
func (br *BackgroundResult) GetAndReset() (bool, error) {
	has, err := br.has, br.err
	br.has, br.err = false, nil
	return has, err
}

// Inverted index tables only
func lastIdInDB(db kv.RoDB, domain *Domain) (lstInDb uint64) {
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		lstInDb = domain.LastStepInDB(tx)
		return nil
	}); err != nil {
		log.Warn("[snapshots] lastIdInDB", "err", err)
	}
	return lstInDb
}

// AggregatorStep is used for incremental reconstitution, it allows
// accessing history in isolated way for each step
type AggregatorStep struct {
	a          *Aggregator
	accounts   *HistoryStep
	storage    *HistoryStep
	code       *HistoryStep
	commitment *HistoryStep
	keyBuf     []byte
}

func (a *Aggregator) StepSize() uint64 { return a.aggregationStep }
func (a *Aggregator) MakeSteps() ([]*AggregatorStep, error) {
	frozenAndIndexed := a.EndTxNumDomainsFrozen()
	accountSteps := a.d[kv.AccountsDomain].MakeSteps(frozenAndIndexed)
	codeSteps := a.d[kv.CodeDomain].MakeSteps(frozenAndIndexed)
	storageSteps := a.d[kv.StorageDomain].MakeSteps(frozenAndIndexed)
	commitmentSteps := a.d[kv.CommitmentDomain].MakeSteps(frozenAndIndexed)
	if len(accountSteps) != len(storageSteps) || len(storageSteps) != len(codeSteps) {
		return nil, fmt.Errorf("different limit of steps (try merge snapshots): accountSteps=%d, storageSteps=%d, codeSteps=%d", len(accountSteps), len(storageSteps), len(codeSteps))
	}
	steps := make([]*AggregatorStep, len(accountSteps))
	for i, accountStep := range accountSteps {
		steps[i] = &AggregatorStep{
			a:          a,
			accounts:   accountStep,
			storage:    storageSteps[i],
			code:       codeSteps[i],
			commitment: commitmentSteps[i],
		}
	}
	return steps, nil
}

func (as *AggregatorStep) TxNumRange() (uint64, uint64) {
	return as.accounts.indexFile.startTxNum, as.accounts.indexFile.endTxNum
}

func (as *AggregatorStep) IterateAccountsTxs() *ScanIteratorInc {
	return as.accounts.iterateTxs()
}

func (as *AggregatorStep) IterateStorageTxs() *ScanIteratorInc {
	return as.storage.iterateTxs()
}

func (as *AggregatorStep) IterateCodeTxs() *ScanIteratorInc {
	return as.code.iterateTxs()
}

func (as *AggregatorStep) ReadAccountDataNoState(addr []byte, txNum uint64) ([]byte, bool, uint64) {
	return as.accounts.GetNoState(addr, txNum)
}

func (as *AggregatorStep) ReadAccountStorageNoState(addr []byte, loc []byte, txNum uint64) ([]byte, bool, uint64) {
	if cap(as.keyBuf) < len(addr)+len(loc) {
		as.keyBuf = make([]byte, len(addr)+len(loc))
	} else if len(as.keyBuf) != len(addr)+len(loc) {
		as.keyBuf = as.keyBuf[:len(addr)+len(loc)]
	}
	copy(as.keyBuf, addr)
	copy(as.keyBuf[len(addr):], loc)
	return as.storage.GetNoState(as.keyBuf, txNum)
}

func (as *AggregatorStep) ReadAccountCodeNoState(addr []byte, txNum uint64) ([]byte, bool, uint64) {
	return as.code.GetNoState(addr, txNum)
}

func (as *AggregatorStep) ReadAccountCodeSizeNoState(addr []byte, txNum uint64) (int, bool, uint64) {
	code, noState, stateTxNum := as.code.GetNoState(addr, txNum)
	return len(code), noState, stateTxNum
}

func (as *AggregatorStep) MaxTxNumAccounts(addr []byte) (bool, uint64) {
	return as.accounts.MaxTxNum(addr)
}

func (as *AggregatorStep) MaxTxNumStorage(addr []byte, loc []byte) (bool, uint64) {
	if cap(as.keyBuf) < len(addr)+len(loc) {
		as.keyBuf = make([]byte, len(addr)+len(loc))
	} else if len(as.keyBuf) != len(addr)+len(loc) {
		as.keyBuf = as.keyBuf[:len(addr)+len(loc)]
	}
	copy(as.keyBuf, addr)
	copy(as.keyBuf[len(addr):], loc)
	return as.storage.MaxTxNum(as.keyBuf)
}

func (as *AggregatorStep) MaxTxNumCode(addr []byte) (bool, uint64) {
	return as.code.MaxTxNum(addr)
}

func (as *AggregatorStep) IterateAccountsHistory(txNum uint64) *HistoryIteratorInc {
	return as.accounts.interateHistoryBeforeTxNum(txNum)
}

func (as *AggregatorStep) IterateStorageHistory(txNum uint64) *HistoryIteratorInc {
	return as.storage.interateHistoryBeforeTxNum(txNum)
}

func (as *AggregatorStep) IterateCodeHistory(txNum uint64) *HistoryIteratorInc {
	return as.code.interateHistoryBeforeTxNum(txNum)
}

func (as *AggregatorStep) Clone() *AggregatorStep {
	return &AggregatorStep{
		a:        as.a,
		accounts: as.accounts.Clone(),
		storage:  as.storage.Clone(),
		code:     as.code.Clone(),
	}
}
