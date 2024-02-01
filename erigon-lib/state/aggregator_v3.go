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

	"golang.org/x/sync/semaphore"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/log/v3"
	rand2 "golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"

	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/metrics"
)

var (
	mxPruneTookAgg = metrics.GetOrCreateSummary(`prune_seconds{type="state"}`)
)

type AggregatorV3 struct {
	db               kv.RoDB
	accounts         *Domain
	storage          *Domain
	code             *Domain
	commitment       *Domain
	tracesTo         *InvertedIndex
	logAddrs         *InvertedIndex
	logTopics        *InvertedIndex
	tracesFrom       *InvertedIndex
	backgroundResult *BackgroundResult
	dirs             datadir.Dirs
	tmpdir           string
	aggregationStep  uint64
	keepInDB         uint64

	minimaxTxNumInFiles atomic.Uint64

	filesMutationLock sync.Mutex
	snapshotBuildSema *semaphore.Weighted

	collateAndBuildWorkers int // minimize amount of background workers by default
	mergeWorkers           int // usually 1

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

func NewAggregatorV3(ctx context.Context, dirs datadir.Dirs, aggregationStep uint64, db kv.RoDB, logger log.Logger) (*AggregatorV3, error) {
	tmpdir := dirs.Tmp
	salt, err := getIndicesSalt(dirs.Snap)
	if err != nil {
		return nil, err
	}

	ctx, ctxCancel := context.WithCancel(ctx)
	a := &AggregatorV3{
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
	}
	cfg := domainCfg{
		hist: histCfg{
			iiCfg:             iiCfg{salt: salt, dirs: dirs},
			withLocalityIndex: false, withExistenceIndex: true, compression: CompressNone, historyLargeValues: false,
		},
	}
	if a.accounts, err = NewDomain(cfg, aggregationStep, "accounts", kv.TblAccountKeys, kv.TblAccountVals, kv.TblAccountHistoryKeys, kv.TblAccountHistoryVals, kv.TblAccountIdx, logger); err != nil {
		return nil, err
	}
	cfg = domainCfg{
		hist: histCfg{
			iiCfg:             iiCfg{salt: salt, dirs: dirs},
			withLocalityIndex: false, withExistenceIndex: true, compression: CompressNone, historyLargeValues: false,
		},
	}
	if a.storage, err = NewDomain(cfg, aggregationStep, "storage", kv.TblStorageKeys, kv.TblStorageVals, kv.TblStorageHistoryKeys, kv.TblStorageHistoryVals, kv.TblStorageIdx, logger); err != nil {
		return nil, err
	}
	cfg = domainCfg{
		hist: histCfg{
			iiCfg:             iiCfg{salt: salt, dirs: dirs},
			withLocalityIndex: false, withExistenceIndex: true, compression: CompressKeys | CompressVals, historyLargeValues: true,
		},
	}
	if a.code, err = NewDomain(cfg, aggregationStep, "code", kv.TblCodeKeys, kv.TblCodeVals, kv.TblCodeHistoryKeys, kv.TblCodeHistoryVals, kv.TblCodeIdx, logger); err != nil {
		return nil, err
	}
	cfg = domainCfg{
		hist: histCfg{
			iiCfg:             iiCfg{salt: salt, dirs: dirs},
			withLocalityIndex: false, withExistenceIndex: true, compression: CompressNone, historyLargeValues: false,
			//dontProduceFiles: true,
		},
		compress: CompressNone,
	}
	if a.commitment, err = NewDomain(cfg, aggregationStep, "commitment", kv.TblCommitmentKeys, kv.TblCommitmentVals, kv.TblCommitmentHistoryKeys, kv.TblCommitmentHistoryVals, kv.TblCommitmentIdx, logger); err != nil {
		return nil, err
	}
	idxCfg := iiCfg{salt: salt, dirs: dirs}
	if a.logAddrs, err = NewInvertedIndex(idxCfg, aggregationStep, "logaddrs", kv.TblLogAddressKeys, kv.TblLogAddressIdx, false, true, nil, logger); err != nil {
		return nil, err
	}
	idxCfg = iiCfg{salt: salt, dirs: dirs}
	if a.logTopics, err = NewInvertedIndex(idxCfg, aggregationStep, "logtopics", kv.TblLogTopicsKeys, kv.TblLogTopicsIdx, false, true, nil, logger); err != nil {
		return nil, err
	}
	idxCfg = iiCfg{salt: salt, dirs: dirs}
	if a.tracesFrom, err = NewInvertedIndex(idxCfg, aggregationStep, "tracesfrom", kv.TblTracesFromKeys, kv.TblTracesFromIdx, false, true, nil, logger); err != nil {
		return nil, err
	}
	idxCfg = iiCfg{salt: salt, dirs: dirs}
	if a.tracesTo, err = NewInvertedIndex(idxCfg, aggregationStep, "tracesto", kv.TblTracesToKeys, kv.TblTracesToIdx, false, true, nil, logger); err != nil {
		return nil, err
	}
	a.KeepStepsInDB(1)
	a.recalcMaxTxNum()

	if dbg.NoSync() {
		a.DisableFsync()
	}

	return a, nil
}

// getIndicesSalt - try read salt for all indices from DB. Or fall-back to new salt creation.
// if db is Read-Only (for example remote RPCDaemon or utilities) - we will not create new indices - and existing indices have salt in metadata.
func getIndicesSalt(baseDir string) (salt *uint32, err error) {
	fpath := filepath.Join(baseDir, "salt.txt")
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

func (a *AggregatorV3) OnFreeze(f OnFreezeFunc) { a.onFreeze = f }
func (a *AggregatorV3) DisableFsync() {
	a.accounts.DisableFsync()
	a.storage.DisableFsync()
	a.code.DisableFsync()
	a.commitment.DisableFsync()
	a.logAddrs.DisableFsync()
	a.logTopics.DisableFsync()
	a.tracesFrom.DisableFsync()
	a.tracesTo.DisableFsync()
}

func (a *AggregatorV3) OpenFolder(readonly bool) error {
	a.filesMutationLock.Lock()
	defer a.filesMutationLock.Unlock()
	eg := &errgroup.Group{}
	eg.Go(func() error { return a.accounts.OpenFolder(readonly) })
	eg.Go(func() error { return a.storage.OpenFolder(readonly) })
	eg.Go(func() error { return a.code.OpenFolder(readonly) })
	eg.Go(func() error { return a.commitment.OpenFolder(readonly) })
	eg.Go(func() error { return a.logAddrs.OpenFolder(readonly) })
	eg.Go(func() error { return a.logTopics.OpenFolder(readonly) })
	eg.Go(func() error { return a.tracesFrom.OpenFolder(readonly) })
	eg.Go(func() error { return a.tracesTo.OpenFolder(readonly) })
	if err := eg.Wait(); err != nil {
		return err
	}
	a.recalcMaxTxNum()
	return nil
}

func (a *AggregatorV3) OpenList(files []string, readonly bool) error {
	//log.Warn("[dbg] OpenList", "l", files)

	a.filesMutationLock.Lock()
	defer a.filesMutationLock.Unlock()
	eg := &errgroup.Group{}
	eg.Go(func() error { return a.accounts.OpenFolder(readonly) })
	eg.Go(func() error { return a.storage.OpenFolder(readonly) })
	eg.Go(func() error { return a.code.OpenFolder(readonly) })
	eg.Go(func() error { return a.commitment.OpenFolder(readonly) })
	eg.Go(func() error { return a.logAddrs.OpenFolder(readonly) })
	eg.Go(func() error { return a.logTopics.OpenFolder(readonly) })
	eg.Go(func() error { return a.tracesFrom.OpenFolder(readonly) })
	eg.Go(func() error { return a.tracesTo.OpenFolder(readonly) })
	if err := eg.Wait(); err != nil {
		return err
	}
	a.recalcMaxTxNum()
	return nil
}

func (a *AggregatorV3) Close() {
	if a.ctxCancel == nil { // invariant: it's safe to call Close multiple times
		return
	}
	a.ctxCancel()
	a.ctxCancel = nil
	a.wg.Wait()

	a.filesMutationLock.Lock()
	defer a.filesMutationLock.Unlock()

	a.accounts.Close()
	a.storage.Close()
	a.code.Close()
	a.commitment.Close()
	a.logAddrs.Close()
	a.logTopics.Close()
	a.tracesFrom.Close()
	a.tracesTo.Close()
}

func (a *AggregatorV3) SetCollateAndBuildWorkers(i int) { a.collateAndBuildWorkers = i }
func (a *AggregatorV3) SetMergeWorkers(i int)           { a.mergeWorkers = i }
func (a *AggregatorV3) SetCompressWorkers(i int) {
	a.accounts.compressWorkers = i
	a.storage.compressWorkers = i
	a.code.compressWorkers = i
	a.commitment.compressWorkers = i
	a.logAddrs.compressWorkers = i
	a.logTopics.compressWorkers = i
	a.tracesFrom.compressWorkers = i
	a.tracesTo.compressWorkers = i
}

func (a *AggregatorV3) HasBackgroundFilesBuild() bool { return a.ps.Has() }
func (a *AggregatorV3) BackgroundProgress() string    { return a.ps.String() }

func (ac *AggregatorV3Context) Files() []string {
	var res []string
	if ac == nil {
		return res
	}
	res = append(res, ac.account.Files()...)
	res = append(res, ac.storage.Files()...)
	res = append(res, ac.code.Files()...)
	res = append(res, ac.commitment.Files()...)
	res = append(res, ac.logAddrs.Files()...)
	res = append(res, ac.logTopics.Files()...)
	res = append(res, ac.tracesFrom.Files()...)
	res = append(res, ac.tracesTo.Files()...)
	return res
}
func (a *AggregatorV3) Files() []string {
	ac := a.MakeContext()
	defer ac.Close()
	return ac.Files()
}

func (a *AggregatorV3) BuildOptionalMissedIndicesInBackground(ctx context.Context, workers int) {
	if ok := a.buildingOptionalIndices.CompareAndSwap(false, true); !ok {
		return
	}
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer a.buildingOptionalIndices.Store(false)
		aggCtx := a.MakeContext()
		defer aggCtx.Close()
		if err := aggCtx.buildOptionalMissedIndices(ctx, workers); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, common2.ErrStopped) {
				return
			}
			log.Warn("[snapshots] BuildOptionalMissedIndicesInBackground", "err", err)
		}
	}()
}

func (a *AggregatorV3) BuildOptionalMissedIndices(ctx context.Context, workers int) error {
	if ok := a.buildingOptionalIndices.CompareAndSwap(false, true); !ok {
		return nil
	}
	defer a.buildingOptionalIndices.Store(false)
	aggCtx := a.MakeContext()
	defer aggCtx.Close()
	if err := aggCtx.buildOptionalMissedIndices(ctx, workers); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, common2.ErrStopped) {
			return nil
		}
		return err
	}
	return nil
}

func (ac *AggregatorV3Context) buildOptionalMissedIndices(ctx context.Context, workers int) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	ps := background.NewProgressSet()
	if ac.account != nil {
		g.Go(func() error { return ac.account.BuildOptionalMissedIndices(ctx, ps) })
	}
	if ac.storage != nil {
		g.Go(func() error { return ac.storage.BuildOptionalMissedIndices(ctx, ps) })
	}
	if ac.code != nil {
		g.Go(func() error { return ac.code.BuildOptionalMissedIndices(ctx, ps) })
	}
	if ac.commitment != nil {
		g.Go(func() error { return ac.commitment.BuildOptionalMissedIndices(ctx, ps) })
	}
	return g.Wait()
}

func (a *AggregatorV3) BuildMissedIndices(ctx context.Context, workers int) error {
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
		a.accounts.BuildMissedIndices(ctx, g, ps)
		a.storage.BuildMissedIndices(ctx, g, ps)
		a.code.BuildMissedIndices(ctx, g, ps)
		a.commitment.BuildMissedIndices(ctx, g, ps)
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
	accounts   StaticFiles
	storage    StaticFiles
	code       StaticFiles
	commitment StaticFiles
	logAddrs   InvertedFiles
	logTopics  InvertedFiles
	tracesFrom InvertedFiles
	tracesTo   InvertedFiles
}

// CleanupOnError - call it on collation fail. It's closing all files
func (sf AggV3StaticFiles) CleanupOnError() {
	sf.accounts.CleanupOnError()
	sf.storage.CleanupOnError()
	sf.code.CleanupOnError()
	sf.logAddrs.CleanupOnError()
	sf.logTopics.CleanupOnError()
	sf.tracesFrom.CleanupOnError()
	sf.tracesTo.CleanupOnError()
}

func (a *AggregatorV3) buildFiles(ctx context.Context, step uint64) error {
	a.logger.Debug("[agg] collate and build", "step", step, "collate_workers", a.collateAndBuildWorkers, "merge_workers", a.mergeWorkers, "compress_workers", a.accounts.compressWorkers)

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
	defer a.recalcMaxTxNum()
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

	for _, d := range []*Domain{a.accounts, a.storage, a.code, a.commitment} {
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

			switch kv.Domain(d.valsTable) {
			case kv.TblAccountVals:
				static.accounts = sf
			case kv.TblStorageVals:
				static.storage = sf
			case kv.TblCodeVals:
				static.code = sf
			case kv.TblCommitmentVals:
				static.commitment = sf
			default:
				panic("unknown domain " + d.valsTable)
			}

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

			switch kv.Domain(d.indexKeysTable) {
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

func (a *AggregatorV3) BuildFiles(toTxNum uint64) (err error) {
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

func (a *AggregatorV3) mergeLoopStep(ctx context.Context) (somethingDone bool, err error) {
	a.logger.Debug("[agg] merge", "collate_workers", a.collateAndBuildWorkers, "merge_workers", a.mergeWorkers, "compress_workers", a.accounts.compressWorkers)

	ac := a.MakeContext()
	defer ac.Close()
	mxRunningMerges.Inc()
	defer mxRunningMerges.Dec()

	closeAll := true
	maxSpan := StepsInColdFile * a.StepSize()
	r := ac.findMergeRange(a.minimaxTxNumInFiles.Load(), maxSpan)
	if !r.any() {
		return false, nil
	}

	outs, err := ac.staticFilesInRange(r)
	defer func() {
		if closeAll {
			outs.Close()
		}
	}()
	if err != nil {
		return false, err
	}

	in, err := ac.mergeFiles(ctx, outs, r)
	if err != nil {
		return true, err
	}
	defer func() {
		if closeAll {
			in.Close()
		}
	}()
	a.integrateMergedFiles(outs, in)
	a.onFreeze(in.FrozenList())
	closeAll = false
	return true, nil
}

func (a *AggregatorV3) MergeLoop(ctx context.Context) error {
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

func (a *AggregatorV3) integrateFiles(sf AggV3StaticFiles, txNumFrom, txNumTo uint64) {
	a.filesMutationLock.Lock()
	defer a.filesMutationLock.Unlock()
	defer a.needSaveFilesListInDB.Store(true)
	defer a.recalcMaxTxNum()

	a.accounts.integrateFiles(sf.accounts, txNumFrom, txNumTo)
	a.storage.integrateFiles(sf.storage, txNumFrom, txNumTo)
	a.code.integrateFiles(sf.code, txNumFrom, txNumTo)
	a.commitment.integrateFiles(sf.commitment, txNumFrom, txNumTo)
	a.logAddrs.integrateFiles(sf.logAddrs, txNumFrom, txNumTo)
	a.logTopics.integrateFiles(sf.logTopics, txNumFrom, txNumTo)
	a.tracesFrom.integrateFiles(sf.tracesFrom, txNumFrom, txNumTo)
	a.tracesTo.integrateFiles(sf.tracesTo, txNumFrom, txNumTo)
}

func (a *AggregatorV3) HasNewFrozenFiles() bool {
	if a == nil {
		return false
	}
	return a.needSaveFilesListInDB.CompareAndSwap(true, false)
}

type flusher interface {
	Flush(ctx context.Context, tx kv.RwTx) error
}

func (ac *AggregatorV3Context) maxTxNumInDomainFiles(cold bool) uint64 {
	return min(
		ac.account.maxTxNumInDomainFiles(cold),
		ac.code.maxTxNumInDomainFiles(cold),
		ac.storage.maxTxNumInDomainFiles(cold),
		ac.commitment.maxTxNumInDomainFiles(cold),
	)
}

func (ac *AggregatorV3Context) CanPrune(tx kv.Tx) bool {
	return ac.somethingToPrune(tx)
}

func (ac *AggregatorV3Context) CanUnwindDomainsToBlockNum(tx kv.Tx) (uint64, error) {
	_, histBlockNumProgress, err := rawdbv3.TxNums.FindBlockNum(tx, ac.CanUnwindDomainsToTxNum())
	return histBlockNumProgress, err
}
func (ac *AggregatorV3Context) CanUnwindDomainsToTxNum() uint64 {
	return ac.maxTxNumInDomainFiles(false)
}
func (ac *AggregatorV3Context) MinUnwindDomainsBlockNum(tx kv.Tx) (uint64, error) {
	_, blockNum, err := rawdbv3.TxNums.FindBlockNum(tx, ac.CanUnwindDomainsToTxNum())
	return blockNum, err
}

func (ac *AggregatorV3Context) CanUnwindBeforeBlockNum(blockNum uint64, tx kv.Tx) (uint64, bool, error) {
	unwindToTxNum, err := rawdbv3.TxNums.Max(tx, blockNum)
	if err != nil {
		return 0, false, err
	}

	// not all blocks have commitment
	//fmt.Printf("CanUnwindBeforeBlockNum: blockNum=%d unwindTo=%d\n", blockNum, unwindToTxNum)
	domains := NewSharedDomains(tx, ac.a.logger)
	defer domains.Close()

	blockNumWithCommitment, _, _, err := domains.LatestCommitmentState(tx, ac.CanUnwindDomainsToTxNum(), unwindToTxNum)
	if err != nil {
		_minBlockNum, _ := ac.MinUnwindDomainsBlockNum(tx)
		return _minBlockNum, false, nil //nolint
	}
	return blockNumWithCommitment, true, nil
}

func (ac *AggregatorV3Context) somethingToPrune(tx kv.Tx) bool {
	if dbg.NoPrune() {
		return false
	}
	return ac.commitment.CanPruneUntil(tx) ||
		ac.account.CanPruneUntil(tx) ||
		ac.code.CanPruneUntil(tx) ||
		ac.storage.CanPruneUntil(tx) ||
		ac.logAddrs.CanPrune(tx) ||
		ac.logTopics.CanPrune(tx) ||
		ac.tracesFrom.CanPrune(tx) ||
		ac.tracesTo.CanPrune(tx)
}

// PruneSmallBatches is not cancellable, it's over when it's over or failed.
// It fills whole timeout with pruning by small batches (of 100 keys) and making some progress
func (ac *AggregatorV3Context) PruneSmallBatches(ctx context.Context, timeout time.Duration, tx kv.RwTx) error {
	started := time.Now()
	localTimeout := time.NewTicker(timeout)
	defer localTimeout.Stop()
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	aggLogEvery := time.NewTicker(600 * time.Second) // to hide specific domain/idx logging
	defer aggLogEvery.Stop()

	const pruneLimit uint64 = 10000

	fullStat := &AggregatorPruneStat{Domains: make(map[string]*DomainPruneStat), Indices: make(map[string]*InvertedIndexPruneStat)}

	for {
		stat, err := ac.Prune(context.Background(), tx, pruneLimit, aggLogEvery)
		if err != nil {
			log.Warn("[snapshots] PruneSmallBatches", "err", err)
			return err
		}
		if stat == nil {
			if fstat := fullStat.String(); fstat != "" {
				log.Info("[snapshots] PruneSmallBatches", "took", time.Since(started).String(), "stat", fstat)
			}
			return nil
		}
		fullStat.Accumulate(stat)

		select {
		case <-logEvery.C:
			ac.a.logger.Info("[snapshots] pruning",
				"until timeout", time.Until(started.Add(timeout)).String(),
				"aggregatedStep", (ac.maxTxNumInDomainFiles(false)-1)/ac.a.StepSize(),
				"stepsRangeInDB", ac.a.StepsRangeInDBAsStr(tx),
				"pruned", fullStat.String(),
			)
		case <-localTimeout.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func (a *AggregatorV3) StepsRangeInDBAsStr(tx kv.Tx) string {
	return strings.Join([]string{
		a.accounts.stepsRangeInDBAsStr(tx),
		a.storage.stepsRangeInDBAsStr(tx),
		a.code.stepsRangeInDBAsStr(tx),
		a.commitment.stepsRangeInDBAsStr(tx),
		a.logAddrs.stepsRangeInDBAsStr(tx),
		a.logTopics.stepsRangeInDBAsStr(tx),
		a.tracesFrom.stepsRangeInDBAsStr(tx),
		a.tracesTo.stepsRangeInDBAsStr(tx),
	}, ", ")
}

type AggregatorPruneStat struct {
	Domains map[string]*DomainPruneStat
	Indices map[string]*InvertedIndexPruneStat
}

func (as *AggregatorPruneStat) String() string {
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
		if _, ok := as.Domains[k]; !ok {
			as.Domains[k] = v
		} else {
			as.Domains[k].Accumulate(v)
		}
	}
	for k, v := range other.Indices {
		if _, ok := as.Indices[k]; !ok {
			as.Indices[k] = v
		} else {
			as.Indices[k].Accumulate(v)
		}
	}
}

func (ac *AggregatorV3Context) Prune(ctx context.Context, tx kv.RwTx, limit uint64, logEvery *time.Ticker) (*AggregatorPruneStat, error) {
	defer mxPruneTookAgg.ObserveDuration(time.Now())

	if limit == 0 {
		limit = uint64(math2.MaxUint64)
	}

	var txFrom, step uint64 // txFrom is always 0 to avoid dangling keys in indices/hist
	txTo := ac.a.minimaxTxNumInFiles.Load()
	if txTo > 0 {
		// txTo is first txNum in next step, has to go 1 tx behind to get correct step number
		step = (txTo - 1) / ac.a.StepSize()
	}

	if txFrom == txTo || !ac.somethingToPrune(tx) {
		return nil, nil
	}

	if logEvery == nil {
		logEvery = time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
	}
	//ac.a.logger.Info("aggregator prune", "step", step,
	//	"txn_range", fmt.Sprintf("[%d,%d)", txFrom, txTo), "limit", limit,
	//	/*"stepsLimit", limit/ac.a.aggregationStep,*/ "stepsRangeInDB", ac.a.StepsRangeInDBAsStr(tx))

	ap, err := ac.account.Prune(ctx, tx, step, txFrom, txTo, limit, logEvery)
	if err != nil {
		return nil, err
	}
	sp, err := ac.storage.Prune(ctx, tx, step, txFrom, txTo, limit, logEvery)
	if err != nil {
		return nil, err
	}
	cp, err := ac.code.Prune(ctx, tx, step, txFrom, txTo, limit, logEvery)
	if err != nil {
		return nil, err
	}
	comps, err := ac.commitment.Prune(ctx, tx, step, txFrom, txTo, limit, logEvery)
	if err != nil {
		return nil, err
	}
	lap, err := ac.logAddrs.Prune(ctx, tx, txFrom, txTo, limit, logEvery, false, nil)
	if err != nil {
		return nil, err
	}
	ltp, err := ac.logTopics.Prune(ctx, tx, txFrom, txTo, limit, logEvery, false, nil)
	if err != nil {
		return nil, err
	}
	tfp, err := ac.tracesFrom.Prune(ctx, tx, txFrom, txTo, limit, logEvery, false, nil)
	if err != nil {
		return nil, err
	}
	ttp, err := ac.tracesTo.Prune(ctx, tx, txFrom, txTo, limit, logEvery, false, nil)
	if err != nil {
		return nil, err
	}
	aggStat := &AggregatorPruneStat{Domains: make(map[string]*DomainPruneStat), Indices: make(map[string]*InvertedIndexPruneStat)}
	aggStat.Domains[ac.account.d.filenameBase] = ap
	aggStat.Domains[ac.storage.d.filenameBase] = sp
	aggStat.Domains[ac.code.d.filenameBase] = cp
	aggStat.Domains[ac.commitment.d.filenameBase] = comps
	aggStat.Indices[ac.logAddrs.ii.filenameBase] = lap
	aggStat.Indices[ac.logTopics.ii.filenameBase] = ltp
	aggStat.Indices[ac.tracesFrom.ii.filenameBase] = tfp
	aggStat.Indices[ac.tracesTo.ii.filenameBase] = ttp

	return aggStat, nil
}

func (ac *AggregatorV3Context) LogStats(tx kv.Tx, tx2block func(endTxNumMinimax uint64) uint64) {
	maxTxNum := ac.maxTxNumInDomainFiles(false)
	if maxTxNum == 0 {
		return
	}

	domainBlockNumProgress := tx2block(maxTxNum)
	str := make([]string, 0, len(ac.account.files))
	for _, item := range ac.account.files {
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
	if len(ac.commitment.files) > 0 {
		lastCommitmentTxNum = ac.commitment.files[len(ac.commitment.files)-1].endTxNum
		lastCommitmentBlockNum = tx2block(lastCommitmentTxNum)
	}
	firstHistoryIndexBlockInDB := tx2block(ac.a.accounts.FirstStepInDB(tx) * ac.a.StepSize())
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	log.Info("[snapshots] History Stat",
		"blocks", fmt.Sprintf("%dk", (domainBlockNumProgress+1)/1000),
		"txs", fmt.Sprintf("%dm", ac.a.minimaxTxNumInFiles.Load()/1_000_000),
		"txNum2blockNum", strings.Join(str, ","),
		"first_history_idx_in_db", firstHistoryIndexBlockInDB,
		"last_comitment_block", lastCommitmentBlockNum,
		"last_comitment_tx_num", lastCommitmentTxNum,
		//"cnt_in_files", strings.Join(str2, ","),
		//"used_files", strings.Join(ac.Files(), ","),
		"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))

}

func (a *AggregatorV3) EndTxNumNoCommitment() uint64 {
	return min(
		a.accounts.endTxNumMinimax(),
		a.storage.endTxNumMinimax(),
		a.code.endTxNumMinimax())
}

func (a *AggregatorV3) EndTxNumMinimax() uint64 { return a.minimaxTxNumInFiles.Load() }
func (a *AggregatorV3) FilesAmount() []int {
	return []int{
		a.accounts.files.Len(),
		a.storage.files.Len(),
		a.code.files.Len(),
		a.commitment.files.Len(),
		a.tracesFrom.files.Len(),
		a.tracesTo.files.Len(),
		a.logAddrs.files.Len(),
		a.logTopics.files.Len(),
	}
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
func (a *AggregatorV3) FirstTxNumOfStep(step uint64) uint64 { // could have some smaller steps to prune// could have some smaller steps to prune
	return FirstTxNumOfStep(step, a.StepSize())
}

func (a *AggregatorV3) EndTxNumDomainsFrozen() uint64 {
	return min(
		a.accounts.endIndexedTxNumMinimax(true),
		a.storage.endIndexedTxNumMinimax(true),
		a.code.endIndexedTxNumMinimax(true),
		a.commitment.endIndexedTxNumMinimax(true),
	)
}

func (a *AggregatorV3) recalcMaxTxNum() {
	min := a.accounts.endTxNumMinimax()
	if txNum := a.storage.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	if txNum := a.code.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	if txNum := a.commitment.endTxNumMinimax(); txNum < min {
		fmt.Printf("[dbg] commitment min: %d, %d\n", txNum/a.aggregationStep, min/a.aggregationStep)
		min = txNum
	}
	if txNum := a.logAddrs.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	if txNum := a.logTopics.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	if txNum := a.tracesFrom.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	if txNum := a.tracesTo.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	a.minimaxTxNumInFiles.Store(min)
}

type RangesV3 struct {
	accounts             DomainRanges
	storage              DomainRanges
	code                 DomainRanges
	commitment           DomainRanges
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
	if r.accounts.any() {
		ss = append(ss, fmt.Sprintf("accounts(%s)", r.accounts.String()))
	}
	if r.storage.any() {
		ss = append(ss, fmt.Sprintf("storage(%s)", r.storage.String()))
	}
	if r.code.any() {
		ss = append(ss, fmt.Sprintf("code(%s)", r.code.String()))
	}
	if r.commitment.any() {
		ss = append(ss, fmt.Sprintf("commitment(%s)", r.commitment.String()))
	}
	if r.logAddrs {
		ss = append(ss, fmt.Sprintf("logAddr=%d-%d", r.logAddrsStartTxNum/r.accounts.aggStep, r.logAddrsEndTxNum/r.accounts.aggStep))
	}
	if r.logTopics {
		ss = append(ss, fmt.Sprintf("logTopic=%d-%d", r.logTopicsStartTxNum/r.accounts.aggStep, r.logTopicsEndTxNum/r.accounts.aggStep))
	}
	if r.tracesFrom {
		ss = append(ss, fmt.Sprintf("traceFrom=%d-%d", r.tracesFromStartTxNum/r.accounts.aggStep, r.tracesFromEndTxNum/r.accounts.aggStep))
	}
	if r.tracesTo {
		ss = append(ss, fmt.Sprintf("traceTo=%d-%d", r.tracesToStartTxNum/r.accounts.aggStep, r.tracesToEndTxNum/r.accounts.aggStep))
	}
	return strings.Join(ss, ", ")
}
func (r RangesV3) any() bool {
	return r.accounts.any() || r.storage.any() || r.code.any() || r.commitment.any() || r.logAddrs || r.logTopics || r.tracesFrom || r.tracesTo
}

func (ac *AggregatorV3Context) findMergeRange(maxEndTxNum, maxSpan uint64) RangesV3 {
	var r RangesV3
	r.accounts = ac.account.findMergeRange(maxEndTxNum, maxSpan)
	r.storage = ac.storage.findMergeRange(maxEndTxNum, maxSpan)
	r.code = ac.code.findMergeRange(maxEndTxNum, maxSpan)
	r.commitment = ac.commitment.findMergeRange(maxEndTxNum, maxSpan)
	r.logAddrs, r.logAddrsStartTxNum, r.logAddrsEndTxNum = ac.logAddrs.findMergeRange(maxEndTxNum, maxSpan)
	r.logTopics, r.logTopicsStartTxNum, r.logTopicsEndTxNum = ac.logTopics.findMergeRange(maxEndTxNum, maxSpan)
	r.tracesFrom, r.tracesFromStartTxNum, r.tracesFromEndTxNum = ac.tracesFrom.findMergeRange(maxEndTxNum, maxSpan)
	r.tracesTo, r.tracesToStartTxNum, r.tracesToEndTxNum = ac.tracesTo.findMergeRange(maxEndTxNum, maxSpan)
	//log.Info(fmt.Sprintf("findMergeRange(%d, %d)=%s\n", maxEndTxNum/ac.a.aggregationStep, maxSpan/ac.a.aggregationStep, r))
	return r
}

type SelectedStaticFilesV3 struct {
	accounts       []*filesItem
	accountsIdx    []*filesItem
	accountsHist   []*filesItem
	storage        []*filesItem
	storageIdx     []*filesItem
	storageHist    []*filesItem
	code           []*filesItem
	codeIdx        []*filesItem
	codeHist       []*filesItem
	commitment     []*filesItem
	commitmentIdx  []*filesItem
	commitmentHist []*filesItem
	logTopics      []*filesItem
	tracesTo       []*filesItem
	tracesFrom     []*filesItem
	logAddrs       []*filesItem
	accountsI      int
	storageI       int
	codeI          int
	commitmentI    int
	logAddrsI      int
	logTopicsI     int
	tracesFromI    int
	tracesToI      int
}

func (sf SelectedStaticFilesV3) Close() {
	clist := [...][]*filesItem{
		sf.accounts, sf.accountsIdx, sf.accountsHist,
		sf.storage, sf.storageIdx, sf.accountsHist,
		sf.code, sf.codeIdx, sf.codeHist,
		sf.commitment, sf.commitmentIdx, sf.commitmentHist,
		sf.logAddrs, sf.logTopics, sf.tracesFrom, sf.tracesTo,
	}
	for _, group := range clist {
		for _, item := range group {
			if item != nil {
				if item.decompressor != nil {
					item.decompressor.Close()
				}
				if item.index != nil {
					item.index.Close()
				}
			}
		}
	}
}

func (ac *AggregatorV3Context) staticFilesInRange(r RangesV3) (sf SelectedStaticFilesV3, err error) {
	if r.accounts.any() {
		sf.accounts, sf.accountsIdx, sf.accountsHist, sf.accountsI = ac.account.staticFilesInRange(r.accounts)
	}
	if r.storage.any() {
		sf.storage, sf.storageIdx, sf.storageHist, sf.storageI = ac.storage.staticFilesInRange(r.storage)
	}
	if r.code.any() {
		sf.code, sf.codeIdx, sf.codeHist, sf.codeI = ac.code.staticFilesInRange(r.code)
	}
	if r.commitment.any() {
		sf.commitment, sf.commitmentIdx, sf.commitmentHist, sf.commitmentI = ac.commitment.staticFilesInRange(r.commitment)
	}
	if r.logAddrs {
		sf.logAddrs, sf.logAddrsI = ac.logAddrs.staticFilesInRange(r.logAddrsStartTxNum, r.logAddrsEndTxNum)
	}
	if r.logTopics {
		sf.logTopics, sf.logTopicsI = ac.logTopics.staticFilesInRange(r.logTopicsStartTxNum, r.logTopicsEndTxNum)
	}
	if r.tracesFrom {
		sf.tracesFrom, sf.tracesFromI = ac.tracesFrom.staticFilesInRange(r.tracesFromStartTxNum, r.tracesFromEndTxNum)
	}
	if r.tracesTo {
		sf.tracesTo, sf.tracesToI = ac.tracesTo.staticFilesInRange(r.tracesToStartTxNum, r.tracesToEndTxNum)
	}
	return sf, err
}

type MergedFilesV3 struct {
	accounts                      *filesItem
	accountsIdx, accountsHist     *filesItem
	storage                       *filesItem
	storageIdx, storageHist       *filesItem
	code                          *filesItem
	codeIdx, codeHist             *filesItem
	commitment                    *filesItem
	commitmentIdx, commitmentHist *filesItem
	logAddrs                      *filesItem
	logTopics                     *filesItem
	tracesFrom                    *filesItem
	tracesTo                      *filesItem
}

func (mf MergedFilesV3) FrozenList() (frozen []string) {
	if mf.accountsHist != nil && mf.accountsHist.frozen {
		frozen = append(frozen, mf.accountsHist.decompressor.FileName())
	}
	if mf.accountsIdx != nil && mf.accountsIdx.frozen {
		frozen = append(frozen, mf.accountsIdx.decompressor.FileName())
	}

	if mf.storageHist != nil && mf.storageHist.frozen {
		frozen = append(frozen, mf.storageHist.decompressor.FileName())
	}
	if mf.storageIdx != nil && mf.storageIdx.frozen {
		frozen = append(frozen, mf.storageIdx.decompressor.FileName())
	}

	if mf.codeHist != nil && mf.codeHist.frozen {
		frozen = append(frozen, mf.codeHist.decompressor.FileName())
	}
	if mf.codeIdx != nil && mf.codeIdx.frozen {
		frozen = append(frozen, mf.codeIdx.decompressor.FileName())
	}

	if mf.logAddrs != nil && mf.logAddrs.frozen {
		frozen = append(frozen, mf.logAddrs.decompressor.FileName())
	}
	if mf.logTopics != nil && mf.logTopics.frozen {
		frozen = append(frozen, mf.logTopics.decompressor.FileName())
	}
	if mf.tracesFrom != nil && mf.tracesFrom.frozen {
		frozen = append(frozen, mf.tracesFrom.decompressor.FileName())
	}
	if mf.tracesTo != nil && mf.tracesTo.frozen {
		frozen = append(frozen, mf.tracesTo.decompressor.FileName())
	}
	return frozen
}
func (mf MergedFilesV3) Close() {
	clist := [...]*filesItem{
		mf.accounts, mf.accountsIdx, mf.accountsHist,
		mf.storage, mf.storageIdx, mf.storageHist,
		mf.code, mf.codeIdx, mf.codeHist,
		mf.commitment, mf.commitmentIdx, mf.commitmentHist,
		mf.logAddrs, mf.logTopics, mf.tracesFrom, mf.tracesTo,
	}

	for _, item := range clist {
		if item != nil {
			if item.decompressor != nil {
				item.decompressor.Close()
			}
			if item.index != nil {
				item.index.Close()
			}
		}
	}
}

func (ac *AggregatorV3Context) mergeFiles(ctx context.Context, files SelectedStaticFilesV3, r RangesV3) (MergedFilesV3, error) {
	var mf MergedFilesV3
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(ac.a.mergeWorkers)
	closeFiles := true
	defer func() {
		if closeFiles {
			mf.Close()
		}
	}()

	if r.accounts.any() {
		log.Info(fmt.Sprintf("[snapshots] merge: %s", r.String()))
		g.Go(func() (err error) {
			mf.accounts, mf.accountsIdx, mf.accountsHist, err = ac.account.mergeFiles(ctx, files.accounts, files.accountsIdx, files.accountsHist, r.accounts, ac.a.ps)
			return err
		})
	}

	if r.storage.any() {
		g.Go(func() (err error) {
			mf.storage, mf.storageIdx, mf.storageHist, err = ac.storage.mergeFiles(ctx, files.storage, files.storageIdx, files.storageHist, r.storage, ac.a.ps)
			return err
		})
	}
	if r.code.any() {
		g.Go(func() (err error) {
			mf.code, mf.codeIdx, mf.codeHist, err = ac.code.mergeFiles(ctx, files.code, files.codeIdx, files.codeHist, r.code, ac.a.ps)
			return err
		})
	}
	if r.commitment.any() {
		//log.Info(fmt.Sprintf("[snapshots] merge commitment: %d-%d", r.accounts.historyStartTxNum/ac.a.aggregationStep, r.accounts.historyEndTxNum/ac.a.aggregationStep))
		g.Go(func() (err error) {
			mf.commitment, mf.commitmentIdx, mf.commitmentHist, err = ac.commitment.mergeFiles(ctx, files.commitment, files.commitmentIdx, files.commitmentHist, r.commitment, ac.a.ps)
			return err
			//var v4Files SelectedStaticFiles
			//var v4MergedF MergedFiles
			//// THIS merge uses strategy with replacement of hisotry keys in commitment.
			//mf.commitment, mf.commitmentIdx, mf.commitmentHist, err = ac.a.commitment.mergeFiles(ctx, v4Files.FillV3(&files), v4MergedF.FillV3(&mf), r.commitment, ac.a.ps)
			//return err
		})
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
	}
	//fmt.Printf("[snapshots] merge done %s\n", r.String())
	return mf, err
}

func (a *AggregatorV3) integrateMergedFiles(outs SelectedStaticFilesV3, in MergedFilesV3) (frozen []string) {
	a.filesMutationLock.Lock()
	defer a.filesMutationLock.Unlock()
	defer a.needSaveFilesListInDB.Store(true)
	defer a.recalcMaxTxNum()

	a.accounts.integrateMergedFiles(outs.accounts, outs.accountsIdx, outs.accountsHist, in.accounts, in.accountsIdx, in.accountsHist)
	a.storage.integrateMergedFiles(outs.storage, outs.storageIdx, outs.storageHist, in.storage, in.storageIdx, in.storageHist)
	a.code.integrateMergedFiles(outs.code, outs.codeIdx, outs.codeHist, in.code, in.codeIdx, in.codeHist)
	a.commitment.integrateMergedFiles(outs.commitment, outs.commitmentIdx, outs.commitmentHist, in.commitment, in.commitmentIdx, in.commitmentHist)
	a.logAddrs.integrateMergedFiles(outs.logAddrs, in.logAddrs)
	a.logTopics.integrateMergedFiles(outs.logTopics, in.logTopics)
	a.tracesFrom.integrateMergedFiles(outs.tracesFrom, in.tracesFrom)
	a.tracesTo.integrateMergedFiles(outs.tracesTo, in.tracesTo)
	a.cleanAfterNewFreeze(in)
	return frozen
}
func (a *AggregatorV3) cleanAfterNewFreeze(in MergedFilesV3) {
	a.accounts.cleanAfterFreeze(in.accounts, in.accountsHist, in.accountsIdx)
	a.storage.cleanAfterFreeze(in.storage, in.storageHist, in.storageIdx)
	a.code.cleanAfterFreeze(in.code, in.codeHist, in.codeIdx)
	a.commitment.cleanAfterFreeze(in.commitment, in.commitmentHist, in.commitmentIdx)
	if in.logAddrs != nil && in.logAddrs.frozen {
		a.logAddrs.cleanAfterFreeze(in.logAddrs.endTxNum)
	}
	if in.logTopics != nil && in.logTopics.frozen {
		a.logTopics.cleanAfterFreeze(in.logTopics.endTxNum)
	}
	if in.tracesFrom != nil && in.tracesFrom.frozen {
		a.tracesFrom.cleanAfterFreeze(in.tracesFrom.endTxNum)
	}
	if in.tracesTo != nil && in.tracesTo.frozen {
		a.tracesTo.cleanAfterFreeze(in.tracesTo.endTxNum)
	}
}

// KeepStepsInDB - usually equal to one a.aggregationStep, but when we exec blocks from snapshots
// we can set it to 0, because no re-org on this blocks are possible
func (a *AggregatorV3) KeepStepsInDB(steps uint64) *AggregatorV3 {
	a.keepInDB = a.FirstTxNumOfStep(steps)
	for _, d := range []*Domain{a.accounts, a.storage, a.code, a.commitment} {
		if d == nil {
			continue
		}
		if d.History.dontProduceFiles {
			d.History.keepTxInDB = a.keepInDB
		}
	}

	return a
}

func (a *AggregatorV3) SetSnapshotBuildSema(semaphore *semaphore.Weighted) {
	a.snapshotBuildSema = semaphore
}

// Returns channel which is closed when aggregation is done
func (a *AggregatorV3) BuildFilesInBackground(txNum uint64) chan struct{} {
	fin := make(chan struct{})

	if (txNum + 1) <= a.minimaxTxNumInFiles.Load()+a.keepInDB {
		close(fin)
		return fin
	}

	if ok := a.buildingFiles.CompareAndSwap(false, true); !ok {
		close(fin)
		return fin
	}

	step := a.minimaxTxNumInFiles.Load() / a.StepSize()
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
		lastInDB := lastIdInDB(a.db, a.accounts)
		hasData := lastInDB > step // `step` must be fully-written - means `step+1` records must be visible
		if !hasData {
			close(fin)
			return
		}

		// trying to create as much small-step-files as possible:
		// - to reduce amount of small merges
		// - to remove old data from db as early as possible
		// - during files build, may happen commit of new data. on each loop step getting latest id in db
		for ; step < lastIdInDB(a.db, a.accounts); step++ { //`step` must be fully-written - means `step+1` records must be visible
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

func (ac *AggregatorV3Context) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int, tx kv.Tx) (timestamps iter.U64, err error) {
	switch name {
	case kv.AccountsHistoryIdx:
		return ac.account.hc.IdxRange(k, fromTs, toTs, asc, limit, tx)
	case kv.StorageHistoryIdx:
		return ac.storage.hc.IdxRange(k, fromTs, toTs, asc, limit, tx)
	case kv.CodeHistoryIdx:
		return ac.code.hc.IdxRange(k, fromTs, toTs, asc, limit, tx)
	case kv.CommitmentHistoryIdx:
		return ac.commitment.hc.IdxRange(k, fromTs, toTs, asc, limit, tx)
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

func (ac *AggregatorV3Context) HistoryGet(name kv.History, key []byte, ts uint64, tx kv.Tx) (v []byte, ok bool, err error) {
	switch name {
	case kv.AccountsHistory:
		v, ok, err = ac.account.hc.GetNoStateWithRecent(key, ts, tx)
		if err != nil {
			return nil, false, err
		}
		if !ok || len(v) == 0 {
			return v, ok, nil
		}
		return v, true, nil
	case kv.StorageHistory:
		return ac.storage.hc.GetNoStateWithRecent(key, ts, tx)
	case kv.CodeHistory:
		return ac.code.hc.GetNoStateWithRecent(key, ts, tx)
	case kv.CommitmentHistory:
		return ac.code.hc.GetNoStateWithRecent(key, ts, tx)
	default:
		panic(fmt.Sprintf("unexpected: %s", name))
	}
}

func (ac *AggregatorV3Context) AccountHistoryRange(startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.KV, error) {
	hr, err := ac.account.hc.HistoryRange(startTxNum, endTxNum, asc, limit, tx)
	if err != nil {
		return nil, err
	}
	return iter.WrapKV(hr), nil
}

func (ac *AggregatorV3Context) StorageHistoryRange(startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.KV, error) {
	hr, err := ac.storage.hc.HistoryRange(startTxNum, endTxNum, asc, limit, tx)
	if err != nil {
		return nil, err
	}
	return iter.WrapKV(hr), nil
}

func (ac *AggregatorV3Context) CodeHistoryRange(startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.KV, error) {
	hr, err := ac.code.hc.HistoryRange(startTxNum, endTxNum, asc, limit, tx)
	if err != nil {
		return nil, err
	}
	return iter.WrapKV(hr), nil
}

type FilesStats22 struct{}

func (a *AggregatorV3) Stats() FilesStats22 {
	var fs FilesStats22
	return fs
}

// AggregatorV3Context guarantee consistent View of files ("snapshots isolation" level https://en.wikipedia.org/wiki/Snapshot_isolation):
//   - long-living consistent view of all files (no limitations)
//   - hiding garbage and files overlaps
//   - protecting useful files from removal
//   - user will not see "partial writes" or "new files appearance"
//   - last reader removing garbage files inside `Close` method
type AggregatorV3Context struct {
	a          *AggregatorV3
	account    *DomainContext
	storage    *DomainContext
	code       *DomainContext
	commitment *DomainContext
	logAddrs   *InvertedIndexContext
	logTopics  *InvertedIndexContext
	tracesFrom *InvertedIndexContext
	tracesTo   *InvertedIndexContext

	id      uint64 // auto-increment id of ctx for logs
	_leakID uint64 // set only if TRACE_AGG=true
}

func (a *AggregatorV3) MakeContext() *AggregatorV3Context {
	ac := &AggregatorV3Context{
		a:          a,
		account:    a.accounts.MakeContext(),
		storage:    a.storage.MakeContext(),
		code:       a.code.MakeContext(),
		commitment: a.commitment.MakeContext(),
		logAddrs:   a.logAddrs.MakeContext(),
		logTopics:  a.logTopics.MakeContext(),
		tracesFrom: a.tracesFrom.MakeContext(),
		tracesTo:   a.tracesTo.MakeContext(),

		id:      a.ctxAutoIncrement.Add(1),
		_leakID: a.leakDetector.Add(),
	}

	return ac
}
func (ac *AggregatorV3Context) ViewID() uint64 { return ac.id }

// --- Domain part START ---

func (ac *AggregatorV3Context) DomainRange(tx kv.Tx, domain kv.Domain, fromKey, toKey []byte, ts uint64, asc order.By, limit int) (it iter.KV, err error) {
	switch domain {
	case kv.AccountsDomain:
		return ac.account.DomainRange(tx, fromKey, toKey, ts, asc, limit)
	case kv.StorageDomain:
		return ac.storage.DomainRange(tx, fromKey, toKey, ts, asc, limit)
	case kv.CodeDomain:
		return ac.code.DomainRange(tx, fromKey, toKey, ts, asc, limit)
	case kv.CommitmentDomain:
		return ac.commitment.DomainRange(tx, fromKey, toKey, ts, asc, limit)
	default:
		panic(domain)
	}
}
func (ac *AggregatorV3Context) DomainRangeLatest(tx kv.Tx, domain kv.Domain, from, to []byte, limit int) (iter.KV, error) {
	switch domain {
	case kv.AccountsDomain:
		return ac.account.DomainRangeLatest(tx, from, to, limit)
	case kv.StorageDomain:
		return ac.storage.DomainRangeLatest(tx, from, to, limit)
	case kv.CodeDomain:
		return ac.code.DomainRangeLatest(tx, from, to, limit)
	case kv.CommitmentDomain:
		return ac.commitment.DomainRangeLatest(tx, from, to, limit)
	default:
		panic(domain)
	}
}

func (ac *AggregatorV3Context) DomainGetAsOf(tx kv.Tx, name kv.Domain, key []byte, ts uint64) (v []byte, ok bool, err error) {
	switch name {
	case kv.AccountsDomain:
		v, err := ac.account.GetAsOf(key, ts, tx)
		return v, v != nil, err
	case kv.StorageDomain:
		v, err := ac.storage.GetAsOf(key, ts, tx)
		return v, v != nil, err
	case kv.CodeDomain:
		v, err := ac.code.GetAsOf(key, ts, tx)
		return v, v != nil, err
	case kv.CommitmentDomain:
		v, err := ac.commitment.GetAsOf(key, ts, tx)
		return v, v != nil, err
	default:
		panic(fmt.Sprintf("unexpected: %s", name))
	}
}
func (ac *AggregatorV3Context) GetLatest(domain kv.Domain, k, k2 []byte, tx kv.Tx) (v []byte, step uint64, ok bool, err error) {
	switch domain {
	case kv.AccountsDomain:
		return ac.account.GetLatest(k, k2, tx)
	case kv.StorageDomain:
		return ac.storage.GetLatest(k, k2, tx)
	case kv.CodeDomain:
		return ac.code.GetLatest(k, k2, tx)
	case kv.CommitmentDomain:
		return ac.commitment.GetLatest(k, k2, tx)
	default:
		panic(fmt.Sprintf("unexpected: %s", domain))
	}
}

// search key in all files of all domains and print file names
func (ac *AggregatorV3Context) DebugKey(domain kv.Domain, k []byte) error {
	switch domain {
	case kv.AccountsDomain:
		l, err := ac.account.DebugKVFilesWithKey(k)
		if err != nil {
			return err
		}
		if len(l) > 0 {
			log.Info("[dbg] found in", "files", l)
		}
	case kv.StorageDomain:
		l, err := ac.code.DebugKVFilesWithKey(k)
		if err != nil {
			return err
		}
		if len(l) > 0 {
			log.Info("[dbg] found in", "files", l)
		}
	case kv.CodeDomain:
		l, err := ac.storage.DebugKVFilesWithKey(k)
		if err != nil {
			return err
		}
		if len(l) > 0 {
			log.Info("[dbg] found in", "files", l)
		}
	case kv.CommitmentDomain:
		l, err := ac.commitment.DebugKVFilesWithKey(k)
		if err != nil {
			return err
		}
		if len(l) > 0 {
			log.Info("[dbg] found in", "files", l)
		}
	default:
		panic(fmt.Sprintf("unexpected: %s", domain))
	}
	return nil
}
func (ac *AggregatorV3Context) DebugEFKey(domain kv.Domain, k []byte) error {
	switch domain {
	case kv.AccountsDomain:
		err := ac.account.DebugEFKey(k)
		if err != nil {
			return err
		}
	case kv.StorageDomain:
		err := ac.code.DebugEFKey(k)
		if err != nil {
			return err
		}
	case kv.CodeDomain:
		err := ac.storage.DebugEFKey(k)
		if err != nil {
			return err
		}
	case kv.CommitmentDomain:
		err := ac.commitment.DebugEFKey(k)
		if err != nil {
			return err
		}
	default:
		panic(fmt.Sprintf("unexpected: %s", domain))
	}
	return nil
}
func (ac *AggregatorV3Context) DebugEFAllValuesAreInRange(ctx context.Context, name kv.InvertedIdx) error {
	switch name {
	case kv.AccountsHistoryIdx:
		err := ac.account.hc.ic.DebugEFAllValuesAreInRange(ctx)
		if err != nil {
			return err
		}
	case kv.StorageHistoryIdx:
		err := ac.code.hc.ic.DebugEFAllValuesAreInRange(ctx)
		if err != nil {
			return err
		}
	case kv.CodeHistoryIdx:
		err := ac.storage.hc.ic.DebugEFAllValuesAreInRange(ctx)
		if err != nil {
			return err
		}
	case kv.CommitmentHistoryIdx:
		err := ac.commitment.hc.ic.DebugEFAllValuesAreInRange(ctx)
		if err != nil {
			return err
		}
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

func (ac *AggregatorV3Context) Close() {
	if ac == nil || ac.a == nil { // invariant: it's safe to call Close multiple times
		return
	}
	ac.a.leakDetector.Del(ac._leakID)
	ac.a = nil

	ac.account.Close()
	ac.storage.Close()
	ac.code.Close()
	ac.commitment.Close()
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
	a          *AggregatorV3
	accounts   *HistoryStep
	storage    *HistoryStep
	code       *HistoryStep
	commitment *HistoryStep
	keyBuf     []byte
}

func (a *AggregatorV3) StepSize() uint64 { return a.aggregationStep }
func (a *AggregatorV3) MakeSteps() ([]*AggregatorStep, error) {
	frozenAndIndexed := a.EndTxNumDomainsFrozen()
	accountSteps := a.accounts.MakeSteps(frozenAndIndexed)
	codeSteps := a.code.MakeSteps(frozenAndIndexed)
	storageSteps := a.storage.MakeSteps(frozenAndIndexed)
	commitmentSteps := a.commitment.MakeSteps(frozenAndIndexed)
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
