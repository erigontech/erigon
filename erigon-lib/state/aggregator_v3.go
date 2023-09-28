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
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"
)

type AggregatorV3 struct {
	rwTx             kv.RwTx
	db               kv.RoDB
	storage          *History
	tracesTo         *InvertedIndex
	backgroundResult *BackgroundResult
	code             *History
	logAddrs         *InvertedIndex
	logTopics        *InvertedIndex
	tracesFrom       *InvertedIndex
	accounts         *History
	logPrefix        string
	dir              string
	tmpdir           string
	aggregationStep  uint64
	keepInDB         uint64

	minimaxTxNumInFiles atomic.Uint64

	filesMutationLock sync.Mutex

	// To keep DB small - need move data to small files ASAP.
	// It means goroutine which creating small files - can't be locked by merge or indexing.
	buildingFiles           atomic.Bool
	mergeingFiles           atomic.Bool
	buildingOptionalIndices atomic.Bool

	//warmupWorking          atomic.Bool
	ctx       context.Context
	ctxCancel context.CancelFunc

	needSaveFilesListInDB atomic.Bool
	wg                    sync.WaitGroup

	onFreeze OnFreezeFunc
	walLock  sync.RWMutex

	ps *background.ProgressSet

	// next fields are set only if agg.doTraceCtx is true. can enable by env: TRACE_AGG=true
	leakDetector *dbg.LeakDetector
	logger       log.Logger
}

type OnFreezeFunc func(frozenFileNames []string)

func NewAggregatorV3(ctx context.Context, dir, tmpdir string, aggregationStep uint64, db kv.RoDB, logger log.Logger) (*AggregatorV3, error) {
	ctx, ctxCancel := context.WithCancel(ctx)
	a := &AggregatorV3{
		ctx:              ctx,
		ctxCancel:        ctxCancel,
		onFreeze:         func(frozenFileNames []string) {},
		dir:              dir,
		tmpdir:           tmpdir,
		aggregationStep:  aggregationStep,
		db:               db,
		keepInDB:         2 * aggregationStep,
		leakDetector:     dbg.NewLeakDetector("agg", dbg.SlowTx()),
		ps:               background.NewProgressSet(),
		backgroundResult: &BackgroundResult{},
		logger:           logger,
	}
	var err error
	if a.accounts, err = NewHistory(dir, a.tmpdir, aggregationStep, "accounts", kv.TblAccountHistoryKeys, kv.TblAccountIdx, kv.TblAccountHistoryVals, false, nil, false, logger); err != nil {
		return nil, err
	}
	if a.storage, err = NewHistory(dir, a.tmpdir, aggregationStep, "storage", kv.TblStorageHistoryKeys, kv.TblStorageIdx, kv.TblStorageHistoryVals, false, nil, false, logger); err != nil {
		return nil, err
	}
	if a.code, err = NewHistory(dir, a.tmpdir, aggregationStep, "code", kv.TblCodeHistoryKeys, kv.TblCodeIdx, kv.TblCodeHistoryVals, true, nil, true, logger); err != nil {
		return nil, err
	}
	if a.logAddrs, err = NewInvertedIndex(dir, a.tmpdir, aggregationStep, "logaddrs", kv.TblLogAddressKeys, kv.TblLogAddressIdx, false, nil, logger); err != nil {
		return nil, err
	}
	if a.logTopics, err = NewInvertedIndex(dir, a.tmpdir, aggregationStep, "logtopics", kv.TblLogTopicsKeys, kv.TblLogTopicsIdx, false, nil, logger); err != nil {
		return nil, err
	}
	if a.tracesFrom, err = NewInvertedIndex(dir, a.tmpdir, aggregationStep, "tracesfrom", kv.TblTracesFromKeys, kv.TblTracesFromIdx, false, nil, logger); err != nil {
		return nil, err
	}
	if a.tracesTo, err = NewInvertedIndex(dir, a.tmpdir, aggregationStep, "tracesto", kv.TblTracesToKeys, kv.TblTracesToIdx, false, nil, logger); err != nil {
		return nil, err
	}
	a.recalcMaxTxNum()

	return a, nil
}
func (a *AggregatorV3) OnFreeze(f OnFreezeFunc) { a.onFreeze = f }

func (a *AggregatorV3) OpenFolder() error {
	a.filesMutationLock.Lock()
	defer a.filesMutationLock.Unlock()
	var err error
	if err = a.accounts.OpenFolder(); err != nil {
		return fmt.Errorf("OpenFolder: %w", err)
	}
	if err = a.storage.OpenFolder(); err != nil {
		return fmt.Errorf("OpenFolder: %w", err)
	}
	if err = a.code.OpenFolder(); err != nil {
		return fmt.Errorf("OpenFolder: %w", err)
	}
	if err = a.logAddrs.OpenFolder(); err != nil {
		return fmt.Errorf("OpenFolder: %w", err)
	}
	if err = a.logTopics.OpenFolder(); err != nil {
		return fmt.Errorf("OpenFolder: %w", err)
	}
	if err = a.tracesFrom.OpenFolder(); err != nil {
		return fmt.Errorf("OpenFolder: %w", err)
	}
	if err = a.tracesTo.OpenFolder(); err != nil {
		return fmt.Errorf("OpenFolder: %w", err)
	}
	a.recalcMaxTxNum()
	return nil
}
func (a *AggregatorV3) OpenList(fNames []string) error {
	a.filesMutationLock.Lock()
	defer a.filesMutationLock.Unlock()

	var err error
	if err = a.accounts.OpenList(fNames); err != nil {
		return err
	}
	if err = a.storage.OpenList(fNames); err != nil {
		return err
	}
	if err = a.code.OpenList(fNames); err != nil {
		return err
	}
	if err = a.logAddrs.OpenList(fNames); err != nil {
		return err
	}
	if err = a.logTopics.OpenList(fNames); err != nil {
		return err
	}
	if err = a.tracesFrom.OpenList(fNames); err != nil {
		return err
	}
	if err = a.tracesTo.OpenList(fNames); err != nil {
		return err
	}
	a.recalcMaxTxNum()
	return nil
}

func (a *AggregatorV3) Close() {
	a.ctxCancel()
	a.wg.Wait()

	a.filesMutationLock.Lock()
	defer a.filesMutationLock.Unlock()

	a.accounts.Close()
	a.storage.Close()
	a.code.Close()
	a.logAddrs.Close()
	a.logTopics.Close()
	a.tracesFrom.Close()
	a.tracesTo.Close()
}

// CleanDir - call it manually on startup of Main application (don't call it from utilities or nother processes)
//   - remove files ignored during opening of aggregator
//   - remove files which marked as deleted but have no readers (usually last reader removing files marked as deleted)
func (a *AggregatorV3) CleanDir() {
	a.accounts.deleteGarbageFiles()
	a.storage.deleteGarbageFiles()
	a.code.deleteGarbageFiles()
	a.logAddrs.deleteGarbageFiles()
	a.logTopics.deleteGarbageFiles()
	a.tracesFrom.deleteGarbageFiles()
	a.tracesTo.deleteGarbageFiles()

	ac := a.MakeContext()
	defer ac.Close()
	ac.a.accounts.cleanAfterFreeze(ac.accounts.frozenTo())
	ac.a.storage.cleanAfterFreeze(ac.storage.frozenTo())
	ac.a.code.cleanAfterFreeze(ac.code.frozenTo())
	ac.a.logAddrs.cleanAfterFreeze(ac.logAddrs.frozenTo())
	ac.a.logTopics.cleanAfterFreeze(ac.logTopics.frozenTo())
	ac.a.tracesFrom.cleanAfterFreeze(ac.tracesFrom.frozenTo())
	ac.a.tracesTo.cleanAfterFreeze(ac.tracesTo.frozenTo())
}

func (a *AggregatorV3) SetWorkers(i int) {
	a.accounts.compressWorkers = i
	a.storage.compressWorkers = i
	a.code.compressWorkers = i
	a.logAddrs.compressWorkers = i
	a.logTopics.compressWorkers = i
	a.tracesFrom.compressWorkers = i
	a.tracesTo.compressWorkers = i
}

func (a *AggregatorV3) HasBackgroundFilesBuild() bool { return a.ps.Has() }
func (a *AggregatorV3) BackgroundProgress() string    { return a.ps.String() }

func (a *AggregatorV3) Files() (res []string) {
	a.filesMutationLock.Lock()
	defer a.filesMutationLock.Unlock()

	res = append(res, a.accounts.Files()...)
	res = append(res, a.storage.Files()...)
	res = append(res, a.code.Files()...)
	res = append(res, a.logAddrs.Files()...)
	res = append(res, a.logTopics.Files()...)
	res = append(res, a.tracesFrom.Files()...)
	res = append(res, a.tracesTo.Files()...)
	return res
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
		if err := aggCtx.BuildOptionalMissedIndices(ctx, workers); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Warn("[snapshots] merge", "err", err)
		}
	}()
}

func (ac *AggregatorV3Context) BuildOptionalMissedIndices(ctx context.Context, workers int) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	if ac.accounts != nil {
		g.Go(func() error { return ac.accounts.BuildOptionalMissedIndices(ctx) })
	}
	if ac.storage != nil {
		g.Go(func() error { return ac.storage.BuildOptionalMissedIndices(ctx) })
	}
	if ac.code != nil {
		g.Go(func() error { return ac.code.BuildOptionalMissedIndices(ctx) })
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
		a.logAddrs.BuildMissedIndices(ctx, g, ps)
		a.logTopics.BuildMissedIndices(ctx, g, ps)
		a.tracesFrom.BuildMissedIndices(ctx, g, ps)
		a.tracesTo.BuildMissedIndices(ctx, g, ps)

		if err := g.Wait(); err != nil {
			return err
		}
		if err := a.OpenFolder(); err != nil {
			return err
		}
	}

	ac := a.MakeContext()
	defer ac.Close()
	return ac.BuildOptionalMissedIndices(ctx, workers)
}

func (a *AggregatorV3) SetLogPrefix(v string) { a.logPrefix = v }

func (a *AggregatorV3) SetTx(tx kv.RwTx) {
	a.rwTx = tx
	a.accounts.SetTx(tx)
	a.storage.SetTx(tx)
	a.code.SetTx(tx)
	a.logAddrs.SetTx(tx)
	a.logTopics.SetTx(tx)
	a.tracesFrom.SetTx(tx)
	a.tracesTo.SetTx(tx)
}

func (a *AggregatorV3) SetTxNum(txNum uint64) {
	a.accounts.SetTxNum(txNum)
	a.storage.SetTxNum(txNum)
	a.code.SetTxNum(txNum)
	a.logAddrs.SetTxNum(txNum)
	a.logTopics.SetTxNum(txNum)
	a.tracesFrom.SetTxNum(txNum)
	a.tracesTo.SetTxNum(txNum)
}

type AggV3Collation struct {
	logAddrs   map[string]*roaring64.Bitmap
	logTopics  map[string]*roaring64.Bitmap
	tracesFrom map[string]*roaring64.Bitmap
	tracesTo   map[string]*roaring64.Bitmap
	accounts   HistoryCollation
	storage    HistoryCollation
	code       HistoryCollation
}

func (c AggV3Collation) Close() {
	c.accounts.Close()
	c.storage.Close()
	c.code.Close()

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

func (a *AggregatorV3) buildFiles(ctx context.Context, step, txFrom, txTo uint64) (AggV3StaticFiles, error) {
	//logEvery := time.NewTicker(60 * time.Second)
	//defer logEvery.Stop()
	//defer func(t time.Time) {
	//	log.Info(fmt.Sprintf("[snapshot] build %d-%d", step, step+1), "took", time.Since(t))
	//}(time.Now())
	var sf AggV3StaticFiles
	var ac AggV3Collation
	closeColl := true
	defer func() {
		if closeColl {
			ac.Close()
		}
	}()
	//var wg sync.WaitGroup
	//wg.Add(7)
	//errCh := make(chan error, 7)
	//go func() {
	//	defer wg.Done()
	var err error
	if err = a.db.View(ctx, func(tx kv.Tx) error {
		ac.accounts, err = a.accounts.collate(step, txFrom, txTo, tx)
		return err
	}); err != nil {
		return sf, err
		//errCh <- err
	}

	if sf.accounts, err = a.accounts.buildFiles(ctx, step, ac.accounts, a.ps); err != nil {
		return sf, err
		//errCh <- err
	}
	//}()
	//
	//go func() {
	//	defer wg.Done()
	//	var err error
	if err = a.db.View(ctx, func(tx kv.Tx) error {
		ac.storage, err = a.storage.collate(step, txFrom, txTo, tx)
		return err
	}); err != nil {
		return sf, err
		//errCh <- err
	}

	if sf.storage, err = a.storage.buildFiles(ctx, step, ac.storage, a.ps); err != nil {
		return sf, err
		//errCh <- err
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if err = a.db.View(ctx, func(tx kv.Tx) error {
		ac.code, err = a.code.collate(step, txFrom, txTo, tx)
		return err
	}); err != nil {
		return sf, err
		//errCh <- err
	}

	if sf.code, err = a.code.buildFiles(ctx, step, ac.code, a.ps); err != nil {
		return sf, err
		//errCh <- err
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if err = a.db.View(ctx, func(tx kv.Tx) error {
		ac.logAddrs, err = a.logAddrs.collate(ctx, txFrom, txTo, tx)
		return err
	}); err != nil {
		return sf, err
		//errCh <- err
	}

	if sf.logAddrs, err = a.logAddrs.buildFiles(ctx, step, ac.logAddrs, a.ps); err != nil {
		return sf, err
		//errCh <- err
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if err = a.db.View(ctx, func(tx kv.Tx) error {
		ac.logTopics, err = a.logTopics.collate(ctx, txFrom, txTo, tx)
		return err
	}); err != nil {
		return sf, err
		//errCh <- err
	}

	if sf.logTopics, err = a.logTopics.buildFiles(ctx, step, ac.logTopics, a.ps); err != nil {
		return sf, err
		//errCh <- err
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if err = a.db.View(ctx, func(tx kv.Tx) error {
		ac.tracesFrom, err = a.tracesFrom.collate(ctx, txFrom, txTo, tx)
		return err
	}); err != nil {
		return sf, err
		//errCh <- err
	}

	if sf.tracesFrom, err = a.tracesFrom.buildFiles(ctx, step, ac.tracesFrom, a.ps); err != nil {
		return sf, err
		//errCh <- err
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if err = a.db.View(ctx, func(tx kv.Tx) error {
		ac.tracesTo, err = a.tracesTo.collate(ctx, txFrom, txTo, tx)
		return err
	}); err != nil {
		return sf, err
		//errCh <- err
	}

	if sf.tracesTo, err = a.tracesTo.buildFiles(ctx, step, ac.tracesTo, a.ps); err != nil {
		return sf, err
		//		errCh <- err
	}
	//}()
	//go func() {
	//	wg.Wait()
	//close(errCh)
	//}()
	//var lastError error
	//for err := range errCh {
	//	if err != nil {
	//		lastError = err
	//	}
	//}
	//if lastError == nil {
	closeColl = false
	//}
	return sf, nil
}

type AggV3StaticFiles struct {
	accounts   HistoryFiles
	storage    HistoryFiles
	code       HistoryFiles
	logAddrs   InvertedFiles
	logTopics  InvertedFiles
	tracesFrom InvertedFiles
	tracesTo   InvertedFiles
}

func (sf AggV3StaticFiles) Close() {
	sf.accounts.Close()
	sf.storage.Close()
	sf.code.Close()
	sf.logAddrs.Close()
	sf.logTopics.Close()
	sf.tracesFrom.Close()
	sf.tracesTo.Close()
}

func (a *AggregatorV3) BuildFiles(toTxNum uint64) (err error) {
	a.BuildFilesInBackground(toTxNum)
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

func (a *AggregatorV3) buildFilesInBackground(ctx context.Context, step uint64) (err error) {
	closeAll := true
	//log.Info("[snapshots] history build", "step", fmt.Sprintf("%d-%d", step, step+1))
	sf, err := a.buildFiles(ctx, step, step*a.aggregationStep, (step+1)*a.aggregationStep)
	if err != nil {
		return err
	}
	defer func() {
		if closeAll {
			sf.Close()
		}
	}()
	a.integrateFiles(sf, step*a.aggregationStep, (step+1)*a.aggregationStep)
	//a.notifyAboutNewSnapshots()

	closeAll = false
	return nil
}

func (a *AggregatorV3) mergeLoopStep(ctx context.Context, workers int) (somethingDone bool, err error) {
	ac := a.MakeContext() // this need, to ensure we do all operations on files in "transaction-style", maybe we will ensure it on type-level in future
	defer ac.Close()

	closeAll := true
	maxSpan := a.aggregationStep * StepsInBiggestFile
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

	in, err := ac.mergeFiles(ctx, outs, r, workers)
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
func (a *AggregatorV3) MergeLoop(ctx context.Context, workers int) error {
	for {
		somethingMerged, err := a.mergeLoopStep(ctx, workers)
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
	a.logAddrs.integrateFiles(sf.logAddrs, txNumFrom, txNumTo)
	a.logTopics.integrateFiles(sf.logTopics, txNumFrom, txNumTo)
	a.tracesFrom.integrateFiles(sf.tracesFrom, txNumFrom, txNumTo)
	a.tracesTo.integrateFiles(sf.tracesTo, txNumFrom, txNumTo)
}

func (a *AggregatorV3) HasNewFrozenFiles() bool {
	return a.needSaveFilesListInDB.CompareAndSwap(true, false)
}

func (a *AggregatorV3) Unwind(ctx context.Context, txUnwindTo uint64) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	if err := a.accounts.prune(ctx, txUnwindTo, math2.MaxUint64, math2.MaxUint64, logEvery); err != nil {
		return err
	}
	if err := a.storage.prune(ctx, txUnwindTo, math2.MaxUint64, math2.MaxUint64, logEvery); err != nil {
		return err
	}
	if err := a.code.prune(ctx, txUnwindTo, math2.MaxUint64, math2.MaxUint64, logEvery); err != nil {
		return err
	}
	if err := a.logAddrs.prune(ctx, txUnwindTo, math2.MaxUint64, math2.MaxUint64, logEvery); err != nil {
		return err
	}
	if err := a.logTopics.prune(ctx, txUnwindTo, math2.MaxUint64, math2.MaxUint64, logEvery); err != nil {
		return err
	}
	if err := a.tracesFrom.prune(ctx, txUnwindTo, math2.MaxUint64, math2.MaxUint64, logEvery); err != nil {
		return err
	}
	if err := a.tracesTo.prune(ctx, txUnwindTo, math2.MaxUint64, math2.MaxUint64, logEvery); err != nil {
		return err
	}
	return nil
}

func (a *AggregatorV3) Warmup(ctx context.Context, txFrom, limit uint64) error {
	if a.db == nil {
		return nil
	}
	e, ctx := errgroup.WithContext(ctx)
	e.Go(func() error {
		return a.db.View(ctx, func(tx kv.Tx) error { return a.accounts.warmup(ctx, txFrom, limit, tx) })
	})
	e.Go(func() error {
		return a.db.View(ctx, func(tx kv.Tx) error { return a.storage.warmup(ctx, txFrom, limit, tx) })
	})
	e.Go(func() error {
		return a.db.View(ctx, func(tx kv.Tx) error { return a.code.warmup(ctx, txFrom, limit, tx) })
	})
	e.Go(func() error {
		return a.db.View(ctx, func(tx kv.Tx) error { return a.logAddrs.warmup(ctx, txFrom, limit, tx) })
	})
	e.Go(func() error {
		return a.db.View(ctx, func(tx kv.Tx) error { return a.logTopics.warmup(ctx, txFrom, limit, tx) })
	})
	e.Go(func() error {
		return a.db.View(ctx, func(tx kv.Tx) error { return a.tracesFrom.warmup(ctx, txFrom, limit, tx) })
	})
	e.Go(func() error {
		return a.db.View(ctx, func(tx kv.Tx) error { return a.tracesTo.warmup(ctx, txFrom, limit, tx) })
	})
	return e.Wait()
}

// StartWrites - pattern: `defer agg.StartWrites().FinishWrites()`
func (a *AggregatorV3) DiscardHistory() *AggregatorV3 {
	a.accounts.DiscardHistory()
	a.storage.DiscardHistory()
	a.code.DiscardHistory()
	a.logAddrs.DiscardHistory(a.tmpdir)
	a.logTopics.DiscardHistory(a.tmpdir)
	a.tracesFrom.DiscardHistory(a.tmpdir)
	a.tracesTo.DiscardHistory(a.tmpdir)
	return a
}

// StartWrites - pattern: `defer agg.StartWrites().FinishWrites()`
func (a *AggregatorV3) StartWrites() *AggregatorV3 {
	a.walLock.Lock()
	defer a.walLock.Unlock()
	a.accounts.StartWrites()
	a.storage.StartWrites()
	a.code.StartWrites()
	a.logAddrs.StartWrites()
	a.logTopics.StartWrites()
	a.tracesFrom.StartWrites()
	a.tracesTo.StartWrites()
	return a
}
func (a *AggregatorV3) StartUnbufferedWrites() *AggregatorV3 {
	a.walLock.Lock()
	defer a.walLock.Unlock()
	a.accounts.StartWrites()
	a.storage.StartWrites()
	a.code.StartWrites()
	a.logAddrs.StartWrites()
	a.logTopics.StartWrites()
	a.tracesFrom.StartWrites()
	a.tracesTo.StartWrites()
	return a
}
func (a *AggregatorV3) FinishWrites() {
	a.walLock.Lock()
	defer a.walLock.Unlock()
	a.accounts.FinishWrites()
	a.storage.FinishWrites()
	a.code.FinishWrites()
	a.logAddrs.FinishWrites()
	a.logTopics.FinishWrites()
	a.tracesFrom.FinishWrites()
	a.tracesTo.FinishWrites()
}

type flusher interface {
	Flush(ctx context.Context, tx kv.RwTx) error
}

func (a *AggregatorV3) rotate() []flusher {
	a.walLock.Lock()
	defer a.walLock.Unlock()
	return []flusher{
		a.accounts.Rotate(),
		a.storage.Rotate(),
		a.code.Rotate(),
		a.logAddrs.Rotate(),
		a.logTopics.Rotate(),
		a.tracesFrom.Rotate(),
		a.tracesTo.Rotate(),
	}
}
func (a *AggregatorV3) Flush(ctx context.Context, tx kv.RwTx) error {
	flushers := a.rotate()
	defer func(t time.Time) { log.Debug("[snapshots] history flush", "took", time.Since(t)) }(time.Now())
	for _, f := range flushers {
		if err := f.Flush(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}

func (a *AggregatorV3) CanPrune(tx kv.Tx) bool {
	return a.CanPruneFrom(tx) < a.minimaxTxNumInFiles.Load()
}
func (a *AggregatorV3) CanPruneFrom(tx kv.Tx) uint64 {
	fst, _ := kv.FirstKey(tx, kv.TblTracesToKeys)
	fst2, _ := kv.FirstKey(tx, kv.TblStorageHistoryKeys)
	if len(fst) > 0 && len(fst2) > 0 {
		fstInDb := binary.BigEndian.Uint64(fst)
		fstInDb2 := binary.BigEndian.Uint64(fst2)
		return cmp.Min(fstInDb, fstInDb2)
	}
	return math2.MaxUint64
}

func (a *AggregatorV3) PruneWithTiemout(ctx context.Context, timeout time.Duration) error {
	t := time.Now()
	for a.CanPrune(a.rwTx) && time.Since(t) < timeout {
		if err := a.Prune(ctx, 1_000); err != nil { // prune part of retired data, before commit
			return err
		}
	}
	return nil
}

func (a *AggregatorV3) StepsRangeInDBAsStr(tx kv.Tx) string {
	return strings.Join([]string{
		a.accounts.stepsRangeInDBAsStr(tx),
		a.storage.stepsRangeInDBAsStr(tx),
		a.code.stepsRangeInDBAsStr(tx),
		a.logAddrs.stepsRangeInDBAsStr(tx),
		a.logTopics.stepsRangeInDBAsStr(tx),
		a.tracesFrom.stepsRangeInDBAsStr(tx),
		a.tracesTo.stepsRangeInDBAsStr(tx),
	}, ", ")
}

func (a *AggregatorV3) Prune(ctx context.Context, limit uint64) error {
	//if limit/a.aggregationStep > StepsInBiggestFile {
	//	ctx, cancel := context.WithCancel(ctx)
	//	defer cancel()
	//
	//	a.wg.Add(1)
	//	go func() {
	//		defer a.wg.Done()
	//		_ = a.Warmup(ctx, 0, cmp.Max(a.aggregationStep, limit)) // warmup is asyn and moving faster than data deletion
	//	}()
	//}
	return a.prune(ctx, 0, a.minimaxTxNumInFiles.Load(), limit)
}

func (a *AggregatorV3) prune(ctx context.Context, txFrom, txTo, limit uint64) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	if err := a.accounts.prune(ctx, txFrom, txTo, limit, logEvery); err != nil {
		return err
	}
	if err := a.storage.prune(ctx, txFrom, txTo, limit, logEvery); err != nil {
		return err
	}
	if err := a.code.prune(ctx, txFrom, txTo, limit, logEvery); err != nil {
		return err
	}
	if err := a.logAddrs.prune(ctx, txFrom, txTo, limit, logEvery); err != nil {
		return err
	}
	if err := a.logTopics.prune(ctx, txFrom, txTo, limit, logEvery); err != nil {
		return err
	}
	if err := a.tracesFrom.prune(ctx, txFrom, txTo, limit, logEvery); err != nil {
		return err
	}
	if err := a.tracesTo.prune(ctx, txFrom, txTo, limit, logEvery); err != nil {
		return err
	}
	return nil
}

func (a *AggregatorV3) LogStats(tx kv.Tx, tx2block func(endTxNumMinimax uint64) uint64) {
	if a.minimaxTxNumInFiles.Load() == 0 {
		return
	}
	histBlockNumProgress := tx2block(a.minimaxTxNumInFiles.Load())
	str := make([]string, 0, a.accounts.InvertedIndex.files.Len())
	a.accounts.InvertedIndex.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			bn := tx2block(item.endTxNum)
			str = append(str, fmt.Sprintf("%d=%dK", item.endTxNum/a.aggregationStep, bn/1_000))
		}
		return true
	})

	c, err := tx.CursorDupSort(a.accounts.InvertedIndex.indexTable)
	if err != nil {
		// TODO pass error properly around
		panic(err)
	}
	_, v, err := c.First()
	if err != nil {
		// TODO pass error properly around
		panic(err)
	}
	var firstHistoryIndexBlockInDB uint64
	if len(v) != 0 {
		firstHistoryIndexBlockInDB = tx2block(binary.BigEndian.Uint64(v))
	}

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	log.Info("[snapshots] History Stat",
		"blocks", fmt.Sprintf("%dk", (histBlockNumProgress+1)/1000),
		"txs", fmt.Sprintf("%dm", a.minimaxTxNumInFiles.Load()/1_000_000),
		"txNum2blockNum", strings.Join(str, ","),
		"first_history_idx_in_db", firstHistoryIndexBlockInDB,
		"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
}

func (a *AggregatorV3) EndTxNumMinimax() uint64 { return a.minimaxTxNumInFiles.Load() }
func (a *AggregatorV3) EndTxNumFrozenAndIndexed() uint64 {
	return cmp.Min(
		cmp.Min(
			a.accounts.endIndexedTxNumMinimax(true),
			a.storage.endIndexedTxNumMinimax(true),
		),
		a.code.endIndexedTxNumMinimax(true),
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
	accounts             HistoryRanges
	storage              HistoryRanges
	code                 HistoryRanges
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

func (r RangesV3) any() bool {
	return r.accounts.any() || r.storage.any() || r.code.any() || r.logAddrs || r.logTopics || r.tracesFrom || r.tracesTo
}

func (ac *AggregatorV3Context) findMergeRange(maxEndTxNum, maxSpan uint64) RangesV3 {
	var r RangesV3
	r.accounts = ac.a.accounts.findMergeRange(maxEndTxNum, maxSpan)
	r.storage = ac.a.storage.findMergeRange(maxEndTxNum, maxSpan)
	r.code = ac.a.code.findMergeRange(maxEndTxNum, maxSpan)
	r.logAddrs, r.logAddrsStartTxNum, r.logAddrsEndTxNum = ac.a.logAddrs.findMergeRange(maxEndTxNum, maxSpan)
	r.logTopics, r.logTopicsStartTxNum, r.logTopicsEndTxNum = ac.a.logTopics.findMergeRange(maxEndTxNum, maxSpan)
	r.tracesFrom, r.tracesFromStartTxNum, r.tracesFromEndTxNum = ac.a.tracesFrom.findMergeRange(maxEndTxNum, maxSpan)
	r.tracesTo, r.tracesToStartTxNum, r.tracesToEndTxNum = ac.a.tracesTo.findMergeRange(maxEndTxNum, maxSpan)
	//log.Info(fmt.Sprintf("findMergeRange(%d, %d)=%+v\n", maxEndTxNum, maxSpan, r))
	return r
}

type SelectedStaticFilesV3 struct {
	logTopics    []*filesItem
	accountsHist []*filesItem
	tracesTo     []*filesItem
	storageIdx   []*filesItem
	storageHist  []*filesItem
	tracesFrom   []*filesItem
	codeIdx      []*filesItem
	codeHist     []*filesItem
	accountsIdx  []*filesItem
	logAddrs     []*filesItem
	codeI        int
	logAddrsI    int
	logTopicsI   int
	storageI     int
	tracesFromI  int
	accountsI    int
	tracesToI    int
}

func (sf SelectedStaticFilesV3) Close() {
	for _, group := range [][]*filesItem{sf.accountsIdx, sf.accountsHist, sf.storageIdx, sf.accountsHist, sf.codeIdx, sf.codeHist,
		sf.logAddrs, sf.logTopics, sf.tracesFrom, sf.tracesTo} {
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
		sf.accountsIdx, sf.accountsHist, sf.accountsI, err = ac.accounts.staticFilesInRange(r.accounts)
		if err != nil {
			return sf, err
		}
	}
	if r.storage.any() {
		sf.storageIdx, sf.storageHist, sf.storageI, err = ac.storage.staticFilesInRange(r.storage)
		if err != nil {
			return sf, err
		}
	}
	if r.code.any() {
		sf.codeIdx, sf.codeHist, sf.codeI, err = ac.code.staticFilesInRange(r.code)
		if err != nil {
			return sf, err
		}
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
	accountsIdx, accountsHist *filesItem
	storageIdx, storageHist   *filesItem
	codeIdx, codeHist         *filesItem
	logAddrs                  *filesItem
	logTopics                 *filesItem
	tracesFrom                *filesItem
	tracesTo                  *filesItem
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
	for _, item := range []*filesItem{mf.accountsIdx, mf.accountsHist, mf.storageIdx, mf.storageHist, mf.codeIdx, mf.codeHist,
		mf.logAddrs, mf.logTopics, mf.tracesFrom, mf.tracesTo} {
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

func (ac *AggregatorV3Context) mergeFiles(ctx context.Context, files SelectedStaticFilesV3, r RangesV3, workers int) (MergedFilesV3, error) {
	var mf MergedFilesV3
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	closeFiles := true
	defer func() {
		if closeFiles {
			mf.Close()
		}
	}()
	if r.accounts.any() {
		g.Go(func() error {
			var err error
			mf.accountsIdx, mf.accountsHist, err = ac.a.accounts.mergeFiles(ctx, files.accountsIdx, files.accountsHist, r.accounts, workers, ac.a.ps)
			return err
		})
	}

	if r.storage.any() {
		g.Go(func() error {
			var err error
			mf.storageIdx, mf.storageHist, err = ac.a.storage.mergeFiles(ctx, files.storageIdx, files.storageHist, r.storage, workers, ac.a.ps)
			return err
		})
	}
	if r.code.any() {
		g.Go(func() error {
			var err error
			mf.codeIdx, mf.codeHist, err = ac.a.code.mergeFiles(ctx, files.codeIdx, files.codeHist, r.code, workers, ac.a.ps)
			return err
		})
	}
	if r.logAddrs {
		g.Go(func() error {
			var err error
			mf.logAddrs, err = ac.a.logAddrs.mergeFiles(ctx, files.logAddrs, r.logAddrsStartTxNum, r.logAddrsEndTxNum, workers, ac.a.ps)
			return err
		})
	}
	if r.logTopics {
		g.Go(func() error {
			var err error
			mf.logTopics, err = ac.a.logTopics.mergeFiles(ctx, files.logTopics, r.logTopicsStartTxNum, r.logTopicsEndTxNum, workers, ac.a.ps)
			return err
		})
	}
	if r.tracesFrom {
		g.Go(func() error {
			var err error
			mf.tracesFrom, err = ac.a.tracesFrom.mergeFiles(ctx, files.tracesFrom, r.tracesFromStartTxNum, r.tracesFromEndTxNum, workers, ac.a.ps)
			return err
		})
	}
	if r.tracesTo {
		g.Go(func() error {
			var err error
			mf.tracesTo, err = ac.a.tracesTo.mergeFiles(ctx, files.tracesTo, r.tracesToStartTxNum, r.tracesToEndTxNum, workers, ac.a.ps)
			return err
		})
	}
	err := g.Wait()
	if err == nil {
		closeFiles = false
	}
	return mf, err
}

func (a *AggregatorV3) integrateMergedFiles(outs SelectedStaticFilesV3, in MergedFilesV3) (frozen []string) {
	a.filesMutationLock.Lock()
	defer a.filesMutationLock.Unlock()
	defer a.needSaveFilesListInDB.Store(true)
	defer a.recalcMaxTxNum()
	a.accounts.integrateMergedFiles(outs.accountsIdx, outs.accountsHist, in.accountsIdx, in.accountsHist)
	a.storage.integrateMergedFiles(outs.storageIdx, outs.storageHist, in.storageIdx, in.storageHist)
	a.code.integrateMergedFiles(outs.codeIdx, outs.codeHist, in.codeIdx, in.codeHist)
	a.logAddrs.integrateMergedFiles(outs.logAddrs, in.logAddrs)
	a.logTopics.integrateMergedFiles(outs.logTopics, in.logTopics)
	a.tracesFrom.integrateMergedFiles(outs.tracesFrom, in.tracesFrom)
	a.tracesTo.integrateMergedFiles(outs.tracesTo, in.tracesTo)
	a.cleanAfterNewFreeze(in)
	return frozen
}
func (a *AggregatorV3) cleanAfterNewFreeze(in MergedFilesV3) {
	if in.accountsHist != nil && in.accountsHist.frozen {
		a.accounts.cleanAfterFreeze(in.accountsHist.endTxNum)
	}
	if in.storageHist != nil && in.storageHist.frozen {
		a.storage.cleanAfterFreeze(in.storageHist.endTxNum)
	}
	if in.codeHist != nil && in.codeHist.frozen {
		a.code.cleanAfterFreeze(in.codeHist.endTxNum)
	}
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

// KeepInDB - usually equal to one a.aggregationStep, but when we exec blocks from snapshots
// we can set it to 0, because no re-org on this blocks are possible
func (a *AggregatorV3) KeepInDB(v uint64) { a.keepInDB = v }

func (a *AggregatorV3) BuildFilesInBackground(txNum uint64) {
	if (txNum + 1) <= a.minimaxTxNumInFiles.Load()+a.aggregationStep+a.keepInDB { // Leave one step worth in the DB
		return
	}

	if ok := a.buildingFiles.CompareAndSwap(false, true); !ok {
		return
	}

	step := a.minimaxTxNumInFiles.Load() / a.aggregationStep
	toTxNum := (step + 1) * a.aggregationStep
	hasData := false
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer a.buildingFiles.Store(false)

		// check if db has enough data (maybe we didn't commit them yet)
		lastInDB := lastIdInDB(a.db, a.accounts.indexKeysTable)
		hasData = lastInDB >= toTxNum
		if !hasData {
			return
		}

		// trying to create as much small-step-files as possible:
		// - to reduce amount of small merges
		// - to remove old data from db as early as possible
		// - during files build, may happen commit of new data. on each loop step getting latest id in db
		for step < lastIdInDB(a.db, a.accounts.indexKeysTable)/a.aggregationStep {
			if err := a.buildFilesInBackground(a.ctx, step); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Warn("[snapshots] buildFilesInBackground", "err", err)
				break
			}
			step++
		}

		if ok := a.mergeingFiles.CompareAndSwap(false, true); !ok {
			return
		}
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			defer a.mergeingFiles.Store(false)
			if err := a.MergeLoop(a.ctx, 1); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Warn("[snapshots] merge", "err", err)
			}

			a.BuildOptionalMissedIndicesInBackground(a.ctx, 1)
		}()
	}()
}

func (a *AggregatorV3) BatchHistoryWriteStart() *AggregatorV3 {
	a.walLock.RLock()
	return a
}
func (a *AggregatorV3) BatchHistoryWriteEnd() {
	a.walLock.RUnlock()
}

func (a *AggregatorV3) AddAccountPrev(addr []byte, prev []byte) error {
	return a.accounts.AddPrevValue(addr, nil, prev)
}

func (a *AggregatorV3) AddStoragePrev(addr []byte, loc []byte, prev []byte) error {
	return a.storage.AddPrevValue(addr, loc, prev)
}

// AddCodePrev - addr+inc => code
func (a *AggregatorV3) AddCodePrev(addr []byte, prev []byte) error {
	return a.code.AddPrevValue(addr, nil, prev)
}

// nolint
func (a *AggregatorV3) PutIdx(idx kv.InvertedIdx, key []byte) error {
	switch idx {
	case kv.TblTracesFromIdx:
		return a.tracesFrom.Add(key)
	case kv.TblTracesToIdx:
		return a.tracesTo.Add(key)
	case kv.TblLogAddressIdx:
		return a.logAddrs.Add(key)
	case kv.LogTopicIndex:
		return a.logTopics.Add(key)
	default:
		panic(idx)
	}
}

// DisableReadAhead - usage: `defer d.EnableReadAhead().DisableReadAhead()`. Please don't use this funcs without `defer` to avoid leak.
func (a *AggregatorV3) DisableReadAhead() {
	a.accounts.DisableReadAhead()
	a.storage.DisableReadAhead()
	a.code.DisableReadAhead()
	a.logAddrs.DisableReadAhead()
	a.logTopics.DisableReadAhead()
	a.tracesFrom.DisableReadAhead()
	a.tracesTo.DisableReadAhead()
}
func (a *AggregatorV3) EnableReadAhead() *AggregatorV3 {
	a.accounts.EnableReadAhead()
	a.storage.EnableReadAhead()
	a.code.EnableReadAhead()
	a.logAddrs.EnableReadAhead()
	a.logTopics.EnableReadAhead()
	a.tracesFrom.EnableReadAhead()
	a.tracesTo.EnableReadAhead()
	return a
}
func (a *AggregatorV3) EnableMadvWillNeed() *AggregatorV3 {
	a.accounts.EnableMadvWillNeed()
	a.storage.EnableMadvWillNeed()
	a.code.EnableMadvWillNeed()
	a.logAddrs.EnableMadvWillNeed()
	a.logTopics.EnableMadvWillNeed()
	a.tracesFrom.EnableMadvWillNeed()
	a.tracesTo.EnableMadvWillNeed()
	return a
}
func (a *AggregatorV3) EnableMadvNormal() *AggregatorV3 {
	a.accounts.EnableMadvNormalReadAhead()
	a.storage.EnableMadvNormalReadAhead()
	a.code.EnableMadvNormalReadAhead()
	a.logAddrs.EnableMadvNormalReadAhead()
	a.logTopics.EnableMadvNormalReadAhead()
	a.tracesFrom.EnableMadvNormalReadAhead()
	a.tracesTo.EnableMadvNormalReadAhead()
	return a
}

func (ac *AggregatorV3Context) IndexRange(name kv.InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int, tx kv.Tx) (timestamps iter.U64, err error) {
	switch name {
	case kv.AccountsHistoryIdx:
		return ac.accounts.IdxRange(k, fromTs, toTs, asc, limit, tx)
	case kv.StorageHistoryIdx:
		return ac.storage.IdxRange(k, fromTs, toTs, asc, limit, tx)
	case kv.CodeHistoryIdx:
		return ac.code.IdxRange(k, fromTs, toTs, asc, limit, tx)
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

func (ac *AggregatorV3Context) ReadAccountDataNoStateWithRecent(addr []byte, txNum uint64, tx kv.Tx) ([]byte, bool, error) {
	return ac.accounts.GetNoStateWithRecent(addr, txNum, tx)
}

func (ac *AggregatorV3Context) ReadAccountDataNoState(addr []byte, txNum uint64) ([]byte, bool, error) {
	return ac.accounts.GetNoState(addr, txNum)
}

func (ac *AggregatorV3Context) ReadAccountStorageNoStateWithRecent(addr []byte, loc []byte, txNum uint64, tx kv.Tx) ([]byte, bool, error) {
	if cap(ac.keyBuf) < len(addr)+len(loc) {
		ac.keyBuf = make([]byte, len(addr)+len(loc))
	} else if len(ac.keyBuf) != len(addr)+len(loc) {
		ac.keyBuf = ac.keyBuf[:len(addr)+len(loc)]
	}
	copy(ac.keyBuf, addr)
	copy(ac.keyBuf[len(addr):], loc)
	return ac.storage.GetNoStateWithRecent(ac.keyBuf, txNum, tx)
}
func (ac *AggregatorV3Context) ReadAccountStorageNoStateWithRecent2(key []byte, txNum uint64, tx kv.Tx) ([]byte, bool, error) {
	return ac.storage.GetNoStateWithRecent(key, txNum, tx)
}

func (ac *AggregatorV3Context) ReadAccountStorageNoState(addr []byte, loc []byte, txNum uint64) ([]byte, bool, error) {
	if cap(ac.keyBuf) < len(addr)+len(loc) {
		ac.keyBuf = make([]byte, len(addr)+len(loc))
	} else if len(ac.keyBuf) != len(addr)+len(loc) {
		ac.keyBuf = ac.keyBuf[:len(addr)+len(loc)]
	}
	copy(ac.keyBuf, addr)
	copy(ac.keyBuf[len(addr):], loc)
	return ac.storage.GetNoState(ac.keyBuf, txNum)
}

func (ac *AggregatorV3Context) ReadAccountCodeNoStateWithRecent(addr []byte, txNum uint64, tx kv.Tx) ([]byte, bool, error) {
	return ac.code.GetNoStateWithRecent(addr, txNum, tx)
}
func (ac *AggregatorV3Context) ReadAccountCodeNoState(addr []byte, txNum uint64) ([]byte, bool, error) {
	return ac.code.GetNoState(addr, txNum)
}

func (ac *AggregatorV3Context) ReadAccountCodeSizeNoStateWithRecent(addr []byte, txNum uint64, tx kv.Tx) (int, bool, error) {
	code, noState, err := ac.code.GetNoStateWithRecent(addr, txNum, tx)
	if err != nil {
		return 0, false, err
	}
	return len(code), noState, nil
}
func (ac *AggregatorV3Context) ReadAccountCodeSizeNoState(addr []byte, txNum uint64) (int, bool, error) {
	code, noState, err := ac.code.GetNoState(addr, txNum)
	if err != nil {
		return 0, false, err
	}
	return len(code), noState, nil
}

func (ac *AggregatorV3Context) AccountHistoryRange(startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.KV, error) {
	return ac.accounts.HistoryRange(startTxNum, endTxNum, asc, limit, tx)
}

func (ac *AggregatorV3Context) StorageHistoryRange(startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.KV, error) {
	return ac.storage.HistoryRange(startTxNum, endTxNum, asc, limit, tx)
}

func (ac *AggregatorV3Context) CodeHistoryRange(startTxNum, endTxNum int, asc order.By, limit int, tx kv.Tx) (iter.KV, error) {
	return ac.code.HistoryRange(startTxNum, endTxNum, asc, limit, tx)
}

func (ac *AggregatorV3Context) AccountHistoricalStateRange(startTxNum uint64, from, to []byte, limit int, tx kv.Tx) iter.KV {
	return ac.accounts.WalkAsOf(startTxNum, from, to, tx, limit)
}

func (ac *AggregatorV3Context) StorageHistoricalStateRange(startTxNum uint64, from, to []byte, limit int, tx kv.Tx) iter.KV {
	return ac.storage.WalkAsOf(startTxNum, from, to, tx, limit)
}

func (ac *AggregatorV3Context) CodeHistoricalStateRange(startTxNum uint64, from, to []byte, limit int, tx kv.Tx) iter.KV {
	return ac.code.WalkAsOf(startTxNum, from, to, tx, limit)
}

type FilesStats22 struct {
}

func (a *AggregatorV3) Stats() FilesStats22 {
	var fs FilesStats22
	return fs
}

type AggregatorV3Context struct {
	a          *AggregatorV3
	accounts   *HistoryContext
	storage    *HistoryContext
	code       *HistoryContext
	logAddrs   *InvertedIndexContext
	logTopics  *InvertedIndexContext
	tracesFrom *InvertedIndexContext
	tracesTo   *InvertedIndexContext
	keyBuf     []byte

	id uint64 // set only if TRACE_AGG=true
}

func (a *AggregatorV3) MakeContext() *AggregatorV3Context {
	ac := &AggregatorV3Context{
		a:          a,
		accounts:   a.accounts.MakeContext(),
		storage:    a.storage.MakeContext(),
		code:       a.code.MakeContext(),
		logAddrs:   a.logAddrs.MakeContext(),
		logTopics:  a.logTopics.MakeContext(),
		tracesFrom: a.tracesFrom.MakeContext(),
		tracesTo:   a.tracesTo.MakeContext(),

		id: a.leakDetector.Add(),
	}

	return ac
}
func (ac *AggregatorV3Context) Close() {
	ac.a.leakDetector.Del(ac.id)
	ac.accounts.Close()
	ac.storage.Close()
	ac.code.Close()
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

func lastIdInDB(db kv.RoDB, table string) (lstInDb uint64) {
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		lst, _ := kv.LastKey(tx, table)
		if len(lst) > 0 {
			lstInDb = binary.BigEndian.Uint64(lst)
		}
		return nil
	}); err != nil {
		log.Warn("[snapshots] lastIdInDB", "err", err)
	}
	return lstInDb
}

// AggregatorStep is used for incremental reconstitution, it allows
// accessing history in isolated way for each step
type AggregatorStep struct {
	a        *AggregatorV3
	accounts *HistoryStep
	storage  *HistoryStep
	code     *HistoryStep
	keyBuf   []byte
}

func (a *AggregatorV3) MakeSteps() ([]*AggregatorStep, error) {
	frozenAndIndexed := a.EndTxNumFrozenAndIndexed()
	accountSteps := a.accounts.MakeSteps(frozenAndIndexed)
	codeSteps := a.code.MakeSteps(frozenAndIndexed)
	storageSteps := a.storage.MakeSteps(frozenAndIndexed)
	if len(accountSteps) != len(storageSteps) || len(storageSteps) != len(codeSteps) {
		return nil, fmt.Errorf("different limit of steps (try merge snapshots): accountSteps=%d, storageSteps=%d, codeSteps=%d", len(accountSteps), len(storageSteps), len(codeSteps))
	}
	steps := make([]*AggregatorStep, len(accountSteps))
	for i, accountStep := range accountSteps {
		steps[i] = &AggregatorStep{
			a:        a,
			accounts: accountStep,
			storage:  storageSteps[i],
			code:     codeSteps[i],
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
