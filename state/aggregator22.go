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
	"fmt"
	math2 "math"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
)

type Aggregator22 struct {
	dir             string
	aggregationStep uint64
	accounts        *History
	storage         *History
	code            *History
	logAddrs        *InvertedIndex
	logTopics       *InvertedIndex
	tracesFrom      *InvertedIndex
	tracesTo        *InvertedIndex
	txNum           uint64
	logPrefix       string
	rwTx            kv.RwTx
	maxTxNum        uint64
}

func NewAggregator22(dir string, aggregationStep uint64) (*Aggregator22, error) {
	return &Aggregator22{dir: dir, aggregationStep: aggregationStep}, nil
}

func (a *Aggregator22) ReopenFiles() error {
	dir := a.dir
	aggregationStep := a.aggregationStep
	var err error
	if a.accounts, err = NewHistory(dir, aggregationStep, "accounts", kv.AccountHistoryKeys, kv.AccountIdx, kv.AccountHistoryVals, kv.AccountSettings, false /* compressVals */); err != nil {
		return fmt.Errorf("ReopenFiles: %w", err)
	}
	if a.storage, err = NewHistory(dir, aggregationStep, "storage", kv.StorageHistoryKeys, kv.StorageIdx, kv.StorageHistoryVals, kv.StorageSettings, false /* compressVals */); err != nil {
		return fmt.Errorf("ReopenFiles: %w", err)
	}
	if a.code, err = NewHistory(dir, aggregationStep, "code", kv.CodeHistoryKeys, kv.CodeIdx, kv.CodeHistoryVals, kv.CodeSettings, true /* compressVals */); err != nil {
		return fmt.Errorf("ReopenFiles: %w", err)
	}
	if a.logAddrs, err = NewInvertedIndex(dir, aggregationStep, "logaddrs", kv.LogAddressKeys, kv.LogAddressIdx); err != nil {
		return fmt.Errorf("ReopenFiles: %w", err)
	}
	if a.logTopics, err = NewInvertedIndex(dir, aggregationStep, "logtopics", kv.LogTopicsKeys, kv.LogTopicsIdx); err != nil {
		return fmt.Errorf("ReopenFiles: %w", err)
	}
	if a.tracesFrom, err = NewInvertedIndex(dir, aggregationStep, "tracesfrom", kv.TracesFromKeys, kv.TracesFromIdx); err != nil {
		return fmt.Errorf("ReopenFiles: %w", err)
	}
	if a.tracesTo, err = NewInvertedIndex(dir, aggregationStep, "tracesto", kv.TracesToKeys, kv.TracesToIdx); err != nil {
		return fmt.Errorf("ReopenFiles: %w", err)
	}
	a.maxTxNum = a.EndTxNumMinimax()
	return nil
}

func (a *Aggregator22) Close() {
	a.closeFiles()
}

func (a *Aggregator22) Files() (res []string) {
	res = append(res, a.accounts.Files()...)
	res = append(res, a.storage.Files()...)
	res = append(res, a.code.Files()...)
	res = append(res, a.logAddrs.Files()...)
	res = append(res, a.logTopics.Files()...)
	res = append(res, a.tracesFrom.Files()...)
	res = append(res, a.tracesTo.Files()...)
	return res
}

func (a *Aggregator22) closeFiles() {
	if a.accounts != nil {
		a.accounts.Close()
	}
	if a.storage != nil {
		a.storage.Close()
	}
	if a.code != nil {
		a.code.Close()
	}
	if a.logAddrs != nil {
		a.logAddrs.Close()
	}
	if a.logTopics != nil {
		a.logTopics.Close()
	}
	if a.tracesFrom != nil {
		a.tracesFrom.Close()
	}
	if a.tracesTo != nil {
		a.tracesTo.Close()
	}
}

func (a *Aggregator22) SetLogPrefix(v string) { a.logPrefix = v }

func (a *Aggregator22) SetTx(tx kv.RwTx) {
	a.rwTx = tx
	a.accounts.SetTx(tx)
	a.storage.SetTx(tx)
	a.code.SetTx(tx)
	a.logAddrs.SetTx(tx)
	a.logTopics.SetTx(tx)
	a.tracesFrom.SetTx(tx)
	a.tracesTo.SetTx(tx)
}

func (a *Aggregator22) SetTxNum(txNum uint64) {
	a.txNum = txNum
	a.accounts.SetTxNum(txNum)
	a.storage.SetTxNum(txNum)
	a.code.SetTxNum(txNum)
	a.logAddrs.SetTxNum(txNum)
	a.logTopics.SetTxNum(txNum)
	a.tracesFrom.SetTxNum(txNum)
	a.tracesTo.SetTxNum(txNum)
}

type Agg22Collation struct {
	accounts   HistoryCollation
	storage    HistoryCollation
	code       HistoryCollation
	logAddrs   map[string]*roaring64.Bitmap
	logTopics  map[string]*roaring64.Bitmap
	tracesFrom map[string]*roaring64.Bitmap
	tracesTo   map[string]*roaring64.Bitmap
}

func (c Agg22Collation) Close() {
	c.accounts.Close()
	c.storage.Close()
	c.code.Close()
}

func (a *Aggregator22) collate(step uint64, txFrom, txTo uint64, roTx kv.Tx) (Agg22Collation, error) {
	var ac Agg22Collation
	var err error
	closeColl := true
	defer func() {
		if closeColl {
			ac.Close()
		}
	}()
	if ac.accounts, err = a.accounts.collate(step, txFrom, txTo, roTx); err != nil {
		return Agg22Collation{}, err
	}
	if ac.storage, err = a.storage.collate(step, txFrom, txTo, roTx); err != nil {
		return Agg22Collation{}, err
	}
	if ac.code, err = a.code.collate(step, txFrom, txTo, roTx); err != nil {
		return Agg22Collation{}, err
	}
	if ac.logAddrs, err = a.logAddrs.collate(txFrom, txTo, roTx); err != nil {
		return Agg22Collation{}, err
	}
	if ac.logTopics, err = a.logTopics.collate(txFrom, txTo, roTx); err != nil {
		return Agg22Collation{}, err
	}
	if ac.tracesFrom, err = a.tracesFrom.collate(txFrom, txTo, roTx); err != nil {
		return Agg22Collation{}, err
	}
	if ac.tracesTo, err = a.tracesTo.collate(txFrom, txTo, roTx); err != nil {
		return Agg22Collation{}, err
	}
	closeColl = false
	return ac, nil
}

type Agg22StaticFiles struct {
	accounts   HistoryFiles
	storage    HistoryFiles
	code       HistoryFiles
	logAddrs   InvertedFiles
	logTopics  InvertedFiles
	tracesFrom InvertedFiles
	tracesTo   InvertedFiles
}

func (sf Agg22StaticFiles) Close() {
	sf.accounts.Close()
	sf.storage.Close()
	sf.code.Close()
	sf.logAddrs.Close()
	sf.logTopics.Close()
	sf.tracesFrom.Close()
	sf.tracesTo.Close()
}

func (a *Aggregator22) buildFiles(step uint64, collation Agg22Collation) (Agg22StaticFiles, error) {
	var sf Agg22StaticFiles
	closeFiles := true
	defer func() {
		if closeFiles {
			sf.Close()
		}
	}()
	//var wg sync.WaitGroup
	//wg.Add(7)
	errCh := make(chan error, 7)
	//go func() {
	//	defer wg.Done()
	var err error
	if sf.accounts, err = a.accounts.buildFiles(step, collation.accounts); err != nil {
		errCh <- err
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if sf.storage, err = a.storage.buildFiles(step, collation.storage); err != nil {
		errCh <- err
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if sf.code, err = a.code.buildFiles(step, collation.code); err != nil {
		errCh <- err
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if sf.logAddrs, err = a.logAddrs.buildFiles(step, collation.logAddrs); err != nil {
		errCh <- err
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if sf.logTopics, err = a.logTopics.buildFiles(step, collation.logTopics); err != nil {
		errCh <- err
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if sf.tracesFrom, err = a.tracesFrom.buildFiles(step, collation.tracesFrom); err != nil {
		errCh <- err
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if sf.tracesTo, err = a.tracesTo.buildFiles(step, collation.tracesTo); err != nil {
		errCh <- err
	}
	//}()
	//go func() {
	//	wg.Wait()
	close(errCh)
	//}()
	var lastError error
	for err := range errCh {
		lastError = err
	}
	if lastError == nil {
		closeFiles = false
	}
	return sf, lastError
}

func (a *Aggregator22) integrateFiles(sf Agg22StaticFiles, txNumFrom, txNumTo uint64) {
	a.accounts.integrateFiles(sf.accounts, txNumFrom, txNumTo)
	a.storage.integrateFiles(sf.storage, txNumFrom, txNumTo)
	a.code.integrateFiles(sf.code, txNumFrom, txNumTo)
	a.logAddrs.integrateFiles(sf.logAddrs, txNumFrom, txNumTo)
	a.logTopics.integrateFiles(sf.logTopics, txNumFrom, txNumTo)
	a.tracesFrom.integrateFiles(sf.tracesFrom, txNumFrom, txNumTo)
	a.tracesTo.integrateFiles(sf.tracesTo, txNumFrom, txNumTo)
}

func (a *Aggregator22) Unwind(ctx context.Context, txUnwindTo uint64, stateLoad etl.LoadFunc) error {
	stateChanges := etl.NewCollector(a.logPrefix, "", etl.NewOldestEntryBuffer(etl.BufferOptimalSize))
	defer stateChanges.Close()

	if err := a.accounts.pruneF(txUnwindTo, math2.MaxUint64, func(txNum uint64, k, v []byte) error {
		if err := stateChanges.Collect(k, v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if err := a.storage.pruneF(txUnwindTo, math2.MaxUint64, func(txNu uint64, k, v []byte) error {
		if err := stateChanges.Collect(k, v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	if err := stateChanges.Load(a.rwTx, kv.PlainState, stateLoad, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := a.logAddrs.prune(txUnwindTo, math2.MaxUint64); err != nil {
		return err
	}
	if err := a.logTopics.prune(txUnwindTo, math2.MaxUint64); err != nil {
		return err
	}
	if err := a.tracesFrom.prune(txUnwindTo, math2.MaxUint64); err != nil {
		return err
	}
	if err := a.tracesTo.prune(txUnwindTo, math2.MaxUint64); err != nil {
		return err
	}
	return nil
}

func (a *Aggregator22) prune(step uint64, txFrom, txTo uint64) error {
	if err := a.accounts.prune(step, txFrom, txTo); err != nil {
		return err
	}
	if err := a.storage.prune(step, txFrom, txTo); err != nil {
		return err
	}
	if err := a.code.prune(step, txFrom, txTo); err != nil {
		return err
	}
	if err := a.logAddrs.prune(txFrom, txTo); err != nil {
		return err
	}
	if err := a.logTopics.prune(txFrom, txTo); err != nil {
		return err
	}
	if err := a.tracesFrom.prune(txFrom, txTo); err != nil {
		return err
	}
	if err := a.tracesTo.prune(txFrom, txTo); err != nil {
		return err
	}
	return nil
}

func (a *Aggregator22) EndTxNumMinimax() uint64 {
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
	return min
}

type Ranges22 struct {
	accounts                                 HistoryRanges
	storage                                  HistoryRanges
	code                                     HistoryRanges
	logAddrsStartTxNum, logAddrsEndTxNum     uint64
	logAddrs                                 bool
	logTopicsStartTxNum, logTopicsEndTxNum   uint64
	logTopics                                bool
	tracesFromStartTxNum, tracesFromEndTxNum uint64
	tracesFrom                               bool
	tracesToStartTxNum, tracesToEndTxNum     uint64
	tracesTo                                 bool
}

func (r Ranges22) any() bool {
	return r.accounts.any() || r.storage.any() || r.code.any() || r.logAddrs || r.logTopics || r.tracesFrom || r.tracesTo
}

func (a *Aggregator22) findMergeRange(maxEndTxNum, maxSpan uint64) Ranges22 {
	var r Ranges22
	r.accounts = a.accounts.findMergeRange(maxEndTxNum, maxSpan)
	r.storage = a.storage.findMergeRange(maxEndTxNum, maxSpan)
	r.code = a.code.findMergeRange(maxEndTxNum, maxSpan)
	r.logAddrs, r.logAddrsStartTxNum, r.logAddrsEndTxNum = a.logAddrs.findMergeRange(maxEndTxNum, maxSpan)
	r.logTopics, r.logTopicsStartTxNum, r.logTopicsEndTxNum = a.logTopics.findMergeRange(maxEndTxNum, maxSpan)
	r.tracesFrom, r.tracesFromStartTxNum, r.tracesFromEndTxNum = a.tracesFrom.findMergeRange(maxEndTxNum, maxSpan)
	r.tracesTo, r.tracesToStartTxNum, r.tracesToEndTxNum = a.tracesTo.findMergeRange(maxEndTxNum, maxSpan)
	fmt.Printf("findMergeRange(%d, %d)=%+v\n", maxEndTxNum, maxSpan, r)
	return r
}

type SelectedStaticFiles22 struct {
	accountsIdx, accountsHist []*filesItem
	accountsI                 int
	storageIdx, storageHist   []*filesItem
	storageI                  int
	codeIdx, codeHist         []*filesItem
	codeI                     int
	logAddrs                  []*filesItem
	logAddrsI                 int
	logTopics                 []*filesItem
	logTopicsI                int
	tracesFrom                []*filesItem
	tracesFromI               int
	tracesTo                  []*filesItem
	tracesToI                 int
}

func (sf SelectedStaticFiles22) Close() {
	for _, group := range [][]*filesItem{sf.accountsIdx, sf.accountsHist, sf.storageIdx, sf.accountsHist, sf.codeIdx, sf.codeHist,
		sf.logAddrs, sf.logTopics, sf.tracesFrom, sf.tracesTo} {
		for _, item := range group {
			if item != nil {
				if item.decompressor != nil {
					item.decompressor.Close()
				}
				if item.decompressor != nil {
					item.index.Close()
				}
			}
		}
	}
}

func (a *Aggregator22) staticFilesInRange(r Ranges22) SelectedStaticFiles22 {
	var sf SelectedStaticFiles22
	if r.accounts.any() {
		sf.accountsIdx, sf.accountsHist, sf.accountsI = a.accounts.staticFilesInRange(r.accounts)
	}
	if r.storage.any() {
		sf.storageIdx, sf.storageHist, sf.storageI = a.storage.staticFilesInRange(r.storage)
	}
	if r.code.any() {
		sf.codeIdx, sf.codeHist, sf.codeI = a.code.staticFilesInRange(r.code)
	}
	if r.logAddrs {
		sf.logAddrs, sf.logAddrsI = a.logAddrs.staticFilesInRange(r.logAddrsStartTxNum, r.logAddrsEndTxNum)
	}
	if r.logTopics {
		sf.logTopics, sf.logTopicsI = a.logTopics.staticFilesInRange(r.logTopicsStartTxNum, r.logTopicsEndTxNum)
	}
	if r.tracesFrom {
		sf.tracesFrom, sf.tracesFromI = a.tracesFrom.staticFilesInRange(r.tracesFromStartTxNum, r.tracesFromEndTxNum)
	}
	if r.tracesTo {
		sf.tracesTo, sf.tracesToI = a.tracesTo.staticFilesInRange(r.tracesToStartTxNum, r.tracesToEndTxNum)
	}
	return sf
}

type MergedFiles22 struct {
	accountsIdx, accountsHist *filesItem
	storageIdx, storageHist   *filesItem
	codeIdx, codeHist         *filesItem
	logAddrs                  *filesItem
	logTopics                 *filesItem
	tracesFrom                *filesItem
	tracesTo                  *filesItem
}

func (mf MergedFiles22) Close() {
	for _, item := range []*filesItem{mf.accountsIdx, mf.accountsHist, mf.storageIdx, mf.storageHist, mf.codeIdx, mf.codeHist,
		mf.logAddrs, mf.logTopics, mf.tracesFrom, mf.tracesTo} {
		if item != nil {
			if item.decompressor != nil {
				item.decompressor.Close()
			}
			if item.decompressor != nil {
				item.index.Close()
			}
		}
	}
}

func (a *Aggregator22) mergeFiles(files SelectedStaticFiles22, r Ranges22, maxSpan uint64) (MergedFiles22, error) {
	var mf MergedFiles22
	closeFiles := true
	defer func() {
		if closeFiles {
			mf.Close()
		}
	}()
	//var wg sync.WaitGroup
	//wg.Add(7)
	errCh := make(chan error, 7)
	//go func() {
	//	defer wg.Done()
	var err error
	if r.accounts.any() {
		if mf.accountsIdx, mf.accountsHist, err = a.accounts.mergeFiles(files.accountsIdx, files.accountsHist, r.accounts, maxSpan); err != nil {
			errCh <- err
		}
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if r.storage.any() {
		if mf.storageIdx, mf.storageHist, err = a.storage.mergeFiles(files.storageIdx, files.storageHist, r.storage, maxSpan); err != nil {
			errCh <- err
		}
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if r.code.any() {
		if mf.codeIdx, mf.codeHist, err = a.code.mergeFiles(files.codeIdx, files.codeHist, r.code, maxSpan); err != nil {
			errCh <- err
		}
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if r.logAddrs {
		if mf.logAddrs, err = a.logAddrs.mergeFiles(files.logAddrs, r.logAddrsStartTxNum, r.logAddrsEndTxNum, maxSpan); err != nil {
			errCh <- err
		}
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if r.logTopics {
		if mf.logTopics, err = a.logTopics.mergeFiles(files.logTopics, r.logTopicsStartTxNum, r.logTopicsEndTxNum, maxSpan); err != nil {
			errCh <- err
		}
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if r.tracesFrom {
		if mf.tracesFrom, err = a.tracesFrom.mergeFiles(files.tracesFrom, r.tracesFromStartTxNum, r.tracesFromEndTxNum, maxSpan); err != nil {
			errCh <- err
		}
	}
	//}()
	//go func() {
	//	defer wg.Done()
	//	var err error
	if r.tracesTo {
		if mf.tracesTo, err = a.tracesTo.mergeFiles(files.tracesTo, r.tracesToStartTxNum, r.tracesToEndTxNum, maxSpan); err != nil {
			errCh <- err
		}
	}
	//}()
	//go func() {
	//	wg.Wait()
	close(errCh)
	//}()
	var lastError error
	for err := range errCh {
		lastError = err
	}
	if lastError == nil {
		closeFiles = false
	}
	return mf, lastError
}

func (a *Aggregator22) integrateMergedFiles(outs SelectedStaticFiles22, in MergedFiles22) {
	a.accounts.integrateMergedFiles(outs.accountsIdx, outs.accountsHist, in.accountsIdx, in.accountsHist)
	a.storage.integrateMergedFiles(outs.storageIdx, outs.storageHist, in.storageIdx, in.storageHist)
	a.code.integrateMergedFiles(outs.codeIdx, outs.codeHist, in.codeIdx, in.codeHist)
	a.logAddrs.integrateMergedFiles(outs.logAddrs, in.logAddrs)
	a.logTopics.integrateMergedFiles(outs.logTopics, in.logTopics)
	a.tracesFrom.integrateMergedFiles(outs.tracesFrom, in.tracesFrom)
	a.tracesTo.integrateMergedFiles(outs.tracesTo, in.tracesTo)
}

func (a *Aggregator22) deleteFiles(outs SelectedStaticFiles22) error {
	if err := a.accounts.deleteFiles(outs.accountsIdx, outs.accountsHist); err != nil {
		return err
	}
	if err := a.storage.deleteFiles(outs.storageIdx, outs.storageHist); err != nil {
		return err
	}
	if err := a.code.deleteFiles(outs.codeIdx, outs.codeHist); err != nil {
		return err
	}
	if err := a.logAddrs.deleteFiles(outs.logAddrs); err != nil {
		return err
	}
	if err := a.logTopics.deleteFiles(outs.logTopics); err != nil {
		return err
	}
	if err := a.tracesFrom.deleteFiles(outs.tracesFrom); err != nil {
		return err
	}
	if err := a.tracesTo.deleteFiles(outs.tracesTo); err != nil {
		return err
	}
	return nil
}

func (a *Aggregator22) ReadyToFinishTx() bool {
	return (a.txNum+1)%a.aggregationStep == 0
}

func (a *Aggregator22) FinishTx() error {
	if (a.txNum + 1) <= a.maxTxNum+a.aggregationStep {
		return nil
	}
	if (a.txNum+1)%a.aggregationStep != 0 {
		return nil
	}
	closeAll := true
	step := a.txNum / a.aggregationStep
	if step == 0 {
		return nil
	}
	step-- // Leave one step worth in the DB
	collation, err := a.collate(step, step*a.aggregationStep, (step+1)*a.aggregationStep, a.rwTx)
	if err != nil {
		return err
	}
	defer func() {
		if closeAll {
			collation.Close()
		}
	}()
	sf, err := a.buildFiles(step, collation)
	if err != nil {
		return err
	}
	defer func() {
		if closeAll {
			sf.Close()
		}
	}()
	a.integrateFiles(sf, step*a.aggregationStep, (step+1)*a.aggregationStep)
	if err = a.prune(step, step*a.aggregationStep, (step+1)*a.aggregationStep); err != nil {
		return err
	}
	maxEndTxNum := a.EndTxNumMinimax()
	maxSpan := uint64(32) * a.aggregationStep
	for r := a.findMergeRange(maxEndTxNum, maxSpan); r.any(); r = a.findMergeRange(maxEndTxNum, maxSpan) {
		outs := a.staticFilesInRange(r)
		defer func() {
			if closeAll {
				outs.Close()
			}
		}()
		in, err := a.mergeFiles(outs, r, maxSpan)
		if err != nil {
			return err
		}
		defer func() {
			if closeAll {
				in.Close()
			}
		}()
		a.integrateMergedFiles(outs, in)
		if err = a.deleteFiles(outs); err != nil {
			return err
		}
	}
	closeAll = false
	return nil
}

func (a *Aggregator22) AddAccountPrev(addr []byte, prev []byte) error {
	if err := a.accounts.AddPrevValue(addr, nil, prev); err != nil {
		return err
	}
	return nil
}

func (a *Aggregator22) AddStoragePrev(addr []byte, loc []byte, prev []byte) error {
	if err := a.storage.AddPrevValue(addr, loc, prev); err != nil {
		return err
	}
	return nil
}

// AddCodePrev - addr+inc => code
func (a *Aggregator22) AddCodePrev(addr []byte, prev []byte) error {
	if err := a.code.AddPrevValue(addr, nil, prev); err != nil {
		return err
	}
	return nil
}

func (a *Aggregator22) AddTraceFrom(addr []byte) error {
	return a.tracesFrom.Add(addr)
}

func (a *Aggregator22) AddTraceTo(addr []byte) error {
	return a.tracesTo.Add(addr)
}

func (a *Aggregator22) AddLogAddr(addr []byte) error {
	return a.logAddrs.Add(addr)
}

func (a *Aggregator22) AddLogTopic(topic []byte) error {
	return a.logTopics.Add(topic)
}

func (ac *Aggregator22Context) LogAddrIterator(addr []byte, startTxNum, endTxNum uint64, roTx kv.Tx) InvertedIterator {
	return ac.logAddrs.IterateRange(addr, startTxNum, endTxNum, roTx)
}

func (ac *Aggregator22Context) LogTopicIterator(topic []byte, startTxNum, endTxNum uint64, roTx kv.Tx) InvertedIterator {
	return ac.logTopics.IterateRange(topic, startTxNum, endTxNum, roTx)
}

func (ac *Aggregator22Context) TraceFromIterator(addr []byte, startTxNum, endTxNum uint64, roTx kv.Tx) InvertedIterator {
	return ac.tracesFrom.IterateRange(addr, startTxNum, endTxNum, roTx)
}

func (ac *Aggregator22Context) TraceToIterator(addr []byte, startTxNum, endTxNum uint64, roTx kv.Tx) InvertedIterator {
	return ac.tracesTo.IterateRange(addr, startTxNum, endTxNum, roTx)
}

func (ac *Aggregator22Context) IsMaxAccountsTxNum(addr []byte, txNum uint64) bool {
	return ac.accounts.IsMaxTxNum(addr, txNum)
}

func (ac *Aggregator22Context) IsMaxStorageTxNum(addr []byte, loc []byte, txNum uint64) bool {
	if cap(ac.keyBuf) < len(addr)+len(loc) {
		ac.keyBuf = make([]byte, len(addr)+len(loc))
	} else if len(ac.keyBuf) != len(addr)+len(loc) {
		ac.keyBuf = ac.keyBuf[:len(addr)+len(loc)]
	}
	copy(ac.keyBuf, addr)
	copy(ac.keyBuf[len(addr):], loc)
	return ac.storage.IsMaxTxNum(ac.keyBuf, txNum)
}

func (ac *Aggregator22Context) IsMaxCodeTxNum(addr []byte, txNum uint64) bool {
	return ac.code.IsMaxTxNum(addr, txNum)
}

func (ac *Aggregator22Context) ReadAccountDataNoStateWithRecent(addr []byte, txNum uint64) ([]byte, bool, error) {
	return ac.accounts.GetNoStateWithRecent(addr, txNum, ac.tx)
}

func (ac *Aggregator22Context) ReadAccountDataNoState(addr []byte, txNum uint64) ([]byte, bool, error) {
	return ac.accounts.GetNoState(addr, txNum)
}

func (ac *Aggregator22Context) ReadAccountStorageNoStateWithRecent(addr []byte, loc []byte, txNum uint64) ([]byte, bool, error) {
	if cap(ac.keyBuf) < len(addr)+len(loc) {
		ac.keyBuf = make([]byte, len(addr)+len(loc))
	} else if len(ac.keyBuf) != len(addr)+len(loc) {
		ac.keyBuf = ac.keyBuf[:len(addr)+len(loc)]
	}
	copy(ac.keyBuf, addr)
	copy(ac.keyBuf[len(addr):], loc)
	return ac.storage.GetNoStateWithRecent(ac.keyBuf, txNum, ac.tx)
}

func (ac *Aggregator22Context) ReadAccountStorageNoState(addr []byte, loc []byte, txNum uint64) ([]byte, bool, error) {
	if cap(ac.keyBuf) < len(addr)+len(loc) {
		ac.keyBuf = make([]byte, len(addr)+len(loc))
	} else if len(ac.keyBuf) != len(addr)+len(loc) {
		ac.keyBuf = ac.keyBuf[:len(addr)+len(loc)]
	}
	copy(ac.keyBuf, addr)
	copy(ac.keyBuf[len(addr):], loc)
	return ac.storage.GetNoState(ac.keyBuf, txNum)
}

func (ac *Aggregator22Context) ReadAccountCodeNoStateWithRecent(addr []byte, txNum uint64) ([]byte, bool, error) {
	return ac.code.GetNoStateWithRecent(addr, txNum, ac.tx)
}
func (ac *Aggregator22Context) ReadAccountCodeNoState(addr []byte, txNum uint64) ([]byte, bool, error) {
	return ac.code.GetNoState(addr, txNum)
}

func (ac *Aggregator22Context) ReadAccountCodeSizeNoStateWithRecent(addr []byte, txNum uint64) (int, bool, error) {
	code, noState, err := ac.code.GetNoStateWithRecent(addr, txNum, ac.tx)
	if err != nil {
		return 0, false, err
	}
	return len(code), noState, nil
}
func (ac *Aggregator22Context) ReadAccountCodeSizeNoState(addr []byte, txNum uint64) (int, bool, error) {
	code, noState, err := ac.code.GetNoState(addr, txNum)
	if err != nil {
		return 0, false, err
	}
	return len(code), noState, nil
}

func (ac *Aggregator22Context) IterateAccountsHistory(fromKey, toKey []byte, txNum uint64) *HistoryIterator {
	return ac.accounts.iterateHistoryBeforeTxNum(fromKey, toKey, txNum)
}

func (ac *Aggregator22Context) IterateStorageHistory(fromKey, toKey []byte, txNum uint64) *HistoryIterator {
	return ac.storage.iterateHistoryBeforeTxNum(fromKey, toKey, txNum)
}

func (ac *Aggregator22Context) IterateCodeHistory(fromKey, toKey []byte, txNum uint64) *HistoryIterator {
	return ac.code.iterateHistoryBeforeTxNum(fromKey, toKey, txNum)
}

func (ac *Aggregator22Context) IterateAccountsReconTxs(fromKey, toKey []byte, txNum uint64) *ScanIterator {
	return ac.accounts.iterateReconTxs(fromKey, toKey, txNum)
}

func (ac *Aggregator22Context) IterateStorageReconTxs(fromKey, toKey []byte, txNum uint64) *ScanIterator {
	return ac.storage.iterateReconTxs(fromKey, toKey, txNum)
}

func (ac *Aggregator22Context) IterateCodeReconTxs(fromKey, toKey []byte, txNum uint64) *ScanIterator {
	return ac.code.iterateReconTxs(fromKey, toKey, txNum)
}

type FilesStats22 struct {
}

func (a *Aggregator22) Stats() FilesStats22 {
	var fs FilesStats22
	return fs
}

func (a *Aggregator22) Code() *History     { return a.code }
func (a *Aggregator22) Accounts() *History { return a.accounts }
func (a *Aggregator22) Storage() *History  { return a.storage }

type Aggregator22Context struct {
	a          *Aggregator22
	keyBuf     []byte
	accounts   *HistoryContext
	storage    *HistoryContext
	code       *HistoryContext
	logAddrs   *InvertedIndexContext
	logTopics  *InvertedIndexContext
	tracesFrom *InvertedIndexContext
	tracesTo   *InvertedIndexContext

	tx kv.Tx
}

func (a *Aggregator22) MakeContext() *Aggregator22Context {
	return &Aggregator22Context{
		a:          a,
		accounts:   a.accounts.MakeContext(),
		storage:    a.storage.MakeContext(),
		code:       a.code.MakeContext(),
		logAddrs:   a.logAddrs.MakeContext(),
		logTopics:  a.logTopics.MakeContext(),
		tracesFrom: a.tracesFrom.MakeContext(),
		tracesTo:   a.tracesTo.MakeContext(),
	}
}
func (ac *Aggregator22Context) SetTx(tx kv.Tx) { ac.tx = tx }
