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
	"fmt"
	"math"
	"math/bits"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
)

// Reconstruction of the aggregator in another package, `aggregator`

type Aggregator struct {
	aggregationStep uint64
	accounts        *Domain
	storage         *Domain
	code            *Domain
	commitment      *DomainCommitted
	logAddrs        *InvertedIndex
	logTopics       *InvertedIndex
	tracesFrom      *InvertedIndex
	tracesTo        *InvertedIndex
	txNum           uint64
	seekTxNum       uint64
	blockNum        uint64
	commitFn        func(txNum uint64) error
	rwTx            kv.RwTx
	stats           FilesStats
	tmpdir          string
	defaultCtx      *AggregatorContext
}

func NewAggregator(
	dir, tmpdir string,
	aggregationStep uint64,
) (*Aggregator, error) {

	a := &Aggregator{aggregationStep: aggregationStep, tmpdir: tmpdir}

	closeAgg := true
	defer func() {
		if closeAgg {
			a.Close()
		}
	}()
	err := os.MkdirAll(dir, 0764)
	if err != nil {
		return nil, err
	}
	if a.accounts, err = NewDomain(dir, tmpdir, aggregationStep, "accounts", kv.AccountKeys, kv.AccountVals, kv.AccountHistoryKeys, kv.AccountHistoryVals, kv.AccountSettings, kv.AccountIdx, 0 /* prefixLen */, false /* compressVals */); err != nil {
		return nil, err
	}
	if a.storage, err = NewDomain(dir, tmpdir, aggregationStep, "storage", kv.StorageKeys, kv.StorageVals, kv.StorageHistoryKeys, kv.StorageHistoryVals, kv.StorageSettings, kv.StorageIdx, 20 /* prefixLen */, false /* compressVals */); err != nil {
		return nil, err
	}
	if a.code, err = NewDomain(dir, tmpdir, aggregationStep, "code", kv.CodeKeys, kv.CodeVals, kv.CodeHistoryKeys, kv.CodeHistoryVals, kv.CodeSettings, kv.CodeIdx, 0 /* prefixLen */, true /* compressVals */); err != nil {
		return nil, err
	}

	commitd, err := NewDomain(dir, tmpdir, aggregationStep, "commitment", kv.CommitmentKeys, kv.CommitmentVals, kv.CommitmentHistoryKeys, kv.CommitmentHistoryVals, kv.CommitmentSettings, kv.CommitmentIdx, 0 /* prefixLen */, false /* compressVals */)
	if err != nil {
		return nil, err
	}
	a.commitment = NewCommittedDomain(commitd, CommitmentModeDirect)

	if a.logAddrs, err = NewInvertedIndex(dir, tmpdir, aggregationStep, "logaddrs", kv.LogAddressKeys, kv.LogAddressIdx); err != nil {
		return nil, err
	}
	if a.logTopics, err = NewInvertedIndex(dir, tmpdir, aggregationStep, "logtopics", kv.LogTopicsKeys, kv.LogTopicsIdx); err != nil {
		return nil, err
	}
	if a.tracesFrom, err = NewInvertedIndex(dir, tmpdir, aggregationStep, "tracesfrom", kv.TracesFromKeys, kv.TracesFromIdx); err != nil {
		return nil, err
	}
	if a.tracesTo, err = NewInvertedIndex(dir, tmpdir, aggregationStep, "tracesto", kv.TracesToKeys, kv.TracesToIdx); err != nil {
		return nil, err
	}
	closeAgg = false

	a.defaultCtx = a.MakeContext()
	a.commitment.patriciaTrie.ResetFns(a.defaultCtx.branchFn, a.defaultCtx.accountFn, a.defaultCtx.storageFn)
	return a, nil
}

func (a *Aggregator) GetAndResetStats() DomainStats {
	stats := DomainStats{}
	stats.Accumulate(a.accounts.GetAndResetStats())
	stats.Accumulate(a.storage.GetAndResetStats())
	stats.Accumulate(a.code.GetAndResetStats())
	stats.Accumulate(a.commitment.GetAndResetStats())

	var tto, tfrom, ltopics, laddr DomainStats
	tto.FilesCount, tto.DataSize, tto.IndexSize = a.tracesTo.collectFilesStat()
	tfrom.FilesCount, tfrom.DataSize, tfrom.DataSize = a.tracesFrom.collectFilesStat()
	ltopics.FilesCount, ltopics.DataSize, ltopics.IndexSize = a.logTopics.collectFilesStat()
	laddr.FilesCount, laddr.DataSize, laddr.IndexSize = a.logAddrs.collectFilesStat()

	stats.Accumulate(tto)
	stats.Accumulate(tfrom)
	stats.Accumulate(ltopics)
	stats.Accumulate(laddr)
	return stats
}

func (a *Aggregator) Close() {
	if a.accounts != nil {
		a.accounts.Close()
	}
	if a.storage != nil {
		a.storage.Close()
	}
	if a.code != nil {
		a.code.Close()
	}
	if a.commitment != nil {
		a.commitment.Close()
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

func (a *Aggregator) SetTx(tx kv.RwTx) {
	a.rwTx = tx
	a.accounts.SetTx(tx)
	a.storage.SetTx(tx)
	a.code.SetTx(tx)
	a.commitment.SetTx(tx)
	a.logAddrs.SetTx(tx)
	a.logTopics.SetTx(tx)
	a.tracesFrom.SetTx(tx)
	a.tracesTo.SetTx(tx)
}

func (a *Aggregator) SetTxNum(txNum uint64) {
	a.txNum = txNum
	a.accounts.SetTxNum(txNum)
	a.storage.SetTxNum(txNum)
	a.code.SetTxNum(txNum)
	a.commitment.SetTxNum(txNum)
	a.logAddrs.SetTxNum(txNum)
	a.logTopics.SetTxNum(txNum)
	a.tracesFrom.SetTxNum(txNum)
	a.tracesTo.SetTxNum(txNum)
}

// todo useless
func (a *Aggregator) SetBlockNum(bn uint64) { a.blockNum = bn }

func (a *Aggregator) SetWorkers(i int) {
	a.accounts.workers = i
	a.storage.workers = i
	a.code.workers = i
	a.commitment.workers = i
	a.logAddrs.workers = i
	a.logTopics.workers = i
	a.tracesFrom.workers = i
	a.tracesTo.workers = i
}

func (a *Aggregator) SetCommitmentMode(mode CommitmentMode) {
	a.commitment.mode = mode
}

func (a *Aggregator) EndTxNumMinimax() uint64 {
	min := a.accounts.endTxNumMinimax()
	if txNum := a.storage.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	if txNum := a.code.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	if txNum := a.commitment.endTxNumMinimax(); txNum < min {
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

func (a *Aggregator) SeekCommitment() (txNum uint64, err error) {
	filesTxNum := a.EndTxNumMinimax()
	txNum, err = a.commitment.SeekCommitment(a.aggregationStep, filesTxNum)
	if err != nil {
		return 0, err
	}
	if txNum == 0 {
		return
	}
	a.seekTxNum = txNum + 1
	return txNum + 1, nil
}

func (a *Aggregator) aggregate(ctx context.Context, step uint64) error {
	defer func(t time.Time) {
		log.Info("[snapshots] aggregation step is done", "step", step, "took", time.Since(t))
	}(time.Now())

	var (
		logEvery = time.NewTicker(time.Second * 30)
		wg       sync.WaitGroup
		errCh    = make(chan error, 8)
		maxSpan  = 32 * a.aggregationStep
		txFrom   = step * a.aggregationStep
		txTo     = (step + 1) * a.aggregationStep
		//workers  = 1
	)
	defer logEvery.Stop()

	for _, d := range []*Domain{a.accounts, a.storage, a.code, a.commitment.Domain} {
		wg.Add(1)

		collation, err := d.collate(ctx, step, txFrom, txTo, d.tx, logEvery)
		if err != nil {
			collation.Close()
			return fmt.Errorf("domain collation %q has failed: %w", d.filenameBase, err)
		}

		go func(wg *sync.WaitGroup, d *Domain, collation Collation) {
			defer wg.Done()

			defer func(t time.Time) {
				log.Info("[snapshots] domain collate-build is done", "took", time.Since(t), "domain", d.filenameBase)
			}(time.Now())

			sf, err := d.buildFiles(ctx, step, collation)
			collation.Close()
			if err != nil {
				errCh <- err
				sf.Close()
				return
			}

			d.integrateFiles(sf, step*a.aggregationStep, (step+1)*a.aggregationStep)
		}(&wg, d, collation)

		if err := d.prune(ctx, step, txFrom, txTo, math.MaxUint64, logEvery); err != nil {
			return err
		}
	}

	for _, d := range []*InvertedIndex{a.logTopics, a.logAddrs, a.tracesFrom, a.tracesTo} {
		wg.Add(1)

		collation, err := d.collate(ctx, step*a.aggregationStep, (step+1)*a.aggregationStep, d.tx, logEvery)
		if err != nil {
			return fmt.Errorf("index collation %q has failed: %w", d.filenameBase, err)
		}

		go func(wg *sync.WaitGroup, d *InvertedIndex, tx kv.Tx) {
			defer wg.Done()
			defer func(t time.Time) {
				log.Info("[snapshots] index collate-build is done", "took", time.Since(t), "domain", d.filenameBase)
			}(time.Now())

			sf, err := d.buildFiles(ctx, step, collation)
			if err != nil {
				errCh <- err
				sf.Close()
				return
			}
			d.integrateFiles(sf, step*a.aggregationStep, (step+1)*a.aggregationStep)
		}(&wg, d, d.tx)

		if err := d.prune(ctx, txFrom, txTo, math.MaxUint64, logEvery); err != nil {
			return err
		}
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		log.Warn("domain collate-buildFiles failed", "err", err)
		return fmt.Errorf("domain collate-build failed: %w", err)
	}

	maxEndTxNum := a.EndTxNumMinimax()
	closeAll := true
	for r := a.findMergeRange(maxEndTxNum, maxSpan); r.any(); r = a.findMergeRange(maxEndTxNum, maxSpan) {
		outs := a.staticFilesInRange(r)
		defer func() {
			if closeAll {
				outs.Close()
			}
		}()

		in, err := a.mergeFiles(ctx, outs, r, 1)
		if err != nil {
			return err
		}
		a.integrateMergedFiles(outs, in)
		defer func() {
			if closeAll {
				in.Close()
			}
		}()

		if err = a.deleteFiles(outs); err != nil {
			return err
		}
	}

	closeAll = false
	return nil
}

type Ranges struct {
	accounts             DomainRanges
	storage              DomainRanges
	code                 DomainRanges
	commitment           DomainRanges
	logTopicsEndTxNum    uint64
	logAddrsEndTxNum     uint64
	logTopicsStartTxNum  uint64
	logAddrsStartTxNum   uint64
	tracesFromStartTxNum uint64
	tracesFromEndTxNum   uint64
	tracesToStartTxNum   uint64
	tracesToEndTxNum     uint64
	logAddrs             bool
	logTopics            bool
	tracesFrom           bool
	tracesTo             bool
}

func (r Ranges) any() bool {
	return r.accounts.any() || r.storage.any() || r.code.any() || r.commitment.any() //|| r.logAddrs || r.logTopics || r.tracesFrom || r.tracesTo
}

func (a *Aggregator) findMergeRange(maxEndTxNum, maxSpan uint64) Ranges {
	var r Ranges
	r.accounts = a.accounts.findMergeRange(maxEndTxNum, maxSpan)
	r.storage = a.storage.findMergeRange(maxEndTxNum, maxSpan)
	r.code = a.code.findMergeRange(maxEndTxNum, maxSpan)
	r.commitment = a.commitment.findMergeRange(maxEndTxNum, maxSpan)
	r.logAddrs, r.logAddrsStartTxNum, r.logAddrsEndTxNum = a.logAddrs.findMergeRange(maxEndTxNum, maxSpan)
	r.logTopics, r.logTopicsStartTxNum, r.logTopicsEndTxNum = a.logTopics.findMergeRange(maxEndTxNum, maxSpan)
	r.tracesFrom, r.tracesFromStartTxNum, r.tracesFromEndTxNum = a.tracesFrom.findMergeRange(maxEndTxNum, maxSpan)
	r.tracesTo, r.tracesToStartTxNum, r.tracesToEndTxNum = a.tracesTo.findMergeRange(maxEndTxNum, maxSpan)
	log.Info(fmt.Sprintf("findMergeRange(%d, %d)=%+v\n", maxEndTxNum, maxSpan, r))
	return r
}

type SelectedStaticFiles struct {
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
	tracesTo       []*filesItem
	tracesFrom     []*filesItem
	logTopics      []*filesItem
	logAddrs       []*filesItem
	codeI          int
	storageI       int
	accountsI      int
	commitmentI    int
	logAddrsI      int
	tracesFromI    int
	logTopicsI     int
	tracesToI      int
}

func (sf SelectedStaticFiles) Close() {
	for _, group := range [][]*filesItem{
		sf.accounts, sf.accountsIdx, sf.accountsHist,
		sf.storage, sf.storageIdx, sf.storageHist,
		sf.code, sf.codeIdx, sf.codeHist,
		sf.commitment, sf.commitmentIdx, sf.commitmentHist,
		//sf.logAddrs, sf.logTopics, sf.tracesFrom, sf.tracesTo,
	} {
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

func (a *Aggregator) staticFilesInRange(r Ranges) SelectedStaticFiles {
	var sf SelectedStaticFiles
	if r.accounts.any() {
		sf.accounts, sf.accountsIdx, sf.accountsHist, sf.accountsI = a.accounts.staticFilesInRange(r.accounts)
	}
	if r.storage.any() {
		sf.storage, sf.storageIdx, sf.storageHist, sf.storageI = a.storage.staticFilesInRange(r.storage)
	}
	if r.code.any() {
		sf.code, sf.codeIdx, sf.codeHist, sf.codeI = a.code.staticFilesInRange(r.code)
	}
	if r.commitment.any() {
		sf.commitment, sf.commitmentIdx, sf.commitmentHist, sf.commitmentI = a.commitment.staticFilesInRange(r.commitment)
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

type MergedFiles struct {
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

func (mf MergedFiles) Close() {
	for _, item := range []*filesItem{
		mf.accounts, mf.accountsIdx, mf.accountsHist,
		mf.storage, mf.storageIdx, mf.storageHist,
		mf.code, mf.codeIdx, mf.codeHist,
		mf.commitment, mf.commitmentIdx, mf.commitmentHist,
		//mf.logAddrs, mf.logTopics, mf.tracesFrom, mf.tracesTo,
	} {
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

func (a *Aggregator) mergeFiles(ctx context.Context, files SelectedStaticFiles, r Ranges, workers int) (MergedFiles, error) {
	defer func(t time.Time) { log.Info("[snapshots] merge", "took", time.Since(t)) }(time.Now())
	var mf MergedFiles
	closeFiles := true
	defer func() {
		if closeFiles {
			mf.Close()
		}
	}()

	var (
		errCh      = make(chan error, 8)
		wg         sync.WaitGroup
		predicates sync.WaitGroup
	)

	predicates.Add(2)
	wg.Add(8)

	go func() {
		defer wg.Done()
		defer predicates.Done()
		var err error
		if r.accounts.any() {
			if mf.accounts, mf.accountsIdx, mf.accountsHist, err = a.accounts.mergeFiles(ctx, files.accounts, files.accountsIdx, files.accountsHist, r.accounts, workers); err != nil {
				errCh <- err
			}
		}
	}()
	go func() {
		defer wg.Done()
		defer predicates.Done()
		var err error
		if r.storage.any() {
			if mf.storage, mf.storageIdx, mf.storageHist, err = a.storage.mergeFiles(ctx, files.storage, files.storageIdx, files.storageHist, r.storage, workers); err != nil {
				errCh <- err
			}
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if r.code.any() {
			if mf.code, mf.codeIdx, mf.codeHist, err = a.code.mergeFiles(ctx, files.code, files.codeIdx, files.codeHist, r.code, workers); err != nil {
				errCh <- err
			}
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if r.logAddrs {
			if mf.logAddrs, err = a.logAddrs.mergeFiles(ctx, files.logAddrs, r.logAddrsStartTxNum, r.logAddrsEndTxNum, workers); err != nil {
				errCh <- err
			}
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if r.logTopics {
			if mf.logTopics, err = a.logTopics.mergeFiles(ctx, files.logTopics, r.logTopicsStartTxNum, r.logTopicsEndTxNum, workers); err != nil {
				errCh <- err
			}
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if r.tracesFrom {
			if mf.tracesFrom, err = a.tracesFrom.mergeFiles(ctx, files.tracesFrom, r.tracesFromStartTxNum, r.tracesFromEndTxNum, workers); err != nil {
				errCh <- err
			}
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if r.tracesTo {
			if mf.tracesTo, err = a.tracesTo.mergeFiles(ctx, files.tracesTo, r.tracesToStartTxNum, r.tracesToEndTxNum, workers); err != nil {
				errCh <- err
			}
		}
	}()

	go func() {
		defer wg.Done()
		predicates.Wait()

		var err error
		// requires storage|accounts to be merged at this point
		if r.commitment.any() {
			if mf.commitment, mf.commitmentIdx, mf.commitmentHist, err = a.commitment.mergeFiles(ctx, files, mf, r.commitment, workers); err != nil {
				errCh <- err
			}
		}
	}()

	go func() {
		wg.Wait()

		close(errCh)
	}()

	var lastError error
	for err := range errCh {
		lastError = err
	}
	if lastError == nil {
		closeFiles = false
	}
	return mf, lastError
}

func (a *Aggregator) integrateMergedFiles(outs SelectedStaticFiles, in MergedFiles) {
	a.accounts.integrateMergedFiles(outs.accounts, outs.accountsIdx, outs.accountsHist, in.accounts, in.accountsIdx, in.accountsHist)
	a.storage.integrateMergedFiles(outs.storage, outs.storageIdx, outs.storageHist, in.storage, in.storageIdx, in.storageHist)
	a.code.integrateMergedFiles(outs.code, outs.codeIdx, outs.codeHist, in.code, in.codeIdx, in.codeHist)
	a.commitment.integrateMergedFiles(outs.commitment, outs.commitmentIdx, outs.commitmentHist, in.commitment, in.commitmentIdx, in.commitmentHist)
	a.logAddrs.integrateMergedFiles(outs.logAddrs, in.logAddrs)
	a.logTopics.integrateMergedFiles(outs.logTopics, in.logTopics)
	a.tracesFrom.integrateMergedFiles(outs.tracesFrom, in.tracesFrom)
	a.tracesTo.integrateMergedFiles(outs.tracesTo, in.tracesTo)
}

func (a *Aggregator) deleteFiles(outs SelectedStaticFiles) error {
	if err := a.accounts.deleteFiles(outs.accounts, outs.accountsIdx, outs.accountsHist); err != nil {
		return err
	}
	if err := a.storage.deleteFiles(outs.storage, outs.storageIdx, outs.storageHist); err != nil {
		return err
	}
	if err := a.code.deleteFiles(outs.code, outs.codeIdx, outs.codeHist); err != nil {
		return err
	}
	if err := a.commitment.deleteFiles(outs.commitment, outs.commitmentIdx, outs.commitmentHist); err != nil {
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

func (ac *AggregatorContext) ReadAccountData(addr []byte, roTx kv.Tx) ([]byte, error) {
	return ac.accounts.Get(addr, nil, roTx)
}

func (ac *AggregatorContext) ReadAccountDataBeforeTxNum(addr []byte, txNum uint64, roTx kv.Tx) ([]byte, error) {
	return ac.accounts.GetBeforeTxNum(addr, txNum, roTx)
}

func (ac *AggregatorContext) ReadAccountStorage(addr []byte, loc []byte, roTx kv.Tx) ([]byte, error) {
	return ac.storage.Get(addr, loc, roTx)
}

func (ac *AggregatorContext) ReadAccountStorageBeforeTxNum(addr []byte, loc []byte, txNum uint64, roTx kv.Tx) ([]byte, error) {
	if cap(ac.keyBuf) < len(addr)+len(loc) {
		ac.keyBuf = make([]byte, len(addr)+len(loc))
	} else if len(ac.keyBuf) != len(addr)+len(loc) {
		ac.keyBuf = ac.keyBuf[:len(addr)+len(loc)]
	}
	copy(ac.keyBuf, addr)
	copy(ac.keyBuf[len(addr):], loc)
	return ac.storage.GetBeforeTxNum(ac.keyBuf, txNum, roTx)
}

func (ac *AggregatorContext) ReadAccountCode(addr []byte, roTx kv.Tx) ([]byte, error) {
	return ac.code.Get(addr, nil, roTx)
}

func (ac *AggregatorContext) ReadCommitment(addr []byte, roTx kv.Tx) ([]byte, error) {
	return ac.commitment.Get(addr, nil, roTx)
}

func (ac *AggregatorContext) ReadCommitmentBeforeTxNum(addr []byte, txNum uint64, roTx kv.Tx) ([]byte, error) {
	return ac.commitment.GetBeforeTxNum(addr, txNum, roTx)
}

func (ac *AggregatorContext) ReadAccountCodeBeforeTxNum(addr []byte, txNum uint64, roTx kv.Tx) ([]byte, error) {
	return ac.code.GetBeforeTxNum(addr, txNum, roTx)
}

func (ac *AggregatorContext) ReadAccountCodeSize(addr []byte, roTx kv.Tx) (int, error) {
	code, err := ac.code.Get(addr, nil, roTx)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (ac *AggregatorContext) ReadAccountCodeSizeBeforeTxNum(addr []byte, txNum uint64, roTx kv.Tx) (int, error) {
	code, err := ac.code.GetBeforeTxNum(addr, txNum, roTx)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func bytesToUint64(buf []byte) (x uint64) {
	for i, b := range buf {
		x = x<<8 + uint64(b)
		if i == 7 {
			return
		}
	}
	return
}

func (a *AggregatorContext) branchFn(prefix []byte) ([]byte, error) {
	// Look in the summary table first
	stateValue, err := a.ReadCommitment(prefix, a.a.rwTx)
	if err != nil {
		return nil, fmt.Errorf("failed read branch %x: %w", commitment.CompactedKeyToHex(prefix), err)
	}
	if stateValue == nil {
		return nil, nil
	}
	// fmt.Printf("Returning branch data prefix [%x], mergeVal=[%x]\n", commitment.CompactedKeyToHex(prefix), stateValue)
	return stateValue[2:], nil // Skip touchMap but keep afterMap
}

func (a *AggregatorContext) accountFn(plainKey []byte, cell *commitment.Cell) error {
	encAccount, err := a.ReadAccountData(plainKey, a.a.rwTx)
	if err != nil {
		return err
	}
	cell.Nonce = 0
	cell.Balance.Clear()
	copy(cell.CodeHash[:], commitment.EmptyCodeHash)
	if len(encAccount) > 0 {
		nonce, balance, chash := DecodeAccountBytes(encAccount)
		cell.Nonce = nonce
		cell.Balance.Set(balance)
		if chash != nil {
			copy(cell.CodeHash[:], chash)
		}
	}

	code, err := a.ReadAccountCode(plainKey, a.a.rwTx)
	if err != nil {
		return err
	}
	if code != nil {
		a.a.commitment.keccak.Reset()
		a.a.commitment.keccak.Write(code)
		copy(cell.CodeHash[:], a.a.commitment.keccak.Sum(nil))
	}
	cell.Delete = len(encAccount) == 0 && len(code) == 0
	return nil
}

func (a *AggregatorContext) storageFn(plainKey []byte, cell *commitment.Cell) error {
	// Look in the summary table first
	enc, err := a.ReadAccountStorage(plainKey[:length.Addr], plainKey[length.Addr:], a.a.rwTx)
	if err != nil {
		return err
	}
	cell.StorageLen = len(enc)
	copy(cell.Storage[:], enc)
	cell.Delete = cell.StorageLen == 0
	return nil
}

// Evaluates commitment for processed state. Commit=true - store trie state after evaluation
func (a *Aggregator) ComputeCommitment(saveStateAfter, trace bool) (rootHash []byte, err error) {
	rootHash, branchNodeUpdates, err := a.commitment.ComputeCommitment(trace)
	if err != nil {
		return nil, err
	}
	if a.seekTxNum > a.txNum {
		saveStateAfter = false
	}

	for pref, update := range branchNodeUpdates {
		prefix := []byte(pref)

		stateValue, err := a.defaultCtx.ReadCommitment(prefix, a.rwTx)
		if err != nil {
			return nil, err
		}

		stated := commitment.BranchData(stateValue)
		merged, err := a.commitment.branchMerger.Merge(stated, update)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(stated, merged) {
			continue
		}
		if trace {
			fmt.Printf("computeCommitment merge [%x] [%x]+[%x]=>[%x]\n", prefix, stated, update, merged)
		}
		if err = a.UpdateCommitmentData(prefix, merged); err != nil {
			return nil, err
		}
	}

	if saveStateAfter {
		if err := a.commitment.storeCommitmentState(a.blockNum, a.txNum); err != nil {
			return nil, err
		}
	}

	return rootHash, nil
}

func (a *Aggregator) ReadyToFinishTx() bool {
	return (a.txNum+1)%a.aggregationStep == 0 && a.seekTxNum < a.txNum
}

func (a *Aggregator) SetCommitFn(fn func(txNum uint64) error) {
	a.commitFn = fn
}

func (a *Aggregator) FinishTx() error {
	atomic.AddUint64(&a.stats.TxCount, 1)

	if !a.ReadyToFinishTx() {
		return nil
	}
	_, err := a.ComputeCommitment(true, false)
	if err != nil {
		return err
	}
	step := a.txNum / a.aggregationStep
	if step == 0 {
		if a.commitFn != nil {
			if err := a.commitFn(a.txNum); err != nil {
				return fmt.Errorf("aggregator: db commit on finishTx failed, txNum=%d err=%w", a.txNum, err)
			}
		}
		return nil
	}
	step-- // Leave one step worth in the DB
	if err := a.Flush(); err != nil {
		return err
	}

	ctx := context.Background()
	if err := a.aggregate(ctx, step); err != nil {
		return err
	}

	if a.commitFn != nil {
		if err := a.commitFn(a.txNum); err != nil {
			return err
		}
	}

	//a.defaultCtx = a.MakeContext()

	return nil
}

func (a *Aggregator) UpdateAccountData(addr []byte, account []byte) error {
	a.commitment.TouchPlainKey(addr, account, a.commitment.TouchPlainKeyAccount)
	return a.accounts.Put(addr, nil, account)
}

func (a *Aggregator) UpdateAccountCode(addr []byte, code []byte) error {
	a.commitment.TouchPlainKey(addr, code, a.commitment.TouchPlainKeyCode)
	if len(code) == 0 {
		return a.code.Delete(addr, nil)
	}
	return a.code.Put(addr, nil, code)
}

func (a *Aggregator) UpdateCommitmentData(prefix []byte, code []byte) error {
	return a.commitment.Put(prefix, nil, code)
}

func (a *Aggregator) DeleteAccount(addr []byte) error {
	a.commitment.TouchPlainKey(addr, nil, a.commitment.TouchPlainKeyAccount)

	if err := a.accounts.Delete(addr, nil); err != nil {
		return err
	}
	if err := a.code.Delete(addr, nil); err != nil {
		return err
	}
	var e error
	if err := a.storage.defaultDc.IteratePrefix(addr, func(k, _ []byte) {
		a.commitment.TouchPlainKey(k, nil, a.commitment.TouchPlainKeyStorage)
		if e == nil {
			e = a.storage.Delete(k, nil)
		}
	}); err != nil {
		return err
	}
	return e
}

func (a *Aggregator) WriteAccountStorage(addr, loc []byte, value []byte) error {
	composite := make([]byte, len(addr)+len(loc))
	copy(composite, addr)
	copy(composite[length.Addr:], loc)

	a.commitment.TouchPlainKey(composite, value, a.commitment.TouchPlainKeyStorage)
	if len(value) == 0 {
		return a.storage.Delete(addr, loc)
	}
	return a.storage.Put(addr, loc, value)
}

func (a *Aggregator) AddTraceFrom(addr []byte) error {
	return a.tracesFrom.Add(addr)
}

func (a *Aggregator) AddTraceTo(addr []byte) error {
	return a.tracesTo.Add(addr)
}

func (a *Aggregator) AddLogAddr(addr []byte) error {
	return a.logAddrs.Add(addr)
}

func (a *Aggregator) AddLogTopic(topic []byte) error {
	return a.logTopics.Add(topic)
}

func (ac *AggregatorContext) LogAddrIterator(addr []byte, startTxNum, endTxNum uint64, roTx kv.Tx) InvertedIterator {
	return ac.logAddrs.IterateRange(addr, startTxNum, endTxNum, roTx)
}

func (ac *AggregatorContext) LogTopicIterator(topic []byte, startTxNum, endTxNum uint64, roTx kv.Tx) InvertedIterator {
	return ac.logTopics.IterateRange(topic, startTxNum, endTxNum, roTx)
}

func (ac *AggregatorContext) TraceFromIterator(addr []byte, startTxNum, endTxNum uint64, roTx kv.Tx) InvertedIterator {
	return ac.tracesFrom.IterateRange(addr, startTxNum, endTxNum, roTx)
}

func (ac *AggregatorContext) TraceToIterator(addr []byte, startTxNum, endTxNum uint64, roTx kv.Tx) InvertedIterator {
	return ac.tracesTo.IterateRange(addr, startTxNum, endTxNum, roTx)
}

// StartWrites - pattern: `defer agg.StartWrites().FinishWrites()`
func (a *Aggregator) StartWrites() *Aggregator {
	a.accounts.StartWrites(a.tmpdir)
	a.storage.StartWrites(a.tmpdir)
	a.code.StartWrites(a.tmpdir)
	a.commitment.StartWrites(a.tmpdir)
	a.logAddrs.StartWrites(a.tmpdir)
	a.logTopics.StartWrites(a.tmpdir)
	a.tracesFrom.StartWrites(a.tmpdir)
	a.tracesTo.StartWrites(a.tmpdir)
	return a
}
func (a *Aggregator) FinishWrites() {
	a.accounts.FinishWrites()
	a.storage.FinishWrites()
	a.code.FinishWrites()
	a.commitment.FinishWrites()
	a.logAddrs.FinishWrites()
	a.logTopics.FinishWrites()
	a.tracesFrom.FinishWrites()
	a.tracesTo.FinishWrites()
}

// Flush - must be called before Collate, if you did some writes
func (a *Aggregator) Flush() error {
	// TODO: Add support of commitment!
	flushers := []flusher{
		a.accounts.Rotate(),
		a.storage.Rotate(),
		a.code.Rotate(),
		a.commitment.Domain.Rotate(),
		a.logAddrs.Rotate(),
		a.logTopics.Rotate(),
		a.tracesFrom.Rotate(),
		a.tracesTo.Rotate(),
	}
	defer func(t time.Time) { log.Debug("[snapshots] history flush", "took", time.Since(t)) }(time.Now())
	for _, f := range flushers {
		if err := f.Flush(a.rwTx); err != nil {
			return err
		}
	}
	return nil
}

type FilesStats struct {
	TxCount    uint64
	FilesCount uint64
	IdxSize    uint64
	DataSize   uint64
}

func (a *Aggregator) Stats() FilesStats {
	res := a.stats
	stat := a.GetAndResetStats()
	res.IdxSize = stat.IndexSize
	res.DataSize = stat.DataSize
	res.FilesCount = stat.FilesCount
	return res
}

type AggregatorContext struct {
	a          *Aggregator
	accounts   *DomainContext
	storage    *DomainContext
	code       *DomainContext
	commitment *DomainContext
	logAddrs   *InvertedIndexContext
	logTopics  *InvertedIndexContext
	tracesFrom *InvertedIndexContext
	tracesTo   *InvertedIndexContext
	keyBuf     []byte
}

func (a *Aggregator) MakeContext() *AggregatorContext {
	return &AggregatorContext{
		a:          a,
		accounts:   a.accounts.MakeContext(),
		storage:    a.storage.MakeContext(),
		code:       a.code.MakeContext(),
		commitment: a.commitment.MakeContext(),
		logAddrs:   a.logAddrs.MakeContext(),
		logTopics:  a.logTopics.MakeContext(),
		tracesFrom: a.tracesFrom.MakeContext(),
		tracesTo:   a.tracesTo.MakeContext(),
	}
}

func DecodeAccountBytes(enc []byte) (nonce uint64, balance *uint256.Int, hash []byte) {
	balance = new(uint256.Int)

	if len(enc) > 0 {
		pos := 0
		nonceBytes := int(enc[pos])
		pos++
		if nonceBytes > 0 {
			nonce = bytesToUint64(enc[pos : pos+nonceBytes])
			pos += nonceBytes
		}
		balanceBytes := int(enc[pos])
		pos++
		if balanceBytes > 0 {
			balance.SetBytes(enc[pos : pos+balanceBytes])
			pos += balanceBytes
		}
		codeHashBytes := int(enc[pos])
		pos++
		if codeHashBytes > 0 {
			codeHash := make([]byte, length.Hash)
			copy(codeHash, enc[pos:pos+codeHashBytes])
		}
	}
	return
}

func EncodeAccountBytes(nonce uint64, balance *uint256.Int, hash []byte, incarnation uint64) []byte {
	l := int(1)
	if nonce > 0 {
		l += (bits.Len64(nonce) + 7) / 8
	}
	l++
	if !balance.IsZero() {
		l += balance.ByteLen()
	}
	l++
	if len(hash) == length.Hash {
		l += 32
	}
	l++
	if incarnation > 0 {
		l += (bits.Len64(incarnation) + 7) / 8
	}
	value := make([]byte, l)
	pos := 0

	if nonce == 0 {
		value[pos] = 0
		pos++
	} else {
		nonceBytes := (bits.Len64(nonce) + 7) / 8
		value[pos] = byte(nonceBytes)
		var nonce = nonce
		for i := nonceBytes; i > 0; i-- {
			value[pos+i] = byte(nonce)
			nonce >>= 8
		}
		pos += nonceBytes + 1
	}
	if balance.IsZero() {
		value[pos] = 0
		pos++
	} else {
		balanceBytes := balance.ByteLen()
		value[pos] = byte(balanceBytes)
		pos++
		balance.WriteToSlice(value[pos : pos+balanceBytes])
		pos += balanceBytes
	}
	if len(hash) == 0 {
		value[pos] = 0
		pos++
	} else {
		value[pos] = 32
		pos++
		copy(value[pos:pos+32], hash[:])
		pos += 32
	}
	if incarnation == 0 {
		value[pos] = 0
	} else {
		incBytes := (bits.Len64(incarnation) + 7) / 8
		value[pos] = byte(incBytes)
		var inc = incarnation
		for i := incBytes; i > 0; i-- {
			value[pos+i] = byte(inc)
			inc >>= 8
		}
	}
	return value
}
