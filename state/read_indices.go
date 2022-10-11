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
	"fmt"
	"math"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
)

type ReadIndices struct {
	aggregationStep uint64
	accounts        *InvertedIndex
	storage         *InvertedIndex
	code            *InvertedIndex
	txNum           uint64
	rwTx            kv.RwTx
	keyBuf          []byte
}

func NewReadIndices(
	dir string,
	aggregationStep uint64,
) (*ReadIndices, error) {
	ri := &ReadIndices{
		aggregationStep: aggregationStep,
	}
	closeIndices := true
	defer func() {
		if closeIndices {
			ri.Close()
		}
	}()
	var err error
	if ri.accounts, err = NewInvertedIndex(dir, aggregationStep, "raccounts", kv.RAccountKeys, kv.RAccountIdx); err != nil {
		return nil, err
	}
	if ri.storage, err = NewInvertedIndex(dir, aggregationStep, "rstorage", kv.RStorageKeys, kv.RStorageIdx); err != nil {
		return nil, err
	}
	if ri.code, err = NewInvertedIndex(dir, aggregationStep, "rcode", kv.RCodeKeys, kv.RCodeIdx); err != nil {
		return nil, err
	}
	closeIndices = false
	return ri, nil
}

func (ri *ReadIndices) Close() {
	if ri.accounts != nil {
		ri.accounts.Close()
	}
	if ri.storage != nil {
		ri.storage.Close()
	}
	if ri.code != nil {
		ri.code.Close()
	}
}

func (ri *ReadIndices) SetTx(tx kv.RwTx) {
	ri.rwTx = tx
	ri.accounts.SetTx(tx)
	ri.storage.SetTx(tx)
	ri.code.SetTx(tx)
}

func (ri *ReadIndices) SetTxNum(txNum uint64) {
	ri.txNum = txNum
	ri.accounts.SetTxNum(txNum)
	ri.storage.SetTxNum(txNum)
	ri.code.SetTxNum(txNum)
}

type RCollation struct {
	accounts map[string]*roaring64.Bitmap
	storage  map[string]*roaring64.Bitmap
	code     map[string]*roaring64.Bitmap
}

func (c RCollation) Close() {
}

func (ri *ReadIndices) collate(step uint64, txFrom, txTo uint64, roTx kv.Tx) (RCollation, error) {
	var c RCollation
	var err error
	closeColl := true
	defer func() {
		if closeColl {
			c.Close()
		}
	}()
	if c.accounts, err = ri.accounts.collate(txFrom, txTo, roTx); err != nil {
		return RCollation{}, err
	}
	if c.storage, err = ri.storage.collate(txFrom, txTo, roTx); err != nil {
		return RCollation{}, err
	}
	if c.code, err = ri.code.collate(txFrom, txTo, roTx); err != nil {
		return RCollation{}, err
	}
	closeColl = false
	return c, nil
}

type RStaticFiles struct {
	accounts InvertedFiles
	storage  InvertedFiles
	code     InvertedFiles
}

func (sf RStaticFiles) Close() {
	sf.accounts.Close()
	sf.storage.Close()
	sf.code.Close()
}

func (ri *ReadIndices) buildFiles(step uint64, collation RCollation) (RStaticFiles, error) {
	var sf RStaticFiles
	closeFiles := true
	defer func() {
		if closeFiles {
			sf.Close()
		}
	}()
	var wg sync.WaitGroup
	wg.Add(3)
	errCh := make(chan error, 3)
	go func() {
		defer wg.Done()
		var err error
		if sf.accounts, err = ri.accounts.buildFiles(step, collation.accounts); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if sf.storage, err = ri.storage.buildFiles(step, collation.storage); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if sf.code, err = ri.code.buildFiles(step, collation.code); err != nil {
			errCh <- err
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
	return sf, lastError
}

func (ri *ReadIndices) integrateFiles(sf RStaticFiles, txNumFrom, txNumTo uint64) {
	ri.accounts.integrateFiles(sf.accounts, txNumFrom, txNumTo)
	ri.storage.integrateFiles(sf.storage, txNumFrom, txNumTo)
	ri.code.integrateFiles(sf.code, txNumFrom, txNumTo)
}

func (ri *ReadIndices) prune(step uint64, txFrom, txTo uint64) error {
	if err := ri.accounts.prune(txFrom, txTo, math.MaxUint64); err != nil {
		return err
	}
	if err := ri.storage.prune(txFrom, txTo, math.MaxUint64); err != nil {
		return err
	}
	if err := ri.code.prune(txFrom, txTo, math.MaxUint64); err != nil {
		return err
	}
	return nil
}

func (ri *ReadIndices) endTxNumMinimax() uint64 {
	min := ri.accounts.endTxNumMinimax()
	if txNum := ri.storage.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	if txNum := ri.code.endTxNumMinimax(); txNum < min {
		min = txNum
	}
	return min
}

type RRanges struct {
	accountsStartTxNum, accountsEndTxNum uint64
	accounts                             bool
	storageStartTxNum, storageEndTxNum   uint64
	storage                              bool
	codeStartTxNum, codeEndTxNum         uint64
	code                                 bool
}

func (r RRanges) any() bool {
	return r.accounts || r.storage || r.code
}

func (ri *ReadIndices) findMergeRange(maxEndTxNum, maxSpan uint64) RRanges {
	var r RRanges
	r.accounts, r.accountsStartTxNum, r.accountsEndTxNum = ri.accounts.findMergeRange(maxEndTxNum, maxSpan)
	r.storage, r.storageStartTxNum, r.storageEndTxNum = ri.storage.findMergeRange(maxEndTxNum, maxSpan)
	r.code, r.codeStartTxNum, r.codeEndTxNum = ri.code.findMergeRange(maxEndTxNum, maxSpan)
	log.Info(fmt.Sprintf("findMergeRange(%d, %d)=%+v\n", maxEndTxNum, maxSpan, r))
	return r
}

type RSelectedStaticFiles struct {
	accounts  []*filesItem
	accountsI int
	storage   []*filesItem
	storageI  int
	code      []*filesItem
	codeI     int
}

func (sf RSelectedStaticFiles) Close() {
	for _, group := range [][]*filesItem{sf.accounts, sf.storage, sf.code} {
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

func (ri *ReadIndices) staticFilesInRange(r RRanges) RSelectedStaticFiles {
	var sf RSelectedStaticFiles
	if r.accounts {
		sf.accounts, sf.accountsI = ri.accounts.staticFilesInRange(r.accountsStartTxNum, r.accountsEndTxNum)
	}
	if r.storage {
		sf.storage, sf.storageI = ri.storage.staticFilesInRange(r.storageStartTxNum, r.storageEndTxNum)
	}
	if r.code {
		sf.code, sf.codeI = ri.code.staticFilesInRange(r.codeStartTxNum, r.codeEndTxNum)
	}
	return sf
}

type RMergedFiles struct {
	accounts *filesItem
	storage  *filesItem
	code     *filesItem
}

func (mf RMergedFiles) Close() {
	for _, item := range []*filesItem{mf.accounts, mf.storage, mf.code} {
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

func (ri *ReadIndices) mergeFiles(files RSelectedStaticFiles, r RRanges, maxSpan uint64) (RMergedFiles, error) {
	var mf RMergedFiles
	closeFiles := true
	defer func() {
		if closeFiles {
			mf.Close()
		}
	}()
	var wg sync.WaitGroup
	wg.Add(3)
	errCh := make(chan error, 3)
	go func() {
		defer wg.Done()
		var err error
		if r.accounts {
			if mf.accounts, err = ri.accounts.mergeFiles(files.accounts, r.accountsStartTxNum, r.accountsEndTxNum, maxSpan); err != nil {
				errCh <- err
			}
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if r.storage {
			if mf.storage, err = ri.storage.mergeFiles(files.storage, r.storageStartTxNum, r.storageEndTxNum, maxSpan); err != nil {
				errCh <- err
			}
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if r.code {
			if mf.code, err = ri.code.mergeFiles(files.code, r.codeStartTxNum, r.codeEndTxNum, maxSpan); err != nil {
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

func (ri *ReadIndices) integrateMergedFiles(outs RSelectedStaticFiles, in RMergedFiles) {
	ri.accounts.integrateMergedFiles(outs.accounts, in.accounts)
	ri.storage.integrateMergedFiles(outs.storage, in.storage)
	ri.code.integrateMergedFiles(outs.code, in.code)
}

func (ri *ReadIndices) deleteFiles(outs RSelectedStaticFiles) error {
	if err := ri.accounts.deleteFiles(outs.accounts); err != nil {
		return err
	}
	if err := ri.storage.deleteFiles(outs.storage); err != nil {
		return err
	}
	if err := ri.code.deleteFiles(outs.code); err != nil {
		return err
	}
	return nil
}

func (ri *ReadIndices) ReadAccountData(addr []byte) error {
	return ri.accounts.Add(addr)
}

func (ri *ReadIndices) ReadAccountStorage(addr []byte, loc []byte) error {
	if cap(ri.keyBuf) < len(addr)+len(loc) {
		ri.keyBuf = make([]byte, len(addr)+len(loc))
	} else if len(ri.keyBuf) != len(addr)+len(loc) {
		ri.keyBuf = ri.keyBuf[:len(addr)+len(loc)]
	}
	copy(ri.keyBuf, addr)
	copy(ri.keyBuf[len(addr):], loc)
	return ri.storage.Add(ri.keyBuf)
}

func (ri *ReadIndices) ReadAccountCode(addr []byte) error {
	return ri.code.Add(addr)
}

func (ri *ReadIndices) ReadAccountCodeSize(addr []byte) error {
	return ri.code.Add(addr)
}

func (ri *ReadIndices) FinishTx() error {
	if (ri.txNum+1)%ri.aggregationStep != 0 {
		return nil
	}
	closeAll := true
	step := ri.txNum / ri.aggregationStep
	collation, err := ri.collate(step, step*ri.aggregationStep, (step+1)*ri.aggregationStep, ri.rwTx)
	if err != nil {
		return err
	}
	defer func() {
		if closeAll {
			collation.Close()
		}
	}()
	sf, err := ri.buildFiles(step, collation)
	if err != nil {
		return err
	}
	defer func() {
		if closeAll {
			sf.Close()
		}
	}()
	ri.integrateFiles(sf, step*ri.aggregationStep, (step+1)*ri.aggregationStep)
	if err = ri.prune(step, step*ri.aggregationStep, (step+1)*ri.aggregationStep); err != nil {
		return err
	}
	maxEndTxNum := ri.endTxNumMinimax()
	maxSpan := uint64(32) * ri.aggregationStep
	for r := ri.findMergeRange(maxEndTxNum, maxSpan); r.any(); r = ri.findMergeRange(maxEndTxNum, maxSpan) {
		outs := ri.staticFilesInRange(r)
		defer func() {
			if closeAll {
				outs.Close()
			}
		}()
		in, err := ri.mergeFiles(outs, r, maxSpan)
		if err != nil {
			return err
		}
		defer func() {
			if closeAll {
				in.Close()
			}
		}()
		ri.integrateMergedFiles(outs, in)
		if err = ri.deleteFiles(outs); err != nil {
			return err
		}
	}
	closeAll = false
	return nil
}
