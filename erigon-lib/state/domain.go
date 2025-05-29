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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/version"

	btree2 "github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/datastruct/existence"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
)

var (
	asserts          = dbg.EnvBool("AGG_ASSERTS", false)
	traceFileLife    = dbg.EnvString("AGG_TRACE_FILE_LIFE", "")
	traceGetAsOf     = dbg.EnvString("AGG_TRACE_GET_AS_OF", "")
	tracePutWithPrev = dbg.EnvString("AGG_TRACE_PUT_WITH_PREV", "")
)
var traceGetLatest, _ = kv.String2Domain(dbg.EnvString("AGG_TRACE_GET_LATEST", ""))

// Domain is a part of the state (examples are Accounts, Storage, Code)
// Domain should not have any go routines or locks
//
// Data-Existence in .kv vs .v files:
//  1. key doesn’t exist, then create: .kv - yes, .v - yes
//  2. acc exists, then update/delete: .kv - yes, .v - yes
//  3. acc doesn’t exist, then delete: .kv - no,  .v - no
type Domain struct {
	domainCfg // keep it above *History to avoid unexpected shadowing
	*History

	// Schema:
	//  - .kv - key -> value
	//  - .bt - key -> offset index
	//  - .kvei - key -> existence (bloom filter)

	// dirtyFiles - list of ALL files - including: un-indexed-yet, garbage, merged-into-bigger-one, ...
	// thread-safe, but maybe need 1 RWLock for all trees in Aggregator
	//
	// `_visible.files` derivative from field `file`, but without garbage:
	//  - no files with `canDelete=true`
	//  - no overlaps
	//  - no un-indexed files (`power-off` may happen between .ef and .efi creation)
	//
	// BeginRo() using _visible in zero-copy way
	dirtyFiles *btree2.BTreeG[*filesItem]

	// _visible - underscore in name means: don't use this field directly, use BeginFilesRo()
	// underlying array is immutable - means it's ready for zero-copy use
	_visible *domainVisible

	checker *DependencyIntegrityChecker
}

type domainCfg struct {
	hist histCfg

	name        kv.Domain
	Compression seg.FileCompression
	CompressCfg seg.Cfg
	Accessors   Accessors // list of indexes for given domain
	valuesTable string    // bucket to store domain values; key -> inverted_step + values (Dupsort)
	largeValues bool

	crossDomainIntegrity rangeDomainIntegrityChecker

	// replaceKeysInValues allows to replace commitment branch values with shorter keys.
	// for commitment domain only
	replaceKeysInValues bool

	version DomainVersionTypes
}

func (d domainCfg) GetVersions() VersionTypes {
	return VersionTypes{
		Domain: &d.version,
		Hist:   &d.hist.version,
		II:     &d.hist.iiCfg.version,
	}
}

type domainVisible struct {
	files  []visibleFile
	name   kv.Domain
	caches *sync.Pool
}

func NewDomain(cfg domainCfg, aggStep uint64, logger log.Logger) (*Domain, error) {
	if cfg.hist.iiCfg.dirs.SnapDomain == "" {
		panic("assert: empty `dirs`")
	}
	if cfg.hist.iiCfg.filenameBase == "" {
		panic("assert: emtpy `filenameBase`" + cfg.name.String())
	}

	d := &Domain{
		domainCfg:  cfg,
		dirtyFiles: btree2.NewBTreeGOptions[*filesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		_visible:   newDomainVisible(cfg.name, []visibleFile{}),
	}

	var err error
	if d.History, err = NewHistory(cfg.hist, aggStep, logger); err != nil {
		return nil, err
	}

	if d.version.DataKV.IsZero() {
		panic(fmt.Errorf("assert: forgot to set version of %s", d.name))
	}
	if d.Accessors.Has(AccessorBTree) && d.version.AccessorBT.IsZero() {
		panic(fmt.Errorf("assert: forgot to set version of %s", d.name))
	}
	if d.Accessors.Has(AccessorHashMap) && d.version.AccessorKVI.IsZero() {
		panic(fmt.Errorf("assert: forgot to set version of %s", d.name))
	}
	if d.Accessors.Has(AccessorExistence) && d.version.AccessorKVEI.IsZero() {
		panic(fmt.Errorf("assert: forgot to set version of %s", d.name))
	}

	return d, nil
}
func (d *Domain) SetDependency(checker *DependencyIntegrityChecker) {
	d.checker = checker
}

func (d *Domain) kvFilePath(fromStep, toStep uint64) string {
	return filepath.Join(d.dirs.SnapDomain, fmt.Sprintf("%s-%s.%d-%d.kv", d.version.DataKV.String(), d.filenameBase, fromStep, toStep))
}
func (d *Domain) kviAccessorFilePath(fromStep, toStep uint64) string {
	return filepath.Join(d.dirs.SnapDomain, fmt.Sprintf("%s-%s.%d-%d.kvi", d.version.AccessorKVI.String(), d.filenameBase, fromStep, toStep))
}
func (d *Domain) kvExistenceIdxFilePath(fromStep, toStep uint64) string {
	return filepath.Join(d.dirs.SnapDomain, fmt.Sprintf("%s-%s.%d-%d.kvei", d.version.AccessorKVEI.String(), d.filenameBase, fromStep, toStep))
}
func (d *Domain) kvBtAccessorFilePath(fromStep, toStep uint64) string {
	return filepath.Join(d.dirs.SnapDomain, fmt.Sprintf("%s-%s.%d-%d.bt", d.version.AccessorBT.String(), d.filenameBase, fromStep, toStep))
}

func (d *Domain) kvFilePathMask(fromStep, toStep uint64) string {
	return filepath.Join(d.dirs.SnapDomain, fmt.Sprintf("*-%s.%d-%d.kv", d.filenameBase, fromStep, toStep))
}
func (d *Domain) kviAccessorFilePathMask(fromStep, toStep uint64) string {
	return filepath.Join(d.dirs.SnapDomain, fmt.Sprintf("*-%s.%d-%d.kvi", d.filenameBase, fromStep, toStep))
}
func (d *Domain) kvExistenceIdxFilePathMask(fromStep, toStep uint64) string {
	return filepath.Join(d.dirs.SnapDomain, fmt.Sprintf("*-%s.%d-%d.kvei", d.filenameBase, fromStep, toStep))
}
func (d *Domain) kvBtAccessorFilePathMask(fromStep, toStep uint64) string {
	return filepath.Join(d.dirs.SnapDomain, fmt.Sprintf("*-%s.%d-%d.bt", d.filenameBase, fromStep, toStep))
}

// maxStepInDB - return the latest available step in db (at-least 1 value in such step)
func (d *Domain) maxStepInDB(tx kv.Tx) (lstInDb uint64) {
	lstIdx, _ := kv.LastKey(tx, d.History.keysTable)
	if len(lstIdx) == 0 {
		return 0
	}
	return binary.BigEndian.Uint64(lstIdx) / d.aggregationStep
}

// maxStepInDBNoHistory - return latest available step in db (at-least 1 value in such step)
// Does not use history table to find the latest step
func (d *Domain) maxStepInDBNoHistory(tx kv.Tx) (lstInDb uint64) {
	lstIdx, err := kv.FirstKey(tx, d.valuesTable)
	if err != nil {
		d.logger.Warn("Domain.maxStepInDBNoHistory:", "FirstKey", lstIdx, "err", err)
		return 0
	}
	if len(lstIdx) == 0 {
		return 0
	}
	if d.largeValues {
		return (^binary.BigEndian.Uint64(lstIdx[len(lstIdx)-8:])) / d.aggregationStep
	}
	lstVal, err := tx.GetOne(d.valuesTable, lstIdx)
	if err != nil {
		d.logger.Warn("Domain.maxStepInDBNoHistory:", "GetOne", lstIdx, "err", err)
		return 0
	}

	return ^binary.BigEndian.Uint64(lstVal)
}

func (d *Domain) minStepInDB(tx kv.Tx) (lstInDb uint64) {
	lstIdx, _ := kv.FirstKey(tx, d.History.keysTable)
	if len(lstIdx) == 0 {
		return 0
	}
	return binary.BigEndian.Uint64(lstIdx) / d.aggregationStep
}

func (dt *DomainRoTx) NewWriter() *DomainBufferedWriter { return dt.newWriter(dt.d.dirs.Tmp, false) }

// OpenList - main method to open list of files.
// It's ok if some files was open earlier.
// If some file already open: noop.
// If some file already open but not in provided list: close and remove from `files` field.
func (d *Domain) OpenList(idxFiles, histFiles, domainFiles []string) error {
	if err := d.History.openList(idxFiles, histFiles); err != nil {
		return err
	}

	d.closeWhatNotInList(domainFiles)
	d.scanDirtyFiles(domainFiles)
	if err := d.openDirtyFiles(); err != nil {
		return fmt.Errorf("Domain(%s).openList: %w", d.filenameBase, err)
	}
	d.protectFromHistoryFilesAheadOfDomainFiles()
	return nil
}

// protectFromHistoryFilesAheadOfDomainFiles - in some corner-cases app may see more .ef/.v files than .kv:
//   - `kill -9` in the middle of `buildFiles()`, then `rm -f db` (restore from backup)
//   - `kill -9` in the middle of `buildFiles()`, then `stage_exec --reset` (drop progress - as a hot-fix)
func (d *Domain) protectFromHistoryFilesAheadOfDomainFiles() {
	d.closeFilesAfterStep(d.dirtyFilesEndTxNumMinimax() / d.aggregationStep)
}

func (d *Domain) openFolder() error {
	if d.disable {
		return nil
	}

	idx, histFiles, domainFiles, err := d.fileNamesOnDisk()
	if err != nil {
		return fmt.Errorf("Domain(%s).openFolder: %w", d.filenameBase, err)
	}
	if err := d.OpenList(idx, histFiles, domainFiles); err != nil {
		return err
	}
	return nil
}

func (d *Domain) closeFilesAfterStep(lowerBound uint64) {
	var toClose []*filesItem
	d.dirtyFiles.Scan(func(item *filesItem) bool {
		if item.startTxNum/d.aggregationStep >= lowerBound {
			toClose = append(toClose, item)
		}
		return true
	})
	for _, item := range toClose {
		d.dirtyFiles.Delete(item)
		fName := ""
		if item.decompressor != nil {
			fName = item.decompressor.FileName()
		}
		log.Debug(fmt.Sprintf("[snapshots] closing %s, because step %d was not complete", fName, lowerBound))
		item.closeFiles()
	}

	toClose = toClose[:0]
	d.History.dirtyFiles.Scan(func(item *filesItem) bool {
		if item.startTxNum/d.aggregationStep >= lowerBound {
			toClose = append(toClose, item)
		}
		return true
	})
	for _, item := range toClose {
		d.History.dirtyFiles.Delete(item)
		fName := ""
		if item.decompressor != nil {
			fName = item.decompressor.FileName()
		}
		log.Debug(fmt.Sprintf("[snapshots] closing %s, because step %d was not complete", fName, lowerBound))
		item.closeFiles()
	}

	toClose = toClose[:0]
	d.History.InvertedIndex.dirtyFiles.Scan(func(item *filesItem) bool {
		if item.startTxNum/d.aggregationStep >= lowerBound {
			toClose = append(toClose, item)
		}
		return true
	})
	for _, item := range toClose {
		d.History.InvertedIndex.dirtyFiles.Delete(item)
		fName := ""
		if item.decompressor != nil {
			fName = item.decompressor.FileName()
		}
		log.Debug(fmt.Sprintf("[snapshots] closing %s, because step %d was not complete", fName, lowerBound))
		item.closeFiles()
	}
}

func (d *Domain) scanDirtyFiles(fileNames []string) (garbageFiles []*filesItem) {
	if d.filenameBase == "" {
		panic("assert: empty `filenameBase`")
	}
	l := scanDirtyFiles(fileNames, d.aggregationStep, d.filenameBase, "kv", d.logger)
	for _, dirtyFile := range l {
		//startStep, endStep := dirtyFile.startTxNum/d.aggregationStep, dirtyFile.endTxNum/d.aggregationStep
		//domainName, _ := kv.String2Domain(d.filenameBase)
		//if d.crossDomainIntegrity != nil && !d.crossDomainIntegrity(domainName, d.dirs, startStep, endStep) {
		//	d.logger.Debug("[agg] skip garbage file", "name", d.filenameBase, "startStep", startStep, "endStep", endStep)
		//	continue
		//}
		dirtyFile.frozen = false

		if _, has := d.dirtyFiles.Get(dirtyFile); !has {
			d.dirtyFiles.Set(dirtyFile)
		}
	}
	return garbageFiles
}

func (d *Domain) openDirtyFiles() (err error) {
	invalidFileItems := make([]*filesItem, 0)
	invalidFileItemsLock := sync.Mutex{}
	d.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
			if item.decompressor == nil {
				fPathMask := d.kvFilePathMask(fromStep, toStep)
				fPath, fileVer, ok, err := version.FindFilesWithVersionsByPattern(fPathMask)
				if err != nil {
					_, fName := filepath.Split(fPath)
					d.logger.Debug("[agg] Domain.openDirtyFiles: FileExist err", "f", fName, "err", err)
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					continue
				}
				if !ok {
					_, fName := filepath.Split(fPath)
					d.logger.Debug("[agg] Domain.openDirtyFiles: file does not exists", "f", fName)
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					continue
				}

				if !fileVer.Eq(d.version.DataKV.Current) {
					if !fileVer.Less(d.version.DataKV.MinSupported) {
						d.version.DataKV.Current = fileVer
					} else {
						panic("Version is too low, try to rm kv domain snapshots")
						//return false
					}
				}

				if item.decompressor, err = seg.NewDecompressor(fPath); err != nil {
					_, fName := filepath.Split(fPath)
					if errors.Is(err, &seg.ErrCompressedFileCorrupted{}) {
						d.logger.Debug("[agg] Domain.openDirtyFiles", "err", err, "f", fName)
					} else {
						d.logger.Warn("[agg] Domain.openDirtyFiles", "err", err, "f", fName)
					}
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					// don't interrupt on error. other files may be good. but skip indices open.
					continue
				}
			}

			if item.index == nil && d.Accessors.Has(AccessorHashMap) {
				fPathMask := d.kviAccessorFilePathMask(fromStep, toStep)
				fPath, fileVer, ok, err := version.FindFilesWithVersionsByPattern(fPathMask)
				if err != nil {
					_, fName := filepath.Split(fPath)
					d.logger.Warn("[agg] Domain.openDirtyFiles", "err", err, "f", fName)
				}
				if ok {
					if !fileVer.Eq(d.version.AccessorKVI.Current) {
						if !fileVer.Less(d.version.AccessorKVI.MinSupported) {
							d.version.AccessorKVI.Current = fileVer
						} else {
							panic("Version is too low, try to rm kvi domain snapshots")
							//return false
						}
					}
					if item.index, err = recsplit.OpenIndex(fPath); err != nil {
						_, fName := filepath.Split(fPath)
						d.logger.Warn("[agg] Domain.openDirtyFiles", "err", err, "f", fName)
						// don't interrupt on error. other files may be good
					}
				}
			}
			if item.bindex == nil && d.Accessors.Has(AccessorBTree) {
				fPathMask := d.kvBtAccessorFilePathMask(fromStep, toStep)
				fPath, fileVer, ok, err := version.FindFilesWithVersionsByPattern(fPathMask)
				if err != nil {
					_, fName := filepath.Split(fPath)
					d.logger.Warn("[agg] Domain.openDirtyFiles", "err", err, "f", fName)
				}
				if ok {
					if !fileVer.Eq(d.version.AccessorBT.Current) {
						if !fileVer.Less(d.version.AccessorBT.MinSupported) {
							d.version.AccessorBT.Current = fileVer
						} else {
							panic("Version is too low, try to rm bt domain snapshots")
							//return false
						}
					}
					if item.bindex, err = OpenBtreeIndexWithDecompressor(fPath, DefaultBtreeM, item.decompressor, d.Compression); err != nil {
						_, fName := filepath.Split(fPath)
						d.logger.Warn("[agg] Domain.openDirtyFiles", "err", err, "f", fName)
						// don't interrupt on error. other files may be good
					}
				}
			}
			if item.existence == nil && d.Accessors.Has(AccessorExistence) {
				fPathMask := d.kvExistenceIdxFilePathMask(fromStep, toStep)
				fPath, fileVer, ok, err := version.FindFilesWithVersionsByPattern(fPathMask)
				if err != nil {
					_, fName := filepath.Split(fPath)
					d.logger.Warn("[agg] Domain.openDirtyFiles", "err", err, "f", fName)
				}
				if ok {
					if !fileVer.Eq(d.version.AccessorKVEI.Current) {
						if !fileVer.Less(d.version.AccessorKVEI.MinSupported) {
							d.version.AccessorKVEI.Current = fileVer
						} else {
							panic("Version is too low, try to rm kvei domain snapshots")
							//return false
						}
					}
					if item.existence, err = existence.OpenFilter(fPath); err != nil {
						_, fName := filepath.Split(fPath)
						d.logger.Warn("[agg] Domain.openDirtyFiles", "err", err, "f", fName)
						// don't interrupt on error. other files may be good
					}
				}
			}
		}
		return true
	})

	for _, item := range invalidFileItems {
		item.closeFiles() // just close, not remove from disk
		d.dirtyFiles.Delete(item)
	}

	return nil
}

func (d *Domain) closeWhatNotInList(fNames []string) {
	protectFiles := make(map[string]struct{}, len(fNames))
	for _, f := range fNames {
		protectFiles[f] = struct{}{}
	}
	var toClose []*filesItem
	d.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.decompressor != nil {
				if _, ok := protectFiles[item.decompressor.FileName()]; ok {
					continue
				}
			}
			toClose = append(toClose, item)
		}
		return true
	})
	for _, item := range toClose {
		item.closeFiles()
		d.dirtyFiles.Delete(item)
	}
}

func (d *Domain) reCalcVisibleFiles(toTxNum uint64) {
	var checker func(startTxNum, endTxNum uint64) bool
	if d.checker != nil {
		checker = func(startTxNum, endTxNum uint64) bool {
			return d.checker.CheckDependentPresent(d.name, All, startTxNum, endTxNum)
		}
	}
	d._visible = newDomainVisible(d.name, calcVisibleFiles(d.dirtyFiles, d.Accessors, checker, false, toTxNum))
	d.History.reCalcVisibleFiles(toTxNum)
}

func (d *Domain) Tables() []string { return append(d.History.Tables(), d.valuesTable) }

func (d *Domain) Close() {
	if d == nil {
		return
	}
	d.History.Close()
	d.closeWhatNotInList([]string{})
}

func (w *DomainBufferedWriter) PutWithPrev(k, v []byte, txNum uint64, preval []byte, prevStep uint64) error {
	step := txNum / w.h.ii.aggregationStep
	// This call to update needs to happen before d.tx.Put() later, because otherwise the content of `preval`` slice is invalidated
	if tracePutWithPrev != "" && tracePutWithPrev == w.h.ii.filenameBase {
		fmt.Printf("PutWithPrev(%s, txn %d, key[%x] value[%x] preval[%x])\n", w.h.ii.filenameBase, step, k, v, preval)
	}
	if err := w.h.AddPrevValue(k, txNum, preval); err != nil {
		return err
	}
	if w.diff != nil {
		w.diff.DomainUpdate(k, step, preval, prevStep)
	}
	return w.addValue(k, v, step)
}

func (w *DomainBufferedWriter) DeleteWithPrev(k []byte, txNum uint64, prev []byte, prevStep uint64) (err error) {
	step := txNum / w.h.ii.aggregationStep

	// This call to update needs to happen before d.tx.Delete() later, because otherwise the content of `original`` slice is invalidated
	if tracePutWithPrev != "" && tracePutWithPrev == w.h.ii.filenameBase {
		fmt.Printf("DeleteWithPrev(%s, txn %d, key[%x] preval[%x])\n", w.h.ii.filenameBase, txNum, k, prev)
	}
	if err := w.h.AddPrevValue(k, txNum, prev); err != nil {
		return err
	}
	if w.diff != nil {
		w.diff.DomainUpdate(k, step, prev, prevStep)
	}
	return w.addValue(k, nil, step)
}

func (w *DomainBufferedWriter) SetDiff(diff *kv.DomainDiff) { w.diff = diff }

func (dt *DomainRoTx) newWriter(tmpdir string, discard bool) *DomainBufferedWriter {
	discardHistory := discard || dt.d.historyDisabled

	w := &DomainBufferedWriter{
		discard:   discard,
		aux:       make([]byte, 0, 128),
		valsTable: dt.d.valuesTable,
		largeVals: dt.d.largeValues,
		h:         dt.ht.newWriter(tmpdir, discardHistory),
		values:    etl.NewCollectorWithAllocator(dt.name.String()+"domain.flush", tmpdir, etl.SmallSortableBuffers, dt.d.logger).LogLvl(log.LvlTrace),
	}
	w.values.SortAndFlushInBackground(true)
	return w
}

type DomainBufferedWriter struct {
	values *etl.Collector

	discard bool

	valsTable string
	largeVals bool

	stepBytes [8]byte // current inverted step representation
	aux       []byte  // auxilary buffer for key1 + key2
	aux2      []byte  // auxilary buffer for step + val
	diff      *kv.DomainDiff

	h *historyBufferedWriter
}

func (w *DomainBufferedWriter) Close() {
	if w == nil { // allow dobule-close
		return
	}
	w.h.close()
	if w.values != nil {
		w.values.Close()
	}
}

// nolint
func loadSkipFunc() etl.LoadFunc {
	var preKey, preVal []byte
	return func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if bytes.Equal(k, preKey) {
			preVal = v
			return nil
		}
		if err := next(nil, preKey, preVal); err != nil {
			return err
		}
		if err := next(k, k, v); err != nil {
			return err
		}
		preKey, preVal = k, v
		return nil
	}
}
func (w *DomainBufferedWriter) Flush(ctx context.Context, tx kv.RwTx) error {
	if w.discard {
		return nil
	}
	if err := w.h.Flush(ctx, tx); err != nil {
		return err
	}

	if w.largeVals {
		if err := w.values.Load(tx, w.valsTable, loadFunc, etl.TransformArgs{Quit: ctx.Done(), EmptyVals: true}); err != nil {
			return err
		}
		w.Close()
		return nil
	}

	valuesCursor, err := tx.RwCursorDupSort(w.valsTable)
	if err != nil {
		return err
	}
	defer valuesCursor.Close()
	if err := w.values.Load(tx, w.valsTable, func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		foundVal, err := valuesCursor.SeekBothRange(k, v[:8])
		if err != nil {
			return err
		}
		if len(foundVal) == 0 || !bytes.Equal(foundVal[:8], v[:8]) {
			if err := valuesCursor.Put(k, v); err != nil {
				return err
			}
			return nil
		}
		if err := valuesCursor.DeleteCurrent(); err != nil {
			return err
		}
		if err := valuesCursor.Put(k, v); err != nil {
			return err
		}
		return nil
	}, etl.TransformArgs{Quit: ctx.Done(), EmptyVals: true}); err != nil {
		return err
	}
	w.Close()

	return nil
}

func (w *DomainBufferedWriter) addValue(k, value []byte, step uint64) error {
	if w.discard {
		return nil
	}
	binary.BigEndian.PutUint64(w.stepBytes[:], ^step)

	if w.largeVals {
		kl := len(k)
		w.aux = append(append(w.aux[:0], k...), w.stepBytes[:]...)
		fullkey := w.aux[:kl+8]
		if asserts && step != ^binary.BigEndian.Uint64(w.stepBytes[:]) {
			panic(fmt.Sprintf("assert: %d != %d", step, ^binary.BigEndian.Uint64(w.stepBytes[:])))
		}

		if err := w.values.Collect(fullkey, value); err != nil {
			return err
		}
		return nil
	}

	w.aux2 = append(append(w.aux2[:0], w.stepBytes[:]...), value...)

	if asserts && step != ^binary.BigEndian.Uint64(w.stepBytes[:]) {
		panic(fmt.Sprintf("assert: %d != %d", step, ^binary.BigEndian.Uint64(w.stepBytes[:])))
	}

	//defer func() {
	//	fmt.Printf("addValue     [%p;tx=%d] '%x' -> '%x'\n", w, w.h.ii.txNum, fullkey, value)
	//}()

	if err := w.values.Collect(k, w.aux2); err != nil {
		return err
	}
	return nil
}

// DomainRoTx allows accesing the same domain from multiple go-routines
type DomainRoTx struct {
	files   visibleFiles
	visible *domainVisible
	name    kv.Domain
	ht      *HistoryRoTx
	salt    *uint32

	d *Domain

	readers    []*BtIndex
	idxReaders []*recsplit.IndexReader

	comBuf []byte

	valsCs map[kv.Tx]kv.Cursor

	getFromFileCache *DomainGetFromFileCache
}

func domainReadMetric(name kv.Domain, level int) metrics.Summary {
	if level > 4 {
		level = 5
	}
	return mxsKVGet[name][level]
}

func (dt *DomainRoTx) getLatestFromFile(i int, filekey []byte) (v []byte, ok bool, offset uint64, err error) {
	if dbg.KVReadLevelledMetrics {
		defer domainReadMetric(dt.name, i).ObserveDuration(time.Now())
	}

	g := dt.reader(i)
	if dt.d.Accessors.Has(AccessorBTree) {
		_, v, offset, ok, err = dt.statelessBtree(i).Get(filekey, g)
		if err != nil || !ok {
			return nil, false, 0, err
		}
		//fmt.Printf("getLatestFromBtreeColdFiles key %x shard %d %x\n", filekey, exactColdShard, v)
		return v, true, offset, nil
	}
	if dt.d.Accessors.Has(AccessorHashMap) {
		reader := dt.statelessIdxReader(i)
		if reader.Empty() {
			return nil, false, 0, nil
		}
		offset, ok := reader.TwoLayerLookup(filekey)
		if !ok {
			return nil, false, 0, nil
		}
		g.Reset(offset)

		k, _ := g.Next(nil)
		if !bytes.Equal(filekey, k) { // MPH false-positives protection
			return nil, false, 0, nil
		}
		v, _ := g.Next(nil)
		return v, true, 0, nil
	}
	return nil, false, 0, errors.New("no index defined")

}

func (d *Domain) BeginFilesRo() *DomainRoTx {
	for i := 0; i < len(d._visible.files); i++ {
		if !d._visible.files[i].src.frozen {
			d._visible.files[i].src.refcount.Add(1)
		}
	}

	return &DomainRoTx{
		name:    d.name,
		d:       d,
		ht:      d.History.BeginFilesRo(),
		visible: d._visible,
		files:   d._visible.files,
		salt:    d.salt.Load(),
	}
}

// Collation is the set of compressors created after aggregation
type Collation struct {
	HistoryCollation
	valuesComp  *seg.Compressor
	valuesPath  string
	valuesCount int
}

func (c Collation) Close() {
	if c.valuesComp != nil {
		c.valuesComp.Close()
	}
	c.HistoryCollation.Close()
}

func (d *Domain) dumpStepRangeOnDisk(ctx context.Context, stepFrom, stepTo, txnFrom, txnTo uint64, wal *DomainBufferedWriter, vt valueTransformer) error {
	if d.disable || stepFrom == stepTo {
		return nil
	}
	if stepFrom > stepTo {
		panic(fmt.Errorf("assert: stepFrom=%d > stepTo=%d", stepFrom, stepTo))
	}

	coll, err := d.collateETL(ctx, stepFrom, stepTo, wal.values, vt)
	defer wal.Close()
	if err != nil {
		return err
	}
	wal.Close()

	ps := background.NewProgressSet()
	static, err := d.buildFileRange(ctx, stepFrom, stepTo, coll, ps)
	if err != nil {
		return err
	}

	d.integrateDirtyFiles(static, txnFrom, txnTo)
	// d.reCalcVisibleFiles(d.dirtyFilesEndTxNumMinimax())
	return nil
}

// [stepFrom; stepTo)
// In contrast to collate function collateETL puts contents of wal into file.
func (d *Domain) collateETL(ctx context.Context, stepFrom, stepTo uint64, wal *etl.Collector, vt valueTransformer) (coll Collation, err error) {
	if d.disable {
		return Collation{}, err
	}
	started := time.Now()
	closeCollation := true
	defer func() {
		if closeCollation {
			coll.Close()
		}
		mxCollateTook.ObserveDuration(started)
	}()

	coll.valuesPath = d.kvFilePath(stepFrom, stepTo)
	if coll.valuesComp, err = seg.NewCompressor(ctx, d.filenameBase+".domain.collate", coll.valuesPath, d.dirs.Tmp, d.CompressCfg, log.LvlTrace, d.logger); err != nil {
		return Collation{}, fmt.Errorf("create %s values compressor: %w", d.filenameBase, err)
	}

	// Don't use `d.compress` config in collate. Because collat+build must be very-very fast (to keep db small).
	// Compress files only in `merge` which ok to be slow.
	//comp := seg.NewWriter(coll.valuesComp, seg.CompressNone) //
	compress := seg.CompressNone
	if stepTo-stepFrom > DomainMinStepsToCompress {
		compress = d.Compression
	}
	comp := seg.NewWriter(coll.valuesComp, compress)

	stepBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(stepBytes, ^stepTo)

	kvs := make([]struct {
		k, v []byte
	}, 0, 128)
	var fromTxNum, endTxNum uint64 = 0, stepTo * d.aggregationStep
	if stepFrom > 0 {
		fromTxNum = (stepFrom - 1) * d.aggregationStep
	}

	//var stepInDB []byte
	err = wal.Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if d.largeValues {
			kvs = append(kvs, struct {
				k, v []byte
			}{k[:len(k)-8], v})
		} else {

			if vt != nil {
				v, err = vt(v[8:], fromTxNum, endTxNum)
				if err != nil {
					return fmt.Errorf("vt: %w", err)
				}
			} else {
				v = v[8:]
			}
			if _, err = comp.Write(k); err != nil {
				return fmt.Errorf("add %s values key [%x]: %w", d.filenameBase, k, err)
			}
			if _, err = comp.Write(v); err != nil {
				return fmt.Errorf("add %s values [%x]=>[%x]: %w", d.filenameBase, k, v, err)
			}
		}
		return nil
	}, etl.TransformArgs{Quit: ctx.Done()})

	sort.Slice(kvs, func(i, j int) bool {
		return bytes.Compare(kvs[i].k, kvs[j].k) < 0
	})
	// check if any key is duplicated
	for i := 1; i < len(kvs); i++ {
		if bytes.Equal(kvs[i].k, kvs[i-1].k) {
			return coll, fmt.Errorf("duplicate key [%x]", kvs[i].k)
		}
	}
	for _, kv := range kvs {
		if vt != nil {
			kv.v, err = vt(kv.v, fromTxNum, endTxNum)
		}
		if err != nil {
			return coll, fmt.Errorf("vt: %w", err)
		}
		if _, err = comp.Write(kv.k); err != nil {
			return coll, fmt.Errorf("add %s values key [%x]: %w", d.filenameBase, kv.k, err)
		}
		if _, err = comp.Write(kv.v); err != nil {
			return coll, fmt.Errorf("add %s values [%x]=>[%x]: %w", d.filenameBase, kv.k, kv.v, err)
		}
	}
	// could also do key squeezing

	closeCollation = false
	coll.valuesCount = coll.valuesComp.Count() / 2
	mxCollationSize.SetUint64(uint64(coll.valuesCount))
	return coll, nil
}

// collate gathers domain changes over the specified step, using read-only transaction,
// and returns compressors, elias fano, and bitmaps
// [txFrom; txTo)
func (d *Domain) collate(ctx context.Context, step, txFrom, txTo uint64, roTx kv.Tx) (coll Collation, err error) {
	if d.disable {
		return Collation{}, nil
	}

	{ //assert
		if txFrom%d.aggregationStep != 0 {
			panic(fmt.Errorf("assert: unexpected txFrom=%d", txFrom))
		}
		if txTo%d.aggregationStep != 0 {
			panic(fmt.Errorf("assert: unexpected txTo=%d", txTo))
		}
	}

	started := time.Now()
	defer func() {
		mxCollateTook.ObserveDuration(started)
	}()

	coll.HistoryCollation, err = d.History.collate(ctx, step, txFrom, txTo, roTx)
	if err != nil {
		return Collation{}, err
	}

	closeCollation := true
	defer func() {
		if closeCollation {
			coll.Close()
		}
	}()

	coll.valuesPath = d.kvFilePath(step, step+1)
	if coll.valuesComp, err = seg.NewCompressor(ctx, d.filenameBase+".domain.collate", coll.valuesPath, d.dirs.Tmp, d.CompressCfg, log.LvlTrace, d.logger); err != nil {
		return Collation{}, fmt.Errorf("create %s values compressor: %w", d.filenameBase, err)
	}

	// Don't use `d.compress` config in collate. Because collat+build must be very-very fast (to keep db small).
	// Compress files only in `merge` which ok to be slow.
	comp := seg.NewWriter(coll.valuesComp, seg.CompressNone)

	stepBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(stepBytes, ^step)

	var valsCursor kv.Cursor

	if d.largeValues {
		valsCursor, err = roTx.Cursor(d.valuesTable)
		if err != nil {
			return Collation{}, fmt.Errorf("create %s values cursorDupsort: %w", d.filenameBase, err)
		}
	} else {
		valsCursor, err = roTx.CursorDupSort(d.valuesTable)
		if err != nil {
			return Collation{}, fmt.Errorf("create %s values cursorDupsort: %w", d.filenameBase, err)
		}
	}
	defer valsCursor.Close()

	kvs := make([]struct {
		k, v []byte
	}, 0, 128)

	var stepInDB []byte
	for k, v, err := valsCursor.First(); k != nil; {
		if err != nil {
			return coll, err
		}

		if d.largeValues {
			stepInDB = k[len(k)-8:]
		} else {
			stepInDB = v[:8]
		}
		if !bytes.Equal(stepBytes, stepInDB) { // [txFrom; txTo)
			k, v, err = valsCursor.Next()
			continue
		}

		if d.largeValues {
			kvs = append(kvs, struct {
				k, v []byte
			}{k[:len(k)-8], v})
			k, v, err = valsCursor.Next()
		} else {
			if _, err = comp.Write(k); err != nil {
				return coll, fmt.Errorf("add %s values key [%x]: %w", d.filenameBase, k, err)
			}
			if _, err = comp.Write(v[8:]); err != nil {
				return coll, fmt.Errorf("add %s values [%x]=>[%x]: %w", d.filenameBase, k, v[8:], err)
			}
			k, v, err = valsCursor.(kv.CursorDupSort).NextNoDup()
		}
	}

	sort.Slice(kvs, func(i, j int) bool {
		return bytes.Compare(kvs[i].k, kvs[j].k) < 0
	})
	// check if any key is duplicated
	for i := 1; i < len(kvs); i++ {
		if bytes.Equal(kvs[i].k, kvs[i-1].k) {
			return coll, fmt.Errorf("duplicate key [%x]", kvs[i].k)
		}
	}
	for _, kv := range kvs {
		if _, err = comp.Write(kv.k); err != nil {
			return coll, fmt.Errorf("add %s values key [%x]: %w", d.filenameBase, kv.k, err)
		}
		if _, err = comp.Write(kv.v); err != nil {
			return coll, fmt.Errorf("add %s values [%x]=>[%x]: %w", d.filenameBase, kv.k, kv.v, err)
		}
	}

	closeCollation = false
	coll.valuesCount = coll.valuesComp.Count() / 2
	mxCollationSize.SetUint64(uint64(coll.valuesCount))
	return coll, nil
}

type StaticFiles struct {
	HistoryFiles
	valuesDecomp *seg.Decompressor
	valuesIdx    *recsplit.Index
	valuesBt     *BtIndex
	bloom        *existence.Filter
}

// CleanupOnError - call it on collation fail. It closing all files
func (sf StaticFiles) CleanupOnError() {
	if sf.valuesDecomp != nil {
		sf.valuesDecomp.Close()
	}
	if sf.valuesIdx != nil {
		sf.valuesIdx.Close()
	}
	if sf.valuesBt != nil {
		sf.valuesBt.Close()
	}
	if sf.bloom != nil {
		sf.bloom.Close()
	}
	sf.HistoryFiles.CleanupOnError()
}

// skips history files
func (d *Domain) buildFileRange(ctx context.Context, stepFrom, stepTo uint64, collation Collation, ps *background.ProgressSet) (StaticFiles, error) {
	if d.disable {
		return StaticFiles{}, nil
	}
	mxRunningFilesBuilding.Inc()
	defer mxRunningFilesBuilding.Dec()
	if traceFileLife != "" && d.filenameBase == traceFileLife {
		d.logger.Warn("[agg.dbg] buildFilesRange", "step", fmt.Sprintf("%d-%d", stepFrom, stepTo), "domain", d.filenameBase)
	}

	start := time.Now()
	defer func() {
		mxBuildTook.ObserveDuration(start)
	}()

	valuesComp := collation.valuesComp

	var (
		valuesDecomp *seg.Decompressor
		valuesIdx    *recsplit.Index
		bt           *BtIndex
		bloom        *existence.Filter
		err          error
	)
	closeComp := true
	defer func() {
		if closeComp {
			if valuesComp != nil {
				valuesComp.Close()
			}
			if valuesDecomp != nil {
				valuesDecomp.Close()
			}
			if valuesIdx != nil {
				valuesIdx.Close()
			}
			if bt != nil {
				bt.Close()
			}
			if bloom != nil {
				bloom.Close()
			}
		}
	}()
	if d.noFsync {
		valuesComp.DisableFsync()
	}
	if err = valuesComp.Compress(); err != nil {
		return StaticFiles{}, fmt.Errorf("compress %s values: %w", d.filenameBase, err)
	}
	valuesComp.Close()
	valuesComp = nil
	if valuesDecomp, err = seg.NewDecompressor(collation.valuesPath); err != nil {
		return StaticFiles{}, fmt.Errorf("open %s values decompressor: %w", d.filenameBase, err)
	}

	if d.Accessors.Has(AccessorHashMap) {
		if err = d.buildHashMapAccessor(ctx, stepFrom, stepTo, valuesDecomp, ps); err != nil {
			return StaticFiles{}, fmt.Errorf("build %s values idx: %w", d.filenameBase, err)
		}
		valuesIdx, err = recsplit.OpenIndex(d.kviAccessorFilePath(stepFrom, stepTo))
		if err != nil {
			return StaticFiles{}, err
		}
	}

	if d.Accessors.Has(AccessorBTree) {
		btPath := d.kvBtAccessorFilePath(stepFrom, stepTo)
		bt, err = CreateBtreeIndexWithDecompressor(btPath, DefaultBtreeM, valuesDecomp, d.Compression, *d.salt.Load(), ps, d.dirs.Tmp, d.logger, d.noFsync, d.Accessors)
		if err != nil {
			return StaticFiles{}, fmt.Errorf("build %s .bt idx: %w", d.filenameBase, err)
		}
	}
	if d.Accessors.Has(AccessorExistence) {
		fPath := d.kvExistenceIdxFilePath(stepFrom, stepTo)
		exists, err := dir.FileExist(fPath)
		if err != nil {
			return StaticFiles{}, fmt.Errorf("build %s .kvei: %w", d.filenameBase, err)
		}
		if exists {
			bloom, err = existence.OpenFilter(fPath)
			if err != nil {
				return StaticFiles{}, fmt.Errorf("build %s .kvei: %w", d.filenameBase, err)
			}
		}
	}
	closeComp = false
	return StaticFiles{
		valuesDecomp: valuesDecomp,
		valuesIdx:    valuesIdx,
		valuesBt:     bt,
		bloom:        bloom,
	}, nil
}

// buildFiles performs potentially resource intensive operations of creating
// static files and their indices
func (d *Domain) buildFiles(ctx context.Context, step uint64, collation Collation, ps *background.ProgressSet) (StaticFiles, error) {
	if d.disable {
		return StaticFiles{}, nil
	}

	mxRunningFilesBuilding.Inc()
	defer mxRunningFilesBuilding.Dec()
	if traceFileLife != "" && d.filenameBase == traceFileLife {
		d.logger.Warn("[agg.dbg] buildFiles", "step", step, "domain", d.filenameBase)
	}

	start := time.Now()
	defer func() {
		mxBuildTook.ObserveDuration(start)
	}()

	hStaticFiles, err := d.History.buildFiles(ctx, step, collation.HistoryCollation, ps)
	if err != nil {
		return StaticFiles{}, err
	}
	valuesComp := collation.valuesComp

	var (
		valuesDecomp *seg.Decompressor
		valuesIdx    *recsplit.Index
		bt           *BtIndex
		bloom        *existence.Filter
	)
	closeComp := true
	defer func() {
		if closeComp {
			hStaticFiles.CleanupOnError()
			if valuesComp != nil {
				valuesComp.Close()
			}
			if valuesDecomp != nil {
				valuesDecomp.Close()
			}
			if valuesIdx != nil {
				valuesIdx.Close()
			}
			if bt != nil {
				bt.Close()
			}
			if bloom != nil {
				bloom.Close()
			}
		}
	}()
	if d.noFsync {
		valuesComp.DisableFsync()
	}
	if err = valuesComp.Compress(); err != nil {
		return StaticFiles{}, fmt.Errorf("compress %s values: %w", d.filenameBase, err)
	}
	valuesComp.Close()
	valuesComp = nil
	if valuesDecomp, err = seg.NewDecompressor(collation.valuesPath); err != nil {
		return StaticFiles{}, fmt.Errorf("open %s values decompressor: %w", d.filenameBase, err)
	}

	if d.Accessors.Has(AccessorHashMap) {
		if err = d.buildHashMapAccessor(ctx, step, step+1, valuesDecomp, ps); err != nil {
			return StaticFiles{}, fmt.Errorf("build %s values idx: %w", d.filenameBase, err)
		}
		valuesIdx, err = recsplit.OpenIndex(d.kviAccessorFilePath(step, step+1))
		if err != nil {
			return StaticFiles{}, err
		}
	}

	if d.Accessors.Has(AccessorBTree) {
		btPath := d.kvBtAccessorFilePath(step, step+1)
		bt, err = CreateBtreeIndexWithDecompressor(btPath, DefaultBtreeM, valuesDecomp, d.Compression, *d.salt.Load(), ps, d.dirs.Tmp, d.logger, d.noFsync, d.Accessors)
		if err != nil {
			return StaticFiles{}, fmt.Errorf("build %s .bt idx: %w", d.filenameBase, err)
		}
	}
	if d.Accessors.Has(AccessorExistence) {
		fPath := d.kvExistenceIdxFilePath(step, step+1)
		exists, err := dir.FileExist(fPath)
		if err != nil {
			return StaticFiles{}, fmt.Errorf("build %s .kvei: %w", d.filenameBase, err)
		}
		if exists {
			bloom, err = existence.OpenFilter(fPath)
			if err != nil {
				return StaticFiles{}, fmt.Errorf("build %s .kvei: %w", d.filenameBase, err)
			}
		}
	}
	closeComp = false
	return StaticFiles{
		HistoryFiles: hStaticFiles,
		valuesDecomp: valuesDecomp,
		valuesIdx:    valuesIdx,
		valuesBt:     bt,
		bloom:        bloom,
	}, nil
}

func (d *Domain) buildHashMapAccessor(ctx context.Context, fromStep, toStep uint64, data *seg.Decompressor, ps *background.ProgressSet) error {
	idxPath := d.kviAccessorFilePath(fromStep, toStep)
	cfg := recsplit.RecSplitArgs{
		Enums:              true,
		LessFalsePositives: true,

		BucketSize: recsplit.DefaultBucketSize,
		LeafSize:   recsplit.DefaultLeafSize,
		TmpDir:     d.dirs.Tmp,
		IndexFile:  idxPath,
		Salt:       d.salt.Load(),
		NoFsync:    d.noFsync,
	}
	return buildHashMapAccessor(ctx, data, d.Compression, idxPath, false, cfg, ps, d.logger)
}

func (d *Domain) MissedBtreeAccessors() (l []*filesItem) {
	return d.missedBtreeAccessors(d.dirtyFiles.Items())
}

func (d *Domain) missedBtreeAccessors(source []*filesItem) (l []*filesItem) {
	if !d.Accessors.Has(AccessorBTree) {
		return nil
	}
	return fileItemsWithMissedAccessors(source, d.aggregationStep, func(fromStep uint64, toStep uint64) []string {
		return []string{d.kvBtAccessorFilePath(fromStep, toStep), d.kvExistenceIdxFilePath(fromStep, toStep)}
	})
}

func (d *Domain) MissedMapAccessors() (l []*filesItem) {
	return d.missedMapAccessors(d.dirtyFiles.Items())
}

func (d *Domain) missedMapAccessors(source []*filesItem) (l []*filesItem) {
	if !d.Accessors.Has(AccessorHashMap) {
		return nil
	}
	return fileItemsWithMissedAccessors(source, d.aggregationStep, func(fromStep uint64, toStep uint64) []string {
		return []string{d.kviAccessorFilePath(fromStep, toStep)}
	})
}

// BuildMissedAccessors - produce .efi/.vi/.kvi from .ef/.v/.kv
func (d *Domain) BuildMissedAccessors(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet, domainFiles *MissedAccessorDomainFiles) {
	d.History.BuildMissedAccessors(ctx, g, ps, domainFiles.history)
	for _, item := range domainFiles.missedBtreeAccessors() {
		if item.decompressor == nil {
			log.Warn(fmt.Sprintf("[dbg] BuildMissedAccessors: item with nil decompressor %s %d-%d", d.filenameBase, item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep))
		}
		item := item

		g.Go(func() error {
			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
			idxPath := d.kvBtAccessorFilePath(fromStep, toStep)
			if err := BuildBtreeIndexWithDecompressor(idxPath, item.decompressor, d.Compression, ps, d.dirs.Tmp, *d.salt.Load(), d.logger, d.noFsync, d.Accessors); err != nil {
				return fmt.Errorf("failed to build btree index for %s:  %w", item.decompressor.FileName(), err)
			}
			return nil
		})
	}
	for _, item := range domainFiles.missedMapAccessors() {
		if item.decompressor == nil {
			log.Warn(fmt.Sprintf("[dbg] BuildMissedAccessors: item with nil decompressor %s %d-%d", d.filenameBase, item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep))
		}
		item := item
		g.Go(func() error {
			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
			err := d.buildHashMapAccessor(ctx, fromStep, toStep, item.decompressor, ps)
			if err != nil {
				return fmt.Errorf("build %s values recsplit index: %w", d.filenameBase, err)
			}
			return nil
		})
	}
}

// TODO: exported for idx_optimize.go
// TODO: this utility can be safely deleted after PR https://github.com/erigontech/erigon/pull/12907/ is rolled out in production
func BuildHashMapAccessor(ctx context.Context, d *seg.Decompressor, compressed seg.FileCompression, idxPath string, values bool, cfg recsplit.RecSplitArgs, ps *background.ProgressSet, logger log.Logger) error {
	return buildHashMapAccessor(ctx, d, compressed, idxPath, values, cfg, ps, logger)
}

func buildHashMapAccessor(ctx context.Context, d *seg.Decompressor, compressed seg.FileCompression, idxPath string, values bool, cfg recsplit.RecSplitArgs, ps *background.ProgressSet, logger log.Logger) error {
	_, fileName := filepath.Split(idxPath)
	count := d.Count()
	if !values {
		count = d.Count() / 2
	}
	p := ps.AddNew(fileName, uint64(count))
	defer ps.Delete(p)

	defer d.MadvSequential().DisableReadAhead()

	g := seg.NewReader(d.MakeGetter(), compressed)
	var rs *recsplit.RecSplit
	var err error
	cfg.KeyCount = count
	if rs, err = recsplit.NewRecSplit(cfg, logger); err != nil {
		return fmt.Errorf("create recsplit: %w", err)
	}
	defer rs.Close()
	rs.LogLvl(log.LvlTrace)

	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("buildHashMapAccessor: %s, %s, %s", rs.FileName(), rec, dbg.Stack())
		}
	}()

	var keyPos, valPos uint64
	for {
		word := make([]byte, 0, 256)
		if err := ctx.Err(); err != nil {
			return err
		}
		g.Reset(0)
		for g.HasNext() {
			word, valPos = g.Next(word[:0])
			if values {
				if err = rs.AddKey(word, valPos); err != nil {
					return fmt.Errorf("add idx key [%x]: %w", word, err)
				}
			} else {
				if err = rs.AddKey(word, keyPos); err != nil {
					return fmt.Errorf("add idx key [%x]: %w", word, err)
				}
			}

			// Skip value
			keyPos, _ = g.Skip()

			p.Processed.Add(1)
		}
		if err = rs.Build(ctx); err != nil {
			if rs.Collision() {
				logger.Info("Building recsplit. Collision happened. It's ok. Restarting...")
				rs.ResetNextSalt()
			} else {
				return fmt.Errorf("build idx: %w", err)
			}
		} else {
			break
		}
	}
	return nil
}

func (d *Domain) integrateDirtyFiles(sf StaticFiles, txNumFrom, txNumTo uint64) {
	if d.disable {
		return
	}
	d.History.integrateDirtyFiles(sf.HistoryFiles, txNumFrom, txNumTo)

	fi := newFilesItem(txNumFrom, txNumTo, d.aggregationStep)
	fi.frozen = false
	fi.decompressor = sf.valuesDecomp
	fi.index = sf.valuesIdx
	fi.bindex = sf.valuesBt
	fi.existence = sf.bloom
	d.dirtyFiles.Set(fi)
}

// unwind is similar to prune but the difference is that it restores domain values from the history as of txFrom
// context Flush should be managed by caller.
func (dt *DomainRoTx) unwind(ctx context.Context, rwTx kv.RwTx, step, txNumUnwindTo uint64, domainDiffs []kv.DomainEntryDiff) error {
	// fmt.Printf("[domain][%s] unwinding domain to txNum=%d, step %d\n", d.filenameBase, txNumUnwindTo, step)
	d := dt.d

	sf := time.Now()
	defer mxUnwindTook.ObserveDuration(sf)
	mxRunningUnwind.Inc()
	defer mxRunningUnwind.Dec()
	logEvery := time.NewTicker(time.Second * 30)
	defer logEvery.Stop()

	valsCursor, err := rwTx.RwCursorDupSort(d.valuesTable)
	if err != nil {
		return err
	}
	defer valsCursor.Close()
	// First revert keys
	for i := range domainDiffs {
		keyStr, value, prevStepBytes := domainDiffs[i].Key, domainDiffs[i].Value, domainDiffs[i].PrevStepBytes
		key := toBytesZeroCopy(keyStr)
		if dt.d.largeValues {
			if len(value) == 0 {
				if !bytes.Equal(key[len(key)-8:], prevStepBytes) {
					if err := rwTx.Delete(d.valuesTable, key); err != nil {
						return err
					}
				} else {
					if err := rwTx.Put(d.valuesTable, key, []byte{}); err != nil {
						return err
					}
				}
			} else {
				if err := rwTx.Put(d.valuesTable, key, value); err != nil {
					return err
				}
			}
			continue
		}
		stepBytes := key[len(key)-8:]
		fullKey := key[:len(key)-8]
		// Second, we need to restore the previous value
		valInDB, err := valsCursor.SeekBothRange(fullKey, stepBytes)
		if err != nil {
			return err
		}
		if len(valInDB) > 0 {
			stepInDB := valInDB[:8]
			if bytes.Equal(stepInDB, stepBytes) {
				if err := valsCursor.DeleteCurrent(); err != nil {
					return err
				}
			}
		}

		if !bytes.Equal(stepBytes, prevStepBytes) {
			continue
		}

		if err := valsCursor.Put(fullKey, append(stepBytes, value...)); err != nil {
			return err
		}
	}
	// Compare valsKV with prevSeenKeys
	if _, err := dt.ht.prune(ctx, rwTx, txNumUnwindTo, math.MaxUint64, math.MaxUint64, true, logEvery); err != nil {
		return fmt.Errorf("[domain][%s] unwinding, prune history to txNum=%d, step %d: %w", dt.d.filenameBase, txNumUnwindTo, step, err)
	}
	return nil
}

// getLatestFromFiles doesn't provide same semantics as getLatestFromDB - it returns start/end tx
// of file where the value is stored (not exact step when kv has been set)
// maxTxNum, if > 0, filters out files with bigger txnums from search
func (dt *DomainRoTx) getLatestFromFiles(k []byte, maxTxNum uint64) (v []byte, found bool, fileStartTxNum uint64, fileEndTxNum uint64, err error) {
	if len(dt.files) == 0 {
		return
	}
	if maxTxNum == 0 {
		maxTxNum = math.MaxUint64
	}
	useExistenceFilter := dt.d.Accessors.Has(AccessorExistence)
	useCache := dt.name != kv.CommitmentDomain && maxTxNum == math.MaxUint64

	hi, _ := dt.ht.iit.hashKey(k)
	if useCache && dt.getFromFileCache == nil {
		dt.getFromFileCache = dt.visible.newGetFromFileCache()
	}
	if dt.getFromFileCache != nil && maxTxNum == math.MaxUint64 {
		if cv, ok := dt.getFromFileCache.Get(hi); ok {
			return cv.v, true, dt.files[cv.lvl].startTxNum, dt.files[cv.lvl].endTxNum, nil
		}
	}

	for i := len(dt.files) - 1; i >= 0; i-- {
		if maxTxNum != math.MaxUint64 && dt.files[i].endTxNum > maxTxNum { // skip partially matched files
			continue
		}
		// fmt.Printf("getLatestFromFiles: lim=%d %d %d %d %d\n", maxTxNum, dt.files[i].startTxNum, dt.files[i].endTxNum, dt.files[i].startTxNum/dt.d.aggregationStep, dt.files[i].endTxNum/dt.d.aggregationStep)
		if useExistenceFilter {
			if dt.files[i].src.existence != nil {
				if !dt.files[i].src.existence.ContainsHash(hi) {
					if traceGetLatest == dt.name {
						fmt.Printf("GetLatest(%s, %x) -> existence index %s -> false\n", dt.d.filenameBase, k, dt.files[i].src.existence.FileName)
					}
					continue
				} else {
					if traceGetLatest == dt.name {
						fmt.Printf("GetLatest(%s, %x) -> existence index %s -> true\n", dt.d.filenameBase, k, dt.files[i].src.existence.FileName)
					}
				}
			} else {
				if traceGetLatest == dt.name {
					fmt.Printf("GetLatest(%s, %x) -> existence index is nil %s\n", dt.name.String(), k, dt.files[i].src.decompressor.FileName())
				}
			}
		}

		v, found, _, err = dt.getLatestFromFile(i, k)
		if err != nil {
			return nil, false, 0, 0, err
		}
		if !found {
			if traceGetLatest == dt.name {
				fmt.Printf("GetLatest(%s, %x) -> not found in file %s\n", dt.name.String(), k, dt.files[i].src.decompressor.FileName())
			}
			continue
		}
		if traceGetLatest == dt.name {
			fmt.Printf("GetLatest(%s, %x) -> found in file %s\n", dt.name.String(), k, dt.files[i].src.decompressor.FileName())
		}

		if dt.getFromFileCache != nil {
			dt.getFromFileCache.Add(hi, domainGetFromFileCacheItem{lvl: uint8(i), v: v})
		}
		return v, true, dt.files[i].startTxNum, dt.files[i].endTxNum, nil
	}
	if traceGetLatest == dt.name {
		fmt.Printf("GetLatest(%s, %x) -> not found in %d files\n", dt.name.String(), k, len(dt.files))
	}

	if dt.getFromFileCache != nil {
		dt.getFromFileCache.Add(hi, domainGetFromFileCacheItem{lvl: 0, v: nil})
	}
	return nil, false, 0, 0, nil
}

// Returns the first txNum from available history
func (dt *DomainRoTx) HistoryStartFrom() uint64 {
	if len(dt.ht.files) == 0 {
		return 0
	}
	return dt.ht.files[0].startTxNum
}

// GetAsOf does not always require usage of roTx. If it is possible to determine
// historical value based only on static files, roTx will not be used.
func (dt *DomainRoTx) GetAsOf(key []byte, txNum uint64, roTx kv.Tx) ([]byte, bool, error) {
	if dt.d.disable {
		return nil, false, nil
	}

	v, hOk, err := dt.ht.HistorySeek(key, txNum, roTx)
	if err != nil {
		return nil, false, err
	}
	if hOk {
		if len(v) == 0 { // if history successfuly found marker of key creation
			if traceGetAsOf == dt.d.filenameBase {
				fmt.Printf("DomainGetAsOf(%s  , %x, %d) -> not found in history\n", dt.d.filenameBase, key, txNum)
			}
			return nil, false, nil
		}
		if traceGetAsOf == dt.d.filenameBase {
			fmt.Printf("DomainGetAsOf(%s, %x, %d) -> found in history\n", dt.d.filenameBase, key, txNum)
		}
		return v, v != nil, nil
	}

	var ok bool
	v, _, ok, err = dt.GetLatest(key, roTx)
	if err != nil {
		return nil, false, err
	}
	if traceGetAsOf == dt.d.filenameBase {
		if ok {
			fmt.Printf("DomainGetAsOf(%s, %x, %d) -> found in latest state\n", dt.d.filenameBase, key, txNum)
		} else {
			fmt.Printf("DomainGetAsOf(%s, %x, %d) -> not found in latest state\n", dt.d.filenameBase, key, txNum)
		}
	}
	return v, v != nil, nil
}

func (dt *DomainRoTx) Close() {
	if dt.files == nil { // invariant: it's safe to call Close multiple times
		return
	}
	dt.closeValsCursor()
	files := dt.files
	dt.files = nil
	for i := range files {
		src := files[i].src
		if src == nil || src.frozen {
			continue
		}
		refCnt := src.refcount.Add(-1)
		//GC: last reader responsible to remove useles files: close it and delete
		if refCnt == 0 && src.canDelete.Load() {
			if traceFileLife != "" && dt.d.filenameBase == traceFileLife {
				dt.d.logger.Warn("[agg.dbg] real remove at DomainRoTx.Close", "file", src.decompressor.FileName())
			}
			src.closeFilesAndRemove()
		}
	}
	dt.ht.Close()

	dt.visible.returnGetFromFileCache(dt.getFromFileCache)
}

// statelessFileIndex figures out ordinal of file within required range
func (dt *DomainRoTx) statelessFileIndex(txFrom uint64, txTo uint64) int { //nolint:unused
	for fi, f := range dt.files {
		if f.startTxNum == txFrom && f.endTxNum == txTo {
			return fi
		}
	}
	return -1
}

func (dt *DomainRoTx) reader(i int) *seg.Reader {
	// readers are not stateless - getters contain the current data pointer
	return seg.NewReader(dt.files[i].src.decompressor.MakeGetter(), dt.d.Compression)
}

func (dt *DomainRoTx) statelessIdxReader(i int) *recsplit.IndexReader {
	if dt.idxReaders == nil {
		dt.idxReaders = make([]*recsplit.IndexReader, len(dt.files))
	}
	r := dt.idxReaders[i]
	if r == nil {
		r = dt.files[i].src.index.GetReaderFromPool()
		dt.idxReaders[i] = r
	}
	return r
}

func (dt *DomainRoTx) statelessBtree(i int) *BtIndex {
	if dt.readers == nil {
		dt.readers = make([]*BtIndex, len(dt.files))
	}
	r := dt.readers[i]
	if r == nil {
		r = dt.files[i].src.bindex
		dt.readers[i] = r
	}
	return r
}

func (dt *DomainRoTx) closeValsCursor() {
	for _, c := range dt.valsCs {
		c.Close()
	}
	dt.valsCs = nil
}

func (dt *DomainRoTx) valsCursor(tx kv.Tx) (c kv.Cursor, err error) {
	c = dt.valsCs[tx]

	if c != nil {
		return c, nil
	}

	if dt.valsCs == nil {
		dt.valsCs = map[kv.Tx]kv.Cursor{}
	} else {
		if c = dt.valsCs[tx]; c != nil {
			return c, nil
		}
	}

	if dt.d.largeValues {
		c, err = tx.Cursor(dt.d.valuesTable) //nolint:gocritic
		if err == nil {
			dt.valsCs[tx] = c
		}
		return c, err

	}
	c, err = tx.CursorDupSort(dt.d.valuesTable) //nolint:gocritic
	if err == nil {
		dt.valsCs[tx] = c
	}
	return c, err
}

func (dt *DomainRoTx) getLatestFromDb(key []byte, roTx kv.Tx) ([]byte, uint64, bool, error) {
	if dt == nil {
		return nil, 0, false, nil
	}

	var v []byte
	var foundStep uint64

	valsC, err := dt.valsCursor(roTx)

	if err != nil {
		return nil, 0, false, err
	}

	if dt.d.largeValues {
		fullkey, val, err := valsC.Seek(key)
		if err != nil {
			return nil, 0, false, fmt.Errorf("valsCursor.Seek: %w", err)
		}
		if len(fullkey) == 0 {
			return nil, 0, false, nil // This key is not in DB
		}
		if !bytes.Equal(fullkey[:len(fullkey)-8], key) {
			return nil, 0, false, nil // This key is not in DB
		}

		v = val
		foundStep = ^binary.BigEndian.Uint64(fullkey[len(fullkey)-8:])
	} else {
		_, stepWithVal, err := func() (_ []byte, _ []byte, err error) {
			defer func() {
				if rec := recover(); rec != nil {
					err = fmt.Errorf("%p: seek failed for: %d: reason %s", dt, roTx.ViewID(), rec)
				}
			}()
			return valsC.SeekExact(key)
		}()
		if err != nil {
			return nil, 0, false, fmt.Errorf("valsCursor.SeekExact: %w", err)
		}
		if len(stepWithVal) == 0 {
			return nil, 0, false, nil
		}

		v = stepWithVal[8:]
		foundStep = ^binary.BigEndian.Uint64(stepWithVal[:8])
	}

	if lastTxNumOfStep(foundStep, dt.d.aggregationStep) >= dt.files.EndTxNum() {
		return v, foundStep, true, nil
	}

	return nil, 0, false, err
}

// GetLatest returns value, step in which the value last changed, and bool value which is true if the value
// is present, and false if it is not present (not set or deleted)
func (dt *DomainRoTx) GetLatest(key []byte, roTx kv.Tx) ([]byte, uint64, bool, error) {
	if dt.d.disable {
		return nil, 0, false, nil
	}

	var v []byte
	var foundStep uint64
	var found bool
	var err error

	if traceGetLatest == dt.name {
		defer func() {
			fmt.Printf("GetLatest(%s, '%x' -> '%x') (from db=%t; istep=%x stepInFiles=%d)\n",
				dt.name.String(), key, v, found, foundStep, dt.files.EndTxNum()/dt.d.aggregationStep)
		}()
	}

	v, foundStep, found, err = dt.getLatestFromDb(key, roTx)
	if err != nil {
		return nil, 0, false, fmt.Errorf("getLatestFromDb: %w", err)
	}
	if found {
		return v, foundStep, true, nil
	}

	v, foundInFile, _, endTxNum, err := dt.getLatestFromFiles(key, 0)
	if err != nil {
		return nil, 0, false, fmt.Errorf("getLatestFromFiles: %w", err)
	}
	return v, endTxNum / dt.d.aggregationStep, foundInFile, nil
}

// RangeAsOf - if key doesn't exists in history - then look in latest state
func (dt *DomainRoTx) RangeAsOf(ctx context.Context, tx kv.Tx, fromKey, toKey []byte, ts uint64, asc order.By, limit int) (it stream.KV, err error) {
	if !asc {
		panic("implement me")
	}
	histStateIt, err := dt.ht.RangeAsOf(ctx, ts, fromKey, toKey, asc, kv.Unlim, tx)
	if err != nil {
		return nil, err
	}
	lastestStateIt, err := dt.DebugRangeLatest(tx, fromKey, toKey, kv.Unlim)
	if err != nil {
		return nil, err
	}
	return stream.UnionKV(histStateIt, lastestStateIt, limit), nil
}

func (dt *DomainRoTx) DebugRangeLatest(roTx kv.Tx, fromKey, toKey []byte, limit int) (*DomainLatestIterFile, error) {
	s := &DomainLatestIterFile{
		from: fromKey, to: toKey, limit: limit,
		orderAscend: order.Asc,
		aggStep:     dt.d.aggregationStep,
		roTx:        roTx,
		valsTable:   dt.d.valuesTable,
		logger:      dt.d.logger,
		h:           &CursorHeap{},
	}
	if err := s.init(dt); err != nil {
		s.Close() //it's responsibility of constructor (our) to close resource on error
		return nil, err
	}
	return s, nil
}

// CanPruneUntil returns true if domain OR history tables can be pruned until txNum
func (dt *DomainRoTx) CanPruneUntil(tx kv.Tx, untilTx uint64) bool {
	canDomain, _ := dt.canPruneDomainTables(tx, untilTx)
	canHistory, _ := dt.ht.canPruneUntil(tx, untilTx)
	return canHistory || canDomain
}

func (dt *DomainRoTx) canBuild(dbtx kv.Tx) bool { //nolint
	maxStepInFiles := dt.files.EndTxNum() / dt.d.aggregationStep
	return maxStepInFiles < dt.d.maxStepInDB(dbtx)
}

// checks if there is anything to prune in DOMAIN tables.
// everything that aggregated is prunable.
// history.CanPrune should be called separately because it responsible for different tables
func (dt *DomainRoTx) canPruneDomainTables(tx kv.Tx, untilTx uint64) (can bool, maxStepToPrune uint64) {
	if m := dt.files.EndTxNum(); m > 0 {
		maxStepToPrune = (m - 1) / dt.d.aggregationStep
	}
	var untilStep uint64
	if untilTx > 0 {
		untilStep = (untilTx - 1) / dt.d.aggregationStep
	}
	sm, err := GetExecV3PrunableProgress(tx, []byte(dt.d.valuesTable))
	if err != nil {
		dt.d.logger.Error("get domain pruning progress", "name", dt.d.filenameBase, "error", err)
		return false, maxStepToPrune
	}

	delta := float64(max(maxStepToPrune, sm) - min(maxStepToPrune, sm)) // maxStep could be 0
	switch dt.d.filenameBase {
	case "account":
		mxPrunableDAcc.Set(delta)
	case "storage":
		mxPrunableDSto.Set(delta)
	case "code":
		mxPrunableDCode.Set(delta)
	case "commitment":
		mxPrunableDComm.Set(delta)
	}
	//fmt.Printf("smallestToPrune[%s] minInDB %d inFiles %d until %d\n", dt.d.filenameBase, sm, maxStepToPrune, untilStep)
	return sm <= min(maxStepToPrune, untilStep), maxStepToPrune
}

type DomainPruneStat struct {
	MinStep uint64
	MaxStep uint64
	Values  uint64
	History *InvertedIndexPruneStat
}

func (dc *DomainPruneStat) PrunedNothing() bool {
	return dc.Values == 0 && (dc.History == nil || dc.History.PrunedNothing())
}

func (dc *DomainPruneStat) String() (kvstr string) {
	if dc.PrunedNothing() {
		return ""
	}
	if dc.Values > 0 {
		kvstr = fmt.Sprintf("kv: %s from steps %d-%d", common.PrettyCounter(dc.Values), dc.MinStep, dc.MaxStep)
	}
	if dc.History != nil {
		if kvstr != "" {
			kvstr += ", "
		}
		kvstr += dc.History.String()
	}
	return kvstr
}

func (dc *DomainPruneStat) Accumulate(other *DomainPruneStat) {
	if other == nil {
		return
	}
	dc.MinStep = min(dc.MinStep, other.MinStep)
	dc.MaxStep = max(dc.MaxStep, other.MaxStep)
	dc.Values += other.Values
	if dc.History == nil {
		if other.History != nil {
			dc.History = other.History
		}
	} else {
		dc.History.Accumulate(other.History)
	}
}

func (dt *DomainRoTx) Prune(ctx context.Context, rwTx kv.RwTx, step, txFrom, txTo, limit uint64, logEvery *time.Ticker) (stat *DomainPruneStat, err error) {
	if dt.files.EndTxNum() > 0 {
		txTo = min(txTo, dt.files.EndTxNum())
	}
	return dt.prune(ctx, rwTx, step, txFrom, txTo, limit, logEvery)
}

func (dt *DomainRoTx) prune(ctx context.Context, rwTx kv.RwTx, step, txFrom, txTo, limit uint64, logEvery *time.Ticker) (stat *DomainPruneStat, err error) {
	if limit == 0 {
		limit = math.MaxUint64
	}

	stat = &DomainPruneStat{MinStep: math.MaxUint64}
	if stat.History, err = dt.ht.Prune(ctx, rwTx, txFrom, txTo, limit, false, logEvery); err != nil {
		return nil, fmt.Errorf("prune history at step %d [%d, %d): %w", step, txFrom, txTo, err)
	}
	canPrune, maxPrunableStep := dt.canPruneDomainTables(rwTx, txTo)
	if !canPrune {
		return stat, nil
	}
	if step > maxPrunableStep {
		step = maxPrunableStep
	}

	st := time.Now()
	mxPruneInProgress.Inc()
	defer mxPruneInProgress.Dec()

	var valsCursor kv.RwCursor

	ancientDomainValsCollector := etl.NewCollectorWithAllocator(dt.name.String()+".domain.collate", dt.d.dirs.Tmp, etl.SmallSortableBuffers, dt.d.logger).LogLvl(log.LvlTrace)
	defer ancientDomainValsCollector.Close()

	if dt.d.largeValues {
		valsCursor, err = rwTx.RwCursor(dt.d.valuesTable)
		if err != nil {
			return stat, fmt.Errorf("create %s domain values cursor: %w", dt.name.String(), err)
		}
	} else {
		valsCursor, err = rwTx.RwCursorDupSort(dt.d.valuesTable)
		if err != nil {
			return stat, fmt.Errorf("create %s domain values cursor: %w", dt.name.String(), err)
		}
	}
	defer valsCursor.Close()

	loadFunc := func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		if dt.d.largeValues {
			return valsCursor.Delete(k)
		}
		return valsCursor.(kv.RwCursorDupSort).DeleteExact(k, v)
	}

	prunedKey, err := GetExecV3PruneProgress(rwTx, dt.d.valuesTable)
	if err != nil {
		dt.d.logger.Error("get domain pruning progress", "name", dt.name.String(), "error", err)
	}

	var k, v []byte
	if prunedKey != nil && limit < 100_000 {
		k, v, err = valsCursor.Seek(prunedKey)
	} else {
		k, v, err = valsCursor.First()
	}
	if err != nil {
		return nil, err
	}
	var stepBytes []byte
	for ; k != nil; k, v, err = valsCursor.Next() {
		if err != nil {
			return stat, fmt.Errorf("iterate over %s domain keys: %w", dt.name.String(), err)
		}

		if dt.d.largeValues {
			stepBytes = k[len(k)-8:]
		} else {
			stepBytes = v[:8]
		}

		is := ^binary.BigEndian.Uint64(stepBytes)
		if is > step {
			continue
		}
		if limit == 0 {
			if err := ancientDomainValsCollector.Load(rwTx, dt.d.valuesTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
				return stat, fmt.Errorf("load domain values: %w", err)
			}
			if err := SaveExecV3PruneProgress(rwTx, dt.d.valuesTable, k); err != nil {
				return stat, fmt.Errorf("save domain pruning progress: %s, %w", dt.name.String(), err)
			}
			return stat, nil
		}
		limit--
		stat.Values++
		if err := ancientDomainValsCollector.Collect(k, v); err != nil {
			return nil, err
		}
		stat.MinStep = min(stat.MinStep, is)
		stat.MaxStep = max(stat.MaxStep, is)
		select {
		case <-ctx.Done():
			// consider ctx exiting as incorrect outcome, error is returned
			return stat, ctx.Err()
		case <-logEvery.C:
			dt.d.logger.Info("[snapshots] prune domain", "name", dt.name.String(),
				"pruned keys", stat.Values,
				"steps", fmt.Sprintf("%.2f-%.2f", float64(txFrom)/float64(dt.d.aggregationStep), float64(txTo)/float64(dt.d.aggregationStep)))
		default:
		}
	}
	mxPruneSizeDomain.AddUint64(stat.Values)
	if err := ancientDomainValsCollector.Load(rwTx, dt.d.valuesTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return stat, fmt.Errorf("load domain values: %w", err)
	}
	if err := SaveExecV3PruneProgress(rwTx, dt.d.valuesTable, nil); err != nil {
		return stat, fmt.Errorf("save domain pruning progress: %s, %w", dt.d.filenameBase, err)
	}

	if err := SaveExecV3PrunableProgress(rwTx, []byte(dt.d.valuesTable), step+1); err != nil {
		return stat, err
	}
	mxPruneTookDomain.ObserveDuration(st)
	return stat, nil
}

type SegStreamReader struct {
	s *seg.Reader

	limit int
}

// SegStreamReader implements stream.KV for segment reader.
// limit -1 means no limit.
func NewSegStreamReader(s *seg.Reader, limit int) *SegStreamReader {
	s.Reset(0)
	return &SegStreamReader{
		s: s, limit: limit,
	}
}

func (sr *SegStreamReader) HasNext() bool { return sr.s.HasNext() && (sr.limit == -1 || sr.limit > 0) }
func (sr *SegStreamReader) Close()        { sr.s = nil }

func (sr *SegStreamReader) Next() (k, v []byte, err error) {
	k, _ = sr.s.Next(k)
	if !sr.s.HasNext() {
		return nil, nil, fmt.Errorf("key %x has no associated value: %s", k, sr.s.FileName())
	}
	v, _ = sr.s.Next(v)
	if sr.limit > 0 {
		sr.limit--
	}
	return k, v, nil
}

func (dt *DomainRoTx) stepsRangeInDB(tx kv.Tx) (from, to float64) {
	return dt.ht.iit.stepsRangeInDB(tx)
}

func (dt *DomainRoTx) Tables() (res []string) {
	return []string{dt.d.valuesTable, dt.ht.h.valuesTable, dt.ht.iit.ii.keysTable, dt.ht.iit.ii.valuesTable}
}

func (dt *DomainRoTx) Files() (res VisibleFiles) {
	for _, item := range dt.files {
		if item.src.decompressor != nil {
			res = append(res, item)
		}
	}
	return append(res, dt.ht.Files()...)
}
func (dt *DomainRoTx) Name() kv.Domain { return dt.name }

func (dt *DomainRoTx) HistoryProgress(tx kv.Tx) uint64 { return dt.ht.iit.Progress(tx) }
