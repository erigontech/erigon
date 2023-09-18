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
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	bloomfilter "github.com/holiman/bloomfilter/v2"
	"github.com/holiman/uint256"
	"github.com/pkg/errors"
	btree2 "github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/recsplit"
)

var (
	LatestStateReadWarm          = metrics.GetOrCreateSummary(`latest_state_read{type="warm",found="yes"}`)  //nolint
	LatestStateReadWarmNotFound  = metrics.GetOrCreateSummary(`latest_state_read{type="warm",found="no"}`)   //nolint
	LatestStateReadGrind         = metrics.GetOrCreateSummary(`latest_state_read{type="grind",found="yes"}`) //nolint
	LatestStateReadGrindNotFound = metrics.GetOrCreateSummary(`latest_state_read{type="grind",found="no"}`)  //nolint
	LatestStateReadCold          = metrics.GetOrCreateSummary(`latest_state_read{type="cold",found="yes"}`)  //nolint
	LatestStateReadColdNotFound  = metrics.GetOrCreateSummary(`latest_state_read{type="cold",found="no"}`)   //nolint
	LatestStateReadDB            = metrics.GetOrCreateSummary(`latest_state_read{type="db",found="yes"}`)    //nolint
	LatestStateReadDBNotFound    = metrics.GetOrCreateSummary(`latest_state_read{type="db",found="no"}`)     //nolint

	mxRunningMerges           = metrics.GetOrCreateCounter("domain_running_merges")
	mxRunningCollations       = metrics.GetOrCreateCounter("domain_running_collations")
	mxCollateTook             = metrics.GetOrCreateHistogram("domain_collate_took")
	mxPruneTookDomain         = metrics.GetOrCreateHistogram(`domain_prune_took{type="domain"}`)
	mxPruneTookHistory        = metrics.GetOrCreateHistogram(`domain_prune_took{type="history"}`)
	mxPruneTookIndex          = metrics.GetOrCreateHistogram(`domain_prune_took{type="index"}`)
	mxPruneInProgress         = metrics.GetOrCreateCounter("domain_pruning_progress")
	mxCollationSize           = metrics.GetOrCreateCounter("domain_collation_size")
	mxCollationSizeHist       = metrics.GetOrCreateCounter("domain_collation_hist_size")
	mxPruneSizeDomain         = metrics.GetOrCreateCounter(`domain_prune_size{type="domain"}`)
	mxPruneSizeHistory        = metrics.GetOrCreateCounter(`domain_prune_size{type="history"}`)
	mxPruneSizeIndex          = metrics.GetOrCreateCounter(`domain_prune_size{type="index"}`)
	mxBuildTook               = metrics.GetOrCreateSummary("domain_build_files_took")
	mxStepTook                = metrics.GetOrCreateHistogram("domain_step_took")
	mxCommitmentKeys          = metrics.GetOrCreateCounter("domain_commitment_keys")
	mxCommitmentRunning       = metrics.GetOrCreateCounter("domain_running_commitment")
	mxCommitmentTook          = metrics.GetOrCreateSummary("domain_commitment_took")
	mxCommitmentWriteTook     = metrics.GetOrCreateHistogram("domain_commitment_write_took")
	mxCommitmentBranchUpdates = metrics.GetOrCreateCounter("domain_commitment_updates_applied")
)

// StepsInColdFile - files of this size are completely frozen/immutable.
// files of smaller size are also immutable, but can be removed after merge to bigger files.
const StepsInColdFile = 32

// filesItem corresponding to a pair of files (.dat and .idx)
type filesItem struct {
	decompressor *compress.Decompressor
	index        *recsplit.Index
	bindex       *BtIndex
	bm           *bitmapdb.FixedSizeBitmaps
	bloom        *bloomFilter
	startTxNum   uint64
	endTxNum     uint64

	// Frozen: file of size StepsInColdFile. Completely immutable.
	// Cold: file of size < StepsInColdFile. Immutable, but can be closed/removed after merge to bigger file.
	// Hot: Stored in DB. Providing Snapshot-Isolation by CopyOnWrite.
	frozen   bool         // immutable, don't need atomic
	refcount atomic.Int32 // only for `frozen=false`

	// file can be deleted in 2 cases: 1. when `refcount == 0 && canDelete == true` 2. on app startup when `file.isSubsetOfFrozenFile()`
	// other processes (which also reading files, may have same logic)
	canDelete atomic.Bool
}
type bloomFilter struct {
	*bloomfilter.Filter
	FileName, FilePath string
	f                  *os.File
}

func NewBloom(keysCount uint64, filePath string) (*bloomFilter, error) {
	m := bloomfilter.OptimalM(keysCount, 0.01)
	//TODO: make filters compatible by usinig same seed/keys
	_, fileName := filepath.Split(filePath)
	bloom, err := bloomfilter.New(m)
	if err != nil {
		return nil, fmt.Errorf("%w, %s", err, fileName)
	}
	return &bloomFilter{FilePath: filePath, FileName: fileName, Filter: bloom}, nil
}

func (b *bloomFilter) Build() error {
	log.Trace("[agg] write file", "file", b.FileName)
	//TODO: fsync and tmp-file rename
	if _, err := b.Filter.WriteFile(b.FilePath); err != nil {
		return err
	}
	return nil
}

func OpenBloom(filePath string) (*bloomFilter, error) {
	_, fileName := filepath.Split(filePath)
	f := &bloomFilter{FilePath: filePath, FileName: fileName}
	var err error
	f.Filter, _, err = bloomfilter.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("OpenBloom: %w, %s", err, fileName)
	}
	return f, nil
}
func (b *bloomFilter) Close() {
	if b.f != nil {
		b.f.Close()
		b.f = nil
	}
}

func newFilesItem(startTxNum, endTxNum, stepSize uint64) *filesItem {
	startStep := startTxNum / stepSize
	endStep := endTxNum / stepSize
	frozen := endStep-startStep == StepsInColdFile
	return &filesItem{startTxNum: startTxNum, endTxNum: endTxNum, frozen: frozen}
}

func (i *filesItem) isSubsetOf(j *filesItem) bool {
	return (j.startTxNum <= i.startTxNum && i.endTxNum <= j.endTxNum) && (j.startTxNum != i.startTxNum || i.endTxNum != j.endTxNum)
}

func filesItemLess(i, j *filesItem) bool {
	if i.endTxNum == j.endTxNum {
		return i.startTxNum > j.startTxNum
	}
	return i.endTxNum < j.endTxNum
}
func (i *filesItem) closeFilesAndRemove() {
	if i.decompressor != nil {
		i.decompressor.Close()
		// paranoic-mode on: don't delete frozen files
		if !i.frozen {
			if err := os.Remove(i.decompressor.FilePath()); err != nil {
				log.Trace("remove after close", "err", err, "file", i.decompressor.FileName())
			}
		}
		i.decompressor = nil
	}
	if i.index != nil {
		i.index.Close()
		// paranoic-mode on: don't delete frozen files
		if !i.frozen {
			if err := os.Remove(i.index.FilePath()); err != nil {
				log.Trace("remove after close", "err", err, "file", i.index.FileName())
			}
		}
		i.index = nil
	}
	if i.bindex != nil {
		i.bindex.Close()
		if err := os.Remove(i.bindex.FilePath()); err != nil {
			log.Trace("remove after close", "err", err, "file", i.bindex.FileName())
		}
		i.bindex = nil
	}
	if i.bm != nil {
		i.bm.Close()
		if err := os.Remove(i.bm.FilePath()); err != nil {
			log.Trace("remove after close", "err", err, "file", i.bm.FileName())
		}
		i.bm = nil
	}
	if i.bloom != nil {
		i.bloom.Close()
		if err := os.Remove(i.bloom.FilePath); err != nil {
			log.Trace("remove after close", "err", err, "file", i.bloom.FileName)
		}
		i.bloom = nil
	}
}

type DomainStats struct {
	MergesCount          uint64
	LastCollationTook    time.Duration
	LastPruneTook        time.Duration
	LastPruneHistTook    time.Duration
	LastFileBuildingTook time.Duration
	LastCollationSize    uint64
	LastPruneSize        uint64

	FilesQueries *atomic.Uint64
	TotalQueries *atomic.Uint64
	EfSearchTime time.Duration
	DataSize     uint64
	IndexSize    uint64
	FilesCount   uint64
}

func (ds *DomainStats) Accumulate(other DomainStats) {
	if other.FilesQueries != nil {
		ds.FilesQueries.Add(other.FilesQueries.Load())
	}
	if other.TotalQueries != nil {
		ds.TotalQueries.Add(other.TotalQueries.Load())
	}
	ds.EfSearchTime += other.EfSearchTime
	ds.IndexSize += other.IndexSize
	ds.DataSize += other.DataSize
	ds.FilesCount += other.FilesCount
}

// Domain is a part of the state (examples are Accounts, Storage, Code)
// Domain should not have any go routines or locks
//
// Data-Existence in .kv vs .v files:
//  1. key doesn’t exists, then create: .kv - yes, .v - yes
//  2. acc exists, then update/delete:  .kv - yes, .v - yes
//  3. acc doesn’t exists, then delete: .kv - no,  .v - no
type Domain struct {
	*History
	files *btree2.BTreeG[*filesItem] // thread-safe, but maybe need 1 RWLock for all trees in AggregatorV3

	// roFiles derivative from field `file`, but without garbage:
	//  - no files with `canDelete=true`
	//  - no overlaps
	//  - no un-indexed files (`power-off` may happen between .ef and .efi creation)
	//
	// MakeContext() using this field in zero-copy way
	roFiles   atomic.Pointer[[]ctxItem]
	keysTable string // key -> invertedStep , invertedStep = ^(txNum / aggregationStep), Needs to be table with DupSort
	valsTable string // key + invertedStep -> values
	stats     DomainStats
	wal       *domainWAL

	garbageFiles []*filesItem // files that exist on disk, but ignored on opening folder - because they are garbage

	/*
	   not large:
	    	keys: key -> ^step
	    	vals: key -> ^step+value (DupSort)
	   large:
	    	keys: key -> ^step
	   	  vals: key + ^step -> value
	*/

	domainLargeValues bool
	compression       FileCompression

	dir string
}

type domainCfg struct {
	hist              histCfg
	compress          FileCompression
	domainLargeValues bool
}

func NewDomain(cfg domainCfg, aggregationStep uint64, filenameBase, keysTable, valsTable, indexKeysTable, historyValsTable, indexTable string, logger log.Logger) (*Domain, error) {
	if cfg.hist.iiCfg.dirs.SnapState == "" {
		panic(1)
	}
	d := &Domain{
		dir:         cfg.hist.iiCfg.dirs.SnapState,
		keysTable:   keysTable,
		valsTable:   valsTable,
		compression: cfg.compress,
		files:       btree2.NewBTreeGOptions[*filesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		stats:       DomainStats{FilesQueries: &atomic.Uint64{}, TotalQueries: &atomic.Uint64{}},

		domainLargeValues: cfg.domainLargeValues,
	}
	d.roFiles.Store(&[]ctxItem{})

	var err error
	if d.History, err = NewHistory(cfg.hist, aggregationStep, filenameBase, indexKeysTable, indexTable, historyValsTable, []string{}, logger); err != nil {
		return nil, err
	}

	return d, nil
}
func (d *Domain) kvFilePath(fromStep, toStep uint64) string {
	return filepath.Join(d.dirs.SnapState, fmt.Sprintf("%s.%d-%d.kv", d.filenameBase, fromStep, toStep))
}
func (d *Domain) kvAccessorFilePath(fromStep, toStep uint64) string {
	return filepath.Join(d.dirs.SnapState, fmt.Sprintf("%s.%d-%d.kvi", d.filenameBase, fromStep, toStep))
}
func (d *Domain) kvExistenceIdxFilePath(fromStep, toStep uint64) string {
	return filepath.Join(d.dirs.SnapState, fmt.Sprintf("%s.%d-%d.kvei", d.filenameBase, fromStep, toStep))
}
func (d *Domain) btIdxFilePath(fromStep, toStep uint64) string {
	return filepath.Join(d.dirs.SnapState, fmt.Sprintf("%s.%d-%d.bt", d.filenameBase, fromStep, toStep))
}

// LastStepInDB - return the latest available step in db (at-least 1 value in such step)
func (d *Domain) LastStepInDB(tx kv.Tx) (lstInDb uint64) {
	lstIdx, _ := kv.LastKey(tx, d.History.indexKeysTable)
	if len(lstIdx) == 0 {
		return 0
	}
	return binary.BigEndian.Uint64(lstIdx) / d.aggregationStep
}
func (d *Domain) FirstStepInDB(tx kv.Tx) (lstInDb uint64) {
	lstIdx, _ := kv.FirstKey(tx, d.History.indexKeysTable)
	if len(lstIdx) == 0 {
		return 0
	}
	return binary.BigEndian.Uint64(lstIdx) / d.aggregationStep
}

func (d *Domain) DiscardHistory() {
	d.History.DiscardHistory()
	// can't discard domain wal - it required, but can discard history
	d.wal = d.newWriter(d.tmpdir, true, false)
}

func (d *Domain) StartUnbufferedWrites() {
	d.wal = d.newWriter(d.tmpdir, false, false)
	d.History.StartUnbufferedWrites()
}

func (d *Domain) StartWrites() {
	d.wal = d.newWriter(d.tmpdir, true, false)
	d.History.StartWrites()
}

func (d *Domain) FinishWrites() {
	if d.wal != nil {
		d.wal.close()
		d.wal = nil
	}
	d.History.FinishWrites()
}

// OpenList - main method to open list of files.
// It's ok if some files was open earlier.
// If some file already open: noop.
// If some file already open but not in provided list: close and remove from `files` field.
func (d *Domain) OpenList(idxFiles, histFiles, domainFiles []string) error {
	if err := d.History.OpenList(idxFiles, histFiles); err != nil {
		return err
	}
	return d.openList(domainFiles)
}

func (d *Domain) openList(names []string) error {
	d.closeWhatNotInList(names)
	d.garbageFiles = d.scanStateFiles(names)
	if err := d.openFiles(); err != nil {
		return fmt.Errorf("Domain.OpenList: %s, %w", d.filenameBase, err)
	}
	return nil
}

func (d *Domain) OpenFolder() error {
	idx, histFiles, domainFiles, err := d.fileNamesOnDisk()
	if err != nil {
		return err
	}
	return d.OpenList(idx, histFiles, domainFiles)
}

func (d *Domain) GetAndResetStats() DomainStats {
	r := d.stats
	r.DataSize, r.IndexSize, r.FilesCount = d.collectFilesStats()

	d.stats = DomainStats{FilesQueries: &atomic.Uint64{}, TotalQueries: &atomic.Uint64{}}
	return r
}

func (d *Domain) scanStateFiles(fileNames []string) (garbageFiles []*filesItem) {
	re := regexp.MustCompile("^" + d.filenameBase + ".([0-9]+)-([0-9]+).kv$")
	var err error

	for _, name := range fileNames {
		subs := re.FindStringSubmatch(name)
		if len(subs) != 3 {
			if len(subs) != 0 {
				d.logger.Warn("File ignored by domain scan, more than 3 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startStep, endStep uint64
		if startStep, err = strconv.ParseUint(subs[1], 10, 64); err != nil {
			d.logger.Warn("File ignored by domain scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endStep, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			d.logger.Warn("File ignored by domain scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startStep > endStep {
			d.logger.Warn("File ignored by domain scan, startTxNum > endTxNum", "name", name)
			continue
		}

		startTxNum, endTxNum := startStep*d.aggregationStep, endStep*d.aggregationStep
		var newFile = newFilesItem(startTxNum, endTxNum, d.aggregationStep)
		newFile.frozen = false

		//for _, ext := range d.integrityFileExtensions {
		//	requiredFile := fmt.Sprintf("%s.%d-%d.%s", d.filenameBase, startStep, endStep, ext)
		//	if !dir.FileExist(filepath.Join(d.dir, requiredFile)) {
		//		d.logger.Debug(fmt.Sprintf("[snapshots] skip %s because %s doesn't exists", name, requiredFile))
		//		garbageFiles = append(garbageFiles, newFile)
		//		continue Loop
		//	}
		//}

		if _, has := d.files.Get(newFile); has {
			continue
		}

		addNewFile := true
		var subSets []*filesItem
		d.files.Walk(func(items []*filesItem) bool {
			for _, item := range items {
				if item.isSubsetOf(newFile) {
					subSets = append(subSets, item)
					continue
				}

				if newFile.isSubsetOf(item) {
					if item.frozen {
						addNewFile = false
						garbageFiles = append(garbageFiles, newFile)
					}
					continue
				}
			}
			return true
		})
		if addNewFile {
			d.files.Set(newFile)
		}
	}
	return garbageFiles
}

func (d *Domain) openFiles() (err error) {
	//var totalKeys uint64

	invalidFileItems := make([]*filesItem, 0)
	d.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.decompressor != nil {
				continue
			}
			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
			datPath := d.kvFilePath(fromStep, toStep)
			if !dir.FileExist(datPath) {
				invalidFileItems = append(invalidFileItems, item)
				continue
			}
			if item.decompressor, err = compress.NewDecompressor(datPath); err != nil {
				err = errors.Wrap(err, "decompressor")
				d.logger.Debug("Domain.openFiles: %w, %s", err, datPath)
				return false
			}

			if item.index == nil && !UseBpsTree {
				idxPath := filepath.Join(d.dirs.SnapState, fmt.Sprintf("%s.%d-%d.kvi", d.filenameBase, fromStep, toStep))
				if dir.FileExist(idxPath) {
					if item.index, err = recsplit.OpenIndex(idxPath); err != nil {
						err = errors.Wrap(err, "recsplit index")
						d.logger.Debug("Domain.openFiles: %w, %s", err, idxPath)
						return false
					}
					//totalKeys += item.index.KeyCount()
				}
			}
			if item.bindex == nil {
				bidxPath := d.btIdxFilePath(fromStep, toStep)
				if dir.FileExist(bidxPath) {
					if item.bindex, err = OpenBtreeIndexWithDecompressor(bidxPath, DefaultBtreeM, item.decompressor, d.compression); err != nil {
						err = errors.Wrap(err, "btree index")
						d.logger.Debug("Domain.openFiles: %w, %s", err, bidxPath)
						return false
					}
				}
				//totalKeys += item.bindex.KeyCount()
			}
			if item.bloom == nil {
				idxPath := d.kvExistenceIdxFilePath(fromStep, toStep)
				if dir.FileExist(idxPath) {
					if item.bloom, err = OpenBloom(idxPath); err != nil {
						return false
					}
				}
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	for _, item := range invalidFileItems {
		d.files.Delete(item)
	}

	d.reCalcRoFiles()
	return nil
}

func (d *Domain) closeWhatNotInList(fNames []string) {
	var toDelete []*filesItem
	d.files.Walk(func(items []*filesItem) bool {
	Loop1:
		for _, item := range items {
			for _, protectName := range fNames {
				if item.decompressor != nil && item.decompressor.FileName() == protectName {
					continue Loop1
				}
			}
			toDelete = append(toDelete, item)
		}
		return true
	})
	for _, item := range toDelete {
		if item.decompressor != nil {
			item.decompressor.Close()
			item.decompressor = nil
		}
		if item.index != nil {
			item.index.Close()
			item.index = nil
		}
		if item.bindex != nil {
			item.bindex.Close()
			item.bindex = nil
		}
		d.files.Delete(item)
	}
}

func (d *Domain) reCalcRoFiles() {
	roFiles := ctxFiles(d.files, true, true)
	d.roFiles.Store(&roFiles)
}

func (d *Domain) Close() {
	d.History.Close()
	d.closeWhatNotInList([]string{})
	d.reCalcRoFiles()
}

func (d *Domain) PutWithPrev(key1, key2, val, preval []byte) error {
	// This call to update needs to happen before d.tx.Put() later, because otherwise the content of `preval`` slice is invalidated
	if err := d.History.AddPrevValue(key1, key2, preval); err != nil {
		return err
	}
	return d.wal.addValue(key1, key2, val)
}

func (d *Domain) DeleteWithPrev(key1, key2, prev []byte) (err error) {
	// This call to update needs to happen before d.tx.Delete() later, because otherwise the content of `original`` slice is invalidated
	if err := d.History.AddPrevValue(key1, key2, prev); err != nil {
		return err
	}
	return d.wal.addValue(key1, key2, nil)
}

func (d *Domain) update(key []byte) error {
	var invertedStep [8]byte
	binary.BigEndian.PutUint64(invertedStep[:], ^(d.txNum / d.aggregationStep))
	//fmt.Printf("put: %s, %x, %x\n", d.filenameBase, key, invertedStep[:])
	if err := d.tx.Put(d.keysTable, key, invertedStep[:]); err != nil {
		return err
	}
	return nil
}

func (d *Domain) put(key, val []byte) error {
	if err := d.update(key); err != nil {
		return err
	}
	invertedStep := ^(d.txNum / d.aggregationStep)
	keySuffix := make([]byte, len(key)+8)
	copy(keySuffix, key)
	binary.BigEndian.PutUint64(keySuffix[len(key):], invertedStep)
	//fmt.Printf("put2: %s, %x, %x\n", d.filenameBase, keySuffix, val)
	return d.tx.Put(d.valsTable, keySuffix, val)
}

// Deprecated
func (d *Domain) Put(key1, key2, val []byte) error {
	key := common.Append(key1, key2)
	dc := d.MakeContext()
	original, _, err := dc.GetLatest(key, nil, d.tx)
	if err != nil {
		return err
	}
	dc.Close()
	if bytes.Equal(original, val) {
		return nil
	}
	// This call to update needs to happen before d.tx.Put() later, because otherwise the content of `original`` slice is invalidated
	if err = d.History.AddPrevValue(key1, key2, original); err != nil {
		return err
	}
	return d.put(key, val)
}

// Deprecated
func (d *Domain) Delete(key1, key2 []byte) error {
	key := common.Append(key1, key2)
	dc := d.MakeContext()
	original, found, err := dc.GetLatest(key, nil, d.tx)
	dc.Close()
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	return d.DeleteWithPrev(key1, key2, original)
}

func (d *Domain) newWriter(tmpdir string, buffered, discard bool) *domainWAL {
	w := &domainWAL{d: d,
		tmpdir:      tmpdir,
		buffered:    buffered,
		discard:     discard,
		aux:         make([]byte, 0, 128),
		largeValues: d.domainLargeValues,
	}

	if buffered {
		w.values = etl.NewCollector(d.valsTable, tmpdir, etl.NewSortableBuffer(WALCollectorRAM), d.logger)
		w.values.LogLvl(log.LvlTrace)
		w.keys = etl.NewCollector(d.keysTable, tmpdir, etl.NewSortableBuffer(WALCollectorRAM), d.logger)
		w.keys.LogLvl(log.LvlTrace)
	}
	return w
}

type domainWAL struct {
	d           *Domain
	keys        *etl.Collector
	values      *etl.Collector
	aux         []byte
	tmpdir      string
	buffered    bool
	discard     bool
	largeValues bool
}

func (d *domainWAL) close() {
	if d == nil { // allow dobule-close
		return
	}
	if d.keys != nil {
		d.keys.Close()
	}
	if d.values != nil {
		d.values.Close()
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

func (d *domainWAL) flush(ctx context.Context, tx kv.RwTx) error {
	if d.discard || !d.buffered {
		return nil
	}
	if err := d.keys.Load(tx, d.d.keysTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := d.values.Load(tx, d.d.valsTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	return nil
}

func (d *domainWAL) addValue(key1, key2, value []byte) error {
	if d.discard {
		return nil
	}

	kl := len(key1) + len(key2)
	d.aux = append(append(d.aux[:0], key1...), key2...)
	fullkey := d.aux[:kl+8]
	//TODO: we have ii.txNumBytes, need also have d.stepBytes. update it at d.SetTxNum()
	binary.BigEndian.PutUint64(fullkey[kl:], ^(d.d.txNum / d.d.aggregationStep))
	// defer func() {
	// 	fmt.Printf("addValue %x->%x buffered %t largeVals %t file %s\n", fullkey, value, d.buffered, d.largeValues, d.d.filenameBase)
	// }()

	if d.largeValues {
		if d.buffered {
			if err := d.keys.Collect(fullkey[:kl], fullkey[kl:]); err != nil {
				return err
			}
			if err := d.values.Collect(fullkey, value); err != nil {
				return err
			}
			return nil
		}
		if err := d.d.tx.Put(d.d.keysTable, fullkey[:kl], fullkey[kl:]); err != nil {
			return err
		}
		if err := d.d.tx.Put(d.d.valsTable, fullkey, value); err != nil {
			return err
		}
		return nil
	}

	if d.buffered {
		if err := d.keys.Collect(fullkey[:kl], fullkey[kl:]); err != nil {
			return err
		}
		if err := d.values.Collect(fullkey[:kl], common.Append(fullkey[kl:], value)); err != nil {
			return err
		}
		return nil
	}
	if err := d.d.tx.Put(d.d.keysTable, fullkey[:kl], fullkey[kl:]); err != nil {
		return err
	}
	if err := d.d.tx.Put(d.d.valsTable, fullkey[:kl], common.Append(fullkey[kl:], value)); err != nil {
		return err
	}
	return nil
}

type CursorType uint8

const (
	FILE_CURSOR CursorType = iota
	DB_CURSOR
	RAM_CURSOR
)

// CursorItem is the item in the priority queue used to do merge interation
// over storage of a given account
type CursorItem struct {
	c            kv.CursorDupSort
	iter         btree2.MapIter[string, []byte]
	dg           ArchiveGetter
	dg2          ArchiveGetter
	btCursor     *Cursor
	key          []byte
	val          []byte
	endTxNum     uint64
	latestOffset uint64     // offset of the latest value in the file
	t            CursorType // Whether this item represents state file or DB record, or tree
	reverse      bool
}

type CursorHeap []*CursorItem

func (ch CursorHeap) Len() int {
	return len(ch)
}

func (ch CursorHeap) Less(i, j int) bool {
	cmp := bytes.Compare(ch[i].key, ch[j].key)
	if cmp == 0 {
		// when keys match, the items with later blocks are preferred
		if ch[i].reverse {
			return ch[i].endTxNum > ch[j].endTxNum
		}
		return ch[i].endTxNum < ch[j].endTxNum
	}
	return cmp < 0
}

func (ch *CursorHeap) Swap(i, j int) {
	(*ch)[i], (*ch)[j] = (*ch)[j], (*ch)[i]
}

func (ch *CursorHeap) Push(x interface{}) {
	*ch = append(*ch, x.(*CursorItem))
}

func (ch *CursorHeap) Pop() interface{} {
	old := *ch
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*ch = old[0 : n-1]
	return x
}

// filesItem corresponding to a pair of files (.dat and .idx)
type ctxItem struct {
	getter     *compress.Getter
	reader     *recsplit.IndexReader
	startTxNum uint64
	endTxNum   uint64

	i   int
	src *filesItem
}

func (i *ctxItem) isSubSetOf(j *ctxItem) bool { return i.src.isSubsetOf(j.src) } //nolint
func (i *ctxItem) isSubsetOf(j *ctxItem) bool { return i.src.isSubsetOf(j.src) } //nolint

type ctxLocalityIdx struct {
	reader          *recsplit.IndexReader
	file            *ctxItem
	aggregationStep uint64
}

// DomainContext allows accesing the same domain from multiple go-routines
type DomainContext struct {
	d          *Domain
	files      []ctxItem
	getters    []ArchiveGetter
	readers    []*BtIndex
	idxReaders []*recsplit.IndexReader
	hc         *HistoryContext
	keyBuf     [60]byte // 52b key and 8b for inverted step
	valKeyBuf  [60]byte // 52b key and 8b for inverted step
	numBuf     [8]byte

	keysC kv.CursorDupSort
	valsC kv.Cursor
}

// getFromFile returns exact match for the given key from the given file
func (dc *DomainContext) getFromFile(i int, filekey []byte) ([]byte, bool, error) {
	g := dc.statelessGetter(i)
	if UseBtree || UseBpsTree {
		if dc.d.withExistenceIndex && dc.files[i].src.bloom != nil {
			hi, _ := dc.hc.ic.hashKey(filekey)
			if !dc.files[i].src.bloom.ContainsHash(hi) {
				return nil, false, nil
			}
		}

		_, v, ok, err := dc.statelessBtree(i).Get(filekey, g)
		if err != nil || !ok {
			return nil, false, err
		}
		//fmt.Printf("getLatestFromBtreeColdFiles key %x shard %d %x\n", filekey, exactColdShard, v)
		return v, true, nil
	}

	reader := dc.statelessIdxReader(i)
	if reader.Empty() {
		return nil, false, nil
	}
	offset := reader.Lookup(filekey)
	g.Reset(offset)

	k, _ := g.Next(nil)
	if !bytes.Equal(filekey, k) {
		return nil, false, nil
	}
	v, _ := g.Next(nil)
	return v, true, nil
}

func (dc *DomainContext) getFromFile2(i int, filekey []byte) ([]byte, bool, error) {
	g := dc.statelessGetter(i)
	if UseBtree || UseBpsTree {
		_, v, ok, err := dc.statelessBtree(i).Get(filekey, g)
		if err != nil || !ok {
			return nil, false, err
		}
		//fmt.Printf("getLatestFromBtreeColdFiles key %x shard %d %x\n", filekey, exactColdShard, v)
		return v, true, nil
	}

	reader := dc.statelessIdxReader(i)
	if reader.Empty() {
		return nil, false, nil
	}
	offset := reader.Lookup(filekey)
	g.Reset(offset)

	k, _ := g.Next(nil)
	if !bytes.Equal(filekey, k) {
		return nil, false, nil
	}
	v, _ := g.Next(nil)
	return v, true, nil
}

func (d *Domain) collectFilesStats() (datsz, idxsz, files uint64) {
	d.History.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.index == nil {
				return false
			}
			datsz += uint64(item.decompressor.Size())
			idxsz += uint64(item.index.Size())
			idxsz += uint64(item.bindex.Size())
			files += 3
		}
		return true
	})

	d.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.index == nil {
				return false
			}
			datsz += uint64(item.decompressor.Size())
			idxsz += uint64(item.index.Size())
			idxsz += uint64(item.bindex.Size())
			files += 3
		}
		return true
	})

	fcnt, fsz, isz := d.History.InvertedIndex.collectFilesStat()
	datsz += fsz
	files += fcnt
	idxsz += isz
	return
}

func (d *Domain) MakeContext() *DomainContext {
	files := *d.roFiles.Load()
	for i := 0; i < len(files); i++ {
		if !files[i].src.frozen {
			files[i].src.refcount.Add(1)
		}
	}
	return &DomainContext{
		d:     d,
		hc:    d.History.MakeContext(),
		files: files,
	}
}

// Collation is the set of compressors created after aggregation
type Collation struct {
	HistoryCollation
	valuesComp  *compress.Compressor
	valuesPath  string
	valuesCount int
}

func (c Collation) Close() {
	if c.valuesComp != nil {
		c.valuesComp.Close()
	}
	if c.historyComp != nil {
		c.HistoryCollation.Close()
	}
}

// collate gathers domain changes over the specified step, using read-only transaction,
// and returns compressors, elias fano, and bitmaps
// [txFrom; txTo)
func (d *Domain) collate(ctx context.Context, step, txFrom, txTo uint64, roTx kv.Tx) (coll Collation, err error) {
	mxRunningCollations.Inc()
	started := time.Now()
	defer func() {
		d.stats.LastCollationTook = time.Since(started)
		mxRunningCollations.Dec()
		mxCollateTook.UpdateDuration(started)
	}()

	coll.HistoryCollation, err = d.History.collate(step, txFrom, txTo, roTx)
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
	if coll.valuesComp, err = compress.NewCompressor(context.Background(), "collate values", coll.valuesPath, d.tmpdir, compress.MinPatternScore, d.compressWorkers, log.LvlTrace, d.logger); err != nil {
		return Collation{}, fmt.Errorf("create %s values compressor: %w", d.filenameBase, err)
	}
	comp := NewArchiveWriter(coll.valuesComp, d.compression)

	keysCursor, err := roTx.CursorDupSort(d.keysTable)
	if err != nil {
		return Collation{}, fmt.Errorf("create %s keys cursor: %w", d.filenameBase, err)
	}
	defer keysCursor.Close()

	var (
		pos       uint64
		stepBytes = make([]byte, 8)
		keySuffix = make([]byte, 256+8)
		v         []byte

		valsDup kv.CursorDupSort
	)
	binary.BigEndian.PutUint64(stepBytes, ^step)
	if !d.domainLargeValues {
		valsDup, err = roTx.CursorDupSort(d.valsTable)
		if err != nil {
			return Collation{}, fmt.Errorf("create %s values cursorDupsort: %w", d.filenameBase, err)
		}
		defer valsDup.Close()
	}

	if err := func() error {
		for k, stepInDB, err := keysCursor.First(); k != nil; k, stepInDB, err = keysCursor.Next() {
			if err != nil {
				return err
			}
			pos++
			if !bytes.Equal(stepBytes, stepInDB) {
				continue
			}

			copy(keySuffix, k)
			copy(keySuffix[len(k):], stepInDB)

			switch d.domainLargeValues {
			case true:
				v, err = roTx.GetOne(d.valsTable, keySuffix[:len(k)+8])
			default:
				v, err = valsDup.SeekBothRange(keySuffix[:len(k)], keySuffix[len(k):len(k)+8])
				//fmt.Printf("seek: %x -> %x\n", keySuffix[:len(k)], v)
				for {
					k, _, _ := valsDup.Next()
					if len(k) == 0 {
						break
					}

					if bytes.HasPrefix(k, keySuffix[:len(k)]) {
						//fmt.Printf("next: %x -> %x\n", k, v)
					} else {
						break
					}
				}
			}
			if err != nil {
				return fmt.Errorf("find last %s value for aggregation step k=[%x]: %w", d.filenameBase, k, err)
			}

			if err = comp.AddWord(k); err != nil {
				return fmt.Errorf("add %s values key [%x]: %w", d.filenameBase, k, err)
			}
			if err = comp.AddWord(v); err != nil {
				return fmt.Errorf("add %s values [%x]=>[%x]: %w", d.filenameBase, k, v, err)
			}
			mxCollationSize.Inc()

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		return nil
	}(); err != nil {
		return Collation{}, fmt.Errorf("iterate over %s keys cursor: %w", d.filenameBase, err)
	}

	closeCollation = false
	coll.valuesCount = coll.valuesComp.Count() / 2
	return coll, nil
}

type StaticFiles struct {
	HistoryFiles
	valuesDecomp *compress.Decompressor
	valuesIdx    *recsplit.Index
	valuesBt     *BtIndex
	bloom        *bloomFilter
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
	if sf.historyDecomp != nil {
		sf.historyDecomp.Close()
	}
	if sf.historyIdx != nil {
		sf.historyIdx.Close()
	}
	if sf.efHistoryDecomp != nil {
		sf.efHistoryDecomp.Close()
	}
	if sf.efHistoryIdx != nil {
		sf.efHistoryIdx.Close()
	}
}

// buildFiles performs potentially resource intensive operations of creating
// static files and their indices
func (d *Domain) buildFiles(ctx context.Context, step uint64, collation Collation, ps *background.ProgressSet) (StaticFiles, error) {
	if d.filenameBase == AggTraceFileLife {
		d.logger.Warn("[snapshots] buildFiles", "step", step, "domain", d.filenameBase)
	}

	start := time.Now()
	defer func() {
		d.stats.LastFileBuildingTook = time.Since(start)
	}()

	hStaticFiles, err := d.History.buildFiles(ctx, step, collation.HistoryCollation, ps)
	if err != nil {
		return StaticFiles{}, err
	}
	valuesComp := collation.valuesComp
	var valuesDecomp *compress.Decompressor
	var valuesIdx *recsplit.Index
	closeComp := true
	defer func() {
		if closeComp {
			hStaticFiles.Close()
			if valuesComp != nil {
				valuesComp.Close()
			}
			if valuesDecomp != nil {
				valuesDecomp.Close()
			}
			if valuesIdx != nil {
				valuesIdx.Close()
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
	if valuesDecomp, err = compress.NewDecompressor(collation.valuesPath); err != nil {
		return StaticFiles{}, fmt.Errorf("open %s values decompressor: %w", d.filenameBase, err)
	}

	valuesIdxFileName := fmt.Sprintf("%s.%d-%d.kvi", d.filenameBase, step, step+1)
	valuesIdxPath := filepath.Join(d.dir, valuesIdxFileName)
	if !UseBpsTree {
		if valuesIdx, err = buildIndexThenOpen(ctx, valuesDecomp, d.compression, valuesIdxPath, d.tmpdir, false, d.salt, ps, d.logger, d.noFsync); err != nil {
			return StaticFiles{}, fmt.Errorf("build %s values idx: %w", d.filenameBase, err)
		}
	}

	var bt *BtIndex
	{
		btPath := d.btIdxFilePath(step, step+1)
		bt, err = CreateBtreeIndexWithDecompressor(btPath, DefaultBtreeM, valuesDecomp, d.compression, *d.salt, ps, d.tmpdir, d.logger)
		if err != nil {
			return StaticFiles{}, fmt.Errorf("build %s .bt idx: %w", d.filenameBase, err)
		}
	}
	var bloom *bloomFilter
	{
		fileName := fmt.Sprintf("%s.%d-%d.kvei", d.filenameBase, step, step+1)
		if dir.FileExist(filepath.Join(d.dir, fileName)) {
			bloom, err = OpenBloom(filepath.Join(d.dir, fileName))
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

func (d *Domain) missedBtreeIdxFiles() (l []*filesItem) {
	d.files.Walk(func(items []*filesItem) bool { // don't run slow logic while iterating on btree
		for _, item := range items {
			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
			fPath := d.btIdxFilePath(fromStep, toStep)
			if !dir.FileExist(fPath) {
				l = append(l, item)
			}
		}
		return true
	})
	return l
}
func (d *Domain) missedKviIdxFiles() (l []*filesItem) {
	d.files.Walk(func(items []*filesItem) bool { // don't run slow logic while iterating on btree
		for _, item := range items {
			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
			if !dir.FileExist(filepath.Join(d.dir, fmt.Sprintf("%s.%d-%d.kvi", d.filenameBase, fromStep, toStep))) {
				l = append(l, item)
			}
		}
		return true
	})
	return l
}

//func (d *Domain) missedIdxFilesBloom() (l []*filesItem) {
//	d.files.Walk(func(items []*filesItem) bool { // don't run slow logic while iterating on btree
//		for _, item := range items {
//			fromStep, toStep := item.startTxNum/d.aggregationStep, item.endTxNum/d.aggregationStep
//			if !dir.FileExist(filepath.Join(d.dir, fmt.Sprintf("%s.%d-%d.kvei", d.filenameBase, fromStep, toStep))) {
//				l = append(l, item)
//			}
//		}
//		return true
//	})
//	return l
//}

// BuildMissedIndices - produce .efi/.vi/.kvi from .ef/.v/.kv
func (d *Domain) BuildMissedIndices(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet) {
	d.History.BuildMissedIndices(ctx, g, ps)
	for _, item := range d.missedBtreeIdxFiles() {
		fitem := item
		g.Go(func() error {
			idxPath := fitem.decompressor.FilePath()
			idxPath = strings.TrimSuffix(idxPath, "kv") + "bt"
			if err := BuildBtreeIndexWithDecompressor(idxPath, fitem.decompressor, CompressNone, ps, d.tmpdir, *d.salt, d.logger); err != nil {
				return fmt.Errorf("failed to build btree index for %s:  %w", fitem.decompressor.FileName(), err)
			}
			return nil
		})
	}
	for _, item := range d.missedKviIdxFiles() {
		fitem := item
		g.Go(func() error {
			if UseBpsTree {
				return nil
			}

			idxPath := fitem.decompressor.FilePath()
			idxPath = strings.TrimSuffix(idxPath, "kv") + "kvi"
			ix, err := buildIndexThenOpen(ctx, fitem.decompressor, d.compression, idxPath, d.tmpdir, false, d.salt, ps, d.logger, d.noFsync)
			if err != nil {
				return fmt.Errorf("build %s values recsplit index: %w", d.filenameBase, err)
			}
			ix.Close()
			return nil
		})
	}
	//for _, item := range d.missedIdxFilesBloom() {
	//	fitem := item
	//	g.Go(func() error {
	//		if UseBpsTree {
	//			return nil
	//		}
	//
	//		idxPath := fitem.decompressor.FilePath()
	//		idxPath = strings.TrimSuffix(idxPath, "kv") + "ibl"
	//		ix, err := buildIndexThenOpen(ctx, fitem.decompressor, d.compression, idxPath, d.tmpdir, false, ps, d.logger, d.noFsync)
	//		if err != nil {
	//			return fmt.Errorf("build %s values recsplit index: %w", d.filenameBase, err)
	//		}
	//		ix.Close()
	//		return nil
	//	})
	//}
}

func buildIndexThenOpen(ctx context.Context, d *compress.Decompressor, compressed FileCompression, idxPath, tmpdir string, values bool, salt *uint32, ps *background.ProgressSet, logger log.Logger, noFsync bool) (*recsplit.Index, error) {
	if err := buildIndex(ctx, d, compressed, idxPath, tmpdir, values, salt, ps, logger, noFsync); err != nil {
		return nil, err
	}
	return recsplit.OpenIndex(idxPath)
}
func buildIndexFilterThenOpen(ctx context.Context, d *compress.Decompressor, compressed FileCompression, idxPath, tmpdir string, salt *uint32, ps *background.ProgressSet, logger log.Logger, noFsync bool) (*bloomFilter, error) {
	if err := buildIdxFilter(ctx, d, compressed, idxPath, salt, ps, logger, noFsync); err != nil {
		return nil, err
	}
	if !dir.FileExist(idxPath) {
		return nil, nil
	}
	return OpenBloom(idxPath)
}
func buildIndex(ctx context.Context, d *compress.Decompressor, compressed FileCompression, idxPath, tmpdir string, values bool, salt *uint32, ps *background.ProgressSet, logger log.Logger, noFsync bool) error {
	_, fileName := filepath.Split(idxPath)
	count := d.Count()
	if !values {
		count = d.Count() / 2
	}
	p := ps.AddNew(fileName, uint64(count))
	defer ps.Delete(p)

	defer d.EnableReadAhead().DisableReadAhead()

	g := NewArchiveGetter(d.MakeGetter(), compressed)
	var rs *recsplit.RecSplit
	var err error
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:    count,
		Enums:       false,
		BucketSize:  2000,
		LeafSize:    8,
		TmpDir:      tmpdir,
		IndexFile:   idxPath,
		Salt:        salt,
		EtlBufLimit: etl.BufferOptimalSize / 2,
	}, logger); err != nil {
		return fmt.Errorf("create recsplit: %w", err)
	}
	defer rs.Close()
	rs.LogLvl(log.LvlTrace)
	if noFsync {
		rs.DisableFsync()
	}

	word := make([]byte, 0, 256)
	var keyPos, valPos uint64
	for {
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

func (d *Domain) integrateFiles(sf StaticFiles, txNumFrom, txNumTo uint64) {
	d.History.integrateFiles(sf.HistoryFiles, txNumFrom, txNumTo)

	fi := newFilesItem(txNumFrom, txNumTo, d.aggregationStep)
	fi.frozen = false
	fi.decompressor = sf.valuesDecomp
	fi.index = sf.valuesIdx
	fi.bindex = sf.valuesBt
	fi.bloom = sf.bloom
	d.files.Set(fi)

	d.reCalcRoFiles()
}

// unwind is similar to prune but the difference is that it restores domain values from the history as of txFrom
func (dc *DomainContext) Unwind(ctx context.Context, rwTx kv.RwTx, step, txFrom, txTo, limit uint64, f func(step uint64, k, v []byte) error) error {
	d := dc.d
	keysCursorForDeletes, err := rwTx.RwCursorDupSort(d.keysTable)
	if err != nil {
		return fmt.Errorf("create %s domain cursor: %w", d.filenameBase, err)
	}
	defer keysCursorForDeletes.Close()
	keysCursor, err := rwTx.RwCursorDupSort(d.keysTable)
	if err != nil {
		return fmt.Errorf("create %s domain cursor: %w", d.filenameBase, err)
	}
	defer keysCursor.Close()

	var k, v []byte
	var valsC kv.RwCursor
	var valsCDup kv.RwCursorDupSort

	if d.domainLargeValues {
		valsC, err = rwTx.RwCursor(d.valsTable)
		if err != nil {
			return err
		}
		defer valsC.Close()
	} else {
		valsCDup, err = rwTx.RwCursorDupSort(d.valsTable)
		if err != nil {
			return err
		}
		defer valsCDup.Close()
	}
	if err != nil {
		return err
	}

	//fmt.Printf("unwind %s txs [%d; %d) step %d\n", d.filenameBase, txFrom, txTo, step)

	stepBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(stepBytes, ^step)

	restore := d.newWriter(filepath.Join(d.tmpdir, "unwind"+d.filenameBase), true, false)

	for k, v, err = keysCursor.First(); err == nil && k != nil; k, v, err = keysCursor.Next() {
		if !bytes.Equal(v, stepBytes) {
			continue
		}

		edgeRecords, err := d.History.unwindKey(k, txFrom, rwTx)
		//fmt.Printf("unwind %x to tx %d edges %+v\n", k, txFrom, edgeRecords)
		if err != nil {
			return err
		}
		switch len(edgeRecords) {
		case 1: // its value should be nil, actual value is in domain, BUT if txNum exactly match, need to restore
			//fmt.Printf("recent %x txn %d '%x'\n", k, edgeRecords[0].TxNum, edgeRecords[0].Value)
			if edgeRecords[0].TxNum == txFrom && edgeRecords[0].Value != nil {
				d.SetTxNum(edgeRecords[0].TxNum)
				if err := restore.addValue(k, nil, edgeRecords[0].Value); err != nil {
					return err
				}
			} else if edgeRecords[0].TxNum < txFrom {
				continue
			}
		case 2: // here one first value is before txFrom (holds txNum when value was set) and second is after (actual value at that txNum)
			l, r := edgeRecords[0], edgeRecords[1]
			if r.TxNum >= txFrom /*&& l.TxNum < txFrom*/ && r.Value != nil {
				d.SetTxNum(l.TxNum)
				if err := restore.addValue(k, nil, r.Value); err != nil {
					return err
				}
			} else {
				continue
			}
			//fmt.Printf("restore %x txn [%d, %d] '%x' '%x'\n", k, l.TxNum, r.TxNum, l.Value, r.Value)
		}

		seek := common.Append(k, stepBytes)
		if d.domainLargeValues {
			kk, vv, err := valsC.SeekExact(seek)
			if err != nil {
				return err
			}
			if f != nil {
				if err := f(step, kk, vv); err != nil {
					return err
				}
			}
			if kk != nil {
				//fmt.Printf("rm large value %x v %x\n", kk, vv)
				if err = valsC.DeleteCurrent(); err != nil {
					return err
				}
			}
		} else {
			vv, err := valsCDup.SeekBothRange(seek, stepBytes)
			if err != nil {
				return err
			}
			if f != nil {
				if err := f(step, k, vv); err != nil {
					return err
				}
			}
			//fmt.Printf("rm %d dupes %x v %x\n", dups, seek, vv)
			if err = valsCDup.DeleteCurrentDuplicates(); err != nil {
				return err
			}
		}

		// This DeleteCurrent needs to the last in the loop iteration, because it invalidates k and v
		if _, _, err = keysCursorForDeletes.SeekBothExact(k, v); err != nil {
			return err
		}
		if err = keysCursorForDeletes.DeleteCurrent(); err != nil {
			return err
		}
	}
	if err != nil {
		return fmt.Errorf("iterate over %s domain keys: %w", d.filenameBase, err)
	}

	if err = restore.flush(ctx, rwTx); err != nil {
		return err
	}

	logEvery := time.NewTicker(time.Second * 30)
	defer logEvery.Stop()

	if err := dc.hc.Prune(ctx, rwTx, txFrom, txTo, limit, logEvery); err != nil {
		return fmt.Errorf("prune history at step %d [%d, %d): %w", step, txFrom, txTo, err)
	}
	return nil
}

func (d *Domain) isEmpty(tx kv.Tx) (bool, error) {
	k, err := kv.FirstKey(tx, d.keysTable)
	if err != nil {
		return false, err
	}
	k2, err := kv.FirstKey(tx, d.valsTable)
	if err != nil {
		return false, err
	}
	isEmptyHist, err := d.History.isEmpty(tx)
	if err != nil {
		return false, err
	}
	return k == nil && k2 == nil && isEmptyHist, nil
}

// nolint
func (d *Domain) warmup(ctx context.Context, txFrom, limit uint64, tx kv.Tx) error {
	domainKeysCursor, err := tx.CursorDupSort(d.keysTable)
	if err != nil {
		return fmt.Errorf("create %s domain cursor: %w", d.filenameBase, err)
	}
	defer domainKeysCursor.Close()
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	idxC, err := tx.CursorDupSort(d.keysTable)
	if err != nil {
		return err
	}
	defer idxC.Close()
	valsC, err := tx.Cursor(d.valsTable)
	if err != nil {
		return err
	}
	defer valsC.Close()
	k, v, err := domainKeysCursor.Seek(txKey[:])
	if err != nil {
		return err
	}
	if k == nil {
		return nil
	}
	txFrom = binary.BigEndian.Uint64(k)
	txTo := txFrom + d.aggregationStep
	if limit != math.MaxUint64 && limit != 0 {
		txTo = txFrom + limit
	}
	for ; k != nil; k, v, err = domainKeysCursor.Next() {
		if err != nil {
			return fmt.Errorf("iterate over %s domain keys: %w", d.filenameBase, err)
		}
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		_, _, _ = valsC.Seek(v[len(v)-8:])
		_, _ = idxC.SeekBothRange(v[:len(v)-8], k)

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	return d.History.warmup(ctx, txFrom, limit, tx)
}

func (d *Domain) Rotate() flusher {
	hf := d.History.Rotate()
	if d.wal != nil {
		w := d.wal
		if w.buffered {
			if err := w.keys.Flush(); err != nil {
				panic(err)
			}
			if err := w.values.Flush(); err != nil {
				panic(err)
			}
		}
		hf.d = w
		d.wal = d.newWriter(d.wal.tmpdir, d.wal.buffered, d.wal.discard)
	}
	return hf
}

var (
	UseBtree = true // if true, will use btree for all files
)

func (dc *DomainContext) getLatestFromFilesWithExistenceIndex(filekey []byte) (v []byte, found bool, err error) {
	hi, _ := dc.hc.ic.hashKey(filekey)

	for i := len(dc.files) - 1; i >= 0; i-- {
		if dc.d.withExistenceIndex && dc.files[i].src.bloom != nil {
			if !dc.files[i].src.bloom.ContainsHash(hi) {
				continue
			}
		}

		t := time.Now()
		v, found, err = dc.getFromFile2(i, filekey)
		if err != nil {
			return nil, false, err
		}
		if !found {
			LatestStateReadGrindNotFound.UpdateDuration(t)
			continue
		}
		LatestStateReadGrind.UpdateDuration(t)
		return v, true, nil
	}
	return nil, false, nil
}
func (dc *DomainContext) getLatestFromFiles(filekey []byte) (v []byte, found bool, err error) {
	if dc.d.withExistenceIndex {
		return dc.getLatestFromFilesWithExistenceIndex(filekey)
	}

	if v, found, err = dc.getLatestFromWarmFiles(filekey); err != nil {
		return nil, false, err
	} else if found {
		return v, true, nil
	}

	if v, found, err = dc.getLatestFromColdFilesGrind(filekey); err != nil {
		return nil, false, err
	} else if found {
		return v, true, nil
	}

	// still not found, search in indexed cold shards
	return dc.getLatestFromColdFiles(filekey)
}

func (dc *DomainContext) getLatestFromWarmFiles(filekey []byte) ([]byte, bool, error) {
	exactWarmStep, ok, err := dc.hc.ic.warmLocality.lookupLatest(filekey)
	if err != nil {
		return nil, false, err
	}
	// _ = ok
	if !ok {
		return nil, false, nil
	}

	t := time.Now()
	exactTxNum := exactWarmStep * dc.d.aggregationStep
	for i := len(dc.files) - 1; i >= 0; i-- {
		isUseful := dc.files[i].startTxNum <= exactTxNum && dc.files[i].endTxNum > exactTxNum
		if !isUseful {
			continue
		}

		v, found, err := dc.getFromFile(i, filekey)
		if err != nil {
			return nil, false, err
		}
		if !found {
			LatestStateReadWarmNotFound.UpdateDuration(t)
			t = time.Now()
			continue
		}
		// fmt.Printf("warm [%d] want %x keys i idx %v %v\n", i, filekey, bt.ef.Count(), bt.decompressor.FileName())

		LatestStateReadWarm.UpdateDuration(t)
		return v, found, nil
	}
	return nil, false, nil
}

func (dc *DomainContext) getLatestFromColdFilesGrind(filekey []byte) (v []byte, found bool, err error) {
	// sometimes there is a gap between indexed cold files and indexed warm files. just grind them.
	// possible reasons:
	// - no locality indices at all
	// - cold locality index is "lazy"-built
	// corner cases:
	// - cold and warm segments can overlap
	lastColdIndexedTxNum := dc.hc.ic.coldLocality.indexedTo()
	firstWarmIndexedTxNum, haveWarmIdx := dc.hc.ic.warmLocality.indexedFrom()
	if !haveWarmIdx && len(dc.files) > 0 {
		firstWarmIndexedTxNum = dc.files[len(dc.files)-1].endTxNum
	}

	if firstWarmIndexedTxNum <= lastColdIndexedTxNum {
		return nil, false, nil
	}

	t := time.Now()
	//if firstWarmIndexedTxNum/dc.d.aggregationStep-lastColdIndexedTxNum/dc.d.aggregationStep > 0 && dc.d.withLocalityIndex {
	//	if dc.d.filenameBase != "commitment" {
	//		log.Warn("[dbg] gap between warm and cold locality", "cold", lastColdIndexedTxNum/dc.d.aggregationStep, "warm", firstWarmIndexedTxNum/dc.d.aggregationStep, "nil", dc.hc.ic.coldLocality == nil, "name", dc.d.filenameBase)
	//		if dc.hc.ic.coldLocality != nil && dc.hc.ic.coldLocality.file != nil {
	//			log.Warn("[dbg] gap", "cold_f", dc.hc.ic.coldLocality.file.src.bm.FileName())
	//		}
	//		if dc.hc.ic.warmLocality != nil && dc.hc.ic.warmLocality.file != nil {
	//			log.Warn("[dbg] gap", "warm_f", dc.hc.ic.warmLocality.file.src.bm.FileName())
	//		}
	//	}
	//}

	for i := len(dc.files) - 1; i >= 0; i-- {
		isUseful := dc.files[i].startTxNum >= lastColdIndexedTxNum && dc.files[i].endTxNum <= firstWarmIndexedTxNum
		if !isUseful {
			continue
		}
		v, ok, err := dc.getFromFile(i, filekey)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			LatestStateReadGrindNotFound.UpdateDuration(t)
			t = time.Now()
			continue
		}
		LatestStateReadGrind.UpdateDuration(t)
		return v, true, nil
	}
	return nil, false, nil
}

func (dc *DomainContext) getLatestFromColdFiles(filekey []byte) (v []byte, found bool, err error) {
	// exactColdShard, ok, err := dc.hc.ic.coldLocality.lookupLatest(filekey)
	// if err != nil {
	// 	return nil, false, err
	// }
	// _ = ok
	// if !ok {
	// 	return nil, false, nil
	// }
	//dc.d.stats.FilesQuerie.Add(1)
	t := time.Now()
	// exactTxNum := exactColdShard * StepsInColdFile * dc.d.aggregationStep
	// fmt.Printf("exactColdShard: %d, exactTxNum=%d\n", exactColdShard, exactTxNum)
	for i := len(dc.files) - 1; i >= 0; i-- {
		// isUseful := dc.files[i].startTxNum <= exactTxNum && dc.files[i].endTxNum > exactTxNum
		//fmt.Printf("read3: %s, %t, %d-%d\n", dc.files[i].src.decompressor.FileName(), isUseful, dc.files[i].startTxNum, dc.files[i].endTxNum)
		// if !isUseful {
		// 	continue
		// }
		v, found, err = dc.getFromFile(i, filekey)
		if err != nil {
			return nil, false, err
		}
		if !found {
			LatestStateReadColdNotFound.UpdateDuration(t)
			t = time.Now()
			continue
		}
		LatestStateReadCold.UpdateDuration(t)
		return v, true, nil
	}
	return nil, false, nil
}

// GetAsOf does not always require usage of roTx. If it is possible to determine
// historical value based only on static files, roTx will not be used.
func (dc *DomainContext) GetAsOf(key []byte, txNum uint64, roTx kv.Tx) ([]byte, error) {
	v, hOk, err := dc.hc.GetNoStateWithRecent(key, txNum, roTx)
	if err != nil {
		return nil, err
	}
	if hOk {
		// if history returned marker of key creation
		// domain must return nil
		if len(v) == 0 {
			return nil, nil
		}
		return v, nil
	}
	v, _, err = dc.GetLatest(key, nil, roTx)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (dc *DomainContext) Close() {
	if dc.files == nil { // invariant: it's safe to call Close multiple times
		return
	}
	files := dc.files
	dc.files = nil
	for i := 0; i < len(files); i++ {
		if files[i].src.frozen {
			continue
		}
		refCnt := files[i].src.refcount.Add(-1)
		//GC: last reader responsible to remove useles files: close it and delete
		if refCnt == 0 && files[i].src.canDelete.Load() {
			files[i].src.closeFilesAndRemove()
		}
	}
	//for _, r := range dc.readers {
	//	r.Close()
	//}
	dc.hc.Close()
}

func (dc *DomainContext) statelessGetter(i int) ArchiveGetter {
	if dc.getters == nil {
		dc.getters = make([]ArchiveGetter, len(dc.files))
	}
	r := dc.getters[i]
	if r == nil {
		r = NewArchiveGetter(dc.files[i].src.decompressor.MakeGetter(), dc.d.compression)
		dc.getters[i] = r
	}
	return r
}

func (dc *DomainContext) statelessIdxReader(i int) *recsplit.IndexReader {
	if dc.idxReaders == nil {
		dc.idxReaders = make([]*recsplit.IndexReader, len(dc.files))
	}
	r := dc.idxReaders[i]
	if r == nil {
		r = dc.files[i].src.index.GetReaderFromPool()
		dc.idxReaders[i] = r
	}
	return r
}

func (dc *DomainContext) statelessBtree(i int) *BtIndex {
	if dc.readers == nil {
		dc.readers = make([]*BtIndex, len(dc.files))
	}
	r := dc.readers[i]
	if r == nil {
		r = dc.files[i].src.bindex
		dc.readers[i] = r
	}
	return r
}

func (dc *DomainContext) valsCursor(tx kv.Tx) (c kv.Cursor, err error) {
	if dc.valsC != nil {
		return dc.valsC, nil
	}
	dc.valsC, err = tx.Cursor(dc.d.valsTable)
	if err != nil {
		return nil, err
	}
	return dc.valsC, nil
}

func (dc *DomainContext) keysCursor(tx kv.Tx) (c kv.CursorDupSort, err error) {
	if dc.keysC != nil {
		return dc.keysC, nil
	}
	dc.keysC, err = tx.CursorDupSort(dc.d.keysTable)
	if err != nil {
		return nil, err
	}
	return dc.keysC, nil
}

func (dc *DomainContext) GetLatest(key1, key2 []byte, roTx kv.Tx) ([]byte, bool, error) {
	//t := time.Now()
	key := key1
	if len(key2) > 0 {
		key = append(append(dc.keyBuf[:0], key1...), key2...)
	}

	var (
		v   []byte
		err error
	)

	keysC, err := dc.keysCursor(roTx)
	if err != nil {
		return nil, false, err
	}
	_, foundInvStep, err := keysC.SeekExact(key) // reads first DupSort value
	if err != nil {
		return nil, false, err
	}
	if foundInvStep != nil {
		copy(dc.valKeyBuf[:], key)
		copy(dc.valKeyBuf[len(key):], foundInvStep)

		switch dc.d.domainLargeValues {
		case true:
			valsC, err := dc.valsCursor(roTx)
			if err != nil {
				return nil, false, err
			}
			_, v, err = valsC.SeekExact(dc.valKeyBuf[:len(key)+8])
			if err != nil {
				return nil, false, fmt.Errorf("GetLatest value: %w", err)
			}
		default:
			valsDup, err := roTx.CursorDupSort(dc.d.valsTable)
			if err != nil {
				return nil, false, err
			}
			v, err = valsDup.SeekBothRange(dc.valKeyBuf[:len(key)], dc.valKeyBuf[len(key):len(key)+8])
			if err != nil {
				return nil, false, fmt.Errorf("GetLatest value: %w", err)
			}
		}

		//LatestStateReadDB.UpdateDuration(t)
		return v, true, nil
	}
	//LatestStateReadDBNotFound.UpdateDuration(t)

	v, found, err := dc.getLatestFromFiles(key)
	if err != nil {
		return nil, false, err
	}
	return v, found, nil
}

func (dc *DomainContext) IteratePrefix(roTx kv.Tx, prefix []byte, it func(k, v []byte)) error {
	var cp CursorHeap
	heap.Init(&cp)
	var k, v []byte
	var err error

	//iter := sd.storage.Iter()
	//if iter.Seek(string(prefix)) {
	//	kx := iter.Key()
	//	v = iter.Value()
	//	k = []byte(kx)
	//
	//	if len(kx) > 0 && bytes.HasPrefix(k, prefix) {
	//		heap.Push(&cp, &CursorItem{t: RAM_CURSOR, key: common.Copy(k), val: common.Copy(v), iter: iter, endTxNum: sd.txNum.Load(), reverse: true})
	//	}
	//}

	keysCursor, err := roTx.CursorDupSort(dc.d.keysTable)
	if err != nil {
		return err
	}
	defer keysCursor.Close()
	if k, v, err = keysCursor.Seek(prefix); err != nil {
		return err
	}
	if k != nil && bytes.HasPrefix(k, prefix) {
		keySuffix := make([]byte, len(k)+8)
		copy(keySuffix, k)
		copy(keySuffix[len(k):], v)
		step := ^binary.BigEndian.Uint64(v)
		txNum := step * dc.d.aggregationStep
		if v, err = roTx.GetOne(dc.d.valsTable, keySuffix); err != nil {
			return err
		}
		heap.Push(&cp, &CursorItem{t: DB_CURSOR, key: k, val: v, c: keysCursor, endTxNum: txNum + dc.d.aggregationStep, reverse: true})
	}

	for i, item := range dc.files {
		if UseBtree || UseBpsTree {
			cursor, err := dc.statelessBtree(i).Seek(dc.statelessGetter(i), prefix)
			if err != nil {
				return err
			}
			if cursor == nil {
				continue
			}
			dc.d.stats.FilesQueries.Add(1)
			key := cursor.Key()
			if key != nil && bytes.HasPrefix(key, prefix) {
				val := cursor.Value()
				heap.Push(&cp, &CursorItem{t: FILE_CURSOR, dg: dc.statelessGetter(i), key: key, val: val, btCursor: cursor, endTxNum: item.endTxNum, reverse: true})
			}
		} else {
			ir := dc.statelessIdxReader(i)
			offset := ir.Lookup(prefix)
			g := dc.statelessGetter(i)
			g.Reset(offset)
			if !g.HasNext() {
				continue
			}
			key, _ := g.Next(nil)
			dc.d.stats.FilesQueries.Add(1)
			if key != nil && bytes.HasPrefix(key, prefix) {
				val, lofft := g.Next(nil)
				heap.Push(&cp, &CursorItem{t: FILE_CURSOR, dg: g, latestOffset: lofft, key: key, val: val, endTxNum: item.endTxNum, reverse: true})
			}
		}
	}

	for cp.Len() > 0 {
		lastKey := common.Copy(cp[0].key)
		lastVal := common.Copy(cp[0].val)
		// Advance all the items that have this key (including the top)
		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
			ci1 := heap.Pop(&cp).(*CursorItem)
			//if string(ci1.key) == string(hexutility.MustDecodeString("301f9a245a0adeb61835403f6fd256dd96d103942d747c6d41e95a5d655bc20ab0fac941c854894cc0ed84cdaf557374b49ed723")) {
			//	fmt.Printf("found %x\n", ci1.key)
			//}
			switch ci1.t {
			//case RAM_CURSOR:
			//	if ci1.iter.Next() {
			//		k = []byte(ci1.iter.Key())
			//		if k != nil && bytes.HasPrefix(k, prefix) {
			//			ci1.key = common.Copy(k)
			//			ci1.val = common.Copy(ci1.iter.Value())
			//		}
			//	}
			//	heap.Push(&cp, ci1)
			case FILE_CURSOR:
				if UseBtree || UseBpsTree {
					if ci1.btCursor.Next() {
						ci1.key = ci1.btCursor.Key()
						if ci1.key != nil && bytes.HasPrefix(ci1.key, prefix) {
							ci1.val = ci1.btCursor.Value()
							heap.Push(&cp, ci1)
						}
					}
				} else {
					ci1.dg.Reset(ci1.latestOffset)
					if !ci1.dg.HasNext() {
						break
					}
					key, _ := ci1.dg.Next(nil)
					if key != nil && bytes.HasPrefix(key, prefix) {
						ci1.key = key
						ci1.val, ci1.latestOffset = ci1.dg.Next(nil)
						heap.Push(&cp, ci1)
					}
				}
			case DB_CURSOR:
				k, v, err = ci1.c.NextNoDup()
				if err != nil {
					return err
				}
				if k != nil && bytes.HasPrefix(k, prefix) {
					ci1.key = k
					keySuffix := make([]byte, len(k)+8)
					copy(keySuffix, k)
					copy(keySuffix[len(k):], v)
					if v, err = roTx.GetOne(dc.d.valsTable, keySuffix); err != nil {
						return err
					}
					ci1.val = v
					heap.Push(&cp, ci1)
				}
			}
		}
		if len(lastVal) > 0 {
			it(lastKey, lastVal)
		}
	}
	return nil
}

func (dc *DomainContext) DomainRange(tx kv.Tx, fromKey, toKey []byte, ts uint64, asc order.By, limit int) (it iter.KV, err error) {
	if !asc {
		panic("implement me")
	}
	//histStateIt, err := tx.aggCtx.AccountHistoricalStateRange(asOfTs, fromKey, toKey, limit, tx.MdbxTx)
	//if err != nil {
	//	return nil, err
	//}
	//lastestStateIt, err := tx.aggCtx.DomainRangeLatest(tx.MdbxTx, kv.AccountDomain, fromKey, toKey, limit)
	//if err != nil {
	//	return nil, err
	//}
	histStateIt, err := dc.hc.WalkAsOf(ts, fromKey, toKey, tx, limit)
	if err != nil {
		return nil, err
	}
	lastestStateIt, err := dc.DomainRangeLatest(tx, fromKey, toKey, limit)
	if err != nil {
		return nil, err
	}
	return iter.UnionKV(histStateIt, lastestStateIt, limit), nil
}

func (dc *DomainContext) IteratePrefix2(roTx kv.Tx, fromKey, toKey []byte, limit int) (iter.KV, error) {
	return dc.DomainRangeLatest(roTx, fromKey, toKey, limit)
}

func (dc *DomainContext) DomainRangeLatest(roTx kv.Tx, fromKey, toKey []byte, limit int) (iter.KV, error) {
	fit := &DomainLatestIterFile{from: fromKey, to: toKey, limit: limit, dc: dc,
		roTx:         roTx,
		idxKeysTable: dc.d.keysTable,
		h:            &CursorHeap{},
	}
	if err := fit.init(dc); err != nil {
		return nil, err
	}
	return fit, nil
}

func (dc *DomainContext) CanPruneFrom(tx kv.Tx) uint64 {
	fst, _ := kv.FirstKey(tx, dc.d.indexKeysTable)
	//fst2, _ := kv.FirstKey(tx, dc.d.keysTable)
	//if len(fst) > 0 && len(fst2) > 0 {
	//	fstInDb := binary.BigEndian.Uint64(fst)
	//	fstInDb2 := binary.BigEndian.Uint64(fst2)
	//	return cmp.Min(fstInDb, fstInDb2)
	//}
	if len(fst) > 0 {
		fstInDb := binary.BigEndian.Uint64(fst)
		return cmp.Min(fstInDb, math.MaxUint64)
	}
	return math.MaxUint64
}

func (dc *DomainContext) CanPrune(tx kv.Tx) bool {
	return dc.CanPruneFrom(tx) < dc.maxTxNumInFiles(false)
}

// history prunes keys in range [txFrom; txTo), domain prunes any records with rStep <= step.
// In case of context cancellation pruning stops and returns error, but simply could be started again straight away.
func (dc *DomainContext) Prune(ctx context.Context, rwTx kv.RwTx, step, txFrom, txTo, limit uint64, logEvery *time.Ticker) error {
	if !dc.CanPrune(rwTx) {
		return nil
	}

	st := time.Now()
	mxPruneInProgress.Inc()
	defer mxPruneInProgress.Dec()

	keysCursorForDeletes, err := rwTx.RwCursorDupSort(dc.d.keysTable)
	if err != nil {
		return fmt.Errorf("create %s domain cursor: %w", dc.d.filenameBase, err)
	}
	defer keysCursorForDeletes.Close()
	keysCursor, err := rwTx.RwCursorDupSort(dc.d.keysTable)
	if err != nil {
		return fmt.Errorf("create %s domain cursor: %w", dc.d.filenameBase, err)
	}
	defer keysCursor.Close()

	var (
		k, v          []byte
		prunedKeys    uint64
		prunedMaxStep uint64
		prunedMinStep = uint64(math.MaxUint64)
		seek          = make([]byte, 0, 256)
		valsDup       kv.RwCursorDupSort
	)

	if !dc.d.domainLargeValues {
		valsDup, err = rwTx.RwCursorDupSort(dc.d.valsTable)
		if err != nil {
			return err
		}
		defer valsDup.Close()
	}

	for k, v, err = keysCursor.Last(); k != nil; k, v, err = keysCursor.Prev() {
		if err != nil {
			return fmt.Errorf("iterate over %s domain keys: %w", dc.d.filenameBase, err)
		}
		is := ^binary.BigEndian.Uint64(v)
		if is > step {
			continue
		}
		if limit == 0 {
			return nil
		}
		limit--

		k, v, err = keysCursorForDeletes.SeekBothExact(k, v)
		if err != nil {
			return err
		}
		seek = append(append(seek[:0], k...), v...)
		//if bytes.HasPrefix(seek, hexutility.MustDecodeString("1a4a4de8fe37b308fea3eb786195af8c813e18f8196bcb830a40cd57f169692572197d70495a7c6d0184c5093dcc960e1384239e")) {
		//	fmt.Printf("prune key: %x->%x [%x] step %d dom %s\n", k, v, seek, ^binary.BigEndian.Uint64(v), dc.d.filenameBase)
		//}
		//fmt.Printf("prune key: %x->%x [%x] step %d dom %s\n", k, v, seek, ^binary.BigEndian.Uint64(v), dc.d.filenameBase)

		mxPruneSizeDomain.Inc()
		prunedKeys++

		if dc.d.domainLargeValues {
			//if bytes.HasPrefix(seek, hexutility.MustDecodeString("1a4a4de8fe37b308fea3eb786195af8c813e18f8196bcb830a40cd57f169692572197d70495a7c6d0184c5093dcc960e1384239e")) {
			//	fmt.Printf("prune value: %x step %d dom %s\n", seek, ^binary.BigEndian.Uint64(v), dc.d.filenameBase)
			//}
			//fmt.Printf("prune value: %x step %d dom %s\n", seek, ^binary.BigEndian.Uint64(v), dc.d.filenameBase)
			err = rwTx.Delete(dc.d.valsTable, seek)
		} else {
			sv, err := valsDup.SeekBothRange(seek[:len(k)], seek[len(k):len(k)+len(v)])
			if err != nil {
				return err
			}
			if bytes.HasPrefix(sv, v) {
				//fmt.Printf("prune value: %x->%x, step %d dom %s\n", k, sv, ^binary.BigEndian.Uint64(v), dc.d.filenameBase)
				err = valsDup.DeleteCurrent()
				if err != nil {
					return err
				}
			}
		}
		if err != nil {
			return fmt.Errorf("prune domain value: %w", err)
		}

		if err = keysCursorForDeletes.DeleteCurrent(); err != nil { // invalidates kk, vv
			return err
		}

		if is < prunedMinStep {
			prunedMinStep = is
		}
		if is > prunedMaxStep {
			prunedMaxStep = is
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			dc.d.logger.Info("[snapshots] prune domain", "name", dc.d.filenameBase, "step", step,
				"steps", fmt.Sprintf("%.2f-%.2f", float64(txFrom)/float64(dc.d.aggregationStep), float64(txTo)/float64(dc.d.aggregationStep)))
		default:
		}
	}
	if prunedMinStep == math.MaxUint64 {
		prunedMinStep = 0
	} // minMax pruned step doesn't mean that we pruned all kv pairs for those step - we just pruned some keys of those steps.

	dc.d.logger.Info("[snapshots] prune domain", "name", dc.d.filenameBase, "step range", fmt.Sprintf("[%d, %d] requested %d", prunedMinStep, prunedMaxStep, step), "pruned keys", prunedKeys)
	mxPruneTookDomain.UpdateDuration(st)

	if err := dc.hc.Prune(ctx, rwTx, txFrom, txTo, limit, logEvery); err != nil {
		return fmt.Errorf("prune history at step %d [%d, %d): %w", step, txFrom, txTo, err)
	}
	return nil
}

type DomainLatestIterFile struct {
	dc *DomainContext

	roTx         kv.Tx
	idxKeysTable string

	limit int

	from, to []byte
	nextVal  []byte
	nextKey  []byte

	h *CursorHeap

	k, v, kBackup, vBackup []byte
}

func (hi *DomainLatestIterFile) Close() {
}
func (hi *DomainLatestIterFile) init(dc *DomainContext) error {
	heap.Init(hi.h)
	var k, v []byte
	var err error

	keysCursor, err := hi.roTx.CursorDupSort(dc.d.keysTable)
	if err != nil {
		return err
	}
	if k, v, err = keysCursor.Seek(hi.from); err != nil {
		return err
	}
	if k != nil && (hi.to == nil || bytes.Compare(k, hi.to) < 0) {
		keySuffix := make([]byte, len(k)+8)
		copy(keySuffix, k)
		copy(keySuffix[len(k):], v)
		step := ^binary.BigEndian.Uint64(v)
		txNum := step * dc.d.aggregationStep
		if v, err = hi.roTx.GetOne(dc.d.valsTable, keySuffix); err != nil {
			return err
		}
		heap.Push(hi.h, &CursorItem{t: DB_CURSOR, key: common.Copy(k), val: common.Copy(v), c: keysCursor, endTxNum: txNum, reverse: true})
	}

	for i, item := range dc.files {
		btCursor, err := dc.statelessBtree(i).Seek(dc.statelessGetter(i), hi.from)
		if err != nil {
			return err
		}
		if btCursor == nil {
			continue
		}

		key := btCursor.Key()
		if key != nil && (hi.to == nil || bytes.Compare(key, hi.to) < 0) {
			val := btCursor.Value()
			heap.Push(hi.h, &CursorItem{t: FILE_CURSOR, key: key, val: val, btCursor: btCursor, endTxNum: item.endTxNum, reverse: true})
		}
	}
	return hi.advanceInFiles()
}

func (hi *DomainLatestIterFile) advanceInFiles() error {
	for hi.h.Len() > 0 {
		lastKey := (*hi.h)[0].key
		lastVal := (*hi.h)[0].val

		// Advance all the items that have this key (including the top)
		for hi.h.Len() > 0 && bytes.Equal((*hi.h)[0].key, lastKey) {
			ci1 := heap.Pop(hi.h).(*CursorItem)
			switch ci1.t {
			case FILE_CURSOR:
				if ci1.btCursor.Next() {
					ci1.key = ci1.btCursor.Key()
					ci1.val = ci1.btCursor.Value()
					if ci1.key != nil && (hi.to == nil || bytes.Compare(ci1.key, hi.to) < 0) {
						heap.Push(hi.h, ci1)
					}
				}
			case DB_CURSOR:
				k, v, err := ci1.c.NextNoDup()
				if err != nil {
					return err
				}
				if k != nil && (hi.to == nil || bytes.Compare(k, hi.to) < 0) {
					ci1.key = common.Copy(k)
					keySuffix := make([]byte, len(k)+8)
					copy(keySuffix, k)
					copy(keySuffix[len(k):], v)
					if v, err = hi.roTx.GetOne(hi.dc.d.valsTable, keySuffix); err != nil {
						return err
					}
					ci1.val = common.Copy(v)
					heap.Push(hi.h, ci1)
				}
			}
		}
		if len(lastVal) > 0 {
			hi.nextKey, hi.nextVal = lastKey, lastVal
			return nil // founc
		}
	}
	hi.nextKey = nil
	return nil
}

func (hi *DomainLatestIterFile) HasNext() bool {
	return hi.limit != 0 && hi.nextKey != nil
}

func (hi *DomainLatestIterFile) Next() ([]byte, []byte, error) {
	hi.limit--
	hi.k, hi.v = append(hi.k[:0], hi.nextKey...), append(hi.v[:0], hi.nextVal...)

	// Satisfy iter.Dual Invariant 2
	hi.k, hi.kBackup, hi.v, hi.vBackup = hi.kBackup, hi.k, hi.vBackup, hi.v
	if err := hi.advanceInFiles(); err != nil {
		return nil, nil, err
	}
	return hi.kBackup, hi.vBackup, nil
}

func (d *Domain) stepsRangeInDBAsStr(tx kv.Tx) string {
	a1, a2 := d.History.InvertedIndex.stepsRangeInDB(tx)
	//ad1, ad2 := d.stepsRangeInDB(tx)
	//if ad2-ad1 < 0 {
	//	fmt.Printf("aaa: %f, %f\n", ad1, ad2)
	//}
	return fmt.Sprintf("%s:%.1f", d.filenameBase, a2-a1)
}
func (d *Domain) stepsRangeInDB(tx kv.Tx) (from, to float64) {
	if d.domainLargeValues {
		fst, _ := kv.FirstKey(tx, d.valsTable)
		if len(fst) > 0 {
			to = float64(^binary.BigEndian.Uint64(fst[len(fst)-8:]))
		}
		lst, _ := kv.LastKey(tx, d.valsTable)
		if len(lst) > 0 {
			from = float64(^binary.BigEndian.Uint64(lst[len(lst)-8:]))
		}
		if to == 0 {
			to = from
		}
	} else {
		c, err := tx.Cursor(d.valsTable)
		if err != nil {
			return 0, 0
		}
		_, fst, _ := c.First()
		if len(fst) > 0 {
			to = float64(^binary.BigEndian.Uint64(fst[:8]))
		}
		_, lst, _ := c.Last()
		if len(lst) > 0 {
			from = float64(^binary.BigEndian.Uint64(lst[:8]))
		}
		c.Close()
		if to == 0 {
			to = from
		}
	}
	return from, to
}

func (dc *DomainContext) Files() (res []string) {
	for _, item := range dc.files {
		if item.src.decompressor != nil {
			res = append(res, item.src.decompressor.FileName())
		}
	}
	return append(res, dc.hc.Files()...)
}

type Ranges struct {
	accounts   DomainRanges
	storage    DomainRanges
	code       DomainRanges
	commitment DomainRanges
}

func (r Ranges) String() string {
	return fmt.Sprintf("accounts=%s, storage=%s, code=%s, commitment=%s", r.accounts.String(), r.storage.String(), r.code.String(), r.commitment.String())
}

func (r Ranges) any() bool {
	return r.accounts.any() || r.storage.any() || r.code.any() || r.commitment.any()
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
	codeI          int
	storageI       int
	accountsI      int
	commitmentI    int
}

func (sf SelectedStaticFiles) FillV3(s *SelectedStaticFilesV3) SelectedStaticFiles {
	sf.accounts, sf.accountsIdx, sf.accountsHist = s.accounts, s.accountsIdx, s.accountsHist
	sf.storage, sf.storageIdx, sf.storageHist = s.storage, s.storageIdx, s.storageHist
	sf.code, sf.codeIdx, sf.codeHist = s.code, s.codeIdx, s.codeHist
	sf.commitment, sf.commitmentIdx, sf.commitmentHist = s.commitment, s.commitmentIdx, s.commitmentHist
	sf.codeI, sf.accountsI, sf.storageI, sf.commitmentI = s.codeI, s.accountsI, s.storageI, s.commitmentI
	return sf
}

func (sf SelectedStaticFiles) Close() {
	for _, group := range [][]*filesItem{
		sf.accounts, sf.accountsIdx, sf.accountsHist,
		sf.storage, sf.storageIdx, sf.storageHist,
		sf.code, sf.codeIdx, sf.codeHist,
		sf.commitment, sf.commitmentIdx, sf.commitmentHist,
	} {
		for _, item := range group {
			if item != nil {
				if item.decompressor != nil {
					item.decompressor.Close()
				}
				if item.index != nil {
					item.index.Close()
				}
				if item.bindex != nil {
					item.bindex.Close()
				}
			}
		}
	}
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
}

func (mf MergedFiles) FillV3(m *MergedFilesV3) MergedFiles {
	mf.accounts, mf.accountsIdx, mf.accountsHist = m.accounts, m.accountsIdx, m.accountsHist
	mf.storage, mf.storageIdx, mf.storageHist = m.storage, m.storageIdx, m.storageHist
	mf.code, mf.codeIdx, mf.codeHist = m.code, m.codeIdx, m.codeHist
	mf.commitment, mf.commitmentIdx, mf.commitmentHist = m.commitment, m.commitmentIdx, m.commitmentHist
	return mf
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
			if item.bindex != nil {
				item.bindex.Close()
			}
		}
	}
}

func DecodeAccountBytes(enc []byte) (nonce uint64, balance *uint256.Int, hash []byte) {
	if len(enc) == 0 {
		return
	}
	pos := 0
	nonceBytes := int(enc[pos])
	balance = uint256.NewInt(0)
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
	if codeHashBytes == length.Hash {
		hash = make([]byte, codeHashBytes)
		copy(hash, enc[pos:pos+codeHashBytes])
		pos += codeHashBytes
	}
	if pos >= len(enc) {
		panic(fmt.Errorf("deserialse2: %d >= %d ", pos, len(enc)))
	}
	return
}

func EncodeAccountBytes(nonce uint64, balance *uint256.Int, hash []byte, incarnation uint64) []byte {
	l := int(1)
	if nonce > 0 {
		l += common.BitLenToByteLen(bits.Len64(nonce))
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
		l += common.BitLenToByteLen(bits.Len64(incarnation))
	}
	value := make([]byte, l)
	pos := 0

	if nonce == 0 {
		value[pos] = 0
		pos++
	} else {
		nonceBytes := common.BitLenToByteLen(bits.Len64(nonce))
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
		copy(value[pos:pos+32], hash)
		pos += 32
	}
	if incarnation == 0 {
		value[pos] = 0
	} else {
		incBytes := common.BitLenToByteLen(bits.Len64(incarnation))
		value[pos] = byte(incBytes)
		var inc = incarnation
		for i := incBytes; i > 0; i-- {
			value[pos+i] = byte(inc)
			inc >>= 8
		}
	}
	return value
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
