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
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spaolacci/murmur3"
	btree2 "github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/assert"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/datastruct/existence"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/bitmapdb"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/recsplit/multiencseq"
	"github.com/erigontech/erigon-lib/seg"
	ee "github.com/erigontech/erigon-lib/state/entity_extras"
	"github.com/erigontech/erigon-lib/version"
)

type InvertedIndex struct {
	iiCfg
	noFsync bool // fsync is enabled by default, but tests can manually disable

	aggregationStep uint64 // amount of transactions inside single aggregation step

	// dirtyFiles - list of ALL files - including: un-indexed-yet, garbage, merged-into-bigger-one, ...
	// thread-safe, but maybe need 1 RWLock for all trees in Aggregator
	//
	// _visible derivative from field `file`, but without garbage:
	//  - no files with `canDelete=true`
	//  - no overlaps
	//  - no un-indexed files (`power-off` may happen between .ef and .efi creation)
	//
	// BeginRo() using _visible in zero-copy way
	dirtyFiles *btree2.BTreeG[*filesItem]

	// `_visible.files` - underscore in name means: don't use this field directly, use BeginFilesRo()
	// underlying array is immutable - means it's ready for zero-copy use
	_visible *iiVisible
	logger   log.Logger
}

type iiCfg struct {
	salt    *atomic.Pointer[uint32]
	dirs    datadir.Dirs
	disable bool // totally disable Domain/History/InvertedIndex - ignore all writes, don't produce files

	version IIVersionTypes

	filenameBase string // filename base for all files of this inverted index
	keysTable    string // bucket name for index keys;    txnNum_u64 -> key (k+auto_increment)
	valuesTable  string // bucket name for index values;  k -> txnNum_u64 , Needs to be table with DupSort
	name         kv.InvertedIdx

	Compression   seg.FileCompression // compression type for inverted index keys and values
	CompressorCfg seg.Cfg             // advanced configuration for compressor encodings

	// external checker for integrity of inverted index ranges
	integrity rangeIntegrityChecker
	Accessors Accessors
}

func (ii iiCfg) GetVersions() VersionTypes {
	return VersionTypes{
		II: &ii.version,
	}
}

type iiVisible struct {
	files  []visibleFile
	name   string
	caches *sync.Pool
}

func NewInvertedIndex(cfg iiCfg, aggStep uint64, logger log.Logger) (*InvertedIndex, error) {
	if cfg.dirs.SnapDomain == "" {
		panic("assert: empty `dirs`")
	}
	if cfg.filenameBase == "" {
		panic("assert: empty `filenameBase`")
	}
	//if cfg.compressorCfg.MaxDictPatterns == 0 && cfg.compressorCfg.MaxPatternLen == 0 {
	cfg.CompressorCfg = seg.DefaultCfg
	if cfg.Accessors == 0 {
		cfg.Accessors = AccessorHashMap
	}

	ii := InvertedIndex{
		iiCfg:      cfg,
		dirtyFiles: btree2.NewBTreeGOptions[*filesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		_visible:   newIIVisible(cfg.filenameBase, []visibleFile{}),
		logger:     logger,

		aggregationStep: aggStep,
	}
	if ii.aggregationStep == 0 {
		panic("assert: empty `aggregationStep`")
	}

	if ii.version.DataEF.IsZero() {
		panic(fmt.Errorf("assert: forgot to set version of %s", ii.name))
	}
	if ii.version.AccessorEFI.IsZero() {
		panic(fmt.Errorf("assert: forgot to set version of %s", ii.name))
	}

	return &ii, nil
}

func (ii *InvertedIndex) efAccessorFilePath(fromStep, toStep uint64) string {
	return filepath.Join(ii.dirs.SnapAccessors, fmt.Sprintf("%s-%s.%d-%d.efi", ii.version.AccessorEFI.String(), ii.filenameBase, fromStep, toStep))
}
func (ii *InvertedIndex) efFilePath(fromStep, toStep uint64) string {
	return filepath.Join(ii.dirs.SnapIdx, fmt.Sprintf("%s-%s.%d-%d.ef", ii.version.DataEF.String(), ii.filenameBase, fromStep, toStep))
}

func (ii *InvertedIndex) efAccessorFilePathMask(fromStep, toStep uint64) string {
	return filepath.Join(ii.dirs.SnapAccessors, fmt.Sprintf("*-%s.%d-%d.efi", ii.filenameBase, fromStep, toStep))
}
func (ii *InvertedIndex) efFilePathMask(fromStep, toStep uint64) string {
	return filepath.Join(ii.dirs.SnapIdx, fmt.Sprintf("*-%s.%d-%d.ef", ii.filenameBase, fromStep, toStep))
}

func filesFromDir(dir string) ([]string, error) {
	allFiles, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("filesFromDir: %w, %s", err, dir)
	}
	filtered := make([]string, 0, len(allFiles))
	for _, f := range allFiles {
		if f.IsDir() || !f.Type().IsRegular() {
			continue
		}
		if strings.HasPrefix(f.Name(), ".") { // hidden files
			continue
		}
		filtered = append(filtered, f.Name())
	}
	return filtered, nil
}
func (ii *InvertedIndex) fileNamesOnDisk() (idx, hist, domain []string, err error) {
	idx, err = filesFromDir(ii.dirs.SnapIdx)
	if err != nil {
		return
	}
	hist, err = filesFromDir(ii.dirs.SnapHistory)
	if err != nil {
		return
	}
	domain, err = filesFromDir(ii.dirs.SnapDomain)
	if err != nil {
		return
	}
	return
}

func (ii *InvertedIndex) openList(fNames []string) error {
	ii.closeWhatNotInList(fNames)
	ii.scanDirtyFiles(fNames)
	if err := ii.openDirtyFiles(); err != nil {
		return fmt.Errorf("InvertedIndex(%s).openDirtyFiles: %w", ii.filenameBase, err)
	}
	return nil
}

func (ii *InvertedIndex) openFolder() error {
	if ii.disable {
		return nil
	}
	idxFiles, _, _, err := ii.fileNamesOnDisk()
	if err != nil {
		return err
	}
	return ii.openList(idxFiles)
}

func (ii *InvertedIndex) scanDirtyFiles(fileNames []string) {
	if ii.filenameBase == "" {
		panic("assert: empty `filenameBase`")
	}
	if ii.aggregationStep == 0 {
		panic("assert: empty `aggregationStep`")
	}
	for _, dirtyFile := range scanDirtyFiles(fileNames, ii.aggregationStep, ii.filenameBase, "ef", ii.logger) {
		startStep, endStep := dirtyFile.startTxNum/ii.aggregationStep, dirtyFile.endTxNum/ii.aggregationStep
		if ii.integrity != nil && !ii.integrity(startStep, endStep) {
			ii.logger.Debug("[agg] skip garbage file", "name", ii.filenameBase, "startStep", startStep, "endStep", endStep)
			continue
		}
		if _, has := ii.dirtyFiles.Get(dirtyFile); !has {
			ii.dirtyFiles.Set(dirtyFile)
		}
	}
}

type Accessors = ee.Accessors

const (
	AccessorBTree     Accessors = ee.AccessorBTree
	AccessorHashMap   Accessors = ee.AccessorHashMap
	AccessorExistence Accessors = ee.AccessorExistence
)

func (ii *InvertedIndex) reCalcVisibleFiles(toTxNum uint64) {
	ii._visible = newIIVisible(ii.filenameBase, calcVisibleFiles(ii.dirtyFiles, ii.Accessors, false, toTxNum))
}

func (ii *InvertedIndex) MissedMapAccessors() (l []*filesItem) {
	return ii.missedMapAccessors(ii.dirtyFiles.Items())
}

func (ii *InvertedIndex) missedMapAccessors(source []*filesItem) (l []*filesItem) {
	if !ii.Accessors.Has(AccessorHashMap) {
		return nil
	}
	return fileItemsWithMissedAccessors(source, ii.aggregationStep, func(fromStep, toStep uint64) []string {
		return []string{
			ii.efAccessorFilePath(fromStep, toStep),
		}
	})
}

func (ii *InvertedIndex) buildEfAccessor(ctx context.Context, item *filesItem, ps *background.ProgressSet) (err error) {
	if item.decompressor == nil {
		return fmt.Errorf("buildEfAccessor: passed item with nil decompressor %s %d-%d", ii.filenameBase, item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
	}
	fromStep, toStep := item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep
	return ii.buildMapAccessor(ctx, fromStep, toStep, item.decompressor, ps)
}

// BuildMissedAccessors - produce .efi/.vi/.kvi from .ef/.v/.kv
func (ii *InvertedIndex) BuildMissedAccessors(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet, iiFiles *MissedAccessorIIFiles) {
	for _, item := range iiFiles.missedMapAccessors() {
		item := item
		g.Go(func() error {
			return ii.buildEfAccessor(ctx, item, ps)
		})
	}
}

func (ii *InvertedIndex) openDirtyFiles() error {
	var invalidFileItems []*filesItem
	invalidFileItemsLock := sync.Mutex{}
	ii.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			item := item
			fromStep, toStep := item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep
			if item.decompressor == nil {
				fPathPattern := ii.efFilePathMask(fromStep, toStep)
				fPath, fileVer, ok, err := version.FindFilesWithVersionsByPattern(fPathPattern)
				if err != nil {
					_, fName := filepath.Split(fPath)
					ii.logger.Debug("[agg] InvertedIndex.openDirtyFiles: FindFilesWithVersionsByPattern error", "f", fName, "err", err)
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					continue
				}

				if !ok {
					_, fName := filepath.Split(fPath)
					ii.logger.Debug("[agg] InvertedIndex.openDirtyFiles: file does not exists", "f", fName)
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					continue
				}

				if !fileVer.Eq(ii.version.DataEF.Current) {
					if !fileVer.Less(ii.version.DataEF.MinSupported) {
						ii.version.DataEF.Current = fileVer
					} else {
						panic("Version is too low, try to rm ef snapshots")
						//return false
					}
				}

				if item.decompressor, err = seg.NewDecompressor(fPath); err != nil {
					_, fName := filepath.Split(fPath)
					if errors.Is(err, &seg.ErrCompressedFileCorrupted{}) {
						ii.logger.Debug("[agg] InvertedIndex.openDirtyFiles", "err", err, "f", fName)
					} else {
						ii.logger.Warn("[agg] InvertedIndex.openDirtyFiles", "err", err, "f", fName)
					}
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					// don't interrupt on error. other files may be good. but skip indices open.
					continue
				}
			}

			if item.index == nil {
				fPathPattern := ii.efAccessorFilePathMask(fromStep, toStep)
				fPath, fileVer, ok, err := version.FindFilesWithVersionsByPattern(fPathPattern)
				if err != nil {
					_, fName := filepath.Split(fPath)
					ii.logger.Warn("[agg] InvertedIndex.openDirtyFiles", "err", err, "f", fName)
					// don't interrupt on error. other files may be good
				}
				if ok {
					if !fileVer.Eq(ii.version.AccessorEFI.Current) {
						if !fileVer.Less(ii.version.AccessorEFI.MinSupported) {
							ii.version.AccessorEFI.Current = fileVer
						} else {
							panic("Version is too low, try to rm efi snapshots")
							//return false
						}
					}
					if item.index, err = recsplit.OpenIndex(fPath); err != nil {
						_, fName := filepath.Split(fPath)
						ii.logger.Warn("[agg] InvertedIndex.openDirtyFiles", "err", err, "f", fName)
						// don't interrupt on error. other files may be good
					}
				}
			}
		}

		return true
	})
	for _, item := range invalidFileItems {
		item.closeFiles()
		ii.dirtyFiles.Delete(item)
	}

	return nil
}

func (ii *InvertedIndex) closeWhatNotInList(fNames []string) {
	protectFiles := make(map[string]struct{}, len(fNames))
	for _, f := range fNames {
		protectFiles[f] = struct{}{}
	}
	var toClose []*filesItem
	ii.dirtyFiles.Walk(func(items []*filesItem) bool {
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
		ii.dirtyFiles.Delete(item)
	}
}

func (ii *InvertedIndex) Tables() []string { return []string{ii.keysTable, ii.valuesTable} }

func (ii *InvertedIndex) Close() {
	if ii == nil {
		return
	}
	ii.closeWhatNotInList([]string{})
}

// DisableFsync - just for tests
func (ii *InvertedIndex) DisableFsync() { ii.noFsync = true }

func (iit *InvertedIndexRoTx) Files() (res VisibleFiles) {
	for _, item := range iit.files {
		if item.src.decompressor != nil {
			res = append(res, item)
		}
	}
	return res
}

func (iit *InvertedIndexRoTx) NewWriter() *InvertedIndexBufferedWriter {
	return iit.newWriter(iit.ii.dirs.Tmp, false)
}

type InvertedIndexBufferedWriter struct {
	index, indexKeys *etl.Collector

	tmpdir       string
	discard      bool
	filenameBase string

	indexTable, indexKeysTable string

	aggregationStep uint64
	txNumBytes      [8]byte
	name            kv.InvertedIdx
}

// loadFunc - is analog of etl.Identity, but it signaling to etl - use .Put instead of .AppendDup - to allow duplicates
// maybe in future we will improve etl, to sort dupSort values in the way that allow use .AppendDup
func loadFunc(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
	return next(k, k, v)
}

// Add - !NotThreadSafe. Must use WalRLock/BatchHistoryWriteEnd
func (w *InvertedIndexBufferedWriter) Add(key []byte, txNum uint64) error {
	return w.add(key, key, txNum)
}

func (w *InvertedIndexBufferedWriter) add(key, indexKey []byte, txNum uint64) error {
	if w.discard {
		return nil
	}
	binary.BigEndian.PutUint64(w.txNumBytes[:], txNum)

	if err := w.indexKeys.Collect(w.txNumBytes[:], key); err != nil {
		return err
	}
	if err := w.index.Collect(indexKey, w.txNumBytes[:]); err != nil {
		return err
	}
	return nil
}

func (w *InvertedIndexBufferedWriter) Flush(ctx context.Context, tx kv.RwTx) error {
	if w.discard {
		return nil
	}

	if err := w.index.Load(tx, w.indexTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	if err := w.indexKeys.Load(tx, w.indexKeysTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	w.close()
	return nil
}

func (w *InvertedIndexBufferedWriter) close() {
	if w == nil {
		return
	}
	if w.index != nil {
		w.index.Close()
	}
	if w.indexKeys != nil {
		w.indexKeys.Close()
	}
}

func (iit *InvertedIndexRoTx) newWriter(tmpdir string, discard bool) *InvertedIndexBufferedWriter {
	w := &InvertedIndexBufferedWriter{
		name:            iit.name,
		discard:         discard,
		tmpdir:          tmpdir,
		filenameBase:    iit.ii.filenameBase,
		aggregationStep: iit.ii.aggregationStep,

		indexKeysTable: iit.ii.keysTable,
		indexTable:     iit.ii.valuesTable,

		// etl collector doesn't fsync: means if have enough ram, all files produced by all collectors will be in ram
		indexKeys: etl.NewCollectorWithAllocator(iit.ii.filenameBase+".flush.ii.keys", tmpdir, etl.SmallSortableBuffers, iit.ii.logger).LogLvl(log.LvlTrace),
		index:     etl.NewCollectorWithAllocator(iit.ii.filenameBase+".flush.ii.vals", tmpdir, etl.SmallSortableBuffers, iit.ii.logger).LogLvl(log.LvlTrace),
	}
	w.indexKeys.SortAndFlushInBackground(true)
	w.index.SortAndFlushInBackground(true)
	return w
}

func (ii *InvertedIndex) BeginFilesRo() *InvertedIndexRoTx {
	files := ii._visible.files
	for i := 0; i < len(files); i++ {
		if !files[i].src.frozen {
			files[i].src.refcount.Add(1)
		}
	}
	return &InvertedIndexRoTx{
		ii:      ii,
		visible: ii._visible,
		files:   files,
		name:    ii.name,
		salt:    ii.salt.Load(),
	}
}
func (iit *InvertedIndexRoTx) Close() {
	if iit.files == nil { // invariant: it's safe to call Close multiple times
		return
	}
	files := iit.files
	iit.files = nil
	for i := 0; i < len(files); i++ {
		src := files[i].src
		if src == nil || src.frozen {
			continue
		}
		refCnt := src.refcount.Add(-1)
		//GC: last reader responsible to remove useles files: close it and delete
		if refCnt == 0 && src.canDelete.Load() {
			if traceFileLife != "" && iit.ii.filenameBase == traceFileLife {
				iit.ii.logger.Warn("[agg.dbg] real remove at InvertedIndexRoTx.Close", "file", src.decompressor.FileName())
			}
			src.closeFilesAndRemove()
		}
	}

	for _, r := range iit.readers {
		r.Close()
	}

	iit.visible.returnSeekInFilesCache(iit.seekInFilesCache)
}

type MergeRange struct {
	name      string // entity name
	needMerge bool
	from      uint64
	to        uint64
}

func NewMergeRange(name string, needMerge bool, from uint64, to uint64) *MergeRange {
	return &MergeRange{name: name, needMerge: needMerge, from: from, to: to}
}

func (mr *MergeRange) FromTo() (uint64, uint64) {
	return mr.from, mr.to
}

func (mr *MergeRange) String(prefix string, aggStep uint64) string {
	if prefix != "" {
		prefix += "="
	}
	return fmt.Sprintf("%s%s%d-%d", prefix, mr.name, mr.from/aggStep, mr.to/aggStep)
}

func (mr *MergeRange) Equal(other *MergeRange) bool {
	return mr.from == other.from && mr.to == other.to
}

type InvertedIndexRoTx struct {
	ii      *InvertedIndex
	name    kv.InvertedIdx
	files   visibleFiles
	visible *iiVisible
	getters []*seg.Reader
	readers []*recsplit.IndexReader

	seekInFilesCache *IISeekInFilesCache

	// TODO: retrofit recent optimization in main and reenable the next line
	// ef *multiencseq.SequenceBuilder // re-usable
	salt *uint32
}

// hashKey - change of salt will require re-gen of indices
func (iit *InvertedIndexRoTx) hashKey(k []byte) (uint64, uint64) {
	// this inlinable alloc-free version, it's faster than pre-allocated `hasher` object
	// because `hasher` object is interface and need call many methods on it
	return murmur3.Sum128WithSeed(k, *iit.salt)
}

func (iit *InvertedIndexRoTx) statelessGetter(i int) *seg.Reader {
	if iit.getters == nil {
		iit.getters = make([]*seg.Reader, len(iit.files))
	}
	r := iit.getters[i]
	if r == nil {
		g := iit.files[i].src.decompressor.MakeGetter()
		r = seg.NewReader(g, iit.ii.Compression)
		iit.getters[i] = r
	}
	return r
}
func (iit *InvertedIndexRoTx) statelessIdxReader(i int) *recsplit.IndexReader {
	if iit.readers == nil {
		iit.readers = make([]*recsplit.IndexReader, len(iit.files))
	}
	r := iit.readers[i]
	if r == nil {
		r = iit.files[i].src.index.GetReaderFromPool()
		iit.readers[i] = r
	}
	return r
}

func (iit *InvertedIndexRoTx) seekInFiles(key []byte, txNum uint64) (found bool, equalOrHigherTxNum uint64, err error) {
	if len(iit.files) == 0 {
		return false, 0, nil
	}

	if txNum < iit.files[0].startTxNum {
		return false, 0, fmt.Errorf("seek with txNum=%d but data before txNum=%d is not available", txNum, iit.files[0].startTxNum)
	}
	if iit.files[len(iit.files)-1].endTxNum <= txNum {
		return false, 0, nil
	}

	hi, lo := iit.hashKey(key)

	if iit.seekInFilesCache == nil {
		iit.seekInFilesCache = iit.visible.newSeekInFilesCache()
	}

	if iit.seekInFilesCache != nil {
		iit.seekInFilesCache.total++
		fromCache, ok := iit.seekInFilesCache.Get(hi)
		if ok && fromCache.requested <= txNum {
			if txNum <= fromCache.found {
				iit.seekInFilesCache.hit++
				return true, fromCache.found, nil
			} else if fromCache.found == 0 { //not found
				iit.seekInFilesCache.hit++
				return false, 0, nil
			}
		}
	}

	for i := 0; i < len(iit.files); i++ {
		if iit.files[i].endTxNum <= txNum {
			continue
		}
		offset, ok := iit.statelessIdxReader(i).TwoLayerLookupByHash(hi, lo)
		if !ok {
			continue
		}

		g := iit.statelessGetter(i)
		g.Reset(offset)
		k, _ := g.Next(nil)
		if !bytes.Equal(k, key) {
			continue
		}
		encodedSeq, _ := g.Next(nil)

		// TODO: implement merge Reset+Seek
		// if iit.ef == nil {
		// 	iit.ef = eliasfano32.NewEliasFano(1, 1)
		// }
		// equalOrHigherTxNum, found = iit.ef.Reset(encodedSeq).Seek(txNum)
		equalOrHigherTxNum, found = multiencseq.Seek(iit.files[i].startTxNum, encodedSeq, txNum)
		if !found {
			continue
		}

		if equalOrHigherTxNum < iit.files[i].startTxNum || equalOrHigherTxNum >= iit.files[i].endTxNum {
			return false, equalOrHigherTxNum, fmt.Errorf("inverted_index(%s) at (%x, %d) returned value %d, but it out-of-bounds %d-%d. it may signal that .ef file is broke - can detect by `erigon seg integrity --check=InvertedIndex`, or re-download files", g.FileName(), key, txNum, iit.files[i].startTxNum, iit.files[i].endTxNum, equalOrHigherTxNum)
		}
		if iit.seekInFilesCache != nil && equalOrHigherTxNum-txNum > 0 { // > 0 to improve cache hit-rate
			iit.seekInFilesCache.Add(hi, iiSeekInFilesCacheItem{requested: txNum, found: equalOrHigherTxNum})
		}
		return true, equalOrHigherTxNum, nil
	}

	if iit.seekInFilesCache != nil {
		iit.seekInFilesCache.Add(hi, iiSeekInFilesCacheItem{requested: txNum, found: 0})
	}
	return false, 0, nil
}

// IdxRange - return range of txNums for given `key`
// is to be used in public API, therefore it relies on read-only transaction
// so that iteration can be done even when the inverted index is being updated.
// [startTxNum; endNumTx)

// todo IdxRange operates over ii.valuesTable . Passing `nil` as a key will not return all keys
func (iit *InvertedIndexRoTx) IdxRange(key []byte, startTxNum, endTxNum int, asc order.By, limit int, roTx kv.Tx) (stream.U64, error) {
	filesIt, err := iit.iterateRangeOnFiles(key, startTxNum, endTxNum, asc, limit)
	if err != nil {
		return nil, err
	}
	recentIt, err := iit.recentIterateRange(key, startTxNum, endTxNum, asc, limit, roTx)
	if err != nil {
		return nil, err
	}
	log.Info("[dbg] IdxRange", "addr", fmt.Sprintf("%x", key), "filesIt.HasNext", filesIt.HasNext(), "recentIt.hasNext", recentIt.HasNext())
	return stream.Union[uint64](filesIt, recentIt, asc, limit), nil
}

func (iit *InvertedIndexRoTx) recentIterateRange(key []byte, startTxNum, endTxNum int, asc order.By, limit int, roTx kv.Tx) (stream.U64, error) {
	//optimization: return empty pre-allocated iterator if range is frozen
	if asc {
		isFrozenRange := len(iit.files) > 0 && endTxNum >= 0 && iit.files.EndTxNum() >= uint64(endTxNum)
		if isFrozenRange {
			log.Info("[dbg] recentIterateRange early exit: range frozen already")
			return stream.EmptyU64, nil
		}
	} else {
		isFrozenRange := len(iit.files) > 0 && startTxNum >= 0 && iit.files.EndTxNum() >= uint64(startTxNum)
		if isFrozenRange {
			return stream.EmptyU64, nil
		}
	}

	var from []byte
	if startTxNum >= 0 {
		from = make([]byte, 8)
		binary.BigEndian.PutUint64(from, uint64(startTxNum))
	}

	var to []byte
	if endTxNum >= 0 {
		to = make([]byte, 8)
		binary.BigEndian.PutUint64(to, uint64(endTxNum))
	}
	it, err := roTx.RangeDupSort(iit.ii.valuesTable, key, from, to, asc, limit)
	if err != nil {
		return nil, err
	}
	c, _ := roTx.CursorDupSort(iit.ii.valuesTable)
	see, _ := c.SeekBothRange(key, from)
	log.Info("[dbg] recentIterateRange", "it.HasNext", it.HasNext(), "key", fmt.Sprintf("%x", key), "kk", "see", fmt.Sprintf("%x", see))

	return stream.TransformKV2U64(it, func(_, v []byte) (uint64, error) {
		return binary.BigEndian.Uint64(v), nil
	}), nil
}

// IdxRange is to be used in public API, therefore it relies on read-only transaction
// so that iteration can be done even when the inverted index is being updated.
// [startTxNum; endNumTx)
func (iit *InvertedIndexRoTx) iterateRangeOnFiles(key []byte, startTxNum, endTxNum int, asc order.By, limit int) (*InvertedIdxStreamFiles, error) {
	if asc && (startTxNum >= 0 && endTxNum >= 0) && startTxNum > endTxNum {
		return nil, fmt.Errorf("startTxNum=%d epected to be lower than endTxNum=%d", startTxNum, endTxNum)
	}
	if !asc && (startTxNum >= 0 && endTxNum >= 0) && startTxNum < endTxNum {
		return nil, fmt.Errorf("startTxNum=%d epected to be bigger than endTxNum=%d", startTxNum, endTxNum)
	}

	it := &InvertedIdxStreamFiles{
		key:         key,
		startTxNum:  startTxNum,
		endTxNum:    endTxNum,
		indexTable:  iit.ii.valuesTable,
		orderAscend: asc,
		limit:       limit,
		seq:         &multiencseq.SequenceReader{},
	}
	if asc {
		for i := len(iit.files) - 1; i >= 0; i-- {
			// [from,to) && from < to
			if endTxNum >= 0 && int(iit.files[i].startTxNum) >= endTxNum {
				continue
			}
			if startTxNum >= 0 && iit.files[i].endTxNum <= uint64(startTxNum) {
				break
			}
			if iit.files[i].src.index.KeyCount() == 0 {
				continue
			}
			it.stack = append(it.stack, iit.files[i])
			it.stack[len(it.stack)-1].getter = it.stack[len(it.stack)-1].src.decompressor.MakeGetter()
			it.stack[len(it.stack)-1].reader = it.stack[len(it.stack)-1].src.index.GetReaderFromPool()
			it.hasNext = true
		}
	} else {
		for i := 0; i < len(iit.files); i++ {
			// [from,to) && from > to
			if endTxNum >= 0 && int(iit.files[i].endTxNum) <= endTxNum {
				continue
			}
			if startTxNum >= 0 && iit.files[i].startTxNum > uint64(startTxNum) {
				break
			}
			if iit.files[i].src.index == nil { // assert
				err := fmt.Errorf("why file has not index: %s", iit.files[i].src.decompressor.FileName())
				panic(err)
			}
			if iit.files[i].src.index.KeyCount() == 0 {
				continue
			}
			it.stack = append(it.stack, iit.files[i])
			it.stack[len(it.stack)-1].getter = it.stack[len(it.stack)-1].src.decompressor.MakeGetter()
			it.stack[len(it.stack)-1].reader = it.stack[len(it.stack)-1].src.index.GetReaderFromPool()
			it.hasNext = true
		}
	}
	it.advance()
	return it, nil
}

func (ii *InvertedIndex) minTxNumInDB(tx kv.Tx) uint64 {
	fst, _ := kv.FirstKey(tx, ii.keysTable)
	if len(fst) > 0 {
		fstInDb := binary.BigEndian.Uint64(fst)
		return min(fstInDb, math.MaxUint64)
	}
	return math.MaxUint64
}

func (ii *InvertedIndex) maxTxNumInDB(tx kv.Tx) uint64 {
	lst, _ := kv.LastKey(tx, ii.keysTable)
	if len(lst) > 0 {
		lstInDb := binary.BigEndian.Uint64(lst)
		return max(lstInDb, 0)
	}
	return 0
}

func (iit *InvertedIndexRoTx) Progress(tx kv.Tx) uint64 {
	return max(iit.files.EndTxNum(), iit.ii.maxTxNumInDB(tx))
}

func (iit *InvertedIndexRoTx) CanPrune(tx kv.Tx) bool {
	return iit.ii.minTxNumInDB(tx) < iit.files.EndTxNum()
}

func (iit *InvertedIndexRoTx) canBuild(dbtx kv.Tx) bool { //nolint
	maxStepInFiles := iit.files.EndTxNum() / iit.ii.aggregationStep
	maxStepInDB := iit.ii.maxTxNumInDB(dbtx) / iit.ii.aggregationStep
	return maxStepInFiles < maxStepInDB
}

type InvertedIndexPruneStat struct {
	MinTxNum         uint64
	MaxTxNum         uint64
	PruneCountTx     uint64
	PruneCountValues uint64
}

func (is *InvertedIndexPruneStat) PrunedNothing() bool {
	return is.PruneCountTx == 0 && is.PruneCountValues == 0
}

func (is *InvertedIndexPruneStat) String() string {
	if is.PrunedNothing() {
		return ""
	}
	vstr := ""
	if is.PruneCountValues > 0 {
		vstr = fmt.Sprintf("values: %s,", common.PrettyCounter(is.PruneCountValues))
	}
	return fmt.Sprintf("%s txns: %d from %s-%s",
		vstr, is.PruneCountTx, common.PrettyCounter(is.MinTxNum), common.PrettyCounter(is.MaxTxNum))
}

func (is *InvertedIndexPruneStat) Accumulate(other *InvertedIndexPruneStat) {
	if other == nil {
		return
	}
	is.MinTxNum = min(is.MinTxNum, other.MinTxNum)
	is.MaxTxNum = max(is.MaxTxNum, other.MaxTxNum)
	is.PruneCountTx += other.PruneCountTx
	is.PruneCountValues += other.PruneCountValues
}

func (iit *InvertedIndexRoTx) unwind(ctx context.Context, rwTx kv.RwTx, txFrom, txTo, limit uint64, logEvery *time.Ticker, forced bool, fn func(key []byte, txnum []byte) error) error {
	_, err := iit.Prune(ctx, rwTx, txFrom, txTo, limit, logEvery, forced, fn)
	if err != nil {
		return err
	}
	return nil
}

// [txFrom; txTo)
// forced - prune even if CanPrune returns false, so its true only when we do Unwind.
func (iit *InvertedIndexRoTx) Prune(ctx context.Context, rwTx kv.RwTx, txFrom, txTo, limit uint64, logEvery *time.Ticker, forced bool, fn func(key []byte, txnum []byte) error) (stat *InvertedIndexPruneStat, err error) {
	stat = &InvertedIndexPruneStat{MinTxNum: math.MaxUint64}
	if !forced && !iit.CanPrune(rwTx) {
		return stat, nil
	}

	mxPruneInProgress.Inc()
	defer mxPruneInProgress.Dec()
	defer func(t time.Time) { mxPruneTookIndex.ObserveDuration(t) }(time.Now())

	if limit == 0 { // limits amount of txn to be pruned
		limit = math.MaxUint64
	}

	ii := iit.ii
	//defer func() {
	//	ii.logger.Error("[snapshots] prune index",
	//		"name", ii.filenameBase,
	//		"forced", forced,
	//		"pruned tx", fmt.Sprintf("%.2f-%.2f", float64(minTxnum)/float64(iit.ii.aggregationStep), float64(maxTxnum)/float64(iit.ii.aggregationStep)),
	//		"pruned values", pruneCount,
	//		"tx until limit", limit)
	//}()

	keysCursor, err := rwTx.CursorDupSort(ii.keysTable)
	if err != nil {
		return stat, fmt.Errorf("create %s keys cursor: %w", ii.filenameBase, err)
	}
	defer keysCursor.Close()
	idxDelCursor, err := rwTx.RwCursorDupSort(ii.valuesTable)
	if err != nil {
		return nil, err
	}
	defer idxDelCursor.Close()

	collector := etl.NewCollectorWithAllocator(ii.filenameBase+".prune.ii", ii.dirs.Tmp, etl.SmallSortableBuffers, ii.logger)
	defer collector.Close()
	collector.LogLvl(log.LvlTrace)
	collector.SortAndFlushInBackground(true)

	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)

	// Invariant: if some `txNum=N` pruned - it's pruned Fully
	// Means: can use DeleteCurrentDuplicates all values of given `txNum`
	for k, v, err := keysCursor.Seek(txKey[:]); k != nil; k, v, err = keysCursor.NextNoDup() {
		if err != nil {
			return nil, fmt.Errorf("iterate over %s index keys: %w", ii.filenameBase, err)
		}

		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo || limit == 0 {
			break
		}
		if asserts && txNum < txFrom {
			panic(fmt.Errorf("assert: index pruning txn=%d [%d-%d)", txNum, txFrom, txTo))
		}

		limit--
		stat.MinTxNum = min(stat.MinTxNum, txNum)
		stat.MaxTxNum = max(stat.MaxTxNum, txNum)

		for ; v != nil; _, v, err = keysCursor.NextDup() {
			if err != nil {
				return nil, fmt.Errorf("iterate over %s index keys: %w", ii.filenameBase, err)
			}
			if err := collector.Collect(v, k); err != nil {
				return nil, err
			}
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}

	err = collector.Load(nil, "", func(key, txnm []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if fn != nil {
			if err = fn(key, txnm); err != nil {
				return fmt.Errorf("fn error: %w", err)
			}
		}
		if err = idxDelCursor.DeleteExact(key, txnm); err != nil {
			return err
		}
		mxPruneSizeIndex.Inc()
		stat.PruneCountValues++

		select {
		case <-logEvery.C:
			txNum := binary.BigEndian.Uint64(txnm)
			ii.logger.Info("[snapshots] prune index", "name", ii.filenameBase, "pruned tx", stat.PruneCountTx,
				"pruned values", stat.PruneCountValues,
				"steps", fmt.Sprintf("%.2f-%.2f", float64(txFrom)/float64(ii.aggregationStep), float64(txNum)/float64(ii.aggregationStep)))
		default:
		}
		return nil
	}, etl.TransformArgs{Quit: ctx.Done()})

	if stat.MinTxNum != math.MaxUint64 {
		binary.BigEndian.PutUint64(txKey[:], stat.MinTxNum)
		// This deletion iterator goes last to preserve invariant: if some `txNum=N` pruned - it's pruned Fully
		for txnb, _, err := keysCursor.Seek(txKey[:]); txnb != nil; txnb, _, err = keysCursor.NextNoDup() {
			if err != nil {
				return nil, fmt.Errorf("iterate over %s index keys: %w", ii.filenameBase, err)
			}
			if binary.BigEndian.Uint64(txnb) > stat.MaxTxNum {
				break
			}
			stat.PruneCountTx++
			if err = rwTx.Delete(ii.keysTable, txnb); err != nil {
				return nil, err
			}
		}
	}

	return stat, err
}

func (iit *InvertedIndexRoTx) IterateChangedKeys(startTxNum, endTxNum uint64, roTx kv.Tx) InvertedIterator1 {
	var ii1 InvertedIterator1
	ii1.hasNextInDb = true
	ii1.roTx = roTx
	ii1.indexTable = iit.ii.valuesTable
	for _, item := range iit.files {
		if item.endTxNum <= startTxNum {
			continue
		}
		if item.startTxNum >= endTxNum {
			break
		}
		if item.endTxNum >= endTxNum {
			ii1.hasNextInDb = false
		}
		g := seg.NewReader(item.src.decompressor.MakeGetter(), iit.ii.Compression)
		if g.HasNext() {
			key, _ := g.Next(nil)
			heap.Push(&ii1.h, &ReconItem{startTxNum: item.startTxNum, endTxNum: item.endTxNum, g: g, txNum: ^item.endTxNum, key: key})
			ii1.hasNextInFiles = true
		}
	}
	binary.BigEndian.PutUint64(ii1.startTxKey[:], startTxNum)
	ii1.startTxNum = startTxNum
	ii1.endTxNum = endTxNum
	ii1.advanceInDb()
	ii1.advanceInFiles()
	ii1.advance()
	return ii1
}

// collate [stepFrom, stepTo)
func (ii *InvertedIndex) collate(ctx context.Context, step uint64, roTx kv.Tx) (InvertedIndexCollation, error) {
	stepTo := step + 1
	txFrom, txTo := step*ii.aggregationStep, stepTo*ii.aggregationStep
	start := time.Now()
	defer mxCollateTookIndex.ObserveDuration(start)

	keysCursor, err := roTx.CursorDupSort(ii.keysTable)
	if err != nil {
		return InvertedIndexCollation{}, fmt.Errorf("create %s keys cursor: %w", ii.filenameBase, err)
	}
	defer keysCursor.Close()

	collector := etl.NewCollectorWithAllocator(ii.filenameBase+".collate.ii", ii.iiCfg.dirs.Tmp, etl.SmallSortableBuffers, ii.logger).LogLvl(log.LvlTrace)
	defer collector.Close()

	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)

	for k, v, err := keysCursor.Seek(txKey[:]); k != nil; k, v, err = keysCursor.Next() {
		if err != nil {
			return InvertedIndexCollation{}, fmt.Errorf("iterate over %s keys cursor: %w", ii.filenameBase, err)
		}
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo { // [txFrom; txTo)
			break
		}
		if err := collector.Collect(v, k); err != nil {
			return InvertedIndexCollation{}, fmt.Errorf("collect %s history key [%x]=>txn %d [%x]: %w", ii.filenameBase, k, txNum, k, err)
		}
		select {
		case <-ctx.Done():
			return InvertedIndexCollation{}, ctx.Err()
		default:
		}
	}

	var (
		coll = InvertedIndexCollation{
			iiPath: ii.efFilePath(step, stepTo),
		}
		closeComp bool
	)
	defer func() {
		if closeComp {
			coll.Close()
		}
	}()

	comp, err := seg.NewCompressor(ctx, "collate idx "+ii.filenameBase, coll.iiPath, ii.dirs.Tmp, ii.CompressorCfg, log.LvlTrace, ii.logger)
	if err != nil {
		return InvertedIndexCollation{}, fmt.Errorf("create %s compressor: %w", ii.filenameBase, err)
	}
	coll.writer = seg.NewWriter(comp, ii.Compression)

	var (
		prevEf      []byte
		prevKey     []byte
		initialized bool
		bitmap      = bitmapdb.NewBitmap64()
	)
	defer bitmapdb.ReturnToPool64(bitmap)

	loadBitmapsFunc := func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		txNum := binary.BigEndian.Uint64(v)
		if !initialized {
			prevKey = append(prevKey[:0], k...)
			initialized = true
		}

		if bytes.Equal(prevKey, k) {
			bitmap.Add(txNum)
			prevKey = append(prevKey[:0], k...)
			return nil
		}

		ef := multiencseq.NewBuilder(step*ii.aggregationStep, bitmap.GetCardinality(), bitmap.Maximum())
		it := bitmap.Iterator()
		for it.HasNext() {
			ef.AddOffset(it.Next())
		}
		bitmap.Clear()
		ef.Build()

		prevEf = ef.AppendBytes(prevEf[:0])

		if _, err = coll.writer.Write(prevKey); err != nil {
			return fmt.Errorf("add %s efi index key [%x]: %w", ii.filenameBase, prevKey, err)
		}
		if _, err = coll.writer.Write(prevEf); err != nil {
			return fmt.Errorf("add %s efi index val: %w", ii.filenameBase, err)
		}

		prevKey = append(prevKey[:0], k...)
		txNum = binary.BigEndian.Uint64(v)
		bitmap.Add(txNum)

		return nil
	}

	err = collector.Load(nil, "", loadBitmapsFunc, etl.TransformArgs{Quit: ctx.Done()})
	if err != nil {
		return InvertedIndexCollation{}, err
	}
	if !bitmap.IsEmpty() {
		if err = loadBitmapsFunc(nil, make([]byte, 8), nil, nil); err != nil {
			return InvertedIndexCollation{}, err
		}
	}

	closeComp = false
	return coll, nil
}

type InvertedFiles struct {
	decomp    *seg.Decompressor
	index     *recsplit.Index
	existence *existence.Filter
}

func (sf InvertedFiles) CleanupOnError() {
	if sf.decomp != nil {
		sf.decomp.Close()
	}
	if sf.index != nil {
		sf.index.Close()
	}
}

type InvertedIndexCollation struct {
	iiPath string
	writer *seg.Writer
}

func (ic InvertedIndexCollation) Close() {
	if ic.writer != nil {
		ic.writer.Close()
	}
}

// buildFiles - `step=N` means build file `[N:N+1)` which is equal to [N:N+1)
func (ii *InvertedIndex) buildFiles(ctx context.Context, step uint64, coll InvertedIndexCollation, ps *background.ProgressSet) (InvertedFiles, error) {
	var (
		decomp    *seg.Decompressor
		index     *recsplit.Index
		existence *existence.Filter
		err       error
	)
	mxRunningFilesBuilding.Inc()
	defer mxRunningFilesBuilding.Dec()
	closeComp := true
	defer func() {
		if closeComp {
			coll.Close()
			if decomp != nil {
				decomp.Close()
			}
			if index != nil {
				index.Close()
			}
			if existence != nil {
				existence.Close()
			}
		}
	}()

	if assert.Enable {
		if coll.iiPath == "" && reflect.ValueOf(coll.writer).IsNil() {
			panic("assert: collation is not initialized " + ii.filenameBase)
		}
	}

	{
		p := ps.AddNew(path.Base(coll.iiPath), 1)
		if err = coll.writer.Compress(); err != nil {
			ps.Delete(p)
			return InvertedFiles{}, fmt.Errorf("compress %s: %w", ii.filenameBase, err)
		}
		coll.Close()
		ps.Delete(p)
	}

	if decomp, err = seg.NewDecompressor(coll.iiPath); err != nil {
		return InvertedFiles{}, fmt.Errorf("open %s decompressor: %w", ii.filenameBase, err)
	}

	if err := ii.buildMapAccessor(ctx, step, step+1, decomp, ps); err != nil {
		return InvertedFiles{}, fmt.Errorf("build %s efi: %w", ii.filenameBase, err)
	}
	if index, err = recsplit.OpenIndex(ii.efAccessorFilePath(step, step+1)); err != nil {
		return InvertedFiles{}, err
	}

	closeComp = false
	return InvertedFiles{decomp: decomp, index: index, existence: existence}, nil
}

func (ii *InvertedIndex) buildMapAccessor(ctx context.Context, fromStep, toStep uint64, data *seg.Decompressor, ps *background.ProgressSet) error {
	idxPath := ii.efAccessorFilePath(fromStep, toStep)
	// Design decision: `why Enum=true and LessFalsePositives=true`?
	//
	// Test on: rpcdaemon (erigon shut-down), `--http.compression=false`, after `sync && sudo sysctl vm.drop_caches=3`, query:
	//```sh
	//curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method": "eth_getLogs","params": [{"fromBlock": "0x115B624", "toBlock": "0x115B664"}], "id":1}' -s -o /dev/null  localhost:8545
	//```
	//
	// On compared it with `Enum=false and LessFalsePositives=false` on ethmainnet (on small machine with cloud drives and `sync && sudo sysctl vm.drop_caches=3`):
	//  - `du -hsc *.efi` changed from `24Gb` to `17Gb` (better)
	//  - `vmtouch of .ef` changed from `152M/426G` to `787M/426G` (worse)
	//  - `vmtouch of .efi` changed from `1G/23G` to `633M/16G` (better)
	//  - speed on hot data - not changed. speed on cold data changed from `7min` to `10min`  (worse)
	//  - but most important i see `.ef` files became "randomly warm":
	// From:
	//```sh
	//vmtouch -v /mnt/erigon/snapshots/idx/v1.0-storage.1680-1682.ef
	//[ ooooooooo ooooooo oooooooooooooooooo oooooooo  oo o o  ooo ] 93/81397
	//```
	// To:
	//```sh
	//vmtouch -v /mnt/erigon/snapshots/idx/v1.0-storage.1680-1682.ef
	//[oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo] 16279/81397
	//```
	// It happens because: EVM does read much non-existing keys, like "create storage key if it doesn't exists". And
	// each such non-existing key read `MPH` transforms to random
	// key read. `LessFalsePositives=true` feature filtering-out such cases (with `1/256=0.3%` false-positives).
	cfg := recsplit.RecSplitArgs{
		Enums:              true,
		LessFalsePositives: true,

		BucketSize: recsplit.DefaultBucketSize,
		LeafSize:   recsplit.DefaultLeafSize,
		TmpDir:     ii.dirs.Tmp,
		IndexFile:  idxPath,
		Salt:       ii.salt.Load(),
		NoFsync:    ii.noFsync,
	}
	return buildHashMapAccessor(ctx, data, ii.Compression, idxPath, false, cfg, ps, ii.logger)
}

func (ii *InvertedIndex) integrateDirtyFiles(sf InvertedFiles, txNumFrom, txNumTo uint64) {
	fi := newFilesItem(txNumFrom, txNumTo, ii.aggregationStep)
	fi.decompressor = sf.decomp
	fi.index = sf.index
	fi.existence = sf.existence
	ii.dirtyFiles.Set(fi)
}

func (iit *InvertedIndexRoTx) stepsRangeInDB(tx kv.Tx) (from, to float64) {
	fst, _ := kv.FirstKey(tx, iit.ii.keysTable)
	if len(fst) > 0 {
		from = float64(binary.BigEndian.Uint64(fst)) / float64(iit.ii.aggregationStep)
	}
	lst, _ := kv.LastKey(tx, iit.ii.keysTable)
	if len(lst) > 0 {
		to = float64(binary.BigEndian.Uint64(lst)) / float64(iit.ii.aggregationStep)
	}
	if to == 0 {
		to = from
	}
	return from, to
}
