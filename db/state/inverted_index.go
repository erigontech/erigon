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

	"github.com/erigontech/erigon/db/snaptype"

	"github.com/spaolacci/murmur3"
	btree2 "github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/assert"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/datastruct/existence"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/bitmapdb"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
)

type InvertedIndex struct {
	statecfg.InvIdxCfg
	dirs    datadir.Dirs
	salt    *atomic.Pointer[uint32]
	noFsync bool // fsync is enabled by default, but tests can manually disable

	stepSize uint64 // amount of transactions inside single aggregation step

	// dirtyFiles - list of ALL files - including: un-indexed-yet, garbage, merged-into-bigger-one, ...
	// thread-safe, but maybe need 1 RWLock for all trees in Aggregator
	//
	// _visible derivative from field `file`, but without garbage:
	//  - no files with `canDelete=true`
	//  - no overlaps
	//  - no un-indexed files (`power-off` may happen between .ef and .efi creation)
	//
	// BeginRo() using _visible in zero-copy way
	dirtyFiles *btree2.BTreeG[*FilesItem]

	// `_visible.files` - underscore in name means: don't use this field directly, use BeginFilesRo()
	// underlying array is immutable - means it's ready for zero-copy use
	_visible *iiVisible
	logger   log.Logger

	checker *DependencyIntegrityChecker
}

type iiVisible struct {
	files  []visibleFile
	name   string
	caches *sync.Pool
}

func NewInvertedIndex(cfg statecfg.InvIdxCfg, stepSize uint64, dirs datadir.Dirs, logger log.Logger) (*InvertedIndex, error) {
	if dirs.SnapDomain == "" {
		panic("assert: empty `dirs`")
	}
	if cfg.FilenameBase == "" {
		panic("assert: empty `filenameBase`")
	}
	//if cfg.compressorCfg.MaxDictPatterns == 0 && cfg.compressorCfg.MaxPatternLen == 0 {
	cfg.CompressorCfg = seg.DefaultCfg
	if cfg.Accessors == 0 {
		cfg.Accessors = statecfg.AccessorHashMap
	}

	ii := InvertedIndex{
		InvIdxCfg:  cfg,
		dirs:       dirs,
		salt:       &atomic.Pointer[uint32]{},
		dirtyFiles: btree2.NewBTreeGOptions[*FilesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		_visible:   newIIVisible(cfg.FilenameBase, []visibleFile{}),
		logger:     logger,

		stepSize: stepSize,
	}
	if ii.stepSize == 0 {
		panic("assert: empty `stepSize`")
	}

	if ii.Version.DataEF.IsZero() {
		panic(fmt.Errorf("assert: forgot to set version of %s", ii.Name))
	}
	if ii.Version.AccessorEFI.IsZero() {
		panic(fmt.Errorf("assert: forgot to set version of %s", ii.Name))
	}

	return &ii, nil
}

func (ii *InvertedIndex) efAccessorNewFilePath(fromStep, toStep kv.Step) string {
	if fromStep == toStep {
		panic(fmt.Sprintf("assert: fromStep(%d) == toStep(%d)", fromStep, toStep))
	}
	return filepath.Join(ii.dirs.SnapAccessors, fmt.Sprintf("%s-%s.%d-%d.efi", ii.Version.AccessorEFI.String(), ii.FilenameBase, fromStep, toStep))
}
func (ii *InvertedIndex) efNewFilePath(fromStep, toStep kv.Step) string {
	if fromStep == toStep {
		panic(fmt.Sprintf("assert: fromStep(%d) == toStep(%d)", fromStep, toStep))
	}
	return filepath.Join(ii.dirs.SnapIdx, fmt.Sprintf("%s-%s.%d-%d.ef", ii.Version.DataEF.String(), ii.FilenameBase, fromStep, toStep))
}

func (ii *InvertedIndex) efAccessorFilePathMask(fromStep, toStep kv.Step) string {
	if fromStep == toStep {
		panic(fmt.Sprintf("assert: fromStep(%d) == toStep(%d)", fromStep, toStep))
	}
	return filepath.Join(ii.dirs.SnapAccessors, fmt.Sprintf("*-%s.%d-%d.efi", ii.FilenameBase, fromStep, toStep))
}
func (ii *InvertedIndex) efFilePathMask(fromStep, toStep kv.Step) string {
	return filepath.Join(ii.dirs.SnapIdx, fmt.Sprintf("*-%s.%d-%d.ef", ii.FilenameBase, fromStep, toStep))
}

func filesFromDir(dir string) ([]string, error) {
	allFiles, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("filesFromDir: %w, %s", err, dir)
	}
	filtered := make([]string, 0, len(allFiles))
	for _, f := range allFiles {
		if f.IsDir() || (!f.Type().IsRegular() && f.Type()&os.ModeSymlink == 0) {
			continue
		}
		if strings.HasPrefix(f.Name(), ".") { // hidden files
			continue
		}
		if snaptype.IsTorrentPartial(filepath.Ext(f.Name())) {
			continue
		}
		filtered = append(filtered, f.Name())
	}
	return filtered, nil
}

func (ii *InvertedIndex) openList(fNames []string) error {
	ii.closeWhatNotInList(fNames)
	ii.scanDirtyFiles(fNames)
	if err := ii.openDirtyFiles(); err != nil {
		return fmt.Errorf("InvertedIndex(%s).openDirtyFiles: %w", ii.FilenameBase, err)
	}
	return nil
}

func (ii *InvertedIndex) openFolder(r *ScanDirsResult) error {
	if ii.Disable {
		return nil
	}
	return ii.openList(r.iiFiles)
}

func (ii *InvertedIndex) scanDirtyFiles(fileNames []string) {
	if ii.FilenameBase == "" {
		panic("assert: empty `filenameBase`")
	}
	if ii.stepSize == 0 {
		panic("assert: empty `stepSize`")
	}
	for _, dirtyFile := range filterDirtyFiles(fileNames, ii.stepSize, ii.FilenameBase, "ef", ii.logger) {
		if _, has := ii.dirtyFiles.Get(dirtyFile); !has {
			ii.dirtyFiles.Set(dirtyFile)
		}
	}
}

func (ii *InvertedIndex) SetChecker(checker *DependencyIntegrityChecker) {
	ii.checker = checker
}

func (ii *InvertedIndex) reCalcVisibleFiles(toTxNum uint64) {
	var checker func(startTxNum, endTxNum uint64) bool
	c := ii.checker
	if c != nil {
		ue := FromII(ii.Name)
		checker = func(startTxNum, endTxNum uint64) bool {
			return c.CheckDependentPresent(ue, All, startTxNum, endTxNum)
		}
	}
	ii._visible = newIIVisible(ii.FilenameBase, calcVisibleFiles(ii.dirtyFiles, ii.Accessors, checker, false, toTxNum))
}

func (ii *InvertedIndex) MissedMapAccessors() (l []*FilesItem) {
	return ii.missedMapAccessors(ii.dirtyFiles.Items())
}

func (ii *InvertedIndex) missedMapAccessors(source []*FilesItem) (l []*FilesItem) {
	if !ii.Accessors.Has(statecfg.AccessorHashMap) {
		return nil
	}
	return fileItemsWithMissedAccessors(source, ii.stepSize, func(fromStep, toStep kv.Step) []string {
		fPath, _, _, err := version.FindFilesWithVersionsByPattern(ii.efAccessorFilePathMask(fromStep, toStep))
		if err != nil {
			panic(err)
		}
		return []string{
			fPath,
		}
	})
}

func (ii *InvertedIndex) buildEfAccessor(ctx context.Context, item *FilesItem, ps *background.ProgressSet) (err error) {
	if item.decompressor == nil {
		return fmt.Errorf("buildEfAccessor: passed item with nil decompressor %s %d-%d", ii.FilenameBase, item.startTxNum/ii.stepSize, item.endTxNum/ii.stepSize)
	}
	fromStep, toStep := kv.Step(item.startTxNum/ii.stepSize), kv.Step(item.endTxNum/ii.stepSize)
	return ii.buildMapAccessor(ctx, fromStep, toStep, ii.dataReader(item.decompressor), ps)
}
func (ii *InvertedIndex) dataReader(f *seg.Decompressor) *seg.Reader {
	if !strings.Contains(f.FileName(), ".ef") {
		panic("assert: miss-use " + f.FileName())
	}
	return seg.NewReader(f.MakeGetter(), ii.Compression)
}
func (ii *InvertedIndex) dataWriter(f *seg.Compressor, forceNoCompress bool) *seg.Writer {
	if !strings.Contains(f.FileName(), ".ef") {
		panic("assert: miss-use " + f.FileName())
	}
	if forceNoCompress {
		return seg.NewWriter(f, seg.CompressNone)
	}
	return seg.NewWriter(f, ii.Compression)
}
func (iit *InvertedIndexRoTx) dataReader(f *seg.Decompressor) *seg.Reader {
	return iit.ii.dataReader(f)
}
func (iit *InvertedIndexRoTx) dataWriter(f *seg.Compressor, forceNoCompress bool) *seg.Writer {
	return iit.ii.dataWriter(f, forceNoCompress)
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

func (ii *InvertedIndex) closeWhatNotInList(fNames []string) {
	protectFiles := make(map[string]struct{}, len(fNames))
	for _, f := range fNames {
		protectFiles[f] = struct{}{}
	}
	var toClose []*FilesItem
	ii.dirtyFiles.Walk(func(items []*FilesItem) bool {
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

func (ii *InvertedIndex) Tables() []string { return []string{ii.KeysTable, ii.ValuesTable} }

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

	stepSize   uint64
	txNumBytes [8]byte
	name       kv.InvertedIdx
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
	if iit.ii.stepSize != iit.stepSize {
		panic(fmt.Sprintf("assert: %d %d", iit.ii.stepSize, iit.stepSize))
	}
	w := &InvertedIndexBufferedWriter{
		name:         iit.name,
		discard:      discard,
		tmpdir:       tmpdir,
		filenameBase: iit.ii.FilenameBase,
		stepSize:     iit.stepSize,

		indexKeysTable: iit.ii.KeysTable,
		indexTable:     iit.ii.ValuesTable,

		// etl collector doesn't fsync: means if have enough ram, all files produced by all collectors will be in ram
		indexKeys: etl.NewCollectorWithAllocator(iit.ii.FilenameBase+".ii.keys", tmpdir, etl.SmallSortableBuffers, iit.ii.logger).LogLvl(log.LvlTrace),
		index:     etl.NewCollectorWithAllocator(iit.ii.FilenameBase+".ii.vals", tmpdir, etl.SmallSortableBuffers, iit.ii.logger).LogLvl(log.LvlTrace),
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
		ii:       ii,
		visible:  ii._visible,
		files:    files,
		stepSize: ii.stepSize,
		name:     ii.Name,
		salt:     ii.salt.Load(),
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
			if traceFileLife != "" && iit.ii.FilenameBase == traceFileLife {
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

func (mr *MergeRange) String(prefix string, stepSize uint64) string {
	if prefix != "" {
		prefix += "="
	}
	return fmt.Sprintf("%s%s%d-%d", prefix, mr.name, mr.from/stepSize, mr.to/stepSize)
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
	salt     *uint32
	stepSize uint64
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
		return false, 0, fmt.Errorf("seekInFiles(invIndex=%s,txNum=%d) but data before txNum=%d not available", iit.name.String(), txNum, iit.files[0].startTxNum)
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
			return false, equalOrHigherTxNum, fmt.Errorf("inverted_index(%s) at (%x, %d) returned value %d, but it out-of-bounds %d-%d. it may signal that .ef file is broke - can detect by `erigon snapshots integrity --check=InvertedIndex`, or re-download files", g.FileName(), key, txNum, iit.files[i].startTxNum, iit.files[i].endTxNum, equalOrHigherTxNum)
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
	frozenIt, err := iit.iterateRangeOnFiles(key, startTxNum, endTxNum, asc, limit)
	if err != nil {
		return nil, err
	}
	recentIt, err := iit.recentIterateRange(key, startTxNum, endTxNum, asc, limit, roTx)
	if err != nil {
		return nil, err
	}
	return stream.Union[uint64](frozenIt, recentIt, asc, limit), nil
}

func (iit *InvertedIndexRoTx) recentIterateRange(key []byte, startTxNum, endTxNum int, asc order.By, limit int, roTx kv.Tx) (stream.U64, error) {
	//optimization: return empty pre-allocated iterator if range is frozen
	if asc {
		isFrozenRange := len(iit.files) > 0 && endTxNum >= 0 && iit.files.EndTxNum() >= uint64(endTxNum)
		if isFrozenRange {
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
	it, err := roTx.RangeDupSort(iit.ii.ValuesTable, key, from, to, asc, limit)
	if err != nil {
		return nil, err
	}
	return stream.TransformKV2U64(it, func(_, v []byte) (uint64, error) {
		return binary.BigEndian.Uint64(v), nil
	}), nil
}

// IdxRange is to be used in public API, therefore it relies on read-only transaction
// so that iteration can be done even when the inverted index is being updated.
// [startTxNum; endNumTx)
func (iit *InvertedIndexRoTx) iterateRangeOnFiles(key []byte, startTxNum, endTxNum int, asc order.By, limit int) (*InvertedIdxStreamFiles, error) {
	if asc && (startTxNum >= 0 && endTxNum >= 0) && startTxNum > endTxNum {
		return nil, fmt.Errorf("startTxNum=%d expected to be lower than endTxNum=%d", startTxNum, endTxNum)
	}
	if !asc && (startTxNum >= 0 && endTxNum >= 0) && startTxNum < endTxNum {
		return nil, fmt.Errorf("startTxNum=%d expected to be bigger than endTxNum=%d", startTxNum, endTxNum)
	}

	it := &InvertedIdxStreamFiles{
		key:         key,
		startTxNum:  startTxNum,
		endTxNum:    endTxNum,
		indexTable:  iit.ii.ValuesTable,
		orderAscend: asc,
		limit:       limit,
		seq:         &multiencseq.SequenceReader{},
		accessors:   iit.ii.Accessors,
		ii:          iit,
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

func (iit *InvertedIndexRoTx) CanPrune(tx kv.Tx) bool {
	return iit.ii.minTxNumInDB(tx) < iit.files.EndTxNum()
}

func (iit *InvertedIndexRoTx) canBuild(dbtx kv.Tx) bool { //nolint
	maxStepInFiles := iit.files.EndTxNum() / iit.stepSize
	maxStepInDB := iit.ii.maxTxNumInDB(dbtx) / iit.stepSize
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
	_, err := iit.prune(ctx, rwTx, txFrom, txTo, limit, logEvery, fn)
	if err != nil {
		return err
	}
	return nil
}

// [txFrom; txTo)
// forced - prune even if CanPrune returns false, so its true only when we do Unwind.
func (iit *InvertedIndexRoTx) Prune(ctx context.Context, tx kv.RwTx, txFrom, txTo, limit uint64, logEvery *time.Ticker, forced bool, fn func(key []byte, txnum []byte) error) (stat *InvertedIndexPruneStat, err error) {
	if !forced {
		if iit.files.EndTxNum() > 0 {
			txTo = min(txTo, iit.files.EndTxNum())
		}
		if !iit.CanPrune(tx) {
			return &InvertedIndexPruneStat{MinTxNum: math.MaxUint64}, nil
		}
	}
	return iit.prune(ctx, tx, txFrom, txTo, limit, logEvery, fn)
}

func (iit *InvertedIndexRoTx) prune(ctx context.Context, rwTx kv.RwTx, txFrom, txTo, limit uint64, logEvery *time.Ticker, fn func(key []byte, txnum []byte) error) (stat *InvertedIndexPruneStat, err error) {
	stat = &InvertedIndexPruneStat{MinTxNum: math.MaxUint64}

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
	//		"pruned tx", fmt.Sprintf("%.2f-%.2f", float64(minTxnum)/float64(iit.stepSize), float64(maxTxnum)/float64(iit.stepSize)),
	//		"pruned values", pruneCount,
	//		"tx until limit", limit)
	//}()

	keysCursor, err := rwTx.CursorDupSort(ii.KeysTable)
	if err != nil {
		return stat, fmt.Errorf("create %s keys cursor: %w", ii.FilenameBase, err)
	}
	defer keysCursor.Close()
	idxDelCursor, err := rwTx.RwCursorDupSort(ii.ValuesTable)
	if err != nil {
		return nil, err
	}
	defer idxDelCursor.Close()

	collector := etl.NewCollectorWithAllocator(ii.FilenameBase+".prune.ii", ii.dirs.Tmp, etl.SmallSortableBuffers, ii.logger)
	defer collector.Close()
	collector.LogLvl(log.LvlTrace)
	collector.SortAndFlushInBackground(true)

	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)

	// Invariant: if some `txNum=N` pruned - it's pruned Fully
	// Means: can use DeleteCurrentDuplicates all values of given `txNum`
	for k, v, err := keysCursor.Seek(txKey[:]); k != nil; k, v, err = keysCursor.NextNoDup() {
		if err != nil {
			return nil, fmt.Errorf("iterate over %s index keys: %w", ii.FilenameBase, err)
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
				return nil, fmt.Errorf("iterate over %s index keys: %w", ii.FilenameBase, err)
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
			ii.logger.Info("[snapshots] prune index", "name", ii.FilenameBase, "pruned tx", stat.PruneCountTx,
				"pruned values", stat.PruneCountValues,
				"steps", fmt.Sprintf("%.2f-%.2f", float64(txFrom)/float64(ii.stepSize), float64(txNum)/float64(ii.stepSize)))
		default:
		}
		return nil
	}, etl.TransformArgs{Quit: ctx.Done()})

	if stat.MinTxNum != math.MaxUint64 {
		binary.BigEndian.PutUint64(txKey[:], stat.MinTxNum)
		// This deletion iterator goes last to preserve invariant: if some `txNum=N` pruned - it's pruned Fully
		for txnb, _, err := keysCursor.Seek(txKey[:]); txnb != nil; txnb, _, err = keysCursor.NextNoDup() {
			if err != nil {
				return nil, fmt.Errorf("iterate over %s index keys: %w", ii.FilenameBase, err)
			}
			if binary.BigEndian.Uint64(txnb) > stat.MaxTxNum {
				break
			}
			stat.PruneCountTx++
			if err = rwTx.Delete(ii.KeysTable, txnb); err != nil {
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
	ii1.indexTable = iit.ii.ValuesTable
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
		g := iit.dataReader(item.src.decompressor)
		g.Reset(0)
		wrapper := NewSegReaderWrapper(g)
		if wrapper.HasNext() {
			key, val, err := wrapper.Next()
			if err != nil {
				return ii1
			}
			heap.Push(&ii1.h, &ReconItem{startTxNum: item.startTxNum, endTxNum: item.endTxNum, g: wrapper, key: key, val: val, txNum: ^item.endTxNum})
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
func (ii *InvertedIndex) collate(ctx context.Context, step kv.Step, roTx kv.Tx) (InvertedIndexCollation, error) {
	stepTo := step + 1
	txFrom, txTo := uint64(step)*ii.stepSize, uint64(stepTo)*ii.stepSize
	start := time.Now()
	defer mxCollateTookIndex.ObserveDuration(start)

	keysCursor, err := roTx.CursorDupSort(ii.KeysTable)
	if err != nil {
		return InvertedIndexCollation{}, fmt.Errorf("create %s keys cursor: %w", ii.FilenameBase, err)
	}
	defer keysCursor.Close()

	collector := etl.NewCollectorWithAllocator(ii.FilenameBase+".collate.ii", ii.dirs.Tmp, etl.SmallSortableBuffers, ii.logger).LogLvl(log.LvlTrace)
	defer collector.Close()

	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)

	for k, v, err := keysCursor.Seek(txKey[:]); k != nil; k, v, err = keysCursor.Next() {
		if err != nil {
			return InvertedIndexCollation{}, fmt.Errorf("iterate over %s keys cursor: %w", ii.FilenameBase, err)
		}
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo { // [txFrom; txTo)
			break
		}
		if err := collector.Collect(v, k); err != nil {
			return InvertedIndexCollation{}, fmt.Errorf("collect %s history key [%x]=>txn %d [%x]: %w", ii.FilenameBase, k, txNum, k, err)
		}
		select {
		case <-ctx.Done():
			return InvertedIndexCollation{}, ctx.Err()
		default:
		}
	}

	var (
		coll = InvertedIndexCollation{
			iiPath: ii.efNewFilePath(step, stepTo),
		}
		closeComp bool
	)
	defer func() {
		if closeComp {
			coll.Close()
		}
	}()

	comp, err := seg.NewCompressor(ctx, "collate idx "+ii.FilenameBase, coll.iiPath, ii.dirs.Tmp, ii.CompressorCfg, log.LvlTrace, ii.logger)
	if err != nil {
		return InvertedIndexCollation{}, fmt.Errorf("create %s compressor: %w", ii.FilenameBase, err)
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

		baseTxNum := uint64(step) * ii.stepSize
		ef := multiencseq.NewBuilder(baseTxNum, bitmap.GetCardinality(), bitmap.Maximum())
		it := bitmap.Iterator()
		for it.HasNext() {
			ef.AddOffset(it.Next())
		}
		bitmap.Clear()
		ef.Build()

		prevEf = ef.AppendBytes(prevEf[:0])

		if _, err = coll.writer.Write(prevKey); err != nil {
			return fmt.Errorf("add %s efi index key [%x]: %w", ii.FilenameBase, prevKey, err)
		}
		if _, err = coll.writer.Write(prevEf); err != nil {
			return fmt.Errorf("add %s efi index val: %w", ii.FilenameBase, err)
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
func (ii *InvertedIndex) buildFiles(ctx context.Context, step kv.Step, coll InvertedIndexCollation, ps *background.ProgressSet) (InvertedFiles, error) {
	var (
		decomp          *seg.Decompressor
		mapAccessor     *recsplit.Index
		existenceFilter *existence.Filter
		err             error
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
			if mapAccessor != nil {
				mapAccessor.Close()
			}
			if existenceFilter != nil {
				existenceFilter.Close()
			}
		}
	}()

	if assert.Enable {
		if coll.iiPath == "" && reflect.ValueOf(coll.writer).IsNil() {
			panic("assert: collation is not initialized " + ii.FilenameBase)
		}
	}

	{
		p := ps.AddNew(path.Base(coll.iiPath), 1)
		if err = coll.writer.Compress(); err != nil {
			ps.Delete(p)
			return InvertedFiles{}, fmt.Errorf("compress %s: %w", ii.FilenameBase, err)
		}
		coll.Close()
		ps.Delete(p)
	}

	if decomp, err = seg.NewDecompressor(coll.iiPath); err != nil {
		return InvertedFiles{}, fmt.Errorf("open %s decompressor: %w", ii.FilenameBase, err)
	}

	if err := ii.buildMapAccessor(ctx, step, step+1, ii.dataReader(decomp), ps); err != nil {
		return InvertedFiles{}, fmt.Errorf("build %s efi: %w", ii.FilenameBase, err)
	}
	if ii.Accessors.Has(statecfg.AccessorHashMap) {
		if mapAccessor, err = recsplit.OpenIndex(ii.efAccessorNewFilePath(step, step+1)); err != nil {
			return InvertedFiles{}, err
		}
	}

	closeComp = false
	return InvertedFiles{decomp: decomp, index: mapAccessor, existence: existenceFilter}, nil
}

func (ii *InvertedIndex) buildMapAccessor(ctx context.Context, fromStep, toStep kv.Step, data *seg.Reader, ps *background.ProgressSet) error {
	idxPath := ii.efAccessorNewFilePath(fromStep, toStep)
	versionOfRs := uint8(0)
	if !ii.Version.AccessorEFI.Current.Eq(version.V1_0) { // inner version=1 incompatible with .efi v1.0
		versionOfRs = 1
	}
	cfg := recsplit.RecSplitArgs{
		BucketSize: recsplit.DefaultBucketSize,
		LeafSize:   recsplit.DefaultLeafSize,
		TmpDir:     ii.dirs.Tmp,
		IndexFile:  idxPath,
		Salt:       ii.salt.Load(),
		NoFsync:    ii.noFsync,

		Version:            versionOfRs,
		Enums:              true,
		LessFalsePositives: true,
	}

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

	if err := buildHashMapAccessor(ctx, data, idxPath, false, cfg, ps, ii.logger); err != nil {
		return err
	}
	return nil
}

func (ii *InvertedIndex) integrateDirtyFiles(sf InvertedFiles, txNumFrom, txNumTo uint64) {
	if txNumFrom == txNumTo {
		panic(fmt.Sprintf("assert: txNumFrom(%d) == txNumTo(%d)", txNumFrom, txNumTo))
	}
	fi := newFilesItem(txNumFrom, txNumTo, ii.stepSize)
	fi.decompressor = sf.decomp
	fi.index = sf.index
	fi.existence = sf.existence
	ii.dirtyFiles.Set(fi)
}

func (iit *InvertedIndexRoTx) stepsRangeInDB(tx kv.Tx) (from, to float64) {
	fst, _ := kv.FirstKey(tx, iit.ii.KeysTable)
	if len(fst) > 0 {
		from = float64(binary.BigEndian.Uint64(fst)) / float64(iit.stepSize)
	}
	lst, _ := kv.LastKey(tx, iit.ii.KeysTable)
	if len(lst) > 0 {
		to = float64(binary.BigEndian.Uint64(lst)) / float64(iit.stepSize)
	}
	if to == 0 {
		to = from
	}
	return from, to
}

func (ii *InvertedIndex) minTxNumInDB(tx kv.Tx) uint64 {
	fst, _ := kv.FirstKey(tx, ii.KeysTable)
	if len(fst) > 0 {
		fstInDb := binary.BigEndian.Uint64(fst)
		return min(fstInDb, math.MaxUint64)
	}
	return math.MaxUint64
}

func (ii *InvertedIndex) maxTxNumInDB(tx kv.Tx) uint64 {
	lst, _ := kv.LastKey(tx, ii.KeysTable)
	if len(lst) > 0 {
		lstInDb := binary.BigEndian.Uint64(lst)
		return max(lstInDb, 0)
	}
	return 0
}

func (iit *InvertedIndexRoTx) Progress(tx kv.Tx) uint64 {
	return max(iit.files.EndTxNum(), iit.ii.maxTxNumInDB(tx))
}
