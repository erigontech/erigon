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
	"path/filepath"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/log/v3"
	btree2 "github.com/tidwall/btree"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

type History struct {
	*InvertedIndex

	// Files:
	//  .v - list of values
	//  .vi - txNum+key -> offset in .v
	files *btree2.BTreeG[*filesItem] // thread-safe, but maybe need 1 RWLock for all trees in AggregatorV3

	// roFiles derivative from field `file`, but without garbage (canDelete=true, overlaps, etc...)
	// MakeContext() using this field in zero-copy way
	roFiles atomic.Pointer[[]ctxItem]

	historyValsTable        string // key1+key2+txnNum -> oldValue , stores values BEFORE change
	compressWorkers         int
	compressVals            bool
	integrityFileExtensions []string

	// not large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2 -> txNum + value (DupSort)
	// large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2+txNum -> value (not DupSort)
	largeValues bool // can't use DupSort optimization (aka. prefix-compression) if values size > 4kb

	garbageFiles []*filesItem // files that exist on disk, but ignored on opening folder - because they are garbage

	wal    *historyWAL
	logger log.Logger
}

func NewHistory(dir, tmpdir string, aggregationStep uint64,
	filenameBase, indexKeysTable, indexTable, historyValsTable string,
	compressVals bool, integrityFileExtensions []string, largeValues bool, logger log.Logger) (*History, error) {
	h := History{
		files:                   btree2.NewBTreeGOptions[*filesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		historyValsTable:        historyValsTable,
		compressVals:            compressVals,
		compressWorkers:         1,
		integrityFileExtensions: integrityFileExtensions,
		largeValues:             largeValues,
		logger:                  logger,
	}
	h.roFiles.Store(&[]ctxItem{})
	var err error
	h.InvertedIndex, err = NewInvertedIndex(dir, tmpdir, aggregationStep, filenameBase, indexKeysTable, indexTable, true, append(slices.Clone(h.integrityFileExtensions), "v"), logger)
	if err != nil {
		return nil, fmt.Errorf("NewHistory: %s, %w", filenameBase, err)
	}

	return &h, nil
}

// OpenList - main method to open list of files.
// It's ok if some files was open earlier.
// If some file already open: noop.
// If some file already open but not in provided list: close and remove from `files` field.
func (h *History) OpenList(fNames []string) error {
	if err := h.InvertedIndex.OpenList(fNames); err != nil {
		return err
	}
	return h.openList(fNames)

}
func (h *History) openList(fNames []string) error {
	h.closeWhatNotInList(fNames)
	h.garbageFiles = h.scanStateFiles(fNames)
	if err := h.openFiles(); err != nil {
		return fmt.Errorf("History.OpenList: %s, %w", h.filenameBase, err)
	}
	return nil
}

func (h *History) OpenFolder() error {
	files, err := h.fileNamesOnDisk()
	if err != nil {
		return err
	}
	return h.OpenList(files)
}

// scanStateFiles
// returns `uselessFiles` where file "is useless" means: it's subset of frozen file. such files can be safely deleted. subset of non-frozen file may be useful
func (h *History) scanStateFiles(fNames []string) (garbageFiles []*filesItem) {
	re := regexp.MustCompile("^" + h.filenameBase + ".([0-9]+)-([0-9]+).v$")
	var err error
Loop:
	for _, name := range fNames {
		subs := re.FindStringSubmatch(name)
		if len(subs) != 3 {
			if len(subs) != 0 {
				h.logger.Warn("[snapshots] file ignored by inverted index scan, more than 3 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startStep, endStep uint64
		if startStep, err = strconv.ParseUint(subs[1], 10, 64); err != nil {
			h.logger.Warn("[snapshots] file ignored by inverted index scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endStep, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			h.logger.Warn("[snapshots] file ignored by inverted index scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startStep > endStep {
			h.logger.Warn("[snapshots] file ignored by inverted index scan, startTxNum > endTxNum", "name", name)
			continue
		}

		startTxNum, endTxNum := startStep*h.aggregationStep, endStep*h.aggregationStep
		var newFile = newFilesItem(startTxNum, endTxNum, h.aggregationStep)

		for _, ext := range h.integrityFileExtensions {
			requiredFile := fmt.Sprintf("%s.%d-%d.%s", h.filenameBase, startStep, endStep, ext)
			if !dir.FileExist(filepath.Join(h.dir, requiredFile)) {
				h.logger.Debug(fmt.Sprintf("[snapshots] skip %s because %s doesn't exists", name, requiredFile))
				garbageFiles = append(garbageFiles, newFile)
				continue Loop
			}
		}

		if _, has := h.files.Get(newFile); has {
			continue
		}

		addNewFile := true
		var subSets []*filesItem
		h.files.Walk(func(items []*filesItem) bool {
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
			h.files.Set(newFile)
		}
	}
	return garbageFiles
}

func (h *History) openFiles() error {
	var totalKeys uint64
	var err error
	invalidFileItems := make([]*filesItem, 0)
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.decompressor != nil {
				continue
			}
			fromStep, toStep := item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep
			datPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.v", h.filenameBase, fromStep, toStep))
			if !dir.FileExist(datPath) {
				invalidFileItems = append(invalidFileItems, item)
				continue
			}
			if item.decompressor, err = compress.NewDecompressor(datPath); err != nil {
				h.logger.Debug("Hisrory.openFiles: %w, %s", err, datPath)
				return false
			}

			if item.index != nil {
				continue
			}
			idxPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, fromStep, toStep))
			if dir.FileExist(idxPath) {
				if item.index, err = recsplit.OpenIndex(idxPath); err != nil {
					h.logger.Debug(fmt.Errorf("Hisrory.openFiles: %w, %s", err, idxPath).Error())
					return false
				}
				totalKeys += item.index.KeyCount()
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	for _, item := range invalidFileItems {
		h.files.Delete(item)
	}

	h.reCalcRoFiles()
	return nil
}

func (h *History) closeWhatNotInList(fNames []string) {
	var toDelete []*filesItem
	h.files.Walk(func(items []*filesItem) bool {
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
		h.files.Delete(item)
	}
}

func (h *History) Close() {
	h.InvertedIndex.Close()
	h.closeWhatNotInList([]string{})
	h.reCalcRoFiles()
}

func (h *History) Files() (res []string) {
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.decompressor != nil {
				res = append(res, item.decompressor.FileName())
			}
		}
		return true
	})
	res = append(res, h.InvertedIndex.Files()...)
	return res
}

func (h *History) missedIdxFiles() (l []*filesItem) {
	h.files.Walk(func(items []*filesItem) bool { // don't run slow logic while iterating on btree
		for _, item := range items {
			fromStep, toStep := item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep
			if !dir.FileExist(filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, fromStep, toStep))) {
				l = append(l, item)
			}
		}
		return true
	})
	return l
}

// BuildMissedIndices - produce .efi/.vi/.kvi from .ef/.v/.kv
func (hc *HistoryContext) BuildOptionalMissedIndices(ctx context.Context) (err error) {
	return hc.h.localityIndex.BuildMissedIndices(ctx, hc.ic)
}

func (h *History) buildVi(ctx context.Context, item *filesItem, p *background.Progress) (err error) {
	search := &filesItem{startTxNum: item.startTxNum, endTxNum: item.endTxNum}
	iiItem, ok := h.InvertedIndex.files.Get(search)
	if !ok {
		return nil
	}

	fromStep, toStep := item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep
	fName := fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, fromStep, toStep)
	idxPath := filepath.Join(h.dir, fName)

	//h.logger.Info("[snapshots] build idx", "file", fName)

	p.Name.Store(&fName)
	p.Total.Store(uint64(iiItem.decompressor.Count()) * 2)

	count, err := iterateForVi(item, iiItem, p, h.compressVals, func(v []byte) error { return nil })
	if err != nil {
		return err
	}
	return buildVi(ctx, item, iiItem, idxPath, h.tmpdir, count, p, h.compressVals, h.logger)
}

func (h *History) BuildMissedIndices(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet) {
	h.InvertedIndex.BuildMissedIndices(ctx, g, ps)
	missedFiles := h.missedIdxFiles()
	for _, item := range missedFiles {
		item := item
		g.Go(func() error {
			p := &background.Progress{}
			ps.Add(p)
			defer ps.Delete(p)
			return h.buildVi(ctx, item, p)
		})
	}
}

func iterateForVi(historyItem, iiItem *filesItem, p *background.Progress, compressVals bool, f func(v []byte) error) (count int, err error) {
	var cp CursorHeap
	heap.Init(&cp)
	g := iiItem.decompressor.MakeGetter()
	g.Reset(0)
	if g.HasNext() {
		g2 := historyItem.decompressor.MakeGetter()
		key, _ := g.NextUncompressed()
		val, _ := g.NextUncompressed()
		heap.Push(&cp, &CursorItem{
			t:        FILE_CURSOR,
			dg:       g,
			dg2:      g2,
			key:      key,
			val:      val,
			endTxNum: iiItem.endTxNum,
			reverse:  false,
		})
	}

	// In the loop below, the pair `keyBuf=>valBuf` is always 1 item behind `lastKey=>lastVal`.
	// `lastKey` and `lastVal` are taken from the top of the multi-way merge (assisted by the CursorHeap cp), but not processed right away
	// instead, the pair from the previous iteration is processed first - `keyBuf=>valBuf`. After that, `keyBuf` and `valBuf` are assigned
	// to `lastKey` and `lastVal` correspondingly, and the next step of multi-way merge happens. Therefore, after the multi-way merge loop
	// (when CursorHeap cp is empty), there is a need to process the last pair `keyBuf=>valBuf`, because it was one step behind
	var valBuf []byte
	for cp.Len() > 0 {
		lastKey := common.Copy(cp[0].key)
		// Advance all the items that have this key (including the top)
		//var mergeOnce bool
		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
			ci1 := cp[0]
			keysCount := eliasfano32.Count(ci1.val)
			for i := uint64(0); i < keysCount; i++ {
				if compressVals {
					valBuf, _ = ci1.dg2.Next(valBuf[:0])
				} else {
					valBuf, _ = ci1.dg2.NextUncompressed()
				}
				if err = f(valBuf); err != nil {
					return count, err
				}
			}
			count += int(keysCount)
			if ci1.dg.HasNext() {
				ci1.key, _ = ci1.dg.NextUncompressed()
				ci1.val, _ = ci1.dg.NextUncompressed()
				heap.Fix(&cp, 0)
			} else {
				heap.Remove(&cp, 0)
			}

			p.Processed.Add(1)
		}
	}
	return count, nil
}

func buildVi(ctx context.Context, historyItem, iiItem *filesItem, historyIdxPath, tmpdir string, count int, p *background.Progress, compressVals bool, logger log.Logger) error {
	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:    count,
		Enums:       false,
		BucketSize:  2000,
		LeafSize:    8,
		TmpDir:      tmpdir,
		IndexFile:   historyIdxPath,
		EtlBufLimit: etl.BufferOptimalSize / 2,
	}, logger)
	if err != nil {
		return fmt.Errorf("create recsplit: %w", err)
	}
	rs.LogLvl(log.LvlTrace)
	defer rs.Close()
	var historyKey []byte
	var txKey [8]byte
	var valOffset uint64

	defer iiItem.decompressor.EnableMadvNormal().DisableReadAhead()
	defer historyItem.decompressor.EnableMadvNormal().DisableReadAhead()

	g := iiItem.decompressor.MakeGetter()
	g2 := historyItem.decompressor.MakeGetter()
	var keyBuf, valBuf []byte
	for {
		g.Reset(0)
		g2.Reset(0)
		valOffset = 0
		for g.HasNext() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			keyBuf, _ = g.NextUncompressed()
			valBuf, _ = g.NextUncompressed()
			ef, _ := eliasfano32.ReadEliasFano(valBuf)
			efIt := ef.Iterator()
			for efIt.HasNext() {
				txNum, _ := efIt.Next()
				binary.BigEndian.PutUint64(txKey[:], txNum)
				historyKey = append(append(historyKey[:0], txKey[:]...), keyBuf...)
				if err = rs.AddKey(historyKey, valOffset); err != nil {
					return err
				}
				if compressVals {
					valOffset, _ = g2.Skip()
				} else {
					valOffset, _ = g2.SkipUncompressed()
				}
			}

			p.Processed.Add(1)
		}
		if err = rs.Build(ctx); err != nil {
			if rs.Collision() {
				logger.Info("Building recsplit. Collision happened. It's ok. Restarting...")
				rs.ResetNextSalt()
			} else {
				return fmt.Errorf("build %s idx: %w", historyIdxPath, err)
			}
		} else {
			break
		}
	}
	return nil
}

func (h *History) AddPrevValue(key1, key2, original []byte) (err error) {
	if original == nil {
		original = []byte{}
	}
	return h.wal.addPrevValue(key1, key2, original)
}

func (h *History) DiscardHistory() {
	h.InvertedIndex.StartWrites()
	h.wal = h.newWriter(h.tmpdir, false, true)
}
func (h *History) StartUnbufferedWrites() {
	h.InvertedIndex.StartUnbufferedWrites()
	h.wal = h.newWriter(h.tmpdir, false, false)
}
func (h *History) StartWrites() {
	h.InvertedIndex.StartWrites()
	h.wal = h.newWriter(h.tmpdir, true, false)
}
func (h *History) FinishWrites() {
	h.InvertedIndex.FinishWrites()
	h.wal.close()
	h.wal = nil
}

func (h *History) Rotate() historyFlusher {
	w := h.wal
	h.wal = h.newWriter(h.wal.tmpdir, h.wal.buffered, h.wal.discard)
	return historyFlusher{h: w, i: h.InvertedIndex.Rotate()}
}

type historyFlusher struct {
	h *historyWAL
	i *invertedIndexWAL
}

func (f historyFlusher) Flush(ctx context.Context, tx kv.RwTx) error {
	if err := f.i.Flush(ctx, tx); err != nil {
		return err
	}
	if err := f.h.flush(ctx, tx); err != nil {
		return err
	}
	return nil
}

type historyWAL struct {
	h                *History
	historyVals      *etl.Collector
	tmpdir           string
	autoIncrementBuf []byte
	historyKey       []byte
	buffered         bool
	discard          bool

	// not large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2 -> txNum + value (DupSort)
	// large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2+txNum -> value (not DupSort)
	largeValues bool
}

func (h *historyWAL) close() {
	if h == nil { // allow dobule-close
		return
	}
	if h.historyVals != nil {
		h.historyVals.Close()
	}
}

func (h *History) newWriter(tmpdir string, buffered, discard bool) *historyWAL {
	w := &historyWAL{h: h,
		tmpdir:   tmpdir,
		buffered: buffered,
		discard:  discard,

		autoIncrementBuf: make([]byte, 8),
		historyKey:       make([]byte, 0, 128),
		largeValues:      h.largeValues,
	}
	if buffered {
		w.historyVals = etl.NewCollector(h.historyValsTable, tmpdir, etl.NewSortableBuffer(WALCollectorRAM), h.logger)
		w.historyVals.LogLvl(log.LvlTrace)
	}
	return w
}

func (h *historyWAL) flush(ctx context.Context, tx kv.RwTx) error {
	if h.discard || !h.buffered {
		return nil
	}
	if err := h.historyVals.Load(tx, h.h.historyValsTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	h.close()
	return nil
}

func (h *historyWAL) addPrevValue(key1, key2, original []byte) error {
	if h.discard {
		return nil
	}

	ii := h.h.InvertedIndex
	if h.largeValues {
		lk := len(key1) + len(key2)
		historyKey := h.historyKey[:lk+8]
		copy(historyKey, key1)
		if len(key2) > 0 {
			copy(historyKey[len(key1):], key2)
		}
		copy(historyKey[lk:], h.h.InvertedIndex.txNumBytes[:])

		if !h.buffered {
			if err := h.h.tx.Put(h.h.historyValsTable, historyKey, original); err != nil {
				return err
			}
			if err := ii.tx.Put(ii.indexKeysTable, ii.txNumBytes[:], historyKey[:lk]); err != nil {
				return err
			}
			return nil
		}
		if err := h.historyVals.Collect(historyKey, original); err != nil {
			return err
		}
		if err := ii.wal.indexKeys.Collect(ii.txNumBytes[:], historyKey[:lk]); err != nil {
			return err
		}
		return nil
	}

	lk := len(key1) + len(key2)
	historyKey := h.historyKey[:lk+8+len(original)]
	copy(historyKey, key1)
	copy(historyKey[len(key1):], key2)
	copy(historyKey[lk:], h.h.InvertedIndex.txNumBytes[:])
	copy(historyKey[lk+8:], original)
	historyKey1 := historyKey[:lk]
	historyVal := historyKey[lk:]
	invIdxVal := historyKey[:lk]

	if !h.buffered {
		if err := h.h.tx.Put(h.h.historyValsTable, historyKey1, historyVal); err != nil {
			return err
		}
		if err := ii.tx.Put(ii.indexKeysTable, ii.txNumBytes[:], invIdxVal); err != nil {
			return err
		}
		return nil
	}
	if err := h.historyVals.Collect(historyKey1, historyVal); err != nil {
		return err
	}
	if err := ii.wal.indexKeys.Collect(ii.txNumBytes[:], invIdxVal); err != nil {
		return err
	}
	return nil
}

type HistoryCollation struct {
	historyComp  *compress.Compressor
	indexBitmaps map[string]*roaring64.Bitmap
	historyPath  string
	historyCount int
}

func (c HistoryCollation) Close() {
	if c.historyComp != nil {
		c.historyComp.Close()
	}
	for _, b := range c.indexBitmaps {
		bitmapdb.ReturnToPool64(b)
	}
}

func (h *History) collate(step, txFrom, txTo uint64, roTx kv.Tx) (HistoryCollation, error) {
	var historyComp *compress.Compressor
	var err error
	closeComp := true
	defer func() {
		if closeComp {
			if historyComp != nil {
				historyComp.Close()
			}
		}
	}()
	historyPath := filepath.Join(h.dir, fmt.Sprintf("%s.%d-%d.v", h.filenameBase, step, step+1))
	if historyComp, err = compress.NewCompressor(context.Background(), "collate history", historyPath, h.tmpdir, compress.MinPatternScore, h.compressWorkers, log.LvlTrace, h.logger); err != nil {
		return HistoryCollation{}, fmt.Errorf("create %s history compressor: %w", h.filenameBase, err)
	}
	keysCursor, err := roTx.CursorDupSort(h.indexKeysTable)
	if err != nil {
		return HistoryCollation{}, fmt.Errorf("create %s history cursor: %w", h.filenameBase, err)
	}
	defer keysCursor.Close()
	indexBitmaps := map[string]*roaring64.Bitmap{}
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	var k, v []byte
	for k, v, err = keysCursor.Seek(txKey[:]); err == nil && k != nil; k, v, err = keysCursor.Next() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		var bitmap *roaring64.Bitmap
		var ok bool
		if bitmap, ok = indexBitmaps[string(v)]; !ok {
			bitmap = bitmapdb.NewBitmap64()
			indexBitmaps[string(v)] = bitmap
		}
		bitmap.Add(txNum)
	}
	if err != nil {
		return HistoryCollation{}, fmt.Errorf("iterate over %s history cursor: %w", h.filenameBase, err)
	}
	keys := make([]string, 0, len(indexBitmaps))
	for key := range indexBitmaps {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	historyCount := 0
	keyBuf := make([]byte, 256)

	var c kv.Cursor
	var cd kv.CursorDupSort
	if h.largeValues {
		c, err = roTx.Cursor(h.historyValsTable)
		if err != nil {
			return HistoryCollation{}, err
		}
		defer c.Close()
	} else {
		cd, err = roTx.CursorDupSort(h.historyValsTable)
		if err != nil {
			return HistoryCollation{}, err
		}
		defer cd.Close()
	}
	for _, key := range keys {
		bitmap := indexBitmaps[key]
		it := bitmap.Iterator()
		copy(keyBuf, key)
		keyBuf = keyBuf[:len(key)+8]
		for it.HasNext() {
			txNum := it.Next()
			binary.BigEndian.PutUint64(keyBuf[len(key):], txNum)
			//TODO: use cursor range
			if h.largeValues {
				val, err := roTx.GetOne(h.historyValsTable, keyBuf)
				if err != nil {
					return HistoryCollation{}, fmt.Errorf("get %s history val [%x]: %w", h.filenameBase, k, err)
				}
				if len(val) == 0 {
					val = nil
				}
				if err = historyComp.AddUncompressedWord(val); err != nil {
					return HistoryCollation{}, fmt.Errorf("add %s history val [%x]=>[%x]: %w", h.filenameBase, k, val, err)
				}
			} else {
				val, err := cd.SeekBothRange(keyBuf[:len(key)], keyBuf[len(key):])
				if err != nil {
					return HistoryCollation{}, err
				}
				if val != nil && binary.BigEndian.Uint64(val) == txNum {
					val = val[8:]
				} else {
					val = nil
				}
				if err = historyComp.AddUncompressedWord(val); err != nil {
					return HistoryCollation{}, fmt.Errorf("add %s history val [%x]=>[%x]: %w", h.filenameBase, k, val, err)
				}
			}
			historyCount++
		}
	}
	closeComp = false
	return HistoryCollation{
		historyPath:  historyPath,
		historyComp:  historyComp,
		historyCount: historyCount,
		indexBitmaps: indexBitmaps,
	}, nil
}

type HistoryFiles struct {
	historyDecomp   *compress.Decompressor
	historyIdx      *recsplit.Index
	efHistoryDecomp *compress.Decompressor
	efHistoryIdx    *recsplit.Index
}

func (sf HistoryFiles) Close() {
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
func (h *History) reCalcRoFiles() {
	roFiles := ctxFiles(h.files)
	h.roFiles.Store(&roFiles)
}

// buildFiles performs potentially resource intensive operations of creating
// static files and their indices
func (h *History) buildFiles(ctx context.Context, step uint64, collation HistoryCollation, ps *background.ProgressSet) (HistoryFiles, error) {
	historyComp := collation.historyComp
	if h.noFsync {
		historyComp.DisableFsync()
	}
	var historyDecomp, efHistoryDecomp *compress.Decompressor
	var historyIdx, efHistoryIdx *recsplit.Index
	var efHistoryComp *compress.Compressor
	var rs *recsplit.RecSplit
	closeComp := true
	defer func() {
		if closeComp {
			if historyComp != nil {
				historyComp.Close()
			}
			if historyDecomp != nil {
				historyDecomp.Close()
			}
			if historyIdx != nil {
				historyIdx.Close()
			}
			if efHistoryComp != nil {
				efHistoryComp.Close()
			}
			if efHistoryDecomp != nil {
				efHistoryDecomp.Close()
			}
			if efHistoryIdx != nil {
				efHistoryIdx.Close()
			}
			if rs != nil {
				rs.Close()
			}
		}
	}()

	var historyIdxPath, efHistoryPath string

	{
		historyIdxFileName := fmt.Sprintf("%s.%d-%d.vi", h.filenameBase, step, step+1)
		p := ps.AddNew(historyIdxFileName, 1)
		defer ps.Delete(p)
		historyIdxPath = filepath.Join(h.dir, historyIdxFileName)
		if err := historyComp.Compress(); err != nil {
			return HistoryFiles{}, fmt.Errorf("compress %s history: %w", h.filenameBase, err)
		}
		historyComp.Close()
		historyComp = nil
		ps.Delete(p)
	}

	keys := make([]string, 0, len(collation.indexBitmaps))
	for key := range collation.indexBitmaps {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	{
		var err error
		if historyDecomp, err = compress.NewDecompressor(collation.historyPath); err != nil {
			return HistoryFiles{}, fmt.Errorf("open %s history decompressor: %w", h.filenameBase, err)
		}

		// Build history ef
		efHistoryFileName := fmt.Sprintf("%s.%d-%d.ef", h.filenameBase, step, step+1)

		p := ps.AddNew(efHistoryFileName, 1)
		defer ps.Delete(p)
		efHistoryPath = filepath.Join(h.dir, efHistoryFileName)
		efHistoryComp, err = compress.NewCompressor(ctx, "ef history", efHistoryPath, h.tmpdir, compress.MinPatternScore, h.compressWorkers, log.LvlTrace, h.logger)
		if err != nil {
			return HistoryFiles{}, fmt.Errorf("create %s ef history compressor: %w", h.filenameBase, err)
		}
		if h.noFsync {
			efHistoryComp.DisableFsync()
		}
		var buf []byte
		for _, key := range keys {
			if err = efHistoryComp.AddUncompressedWord([]byte(key)); err != nil {
				return HistoryFiles{}, fmt.Errorf("add %s ef history key [%x]: %w", h.InvertedIndex.filenameBase, key, err)
			}
			bitmap := collation.indexBitmaps[key]
			ef := eliasfano32.NewEliasFano(bitmap.GetCardinality(), bitmap.Maximum())
			it := bitmap.Iterator()
			for it.HasNext() {
				txNum := it.Next()
				ef.AddOffset(txNum)
			}
			ef.Build()
			buf = ef.AppendBytes(buf[:0])
			if err = efHistoryComp.AddUncompressedWord(buf); err != nil {
				return HistoryFiles{}, fmt.Errorf("add %s ef history val: %w", h.filenameBase, err)
			}
		}
		if err = efHistoryComp.Compress(); err != nil {
			return HistoryFiles{}, fmt.Errorf("compress %s ef history: %w", h.filenameBase, err)
		}
		efHistoryComp.Close()
		efHistoryComp = nil
		ps.Delete(p)
	}

	var err error
	if efHistoryDecomp, err = compress.NewDecompressor(efHistoryPath); err != nil {
		return HistoryFiles{}, fmt.Errorf("open %s ef history decompressor: %w", h.filenameBase, err)
	}
	efHistoryIdxFileName := fmt.Sprintf("%s.%d-%d.efi", h.filenameBase, step, step+1)
	efHistoryIdxPath := filepath.Join(h.dir, efHistoryIdxFileName)
	p := ps.AddNew(efHistoryIdxFileName, uint64(len(keys)*2))
	defer ps.Delete(p)
	if efHistoryIdx, err = buildIndexThenOpen(ctx, efHistoryDecomp, efHistoryIdxPath, h.tmpdir, len(keys), false /* values */, p, h.logger, h.noFsync); err != nil {
		return HistoryFiles{}, fmt.Errorf("build %s ef history idx: %w", h.filenameBase, err)
	}
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   collation.historyCount,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     h.tmpdir,
		IndexFile:  historyIdxPath,
	}, h.logger); err != nil {
		return HistoryFiles{}, fmt.Errorf("create recsplit: %w", err)
	}
	rs.LogLvl(log.LvlTrace)
	if h.noFsync {
		rs.DisableFsync()
	}
	var historyKey []byte
	var txKey [8]byte
	var valOffset uint64
	g := historyDecomp.MakeGetter()
	for {
		g.Reset(0)
		valOffset = 0
		for _, key := range keys {
			bitmap := collation.indexBitmaps[key]
			it := bitmap.Iterator()
			for it.HasNext() {
				txNum := it.Next()
				binary.BigEndian.PutUint64(txKey[:], txNum)
				historyKey = append(append(historyKey[:0], txKey[:]...), key...)
				if err = rs.AddKey(historyKey, valOffset); err != nil {
					return HistoryFiles{}, fmt.Errorf("add %s history idx [%x]: %w", h.filenameBase, historyKey, err)
				}
				valOffset, _ = g.Skip()
			}
		}
		if err = rs.Build(ctx); err != nil {
			if rs.Collision() {
				log.Info("Building recsplit. Collision happened. It's ok. Restarting...")
				rs.ResetNextSalt()
			} else {
				return HistoryFiles{}, fmt.Errorf("build idx: %w", err)
			}
		} else {
			break
		}
	}
	rs.Close()
	rs = nil
	if historyIdx, err = recsplit.OpenIndex(historyIdxPath); err != nil {
		return HistoryFiles{}, fmt.Errorf("open idx: %w", err)
	}
	closeComp = false
	return HistoryFiles{
		historyDecomp:   historyDecomp,
		historyIdx:      historyIdx,
		efHistoryDecomp: efHistoryDecomp,
		efHistoryIdx:    efHistoryIdx,
	}, nil
}

func (h *History) integrateFiles(sf HistoryFiles, txNumFrom, txNumTo uint64) {
	h.InvertedIndex.integrateFiles(InvertedFiles{
		decomp: sf.efHistoryDecomp,
		index:  sf.efHistoryIdx,
	}, txNumFrom, txNumTo)

	fi := newFilesItem(txNumFrom, txNumTo, h.aggregationStep)
	fi.decompressor = sf.historyDecomp
	fi.index = sf.historyIdx
	h.files.Set(fi)

	h.reCalcRoFiles()
}

func (h *History) warmup(ctx context.Context, txFrom, limit uint64, tx kv.Tx) error {
	historyKeysCursor, err := tx.CursorDupSort(h.indexKeysTable)
	if err != nil {
		return fmt.Errorf("create %s history cursor: %w", h.filenameBase, err)
	}
	defer historyKeysCursor.Close()
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	valsC, err := tx.Cursor(h.historyValsTable)
	if err != nil {
		return err
	}
	defer valsC.Close()
	k, v, err := historyKeysCursor.Seek(txKey[:])
	if err != nil {
		return err
	}
	if k == nil {
		return nil
	}
	txFrom = binary.BigEndian.Uint64(k)
	txTo := txFrom + h.aggregationStep
	if limit != math.MaxUint64 && limit != 0 {
		txTo = txFrom + limit
	}
	keyBuf := make([]byte, 256)
	for ; err == nil && k != nil; k, v, err = historyKeysCursor.Next() {
		if err != nil {
			return err
		}
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		copy(keyBuf, v)
		binary.BigEndian.PutUint64(keyBuf[len(v):], txNum)
		_, _, _ = valsC.Seek(keyBuf)

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if err != nil {
		return fmt.Errorf("iterate over %s history keys: %w", h.filenameBase, err)
	}

	return nil
}

func (h *History) isEmpty(tx kv.Tx) (bool, error) {
	if h.largeValues {
		k, err := kv.FirstKey(tx, h.historyValsTable)
		if err != nil {
			return false, err
		}
		k2, err := kv.FirstKey(tx, h.indexKeysTable)
		if err != nil {
			return false, err
		}
		return k == nil && k2 == nil, nil
	}
	k, err := kv.FirstKey(tx, h.historyValsTable)
	if err != nil {
		return false, err
	}
	k2, err := kv.FirstKey(tx, h.indexKeysTable)
	if err != nil {
		return false, err
	}
	return k == nil && k2 == nil, nil
}

func (h *History) prune(ctx context.Context, txFrom, txTo, limit uint64, logEvery *time.Ticker) error {
	historyKeysCursorForDeletes, err := h.tx.RwCursorDupSort(h.indexKeysTable)
	if err != nil {
		return fmt.Errorf("create %s history cursor: %w", h.filenameBase, err)
	}
	defer historyKeysCursorForDeletes.Close()
	historyKeysCursor, err := h.tx.RwCursorDupSort(h.indexKeysTable)
	if err != nil {
		return fmt.Errorf("create %s history cursor: %w", h.filenameBase, err)
	}
	defer historyKeysCursor.Close()
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	var k, v []byte
	var valsC kv.RwCursor
	var valsCDup kv.RwCursorDupSort
	if h.largeValues {
		valsC, err = h.tx.RwCursor(h.historyValsTable)
		if err != nil {
			return err
		}
		defer valsC.Close()
	} else {
		valsCDup, err = h.tx.RwCursorDupSort(h.historyValsTable)
		if err != nil {
			return err
		}
		defer valsCDup.Close()
	}
	for k, v, err = historyKeysCursor.Seek(txKey[:]); err == nil && k != nil; k, v, err = historyKeysCursor.Next() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		if limit == 0 {
			return nil
		}
		limit--

		if h.largeValues {
			seek := append(common.Copy(v), k...)
			if err := valsC.Delete(seek); err != nil {
				return err
			}
		} else {
			vv, err := valsCDup.SeekBothRange(v, k)
			if err != nil {
				return err
			}
			if binary.BigEndian.Uint64(vv) != txNum {
				continue
			}
			if err = valsCDup.DeleteCurrent(); err != nil {
				return err
			}
		}

		// This DeleteCurrent needs to the last in the loop iteration, because it invalidates k and v
		if _, _, err = historyKeysCursorForDeletes.SeekBothExact(k, v); err != nil {
			return err
		}
		if err = historyKeysCursorForDeletes.DeleteCurrent(); err != nil {
			return err
		}
	}
	return nil
}

type HistoryContext struct {
	h  *History
	ic *InvertedIndexContext

	files   []ctxItem // have no garbage (canDelete=true, overlaps, etc...)
	getters []*compress.Getter
	readers []*recsplit.IndexReader

	trace bool
}

func (h *History) MakeContext() *HistoryContext {

	var hc = HistoryContext{
		h:     h,
		ic:    h.InvertedIndex.MakeContext(),
		files: *h.roFiles.Load(),

		trace: false,
	}
	for _, item := range hc.files {
		if !item.src.frozen {
			item.src.refcount.Add(1)
		}
	}

	return &hc
}

func (hc *HistoryContext) statelessGetter(i int) *compress.Getter {
	if hc.getters == nil {
		hc.getters = make([]*compress.Getter, len(hc.files))
	}
	r := hc.getters[i]
	if r == nil {
		r = hc.files[i].src.decompressor.MakeGetter()
		hc.getters[i] = r
	}
	return r
}
func (hc *HistoryContext) statelessIdxReader(i int) *recsplit.IndexReader {
	if hc.readers == nil {
		hc.readers = make([]*recsplit.IndexReader, len(hc.files))
	}
	r := hc.readers[i]
	if r == nil {
		r = hc.files[i].src.index.GetReaderFromPool()
		hc.readers[i] = r
	}
	return r
}

func (hc *HistoryContext) Close() {
	hc.ic.Close()
	for _, item := range hc.files {
		if item.src.frozen {
			continue
		}
		refCnt := item.src.refcount.Add(-1)
		//if hc.h.filenameBase == "accounts" && item.src.canDelete.Load() {
		//	log.Warn("[history] HistoryContext.Close: check file to remove", "refCnt", refCnt, "name", item.src.decompressor.FileName())
		//}
		//GC: last reader responsible to remove useles files: close it and delete
		if refCnt == 0 && item.src.canDelete.Load() {
			item.src.closeFilesAndRemove()
		}
	}
	for _, r := range hc.readers {
		r.Close()
	}

}

func (hc *HistoryContext) getFile(from, to uint64) (it ctxItem, ok bool) {
	for _, item := range hc.files {
		if item.startTxNum == from && item.endTxNum == to {
			return item, true
		}
	}
	return it, false
}

func (hc *HistoryContext) GetNoState(key []byte, txNum uint64) ([]byte, bool, error) {
	exactStep1, exactStep2, lastIndexedTxNum, foundExactShard1, foundExactShard2 := hc.h.localityIndex.lookupIdxFiles(hc.ic.loc, key, txNum)

	//fmt.Printf("GetNoState [%x] %d\n", key, txNum)
	var foundTxNum uint64
	var foundEndTxNum uint64
	var foundStartTxNum uint64
	var found bool
	var findInFile = func(item ctxItem) bool {
		reader := hc.ic.statelessIdxReader(item.i)
		if reader.Empty() {
			return true
		}
		offset := reader.Lookup(key)
		g := hc.ic.statelessGetter(item.i)
		g.Reset(offset)
		k, _ := g.NextUncompressed()

		if !bytes.Equal(k, key) {
			//if bytes.Equal(key, hex.MustDecodeString("009ba32869045058a3f05d6f3dd2abb967e338f6")) {
			//	fmt.Printf("not in this shard: %x, %d, %d-%d\n", k, txNum, item.startTxNum/hc.h.aggregationStep, item.endTxNum/hc.h.aggregationStep)
			//}
			return true
		}
		eliasVal, _ := g.NextUncompressed()
		ef, _ := eliasfano32.ReadEliasFano(eliasVal)
		n, ok := ef.Search(txNum)
		if hc.trace {
			n2, _ := ef.Search(n + 1)
			n3, _ := ef.Search(n - 1)
			fmt.Printf("hist: files: %s %d<-%d->%d->%d, %x\n", hc.h.filenameBase, n3, txNum, n, n2, key)
		}
		if ok {
			foundTxNum = n
			foundEndTxNum = item.endTxNum
			foundStartTxNum = item.startTxNum
			found = true
			return false
		}
		return true
	}

	// -- LocaliyIndex opimization --
	// check up to 2 exact files
	if foundExactShard1 {
		from, to := exactStep1*hc.h.aggregationStep, (exactStep1+StepsInBiggestFile)*hc.h.aggregationStep
		item, ok := hc.ic.getFile(from, to)
		if ok {
			findInFile(item)
		}
		//for _, item := range hc.invIndexFiles {
		//	if item.startTxNum == from && item.endTxNum == to {
		//		findInFile(item)
		//	}
		//}
		//exactShard1, ok := hc.invIndexFiles.Get(ctxItem{startTxNum: exactStep1 * hc.h.aggregationStep, endTxNum: (exactStep1 + StepsInBiggestFile) * hc.h.aggregationStep})
		//if ok {
		//	findInFile(exactShard1)
		//}
	}
	if !found && foundExactShard2 {
		from, to := exactStep2*hc.h.aggregationStep, (exactStep2+StepsInBiggestFile)*hc.h.aggregationStep
		item, ok := hc.ic.getFile(from, to)
		if ok {
			findInFile(item)
		}
		//exactShard2, ok := hc.invIndexFiles.Get(ctxItem{startTxNum: exactStep2 * hc.h.aggregationStep, endTxNum: (exactStep2 + StepsInBiggestFile) * hc.h.aggregationStep})
		//if ok {
		//	findInFile(exactShard2)
		//}
	}
	// otherwise search in recent non-fully-merged files (they are out of LocalityIndex scope)
	// searchFrom - variable already set for this
	// if there is no LocaliyIndex available
	// -- LocaliyIndex opimization End --

	if !found {
		for _, item := range hc.ic.files {
			if item.endTxNum <= lastIndexedTxNum {
				continue
			}
			if !findInFile(item) {
				break
			}
		}
		//hc.invIndexFiles.AscendGreaterOrEqual(ctxItem{startTxNum: lastIndexedTxNum, endTxNum: lastIndexedTxNum}, findInFile)
	}

	if found {
		historyItem, ok := hc.getFile(foundStartTxNum, foundEndTxNum)
		if !ok {
			return nil, false, fmt.Errorf("hist file not found: key=%x, %s.%d-%d", key, hc.h.filenameBase, foundStartTxNum/hc.h.aggregationStep, foundEndTxNum/hc.h.aggregationStep)
		}
		var txKey [8]byte
		binary.BigEndian.PutUint64(txKey[:], foundTxNum)
		reader := hc.statelessIdxReader(historyItem.i)
		offset := reader.Lookup2(txKey[:], key)
		//fmt.Printf("offset = %d, txKey=[%x], key=[%x]\n", offset, txKey[:], key)
		g := hc.statelessGetter(historyItem.i)
		g.Reset(offset)
		if hc.h.compressVals {
			v, _ := g.Next(nil)
			return v, true, nil
		}
		v, _ := g.NextUncompressed()
		return v, true, nil
	}
	return nil, false, nil
}

func (hs *HistoryStep) GetNoState(key []byte, txNum uint64) ([]byte, bool, uint64) {
	//fmt.Printf("GetNoState [%x] %d\n", key, txNum)
	if hs.indexFile.reader.Empty() {
		return nil, false, txNum
	}
	offset := hs.indexFile.reader.Lookup(key)
	g := hs.indexFile.getter
	g.Reset(offset)
	k, _ := g.NextUncompressed()
	if !bytes.Equal(k, key) {
		return nil, false, txNum
	}
	//fmt.Printf("Found key=%x\n", k)
	eliasVal, _ := g.NextUncompressed()
	ef, _ := eliasfano32.ReadEliasFano(eliasVal)
	n, ok := ef.Search(txNum)
	if !ok {
		return nil, false, ef.Max()
	}
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], n)
	offset = hs.historyFile.reader.Lookup2(txKey[:], key)
	//fmt.Printf("offset = %d, txKey=[%x], key=[%x]\n", offset, txKey[:], key)
	g = hs.historyFile.getter
	g.Reset(offset)
	if hs.compressVals {
		v, _ := g.Next(nil)
		return v, true, txNum
	}
	v, _ := g.NextUncompressed()
	return v, true, txNum
}

func (hs *HistoryStep) MaxTxNum(key []byte) (bool, uint64) {
	if hs.indexFile.reader.Empty() {
		return false, 0
	}
	offset := hs.indexFile.reader.Lookup(key)
	g := hs.indexFile.getter
	g.Reset(offset)
	k, _ := g.NextUncompressed()
	if !bytes.Equal(k, key) {
		return false, 0
	}
	//fmt.Printf("Found key=%x\n", k)
	eliasVal, _ := g.NextUncompressed()
	return true, eliasfano32.Max(eliasVal)
}

// GetNoStateWithRecent searches history for a value of specified key before txNum
// second return value is true if the value is found in the history (even if it is nil)
func (hc *HistoryContext) GetNoStateWithRecent(key []byte, txNum uint64, roTx kv.Tx) ([]byte, bool, error) {
	v, ok, err := hc.GetNoState(key, txNum)
	if err != nil {
		return nil, ok, err
	}
	if ok {
		return v, true, nil
	}

	// Value not found in history files, look in the recent history
	if roTx == nil {
		return nil, false, fmt.Errorf("roTx is nil")
	}
	return hc.getNoStateFromDB(key, txNum, roTx)
}

func (hc *HistoryContext) getNoStateFromDB(key []byte, txNum uint64, tx kv.Tx) ([]byte, bool, error) {
	if hc.h.largeValues {
		c, err := tx.Cursor(hc.h.historyValsTable)
		if err != nil {
			return nil, false, err
		}
		defer c.Close()
		seek := make([]byte, len(key)+8)
		copy(seek, key)
		binary.BigEndian.PutUint64(seek[len(key):], txNum)
		kAndTxNum, val, err := c.Seek(seek)
		if err != nil {
			return nil, false, err
		}
		if kAndTxNum == nil || !bytes.Equal(kAndTxNum[:len(kAndTxNum)-8], key) {
			return nil, false, nil
		}
		// val == []byte{},m eans key was created in this txNum and doesn't exists before.
		return val, true, nil
	}
	c, err := tx.CursorDupSort(hc.h.historyValsTable)
	if err != nil {
		return nil, false, err
	}
	defer c.Close()
	seek := make([]byte, len(key)+8)
	copy(seek, key)
	binary.BigEndian.PutUint64(seek[len(key):], txNum)
	val, err := c.SeekBothRange(key, seek[len(key):])
	if err != nil {
		return nil, false, err
	}
	if val == nil {
		return nil, false, nil
	}
	// `val == []byte{}` means key was created in this txNum and doesn't exists before.
	return val[8:], true, nil
}

func (hc *HistoryContext) WalkAsOf(startTxNum uint64, from, to []byte, roTx kv.Tx, limit int) iter.KV {
	hi := &StateAsOfIterF{
		from: from, to: to, limit: limit,

		hc:           hc,
		compressVals: hc.h.compressVals,
		startTxNum:   startTxNum,
	}
	for _, item := range hc.ic.files {
		if item.endTxNum <= startTxNum {
			continue
		}
		// TODO: seek(from)
		g := item.src.decompressor.MakeGetter()
		g.Reset(0)
		if g.HasNext() {
			key, offset := g.NextUncompressed()
			heap.Push(&hi.h, &ReconItem{g: g, key: key, startTxNum: item.startTxNum, endTxNum: item.endTxNum, txNum: item.endTxNum, startOffset: offset, lastOffset: offset})
		}
	}
	binary.BigEndian.PutUint64(hi.startTxKey[:], startTxNum)
	if err := hi.advanceInFiles(); err != nil {
		panic(err)
	}

	dbit := &StateAsOfIterDB{
		largeValues: hc.h.largeValues,
		roTx:        roTx,
		valsTable:   hc.h.historyValsTable,
		from:        from, to: to, limit: limit,

		startTxNum: startTxNum,
	}
	binary.BigEndian.PutUint64(dbit.startTxKey[:], startTxNum)
	if err := dbit.advance(); err != nil {
		panic(err)
	}
	return iter.UnionKV(hi, dbit, limit)
}

// StateAsOfIter - returns state range at given time in history
type StateAsOfIterF struct {
	hc    *HistoryContext
	limit int

	from, to []byte
	nextVal  []byte
	nextKey  []byte

	h            ReconHeap
	startTxNum   uint64
	startTxKey   [8]byte
	txnKey       [8]byte
	compressVals bool

	k, v, kBackup, vBackup []byte
}

func (hi *StateAsOfIterF) Close() {
}

func (hi *StateAsOfIterF) advanceInFiles() error {
	for hi.h.Len() > 0 {
		top := heap.Pop(&hi.h).(*ReconItem)
		key := top.key
		var idxVal []byte
		if hi.compressVals {
			idxVal, _ = top.g.Next(nil)
		} else {
			idxVal, _ = top.g.NextUncompressed()
		}
		if top.g.HasNext() {
			if hi.compressVals {
				top.key, _ = top.g.Next(nil)
			} else {
				top.key, _ = top.g.NextUncompressed()
			}
			if hi.to == nil || bytes.Compare(top.key, hi.to) < 0 {
				heap.Push(&hi.h, top)
			}
		}

		if hi.from != nil && bytes.Compare(key, hi.from) < 0 { //TODO: replace by Seek()
			continue
		}

		if bytes.Equal(key, hi.nextKey) {
			continue
		}
		ef, _ := eliasfano32.ReadEliasFano(idxVal)
		n, ok := ef.Search(hi.startTxNum)
		if !ok {
			continue
		}

		hi.nextKey = key
		binary.BigEndian.PutUint64(hi.txnKey[:], n)
		historyItem, ok := hi.hc.getFile(top.startTxNum, top.endTxNum)
		if !ok {
			return fmt.Errorf("no %s file found for [%x]", hi.hc.h.filenameBase, hi.nextKey)
		}
		reader := hi.hc.statelessIdxReader(historyItem.i)
		offset := reader.Lookup2(hi.txnKey[:], hi.nextKey)
		g := hi.hc.statelessGetter(historyItem.i)
		g.Reset(offset)
		if hi.compressVals {
			hi.nextVal, _ = g.Next(nil)
		} else {
			hi.nextVal, _ = g.NextUncompressed()
		}
		return nil
	}
	hi.nextKey = nil
	return nil
}

func (hi *StateAsOfIterF) HasNext() bool {
	return hi.limit != 0 && hi.nextKey != nil
}

func (hi *StateAsOfIterF) Next() ([]byte, []byte, error) {
	hi.limit--
	hi.k, hi.v = append(hi.k[:0], hi.nextKey...), append(hi.v[:0], hi.nextVal...)

	// Satisfy iter.Dual Invariant 2
	hi.k, hi.kBackup, hi.v, hi.vBackup = hi.kBackup, hi.k, hi.vBackup, hi.v
	if err := hi.advanceInFiles(); err != nil {
		return nil, nil, err
	}
	return hi.kBackup, hi.vBackup, nil
}

// StateAsOfIterDB - returns state range at given time in history
type StateAsOfIterDB struct {
	largeValues bool
	roTx        kv.Tx
	valsC       kv.Cursor
	valsCDup    kv.CursorDupSort
	valsTable   string

	from, to []byte
	limit    int

	nextKey, nextVal []byte

	startTxNum uint64
	startTxKey [8]byte

	k, v, kBackup, vBackup []byte
	err                    error
}

func (hi *StateAsOfIterDB) Close() {
	if hi.valsC != nil {
		hi.valsC.Close()
	}
}

func (hi *StateAsOfIterDB) advance() (err error) {
	// not large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2 -> txNum + value (DupSort)
	// large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2+txNum -> value (not DupSort)
	if hi.largeValues {
		return hi.advanceLargeVals()
	}
	return hi.advanceSmallVals()
}
func (hi *StateAsOfIterDB) advanceLargeVals() error {
	var seek []byte
	var err error
	if hi.valsC == nil {
		if hi.valsC, err = hi.roTx.Cursor(hi.valsTable); err != nil {
			return err
		}
		firstKey, _, err := hi.valsC.Seek(hi.from)
		if err != nil {
			return err
		}
		if firstKey == nil {
			hi.nextKey = nil
			return nil
		}
		seek = append(common.Copy(firstKey[:len(firstKey)-8]), hi.startTxKey[:]...)
	} else {
		next, ok := kv.NextSubtree(hi.nextKey)
		if !ok {
			hi.nextKey = nil
			return nil
		}

		seek = append(next, hi.startTxKey[:]...)
	}
	for k, v, err := hi.valsC.Seek(seek); k != nil; k, v, err = hi.valsC.Seek(seek) {
		if err != nil {
			return err
		}
		if hi.to != nil && bytes.Compare(k[:len(k)-8], hi.to) >= 0 {
			break
		}
		if !bytes.Equal(seek[:len(k)-8], k[:len(k)-8]) {
			copy(seek[:len(k)-8], k[:len(k)-8])
			continue
		}
		hi.nextKey = k[:len(k)-8]
		hi.nextVal = v
		return nil
	}
	hi.nextKey = nil
	return nil
}
func (hi *StateAsOfIterDB) advanceSmallVals() error {
	var seek []byte
	var err error
	if hi.valsCDup == nil {
		if hi.valsCDup, err = hi.roTx.CursorDupSort(hi.valsTable); err != nil {
			return err
		}
		seek = hi.from
	} else {
		next, ok := kv.NextSubtree(hi.nextKey)
		if !ok {
			hi.nextKey = nil
			return nil
		}
		seek = next
	}
	for k, _, err := hi.valsCDup.Seek(seek); k != nil; k, _, err = hi.valsCDup.NextNoDup() {
		if err != nil {
			return err
		}
		if hi.to != nil && bytes.Compare(k, hi.to) >= 0 {
			break
		}
		v, err := hi.valsCDup.SeekBothRange(k, hi.startTxKey[:])
		if err != nil {
			return err
		}
		if v == nil {
			continue
		}
		hi.nextKey = k
		hi.nextVal = v[8:]
		return nil
	}
	hi.nextKey = nil
	return nil
}

func (hi *StateAsOfIterDB) HasNext() bool {
	if hi.err != nil {
		return true
	}
	return hi.limit != 0 && hi.nextKey != nil
}

func (hi *StateAsOfIterDB) Next() ([]byte, []byte, error) {
	if hi.err != nil {
		return nil, nil, hi.err
	}
	hi.limit--
	hi.k, hi.v = hi.nextKey, hi.nextVal

	// Satisfy iter.Dual Invariant 2
	hi.k, hi.kBackup, hi.v, hi.vBackup = hi.kBackup, hi.k, hi.vBackup, hi.v
	if err := hi.advance(); err != nil {
		return nil, nil, err
	}
	return hi.kBackup, hi.vBackup, nil
}

func (hc *HistoryContext) iterateChangedFrozen(fromTxNum, toTxNum int, asc order.By, limit int) (iter.KV, error) {
	if asc == false {
		panic("not supported yet")
	}
	if len(hc.ic.files) == 0 {
		return iter.EmptyKV, nil
	}

	if fromTxNum >= 0 && hc.ic.files[len(hc.ic.files)-1].endTxNum <= uint64(fromTxNum) {
		return iter.EmptyKV, nil
	}

	hi := &HistoryChangesIterFiles{
		hc:           hc,
		compressVals: hc.h.compressVals,
		startTxNum:   cmp.Max(0, uint64(fromTxNum)),
		endTxNum:     toTxNum,
		limit:        limit,
	}
	if fromTxNum >= 0 {
		binary.BigEndian.PutUint64(hi.startTxKey[:], uint64(fromTxNum))
	}
	for _, item := range hc.ic.files {
		if fromTxNum >= 0 && item.endTxNum <= uint64(fromTxNum) {
			continue
		}
		if toTxNum >= 0 && item.startTxNum >= uint64(toTxNum) {
			break
		}
		g := item.src.decompressor.MakeGetter()
		g.Reset(0)
		if g.HasNext() {
			key, offset := g.NextUncompressed()
			heap.Push(&hi.h, &ReconItem{g: g, key: key, startTxNum: item.startTxNum, endTxNum: item.endTxNum, txNum: item.endTxNum, startOffset: offset, lastOffset: offset})
		}
	}
	if err := hi.advance(); err != nil {
		return nil, err
	}
	return hi, nil
}

func (hc *HistoryContext) iterateChangedRecent(fromTxNum, toTxNum int, asc order.By, limit int, roTx kv.Tx) (iter.KV, error) {
	if asc == order.Desc {
		panic("not supported yet")
	}
	rangeIsInFiles := toTxNum >= 0 && len(hc.ic.files) > 0 && hc.ic.files[len(hc.ic.files)-1].endTxNum >= uint64(toTxNum)
	if rangeIsInFiles {
		return iter.EmptyKV, nil
	}
	dbi := &HistoryChangesIterDB{
		endTxNum:    toTxNum,
		roTx:        roTx,
		largeValues: hc.h.largeValues,
		valsTable:   hc.h.historyValsTable,
		limit:       limit,
	}
	if fromTxNum >= 0 {
		binary.BigEndian.PutUint64(dbi.startTxKey[:], uint64(fromTxNum))
	}
	if err := dbi.advance(); err != nil {
		return nil, err
	}
	return dbi, nil
}

func (hc *HistoryContext) HistoryRange(fromTxNum, toTxNum int, asc order.By, limit int, roTx kv.Tx) (iter.KV, error) {
	if asc == order.Desc {
		panic("not supported yet")
	}
	itOnFiles, err := hc.iterateChangedFrozen(fromTxNum, toTxNum, asc, limit)
	if err != nil {
		return nil, err
	}
	itOnDB, err := hc.iterateChangedRecent(fromTxNum, toTxNum, asc, limit, roTx)
	if err != nil {
		return nil, err
	}

	return iter.UnionKV(itOnFiles, itOnDB, limit), nil
}

type HistoryChangesIterFiles struct {
	hc           *HistoryContext
	nextVal      []byte
	nextKey      []byte
	h            ReconHeap
	startTxNum   uint64
	endTxNum     int
	startTxKey   [8]byte
	txnKey       [8]byte
	compressVals bool

	k, v, kBackup, vBackup []byte
	err                    error
	limit                  int
}

func (hi *HistoryChangesIterFiles) Close() {
}

func (hi *HistoryChangesIterFiles) advance() error {
	for hi.h.Len() > 0 {
		top := heap.Pop(&hi.h).(*ReconItem)
		key := top.key
		var idxVal []byte
		if hi.compressVals {
			idxVal, _ = top.g.Next(nil)
		} else {
			idxVal, _ = top.g.NextUncompressed()
		}
		if top.g.HasNext() {
			if hi.compressVals {
				top.key, _ = top.g.Next(nil)
			} else {
				top.key, _ = top.g.NextUncompressed()
			}
			heap.Push(&hi.h, top)
		}

		if bytes.Equal(key, hi.nextKey) {
			continue
		}
		ef, _ := eliasfano32.ReadEliasFano(idxVal)
		n, ok := ef.Search(hi.startTxNum) //TODO: if startTxNum==0, can do ef.Get(0)
		if !ok {
			continue
		}
		if int(n) >= hi.endTxNum {
			continue
		}

		hi.nextKey = key
		binary.BigEndian.PutUint64(hi.txnKey[:], n)
		historyItem, ok := hi.hc.getFile(top.startTxNum, top.endTxNum)
		if !ok {
			return fmt.Errorf("HistoryChangesIterFiles: no %s file found for [%x]", hi.hc.h.filenameBase, hi.nextKey)
		}
		reader := hi.hc.statelessIdxReader(historyItem.i)
		offset := reader.Lookup2(hi.txnKey[:], hi.nextKey)
		g := hi.hc.statelessGetter(historyItem.i)
		g.Reset(offset)
		if hi.compressVals {
			hi.nextVal, _ = g.Next(nil)
		} else {
			hi.nextVal, _ = g.NextUncompressed()
		}
		return nil
	}
	hi.nextKey = nil
	return nil
}

func (hi *HistoryChangesIterFiles) HasNext() bool {
	if hi.err != nil { // always true, then .Next() call will return this error
		return true
	}
	if hi.limit == 0 { // limit reached
		return false
	}
	if hi.nextKey == nil { // EndOfTable
		return false
	}
	return true
	//if hi.toPrefix == nil { // s.nextK == nil check is above
	//	return true
	//}
}

func (hi *HistoryChangesIterFiles) Next() ([]byte, []byte, error) {
	if hi.err != nil {
		return nil, nil, hi.err
	}
	hi.limit--
	hi.k, hi.v = append(hi.k[:0], hi.nextKey...), append(hi.v[:0], hi.nextVal...)

	// Satisfy iter.Dual Invariant 2
	hi.k, hi.kBackup, hi.v, hi.vBackup = hi.kBackup, hi.k, hi.vBackup, hi.v
	if err := hi.advance(); err != nil {
		return nil, nil, err
	}
	return hi.kBackup, hi.vBackup, nil
}

type HistoryChangesIterDB struct {
	largeValues     bool
	roTx            kv.Tx
	valsC           kv.Cursor
	valsCDup        kv.CursorDupSort
	valsTable       string
	limit, endTxNum int
	startTxKey      [8]byte

	nextKey, nextVal []byte
	k, v             []byte
	err              error
}

func (hi *HistoryChangesIterDB) Close() {
	if hi.valsC != nil {
		hi.valsC.Close()
	}
	if hi.valsCDup != nil {
		hi.valsCDup.Close()
	}
}
func (hi *HistoryChangesIterDB) advance() (err error) {
	// not large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2 -> txNum + value (DupSort)
	// large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2+txNum -> value (not DupSort)
	if hi.largeValues {
		return hi.advanceLargeVals()
	}
	return hi.advanceSmallVals()
}
func (hi *HistoryChangesIterDB) advanceLargeVals() error {
	var seek []byte
	var err error
	if hi.valsC == nil {
		if hi.valsC, err = hi.roTx.Cursor(hi.valsTable); err != nil {
			return err
		}
		firstKey, _, err := hi.valsC.First()
		if err != nil {
			return err
		}
		if firstKey == nil {
			hi.nextKey = nil
			return nil
		}
		seek = append(common.Copy(firstKey[:len(firstKey)-8]), hi.startTxKey[:]...)
	} else {
		next, ok := kv.NextSubtree(hi.nextKey)
		if !ok {
			hi.nextKey = nil
			return nil
		}

		seek = append(next, hi.startTxKey[:]...)
	}
	for k, v, err := hi.valsC.Seek(seek); k != nil; k, v, err = hi.valsC.Seek(seek) {
		if err != nil {
			return err
		}
		if hi.endTxNum >= 0 && int(binary.BigEndian.Uint64(k[len(k)-8:])) >= hi.endTxNum {
			next, ok := kv.NextSubtree(k[:len(k)-8])
			if !ok {
				hi.nextKey = nil
				return nil
			}
			seek = append(next, hi.startTxKey[:]...)
			continue
		}
		if !bytes.Equal(seek[:len(k)-8], k[:len(k)-8]) {
			copy(seek[:len(k)-8], k[:len(k)-8])
			continue
		}
		hi.nextKey = k[:len(k)-8]
		hi.nextVal = v
		return nil
	}
	hi.nextKey = nil
	return nil
}
func (hi *HistoryChangesIterDB) advanceSmallVals() (err error) {
	var k []byte
	if hi.valsCDup == nil {
		if hi.valsCDup, err = hi.roTx.CursorDupSort(hi.valsTable); err != nil {
			return err
		}

		if k, _, err = hi.valsCDup.First(); err != nil {
			return err
		}
	} else {
		if k, _, err = hi.valsCDup.NextNoDup(); err != nil {
			return err
		}
	}
	for ; k != nil; k, _, err = hi.valsCDup.NextNoDup() {
		if err != nil {
			return err
		}
		v, err := hi.valsCDup.SeekBothRange(k, hi.startTxKey[:])
		if err != nil {
			return err
		}
		if v == nil {
			continue
		}
		foundTxNumVal := v[:8]
		if hi.endTxNum >= 0 && int(binary.BigEndian.Uint64(foundTxNumVal)) >= hi.endTxNum {
			continue
		}
		hi.nextKey = k
		hi.nextVal = v[8:]
		return nil
	}
	hi.nextKey = nil
	return nil
}

func (hi *HistoryChangesIterDB) HasNext() bool {
	if hi.err != nil { // always true, then .Next() call will return this error
		return true
	}
	if hi.limit == 0 { // limit reached
		return false
	}
	if hi.nextKey == nil { // EndOfTable
		return false
	}
	return true
}

func (hi *HistoryChangesIterDB) Next() ([]byte, []byte, error) {
	if hi.err != nil {
		return nil, nil, hi.err
	}
	hi.limit--
	hi.k, hi.v = hi.nextKey, hi.nextVal
	if err := hi.advance(); err != nil {
		return nil, nil, err
	}
	return hi.k, hi.v, nil
}

func (h *History) DisableReadAhead() {
	h.InvertedIndex.DisableReadAhead()
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			item.decompressor.DisableReadAhead()
			if item.index != nil {
				item.index.DisableReadAhead()
			}
		}
		return true
	})
}

func (h *History) EnableReadAhead() *History {
	h.InvertedIndex.EnableReadAhead()
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			item.decompressor.EnableReadAhead()
			if item.index != nil {
				item.index.EnableReadAhead()
			}
		}
		return true
	})
	return h
}
func (h *History) EnableMadvWillNeed() *History {
	h.InvertedIndex.EnableMadvWillNeed()
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			item.decompressor.EnableWillNeed()
			if item.index != nil {
				item.index.EnableWillNeed()
			}
		}
		return true
	})
	return h
}
func (h *History) EnableMadvNormalReadAhead() *History {
	h.InvertedIndex.EnableMadvNormalReadAhead()
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			item.decompressor.EnableMadvNormal()
			if item.index != nil {
				item.index.EnableMadvNormal()
			}
		}
		return true
	})
	return h
}

// HistoryStep used for incremental state reconsitution, it isolates only one snapshot interval
type HistoryStep struct {
	compressVals bool
	indexItem    *filesItem
	indexFile    ctxItem
	historyItem  *filesItem
	historyFile  ctxItem
}

// MakeSteps [0, toTxNum)
func (h *History) MakeSteps(toTxNum uint64) []*HistoryStep {
	var steps []*HistoryStep
	h.InvertedIndex.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.index == nil || !item.frozen || item.startTxNum >= toTxNum {
				continue
			}

			step := &HistoryStep{
				compressVals: h.compressVals,
				indexItem:    item,
				indexFile: ctxItem{
					startTxNum: item.startTxNum,
					endTxNum:   item.endTxNum,
					getter:     item.decompressor.MakeGetter(),
					reader:     recsplit.NewIndexReader(item.index),
				},
			}
			steps = append(steps, step)
		}
		return true
	})
	i := 0
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.index == nil || !item.frozen || item.startTxNum >= toTxNum {
				continue
			}
			steps[i].historyItem = item
			steps[i].historyFile = ctxItem{
				startTxNum: item.startTxNum,
				endTxNum:   item.endTxNum,
				getter:     item.decompressor.MakeGetter(),
				reader:     recsplit.NewIndexReader(item.index),
			}
			i++
		}
		return true
	})
	return steps
}

func (hs *HistoryStep) Clone() *HistoryStep {
	return &HistoryStep{
		compressVals: hs.compressVals,
		indexItem:    hs.indexItem,
		indexFile: ctxItem{
			startTxNum: hs.indexFile.startTxNum,
			endTxNum:   hs.indexFile.endTxNum,
			getter:     hs.indexItem.decompressor.MakeGetter(),
			reader:     recsplit.NewIndexReader(hs.indexItem.index),
		},
		historyItem: hs.historyItem,
		historyFile: ctxItem{
			startTxNum: hs.historyFile.startTxNum,
			endTxNum:   hs.historyFile.endTxNum,
			getter:     hs.historyItem.decompressor.MakeGetter(),
			reader:     recsplit.NewIndexReader(hs.historyItem.index),
		},
	}
}

func (hc *HistoryContext) idxRangeRecent(key []byte, startTxNum, endTxNum int, asc order.By, limit int, roTx kv.Tx) (iter.U64, error) {
	var dbIt iter.U64
	if hc.h.largeValues {
		if asc {
			from := make([]byte, len(key)+8)
			copy(from, key)
			var fromTxNum uint64
			if startTxNum >= 0 {
				fromTxNum = uint64(startTxNum)
			}
			binary.BigEndian.PutUint64(from[len(key):], fromTxNum)

			to := common.Copy(from)
			toTxNum := uint64(math.MaxUint64)
			if endTxNum >= 0 {
				toTxNum = uint64(endTxNum)
			}
			binary.BigEndian.PutUint64(to[len(key):], toTxNum)

			it, err := roTx.RangeAscend(hc.h.historyValsTable, from, to, limit)
			if err != nil {
				return nil, err
			}
			dbIt = iter.TransformKV2U64(it, func(k, _ []byte) (uint64, error) {
				return binary.BigEndian.Uint64(k[len(k)-8:]), nil
			})
		} else {
			panic("implement me")
		}
	} else {
		if asc {
			var from, to []byte
			if startTxNum >= 0 {
				from = make([]byte, 8)
				binary.BigEndian.PutUint64(from, uint64(startTxNum))
			}
			if endTxNum >= 0 {
				to = make([]byte, 8)
				binary.BigEndian.PutUint64(to, uint64(endTxNum))
			}
			it, err := roTx.RangeDupSort(hc.h.historyValsTable, key, from, to, asc, limit)
			if err != nil {
				return nil, err
			}
			dbIt = iter.TransformKV2U64(it, func(_, v []byte) (uint64, error) {
				return binary.BigEndian.Uint64(v), nil
			})
		} else {
			panic("implement me")
		}
	}

	return dbIt, nil
}
func (hc *HistoryContext) IdxRange(key []byte, startTxNum, endTxNum int, asc order.By, limit int, roTx kv.Tx) (iter.U64, error) {
	frozenIt, err := hc.ic.iterateRangeFrozen(key, startTxNum, endTxNum, asc, limit)
	if err != nil {
		return nil, err
	}
	recentIt, err := hc.idxRangeRecent(key, startTxNum, endTxNum, asc, limit, roTx)
	if err != nil {
		return nil, err
	}
	return iter.Union[uint64](frozenIt, recentIt, asc, limit), nil
}
