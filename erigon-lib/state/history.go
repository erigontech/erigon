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
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/kv/backup"
	btree2 "github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/erigon-lib/seg"
)

type History struct {
	*InvertedIndex // indexKeysTable contains mapping txNum -> key1+key2, while index table `key -> {txnums}` is omitted.

	// files - list of ALL files - including: un-indexed-yet, garbage, merged-into-bigger-one, ...
	// thread-safe, but maybe need 1 RWLock for all trees in AggregatorV3
	//
	// roFiles derivative from field `file`, but without garbage:
	//  - no files with `canDelete=true`
	//  - no overlaps
	//  - no un-indexed files (`power-off` may happen between .ef and .efi creation)
	//
	// MakeContext() using roFiles in zero-copy way
	files     *btree2.BTreeG[*filesItem]
	roFiles   atomic.Pointer[[]ctxItem]
	indexList idxList

	// Schema:
	//  .v - list of values
	//  .vi - txNum+key -> offset in .v

	historyValsTable string // key1+key2+txnNum -> oldValue , stores values BEFORE change
	compressWorkers  int
	compression      FileCompression

	//TODO: re-visit this check - maybe we don't need it. It's abot kill in the middle of merge
	integrityCheck func(fromStep, toStep uint64) bool

	// not large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2 -> txNum + value (DupSort)
	// large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2+txNum -> value (not DupSort)
	historyLargeValues bool // can't use DupSort optimization (aka. prefix-compression) if values size > 4kb

	dontProduceFiles bool   // don't produce .v and .ef files. old data will be pruned anyway.
	keepTxInDB       uint64 // When dontProduceFiles=true, keepTxInDB is used to keep this amount of tx in db before pruning
}

type histCfg struct {
	iiCfg       iiCfg
	compression FileCompression

	//historyLargeValues: used to store values > 2kb (pageSize/2)
	//small values - can be stored in more compact ways in db (DupSort feature)
	//historyLargeValues=true - doesn't support keys of various length (all keys must have same length)
	historyLargeValues bool

	withLocalityIndex  bool
	withExistenceIndex bool // move to iiCfg

	dontProduceHistoryFiles bool   // don't produce .v and .ef files. old data will be pruned anyway.
	keepTxInDB              uint64 // When dontProduceHistoryFiles=true, keepTxInDB is used to keep this amount of tx in db before pruning
}

func NewHistory(cfg histCfg, aggregationStep uint64, filenameBase, indexKeysTable, indexTable, historyValsTable string, integrityCheck func(fromStep, toStep uint64) bool, logger log.Logger) (*History, error) {
	h := History{
		files:              btree2.NewBTreeGOptions[*filesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		historyValsTable:   historyValsTable,
		compression:        cfg.compression,
		compressWorkers:    1,
		indexList:          withHashMap,
		integrityCheck:     integrityCheck,
		historyLargeValues: cfg.historyLargeValues,
		dontProduceFiles:   cfg.dontProduceHistoryFiles,
		keepTxInDB:         cfg.keepTxInDB,
	}
	h.roFiles.Store(&[]ctxItem{})
	var err error
	h.InvertedIndex, err = NewInvertedIndex(cfg.iiCfg, aggregationStep, filenameBase, indexKeysTable, indexTable, cfg.withExistenceIndex, func(fromStep, toStep uint64) bool { return dir.FileExist(h.vFilePath(fromStep, toStep)) }, logger)
	if err != nil {
		return nil, fmt.Errorf("NewHistory: %s, %w", filenameBase, err)
	}

	return &h, nil
}

func (h *History) vFilePath(fromStep, toStep uint64) string {
	return filepath.Join(h.dirs.SnapHistory, fmt.Sprintf("v1-%s.%d-%d.v", h.filenameBase, fromStep, toStep))
}
func (h *History) vAccessorFilePath(fromStep, toStep uint64) string {
	return filepath.Join(h.dirs.SnapAccessors, fmt.Sprintf("v1-%s.%d-%d.vi", h.filenameBase, fromStep, toStep))
}

// OpenList - main method to open list of files.
// It's ok if some files was open earlier.
// If some file already open: noop.
// If some file already open but not in provided list: close and remove from `files` field.
func (h *History) OpenList(idxFiles, histNames []string, readonly bool) error {
	if err := h.InvertedIndex.OpenList(idxFiles, readonly); err != nil {
		return err
	}
	return h.openList(histNames)

}
func (h *History) openList(fNames []string) error {
	h.closeWhatNotInList(fNames)
	h.scanStateFiles(fNames)
	if err := h.openFiles(); err != nil {
		return fmt.Errorf("History(%s).openFiles: %w", h.filenameBase, err)
	}
	return nil
}

func (h *History) OpenFolder(readonly bool) error {
	idxFiles, histFiles, _, err := h.fileNamesOnDisk()
	if err != nil {
		return err
	}
	return h.OpenList(idxFiles, histFiles, readonly)
}

// scanStateFiles
// returns `uselessFiles` where file "is useless" means: it's subset of frozen file. such files can be safely deleted. subset of non-frozen file may be useful
func (h *History) scanStateFiles(fNames []string) (garbageFiles []*filesItem) {
	re := regexp.MustCompile("^v([0-9]+)-" + h.filenameBase + ".([0-9]+)-([0-9]+).v$")
	var err error
	for _, name := range fNames {
		subs := re.FindStringSubmatch(name)
		if len(subs) != 4 {
			if len(subs) != 0 {
				h.logger.Warn("[snapshots] file ignored by inverted index scan, more than 3 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startStep, endStep uint64
		if startStep, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			h.logger.Warn("[snapshots] file ignored by inverted index scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endStep, err = strconv.ParseUint(subs[3], 10, 64); err != nil {
			h.logger.Warn("[snapshots] file ignored by inverted index scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startStep > endStep {
			h.logger.Warn("[snapshots] file ignored by inverted index scan, startTxNum > endTxNum", "name", name)
			continue
		}

		startTxNum, endTxNum := startStep*h.aggregationStep, endStep*h.aggregationStep
		var newFile = newFilesItem(startTxNum, endTxNum, h.aggregationStep)

		if h.integrityCheck != nil && !h.integrityCheck(startStep, endStep) {
			continue
		}

		if _, has := h.files.Get(newFile); has {
			continue
		}
		h.files.Set(newFile)
	}
	return garbageFiles
}

func (h *History) openFiles() error {
	var err error
	invalidFileItems := make([]*filesItem, 0)
	h.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			fromStep, toStep := item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep
			if item.decompressor == nil {
				fPath := h.vFilePath(fromStep, toStep)
				if !dir.FileExist(fPath) {
					_, fName := filepath.Split(fPath)
					h.logger.Debug("[agg] History.openFiles: file does not exists", "f", fName)
					invalidFileItems = append(invalidFileItems, item)
					continue
				}
				if item.decompressor, err = seg.NewDecompressor(fPath); err != nil {
					_, fName := filepath.Split(fPath)
					h.logger.Warn("[agg] History.openFiles", "err", err, "f", fName)
					invalidFileItems = append(invalidFileItems, item)
					// don't interrupt on error. other files may be good. but skip indices open.
					continue
				}
			}

			if item.index == nil {
				fPath := h.vAccessorFilePath(fromStep, toStep)
				if dir.FileExist(fPath) {
					if item.index, err = recsplit.OpenIndex(fPath); err != nil {
						_, fName := filepath.Split(fPath)
						h.logger.Warn("[agg] History.openFiles", "err", err, "f", fName)
						// don't interrupt on error. other files may be good
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

func (hc *HistoryContext) Files() (res []string) {
	for _, item := range hc.files {
		if item.src.decompressor != nil {
			res = append(res, item.src.decompressor.FileName())
		}
	}
	return append(res, hc.ic.Files()...)
}

func (h *History) missedIdxFiles() (l []*filesItem) {
	h.files.Walk(func(items []*filesItem) bool { // don't run slow logic while iterating on btree
		for _, item := range items {
			fromStep, toStep := item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep
			if !dir.FileExist(h.vAccessorFilePath(fromStep, toStep)) {
				l = append(l, item)
			}
		}
		return true
	})
	return l
}

func (h *History) buildVi(ctx context.Context, item *filesItem, ps *background.ProgressSet) (err error) {
	if item.decompressor == nil {
		return fmt.Errorf("buildVI: passed item with nil decompressor %s %d-%d", h.filenameBase, item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep)
	}

	search := &filesItem{startTxNum: item.startTxNum, endTxNum: item.endTxNum}
	iiItem, ok := h.InvertedIndex.files.Get(search)
	if !ok {
		return nil
	}

	if iiItem.decompressor == nil {
		return fmt.Errorf("buildVI: got iiItem with nil decompressor %s %d-%d", h.filenameBase, item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep)
	}

	fromStep, toStep := item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep
	idxPath := h.vAccessorFilePath(fromStep, toStep)
	return buildVi(ctx, item, iiItem, idxPath, h.dirs.Tmp, ps, h.InvertedIndex.compression, h.compression, h.salt, h.logger)
}

func (h *History) BuildMissedIndices(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet) {
	h.InvertedIndex.BuildMissedIndices(ctx, g, ps)
	missedFiles := h.missedIdxFiles()
	for _, item := range missedFiles {
		item := item
		g.Go(func() error {
			return h.buildVi(ctx, item, ps)
		})
	}
}

func buildVi(ctx context.Context, historyItem, iiItem *filesItem, historyIdxPath, tmpdir string, ps *background.ProgressSet, compressIindex, compressHist FileCompression, salt *uint32, logger log.Logger) error {
	defer iiItem.decompressor.EnableReadAhead().DisableReadAhead()
	defer historyItem.decompressor.EnableReadAhead().DisableReadAhead()

	_, fName := filepath.Split(historyIdxPath)
	p := ps.AddNew(fName, uint64(iiItem.decompressor.Count()*2))
	defer ps.Delete(p)

	var count uint64
	g := NewArchiveGetter(iiItem.decompressor.MakeGetter(), compressIindex)
	g.Reset(0)
	for g.HasNext() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		g.Skip() // key
		valBuf, _ := g.Next(nil)
		count += eliasfano32.Count(valBuf)
		p.Processed.Add(1)
	}

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   int(count),
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpdir,
		IndexFile:  historyIdxPath,
		Salt:       salt,
	}, logger)
	if err != nil {
		return fmt.Errorf("create recsplit: %w", err)
	}
	rs.LogLvl(log.LvlTrace)
	defer rs.Close()
	var historyKey []byte
	var txKey [8]byte
	var valOffset uint64

	g2 := NewArchiveGetter(historyItem.decompressor.MakeGetter(), compressHist)
	var keyBuf, valBuf []byte
	for {
		g.Reset(0)
		g2.Reset(0)
		valOffset = 0
		for g.HasNext() {
			keyBuf, _ = g.Next(nil)
			valBuf, _ = g.Next(nil)
			ef, _ := eliasfano32.ReadEliasFano(valBuf)
			efIt := ef.Iterator()
			for efIt.HasNext() {
				txNum, err := efIt.Next()
				if err != nil {
					return err
				}
				binary.BigEndian.PutUint64(txKey[:], txNum)
				historyKey = append(append(historyKey[:0], txKey[:]...), keyBuf...)
				if err = rs.AddKey(historyKey, valOffset); err != nil {
					return err
				}
				//if compressHist {
				valOffset, _ = g2.Skip()
				//} else {
				//	valOffset, _ = g2.SkipUncompressed()
				//}
			}

			p.Processed.Add(1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
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

func (w *historyBufferedWriter) AddPrevValue(key1, key2, original []byte, originalStep uint64) (err error) {
	if w.discard {
		return nil
	}

	if original == nil {
		original = []byte{}
	}

	//defer func() {
	//	fmt.Printf("addPrevValue [%p;tx=%d] '%x' -> '%x'\n", w, w.ii.txNum, key1, original)
	//}()

	if w.largeValues {
		lk := len(key1) + len(key2)

		w.historyKey = append(append(append(w.historyKey[:0], key1...), key2...), w.ii.txNumBytes[:]...)
		historyKey := w.historyKey[:lk+8]

		if err := w.historyVals.Collect(historyKey, original); err != nil {
			return err
		}

		if !w.ii.discard {
			if err := w.ii.indexKeys.Collect(w.ii.txNumBytes[:], historyKey[:lk]); err != nil {
				return err
			}
		}
		return nil
	}

	lk := len(key1) + len(key2)
	w.historyKey = append(append(append(append(w.historyKey[:0], key1...), key2...), w.ii.txNumBytes[:]...), original...)
	historyKey := w.historyKey[:lk+8+len(original)]
	historyKey1 := historyKey[:lk]
	historyVal := historyKey[lk:]
	invIdxVal := historyKey[:lk]

	if len(original) > 2048 {
		log.Error("History value is too large while largeValues=false", "h", w.historyValsTable, "histo", string(w.historyKey[:lk]), "len", len(original), "max", len(w.historyKey)-8-len(key1)-len(key2))
		panic("History value is too large while largeValues=false")
	}

	if err := w.historyVals.Collect(historyKey1, historyVal); err != nil {
		return err
	}
	if !w.ii.discard {
		if err := w.ii.indexKeys.Collect(w.ii.txNumBytes[:], invIdxVal); err != nil {
			return err
		}
	}
	return nil
}

func (hc *HistoryContext) NewWriter() *historyBufferedWriter {
	return hc.newWriter(hc.h.dirs.Tmp, false)
}

type historyBufferedWriter struct {
	historyVals      *etl.Collector
	historyKey       []byte
	discard          bool
	historyValsTable string

	// not large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2 -> txNum + value (DupSort)
	// large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2+txNum -> value (not DupSort)
	largeValues bool

	ii *invertedIndexBufferedWriter
}

func (w *historyBufferedWriter) SetTxNum(v uint64) { w.ii.SetTxNum(v) }

func (w *historyBufferedWriter) close() {
	if w == nil { // allow dobule-close
		return
	}
	w.ii.close()
	if w.historyVals != nil {
		w.historyVals.Close()
	}
}

func (hc *HistoryContext) newWriter(tmpdir string, discard bool) *historyBufferedWriter {
	w := &historyBufferedWriter{
		discard: discard,

		historyKey:       make([]byte, 128),
		largeValues:      hc.h.historyLargeValues,
		historyValsTable: hc.h.historyValsTable,
		historyVals:      etl.NewCollector(hc.h.historyValsTable, tmpdir, etl.NewSortableBuffer(WALCollectorRAM), hc.h.logger),

		ii: hc.ic.newWriter(tmpdir, discard),
	}
	w.historyVals.LogLvl(log.LvlTrace)
	w.historyVals.SortAndFlushInBackground(true)
	return w
}

func (w *historyBufferedWriter) Flush(ctx context.Context, tx kv.RwTx) error {
	if w.discard {
		return nil
	}
	if err := w.ii.Flush(ctx, tx); err != nil {
		return err
	}

	if err := w.historyVals.Load(tx, w.historyValsTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	w.close()
	return nil
}

type HistoryCollation struct {
	historyComp  ArchiveWriter
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
	c.indexBitmaps = nil //nolint
}

// [txFrom; txTo)
func (h *History) collate(ctx context.Context, step, txFrom, txTo uint64, roTx kv.Tx) (HistoryCollation, error) {
	if h.dontProduceFiles {
		return HistoryCollation{}, nil
	}

	var historyComp ArchiveWriter
	var err error
	closeComp := true
	defer func() {
		if closeComp {
			if historyComp != nil {
				historyComp.Close()
			}
		}
	}()
	historyPath := h.vFilePath(step, step+1)
	comp, err := seg.NewCompressor(ctx, "collate history", historyPath, h.dirs.Tmp, seg.MinPatternScore, h.compressWorkers, log.LvlTrace, h.logger)
	if err != nil {
		return HistoryCollation{}, fmt.Errorf("create %s history compressor: %w", h.filenameBase, err)
	}
	historyComp = NewArchiveWriter(comp, h.compression)

	keysCursor, err := roTx.CursorDupSort(h.indexKeysTable)
	if err != nil {
		return HistoryCollation{}, fmt.Errorf("create %s history cursor: %w", h.filenameBase, err)
	}
	defer keysCursor.Close()
	indexBitmaps := map[string]*roaring64.Bitmap{}
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	for k, v, err := keysCursor.Seek(txKey[:]); err == nil && k != nil; k, v, err = keysCursor.Next() {
		if err != nil {
			return HistoryCollation{}, fmt.Errorf("iterate over %s history cursor: %w", h.filenameBase, err)
		}
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo { // [txFrom; txTo)
			break
		}
		ks := string(v)
		bitmap, ok := indexBitmaps[ks]
		if !ok {
			bitmap = bitmapdb.NewBitmap64()
			indexBitmaps[ks] = bitmap
		}
		bitmap.Add(txNum)

		select {
		case <-ctx.Done():
			return HistoryCollation{}, ctx.Err()
		default:
		}
	}
	keys := make([]string, 0, len(indexBitmaps))
	for key, bm := range indexBitmaps {
		keys = append(keys, key)
		bm.RunOptimize()
	}
	slices.Sort(keys)
	historyCount := 0

	var c kv.Cursor
	var cd kv.CursorDupSort
	if h.historyLargeValues {
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

	keyBuf := make([]byte, 0, 256)
	for _, key := range keys {
		bitmap := indexBitmaps[key]
		it := bitmap.Iterator()
		keyBuf = append(append(keyBuf[:0], []byte(key)...), make([]byte, 8)...)
		lk := len([]byte(key))

		for it.HasNext() {
			txNum := it.Next()
			binary.BigEndian.PutUint64(keyBuf[lk:], txNum)
			//TODO: use cursor range
			if h.historyLargeValues {
				val, err := roTx.GetOne(h.historyValsTable, keyBuf)
				if err != nil {
					return HistoryCollation{}, fmt.Errorf("getBeforeTxNum %s history val [%x]: %w", h.filenameBase, key, err)
				}
				if len(val) == 0 {
					val = nil
				}
				if err = historyComp.AddWord(val); err != nil {
					return HistoryCollation{}, fmt.Errorf("add %s history val [%x]=>[%x]: %w", h.filenameBase, key, val, err)
				}
			} else {
				val, err := cd.SeekBothRange(keyBuf[:lk], keyBuf[lk:])
				if err != nil {
					return HistoryCollation{}, err
				}
				if val != nil && binary.BigEndian.Uint64(val) == txNum {
					// fmt.Printf("HistCollate [%x]=>[%x]\n", []byte(key), val)
					val = val[8:]
				} else {
					val = nil
				}
				if err = historyComp.AddWord(val); err != nil {
					return HistoryCollation{}, fmt.Errorf("add %s history val [%x]=>[%x]: %w", h.filenameBase, key, val, err)
				}
			}
			historyCount++
		}
	}
	closeComp = false
	mxCollationSizeHist.SetUint64(uint64(historyComp.Count()))
	return HistoryCollation{
		historyPath:  historyPath,
		historyComp:  historyComp,
		historyCount: historyCount,
		indexBitmaps: indexBitmaps,
	}, nil
}

type HistoryFiles struct {
	historyDecomp   *seg.Decompressor
	historyIdx      *recsplit.Index
	efHistoryDecomp *seg.Decompressor
	efHistoryIdx    *recsplit.Index
	efExistence     *ExistenceFilter
}

func (sf HistoryFiles) CleanupOnError() {
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
	if sf.efExistence != nil {
		sf.efExistence.Close()
	}
}
func (h *History) reCalcRoFiles() {
	roFiles := ctxFiles(h.files, h.indexList, false)
	h.roFiles.Store(&roFiles)
}

// buildFiles performs potentially resource intensive operations of creating
// static files and their indices
func (h *History) buildFiles(ctx context.Context, step uint64, collation HistoryCollation, ps *background.ProgressSet) (HistoryFiles, error) {
	if h.dontProduceFiles {
		return HistoryFiles{}, nil
	}

	historyComp := collation.historyComp
	if h.noFsync {
		historyComp.DisableFsync()
	}
	var (
		historyDecomp, efHistoryDecomp *seg.Decompressor
		historyIdx, efHistoryIdx       *recsplit.Index
		efExistence                    *ExistenceFilter
		efHistoryComp                  *seg.Compressor
		rs                             *recsplit.RecSplit
	)
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
			if efExistence != nil {
				efExistence.Close()
			}
			if rs != nil {
				rs.Close()
			}
		}
	}()

	historyIdxPath := h.vAccessorFilePath(step, step+1)
	{
		_, historyIdxFileName := filepath.Split(historyIdxPath)
		p := ps.AddNew(historyIdxFileName, 1)
		defer ps.Delete(p)
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

	efHistoryPath := h.efFilePath(step, step+1)
	{
		var err error
		if historyDecomp, err = seg.NewDecompressor(collation.historyPath); err != nil {
			return HistoryFiles{}, fmt.Errorf("open %s history decompressor: %w", h.filenameBase, err)
		}

		// Build history ef
		_, efHistoryFileName := filepath.Split(efHistoryPath)
		p := ps.AddNew(efHistoryFileName, 1)
		defer ps.Delete(p)
		efHistoryComp, err = seg.NewCompressor(ctx, "ef history", efHistoryPath, h.dirs.Tmp, seg.MinPatternScore, h.compressWorkers, log.LvlTrace, h.logger)
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
	if efHistoryDecomp, err = seg.NewDecompressor(efHistoryPath); err != nil {
		return HistoryFiles{}, fmt.Errorf("open %s ef history decompressor: %w", h.filenameBase, err)
	}
	{
		if err := h.InvertedIndex.buildMapIdx(ctx, step, step+1, efHistoryDecomp, ps); err != nil {
			return HistoryFiles{}, fmt.Errorf("build %s ef history idx: %w", h.filenameBase, err)
		}
		if efHistoryIdx, err = recsplit.OpenIndex(h.InvertedIndex.efAccessorFilePath(step, step+1)); err != nil {
			return HistoryFiles{}, err
		}
	}
	if h.InvertedIndex.withExistenceIndex {
		existenceIdxPath := h.efExistenceIdxFilePath(step, step+1)
		if efExistence, err = buildIndexFilterThenOpen(ctx, efHistoryDecomp, h.compression, existenceIdxPath, h.dirs.Tmp, h.salt, ps, h.logger, h.noFsync); err != nil {
			return HistoryFiles{}, fmt.Errorf("build %s ef history idx: %w", h.filenameBase, err)
		}

	}
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   collation.historyCount,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     h.dirs.Tmp,
		IndexFile:  historyIdxPath,
		Salt:       h.salt,
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
	g := NewArchiveGetter(historyDecomp.MakeGetter(), h.compression)
	for {
		g.Reset(0)
		valOffset = 0
		for _, key := range keys {
			bitmap := collation.indexBitmaps[key]
			it := bitmap.Iterator()
			kb := []byte(key)
			for it.HasNext() {
				txNum := it.Next()
				binary.BigEndian.PutUint64(txKey[:], txNum)
				historyKey = append(append(historyKey[:0], txKey[:]...), kb...)
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
		efExistence:     efExistence,
	}, nil
}

func (h *History) integrateFiles(sf HistoryFiles, txNumFrom, txNumTo uint64) {
	defer h.reCalcRoFiles()
	if h.dontProduceFiles {
		return
	}

	h.InvertedIndex.integrateFiles(InvertedFiles{
		decomp:    sf.efHistoryDecomp,
		index:     sf.efHistoryIdx,
		existence: sf.efExistence,
	}, txNumFrom, txNumTo)

	fi := newFilesItem(txNumFrom, txNumTo, h.aggregationStep)
	fi.decompressor = sf.historyDecomp
	fi.index = sf.historyIdx
	h.files.Set(fi)
}

func (h *History) isEmpty(tx kv.Tx) (bool, error) {
	if h.historyLargeValues {
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

type HistoryRecord struct {
	TxNum uint64
	Value []byte
}

type HistoryContext struct {
	h  *History
	ic *InvertedIndexContext

	files   []ctxItem // have no garbage (canDelete=true, overlaps, etc...)
	getters []ArchiveGetter
	readers []*recsplit.IndexReader

	trace bool

	valsC    kv.Cursor
	valsCDup kv.CursorDupSort

	_bufTs []byte
}

func (h *History) MakeContext() *HistoryContext {
	files := *h.roFiles.Load()
	for i := 0; i < len(files); i++ {
		if !files[i].src.frozen {
			files[i].src.refcount.Add(1)
		}
	}

	return &HistoryContext{
		h:     h,
		ic:    h.InvertedIndex.MakeContext(),
		files: files,
		trace: false,
	}
}

func (hc *HistoryContext) statelessGetter(i int) ArchiveGetter {
	if hc.getters == nil {
		hc.getters = make([]ArchiveGetter, len(hc.files))
	}
	r := hc.getters[i]
	if r == nil {
		g := hc.files[i].src.decompressor.MakeGetter()
		r = NewArchiveGetter(g, hc.h.compression)
		hc.getters[i] = r
	}
	return r
}
func (hc *HistoryContext) statelessIdxReader(i int) *recsplit.IndexReader {
	if hc.readers == nil {
		hc.readers = make([]*recsplit.IndexReader, len(hc.files))
	}
	{
		//assert
		for _, f := range hc.files {
			if f.src.index == nil {
				panic("assert: file has nil index " + f.src.decompressor.FileName())
			}
		}
	}
	r := hc.readers[i]
	if r == nil {
		r = hc.files[i].src.index.GetReaderFromPool()
		hc.readers[i] = r
	}
	return r
}

func (hc *HistoryContext) canPruneUntil(tx kv.Tx, untilTx uint64) (can bool, txTo uint64) {
	minIdxTx, maxIdxTx := hc.ic.smallestTxNum(tx), hc.ic.highestTxNum(tx)
	//defer func() {
	//	fmt.Printf("CanPrune[%s]Until(%d) noFiles=%t txTo %d idxTx [%d-%d] keepTxInDB=%d; result %t\n",
	//		hc.h.filenameBase, untilTx, hc.h.dontProduceHistoryFiles, txTo, minIdxTx, maxIdxTx, hc.h.keepTxInDB, minIdxTx < txTo)
	//}()

	if hc.h.dontProduceFiles {
		if hc.h.keepTxInDB >= maxIdxTx {
			return false, 0
		}
		txTo = min(maxIdxTx-hc.h.keepTxInDB, untilTx) // bound pruning
	} else {
		canPruneIdx := hc.ic.CanPrune(tx)
		if !canPruneIdx {
			return false, 0
		}
		txTo = min(hc.maxTxNumInFiles(false), untilTx)
	}

	switch hc.h.filenameBase {
	case "accounts":
		mxPrunableHAcc.Set(float64(txTo - minIdxTx))
	case "storage":
		mxPrunableHSto.Set(float64(txTo - minIdxTx))
	case "code":
		mxPrunableHCode.Set(float64(txTo - minIdxTx))
	case "commitment":
		mxPrunableHComm.Set(float64(txTo - minIdxTx))
	}
	return minIdxTx < txTo, txTo
}

func (hc *HistoryContext) Warmup(ctx context.Context) (cleanup func()) {
	ctx, cancel := context.WithCancel(ctx)
	wg := &errgroup.Group{}
	wg.Go(func() error {
		backup.WarmupTable(ctx, hc.h.db, hc.h.historyValsTable, log.LvlDebug, 4)
		return nil
	})
	return func() {
		cancel()
		_ = wg.Wait()
	}
}

// Prune [txFrom; txTo)
// `force` flag to prune even if canPruneUntil returns false (when Unwind is needed, canPruneUntil always returns false)
// `useProgress` flag to restore and update prune progress.
//   - E.g. Unwind can't use progress, because it's not linear
//     and will wrongly update progress of steps cleaning and could end up with inconsistent history.
func (hc *HistoryContext) Prune(ctx context.Context, rwTx kv.RwTx, txFrom, txTo, limit uint64, forced, withWarmup bool, logEvery *time.Ticker) (*InvertedIndexPruneStat, error) {
	//fmt.Printf(" pruneH[%s] %t, %d-%d\n", hc.h.filenameBase, hc.CanPruneUntil(rwTx), txFrom, txTo)
	if !forced {
		var can bool
		can, txTo = hc.canPruneUntil(rwTx, txTo)
		if !can {
			return nil, nil
		}
	}
	defer func(t time.Time) { mxPruneTookHistory.ObserveDuration(t) }(time.Now())

	var (
		seek     = make([]byte, 8, 256)
		valsCDup kv.RwCursorDupSort
		err      error
	)

	if !hc.h.historyLargeValues {
		valsCDup, err = rwTx.RwCursorDupSort(hc.h.historyValsTable)
		if err != nil {
			return nil, err
		}
		defer valsCDup.Close()
	}

	pruneValue := func(k, txnm []byte) error {
		txNum := binary.BigEndian.Uint64(txnm)
		if txNum >= txTo || txNum < txFrom { //[txFrom; txTo), but in this case idx record
			return fmt.Errorf("history pruneValue: txNum %d not in pruning range [%d,%d)", txNum, txFrom, txTo)
		}

		if hc.h.historyLargeValues {
			seek = append(append(seek[:0], k...), txnm...)
			if err := rwTx.Delete(hc.h.historyValsTable, seek); err != nil {
				return err
			}
		} else {
			vv, err := valsCDup.SeekBothRange(k, txnm)
			if err != nil {
				return err
			}
			if binary.BigEndian.Uint64(vv) != txNum {
				return fmt.Errorf("history invalid txNum: %d != %d", binary.BigEndian.Uint64(vv), txNum)
			}
			if err = valsCDup.DeleteCurrent(); err != nil {
				return err
			}
		}

		mxPruneSizeHistory.Inc()
		return nil
	}

	if !forced && hc.h.dontProduceFiles {
		forced = true // or index.CanPrune will return false cuz no snapshots made
	}

	if withWarmup {
		cleanup := hc.Warmup(ctx)
		defer cleanup()
	}

	return hc.ic.Prune(ctx, rwTx, txFrom, txTo, limit, logEvery, forced, withWarmup, pruneValue)
}

func (hc *HistoryContext) Close() {
	if hc.files == nil { // invariant: it's safe to call Close multiple times
		return
	}
	files := hc.files
	hc.files = nil
	for i := 0; i < len(files); i++ {
		if files[i].src.frozen {
			continue
		}
		refCnt := files[i].src.refcount.Add(-1)
		//if hc.h.filenameBase == "accounts" && item.src.canDelete.Load() {
		//	log.Warn("[history] HistoryContext.Close: check file to remove", "refCnt", refCnt, "name", item.src.decompressor.FileName())
		//}
		//GC: last reader responsible to remove useles files: close it and delete
		if refCnt == 0 && files[i].src.canDelete.Load() {
			files[i].src.closeFilesAndRemove()
		}
	}
	for _, r := range hc.readers {
		r.Close()
	}

	hc.ic.Close()
}

func (hc *HistoryContext) getFileDeprecated(from, to uint64) (it ctxItem, ok bool) {
	for i := 0; i < len(hc.files); i++ {
		if hc.files[i].startTxNum == from && hc.files[i].endTxNum == to {
			return hc.files[i], true
		}
	}
	return it, false
}
func (hc *HistoryContext) getFile(txNum uint64) (it ctxItem, ok bool) {
	for i := 0; i < len(hc.files); i++ {
		if hc.files[i].startTxNum <= txNum && hc.files[i].endTxNum > txNum {
			return hc.files[i], true
		}
	}
	return it, false
}

func (hc *HistoryContext) GetNoState(key []byte, txNum uint64) ([]byte, bool, error) {
	// Files list of II and History is different
	// it means II can't return index of file, but can return TxNum which History will use to find own file
	ok, histTxNum := hc.ic.Seek(key, txNum)
	if !ok {
		return nil, false, nil
	}
	historyItem, ok := hc.getFile(histTxNum)
	if !ok {
		return nil, false, fmt.Errorf("hist file not found: key=%x, %s.%d-%d", key, hc.h.filenameBase, histTxNum/hc.h.aggregationStep, histTxNum/hc.h.aggregationStep)
	}
	reader := hc.statelessIdxReader(historyItem.i)
	if reader.Empty() {
		return nil, false, nil
	}
	offset, ok := reader.Lookup2(hc.encodeTs(histTxNum), key)
	if !ok {
		return nil, false, nil
	}
	g := hc.statelessGetter(historyItem.i)
	g.Reset(offset)

	v, _ := g.Next(nil)
	if traceGetAsOf == hc.h.filenameBase {
		fmt.Printf("GetAsOf(%s, %x, %d) -> %s, histTxNum=%d, isNil(v)=%t\n", hc.h.filenameBase, key, txNum, g.FileName(), histTxNum, v == nil)
	}
	return v, true, nil
}

func (hs *HistoryStep) GetNoState(key []byte, txNum uint64) ([]byte, bool, uint64) {
	//fmt.Printf("GetNoState [%x] %d\n", key, txNum)
	if hs.indexFile.reader.Empty() {
		return nil, false, txNum
	}
	offset, ok := hs.indexFile.reader.TwoLayerLookup(key)
	if !ok {
		return nil, false, txNum
	}
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
	offset, ok = hs.historyFile.reader.Lookup2(txKey[:], key)
	if !ok {
		return nil, false, txNum
	}
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
	offset, ok := hs.indexFile.reader.TwoLayerLookup(key)
	if !ok {
		return false, 0
	}
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

func (hc *HistoryContext) encodeTs(txNum uint64) []byte {
	if hc._bufTs == nil {
		hc._bufTs = make([]byte, 8)
	}
	binary.BigEndian.PutUint64(hc._bufTs, txNum)
	return hc._bufTs
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

	return hc.getNoStateFromDB(key, txNum, roTx)
}

func (hc *HistoryContext) valsCursor(tx kv.Tx) (c kv.Cursor, err error) {
	if hc.valsC != nil {
		return hc.valsC, nil
	}
	hc.valsC, err = tx.Cursor(hc.h.historyValsTable)
	if err != nil {
		return nil, err
	}
	return hc.valsC, nil
}
func (hc *HistoryContext) valsCursorDup(tx kv.Tx) (c kv.CursorDupSort, err error) {
	if hc.valsCDup != nil {
		return hc.valsCDup, nil
	}
	hc.valsCDup, err = tx.CursorDupSort(hc.h.historyValsTable)
	if err != nil {
		return nil, err
	}
	return hc.valsCDup, nil
}

func (hc *HistoryContext) getNoStateFromDB(key []byte, txNum uint64, tx kv.Tx) ([]byte, bool, error) {
	if hc.h.historyLargeValues {
		c, err := hc.valsCursor(tx)
		if err != nil {
			return nil, false, err
		}
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
		// val == []byte{}, means key was created in this txNum and doesn't exist before.
		return val, true, nil
	}
	c, err := hc.valsCursorDup(tx)
	if err != nil {
		return nil, false, err
	}
	val, err := c.SeekBothRange(key, hc.encodeTs(txNum))
	if err != nil {
		return nil, false, err
	}
	if val == nil {
		return nil, false, nil
	}
	// `val == []byte{}` means key was created in this txNum and doesn't exist before.
	return val[8:], true, nil
}
func (hc *HistoryContext) WalkAsOf(startTxNum uint64, from, to []byte, roTx kv.Tx, limit int) (iter.KV, error) {
	hi := &StateAsOfIterF{
		from: from, to: to, limit: limit,

		hc:         hc,
		startTxNum: startTxNum,
	}
	for _, item := range hc.ic.files {
		if item.endTxNum <= startTxNum {
			continue
		}
		// TODO: seek(from)
		g := NewArchiveGetter(item.src.decompressor.MakeGetter(), hc.h.compression)
		g.Reset(0)
		if g.HasNext() {
			key, offset := g.Next(nil)
			heap.Push(&hi.h, &ReconItem{g: g, key: key, startTxNum: item.startTxNum, endTxNum: item.endTxNum, txNum: item.endTxNum, startOffset: offset, lastOffset: offset})
		}
	}
	binary.BigEndian.PutUint64(hi.startTxKey[:], startTxNum)
	if err := hi.advanceInFiles(); err != nil {
		return nil, err
	}

	dbit := &StateAsOfIterDB{
		largeValues: hc.h.historyLargeValues,
		roTx:        roTx,
		valsTable:   hc.h.historyValsTable,
		from:        from, to: to, limit: limit,

		startTxNum: startTxNum,
	}
	binary.BigEndian.PutUint64(dbit.startTxKey[:], startTxNum)
	if err := dbit.advance(); err != nil {
		panic(err)
	}
	return iter.UnionKV(hi, dbit, limit), nil
}

// StateAsOfIter - returns state range at given time in history
type StateAsOfIterF struct {
	hc    *HistoryContext
	limit int

	from, to []byte
	nextVal  []byte
	nextKey  []byte

	h          ReconHeap
	startTxNum uint64
	startTxKey [8]byte
	txnKey     [8]byte

	k, v, kBackup, vBackup []byte
}

func (hi *StateAsOfIterF) Close() {
}

func (hi *StateAsOfIterF) advanceInFiles() error {
	for hi.h.Len() > 0 {
		top := heap.Pop(&hi.h).(*ReconItem)
		key := top.key
		var idxVal []byte
		//if hi.compressVals {
		idxVal, _ = top.g.Next(nil)
		//} else {
		//	idxVal, _ = top.g.NextUncompressed()
		//}
		if top.g.HasNext() {
			//if hi.compressVals {
			top.key, _ = top.g.Next(nil)
			//} else {
			//	top.key, _ = top.g.NextUncompressed()
			//}
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
		n, ok := eliasfano32.Seek(idxVal, hi.startTxNum)
		if !ok {
			continue
		}

		hi.nextKey = key
		binary.BigEndian.PutUint64(hi.txnKey[:], n)
		historyItem, ok := hi.hc.getFileDeprecated(top.startTxNum, top.endTxNum)
		if !ok {
			return fmt.Errorf("no %s file found for [%x]", hi.hc.h.filenameBase, hi.nextKey)
		}
		reader := hi.hc.statelessIdxReader(historyItem.i)
		offset, ok := reader.Lookup2(hi.txnKey[:], hi.nextKey)
		if !ok {
			continue
		}
		g := hi.hc.statelessGetter(historyItem.i)
		g.Reset(offset)
		hi.nextVal, _ = g.Next(nil)
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
		hc:         hc,
		startTxNum: cmp.Max(0, uint64(fromTxNum)),
		endTxNum:   toTxNum,
		limit:      limit,
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
		g := NewArchiveGetter(item.src.decompressor.MakeGetter(), hc.h.compression)
		g.Reset(0)
		if g.HasNext() {
			key, offset := g.Next(nil)
			heap.Push(&hi.h, &ReconItem{g: g, key: key, startTxNum: item.startTxNum, endTxNum: item.endTxNum, txNum: item.endTxNum, startOffset: offset, lastOffset: offset})
		}
	}
	if err := hi.advance(); err != nil {
		return nil, err
	}
	return hi, nil
}

func (hc *HistoryContext) iterateChangedRecent(fromTxNum, toTxNum int, asc order.By, limit int, roTx kv.Tx) (iter.KVS, error) {
	if asc == order.Desc {
		panic("not supported yet")
	}
	rangeIsInFiles := toTxNum >= 0 && len(hc.ic.files) > 0 && hc.ic.files[len(hc.ic.files)-1].endTxNum >= uint64(toTxNum)
	if rangeIsInFiles {
		return iter.EmptyKVS, nil
	}
	dbi := &HistoryChangesIterDB{
		endTxNum:    toTxNum,
		roTx:        roTx,
		largeValues: hc.h.historyLargeValues,
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

func (hc *HistoryContext) HistoryRange(fromTxNum, toTxNum int, asc order.By, limit int, roTx kv.Tx) (iter.KVS, error) {
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
	return iter.MergeKVS(itOnDB, itOnFiles, limit), nil
}

type HistoryChangesIterFiles struct {
	hc         *HistoryContext
	nextVal    []byte
	nextKey    []byte
	h          ReconHeap
	startTxNum uint64
	endTxNum   int
	startTxKey [8]byte
	txnKey     [8]byte

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
		//if hi.compressVals {
		idxVal, _ = top.g.Next(nil)
		//} else {
		//	idxVal, _ = top.g.NextUncompressed()
		//}
		if top.g.HasNext() {
			//if hi.compressVals {
			top.key, _ = top.g.Next(nil)
			//} else {
			//	top.key, _ = top.g.NextUncompressed()
			//}
			heap.Push(&hi.h, top)
		}

		if bytes.Equal(key, hi.nextKey) {
			continue
		}
		n, ok := eliasfano32.Seek(idxVal, hi.startTxNum)
		if !ok {
			continue
		}
		if int(n) >= hi.endTxNum {
			continue
		}

		hi.nextKey = key
		binary.BigEndian.PutUint64(hi.txnKey[:], n)
		historyItem, ok := hi.hc.getFileDeprecated(top.startTxNum, top.endTxNum)
		if !ok {
			return fmt.Errorf("HistoryChangesIterFiles: no %s file found for [%x]", hi.hc.h.filenameBase, hi.nextKey)
		}
		reader := hi.hc.statelessIdxReader(historyItem.i)
		offset, ok := reader.Lookup2(hi.txnKey[:], hi.nextKey)
		if !ok {
			continue
		}
		g := hi.hc.statelessGetter(historyItem.i)
		g.Reset(offset)
		hi.nextVal, _ = g.Next(nil)
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
	nextStep         uint64
	k, v             []byte
	step             uint64
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
		if hi.nextKey != nil && bytes.Equal(k[:len(k)-8], hi.nextKey) && bytes.Equal(v, hi.nextVal) {
			// stuck on the same key, move to first key larger than seek
			for {
				k, v, err = hi.valsC.Next()
				if err != nil {
					return err
				}
				if k == nil {
					hi.nextKey = nil
					return nil
				}
				if bytes.Compare(seek[:len(seek)-8], k[:len(k)-8]) < 0 {
					break
				}
			}
		}
		//fmt.Printf("[seek=%x][RET=%t] '%x' '%x'\n", seek, bytes.Equal(seek[:len(seek)-8], k[:len(k)-8]), k, v)
		if !bytes.Equal(seek[:len(seek)-8], k[:len(k)-8]) /*|| int(binary.BigEndian.Uint64(k[len(k)-8:])) > hi.endTxNum */ {
			if len(seek) != len(k) {
				seek = append(append(seek[:0], k[:len(k)-8]...), hi.startTxKey[:]...)
				continue
			}
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

func (hi *HistoryChangesIterDB) Next() ([]byte, []byte, uint64, error) {
	if hi.err != nil {
		return nil, nil, 0, hi.err
	}
	hi.limit--
	hi.k, hi.v, hi.step = hi.nextKey, hi.nextVal, hi.nextStep
	if err := hi.advance(); err != nil {
		return nil, nil, 0, err
	}
	return hi.k, hi.v, hi.step, nil
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
				compressVals: h.compression&CompressVals != 0,
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
	if hc.h.historyLargeValues {
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
		var it iter.KV
		var err error
		if asc {
			it, err = roTx.RangeAscend(hc.h.historyValsTable, from, to, limit)
		} else {
			it, err = roTx.RangeDescend(hc.h.historyValsTable, from, to, limit)
		}
		if err != nil {
			return nil, err
		}
		dbIt = iter.TransformKV2U64(it, func(k, v []byte) (uint64, error) {
			if len(k) < 8 {
				return 0, fmt.Errorf("unexpected large key length %d", len(k))
			}
			return binary.BigEndian.Uint64(k[len(k)-8:]), nil
		})
	} else {
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
		dbIt = iter.TransformKV2U64(it, func(k, v []byte) (uint64, error) {
			if len(v) < 8 {
				return 0, fmt.Errorf("unexpected small value length %d", len(v))
			}
			return binary.BigEndian.Uint64(v), nil
		})
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
