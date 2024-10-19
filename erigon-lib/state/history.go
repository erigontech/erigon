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
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	btree2 "github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/bitmapdb"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
	"github.com/erigontech/erigon-lib/seg"
)

type History struct {
	*InvertedIndex // indexKeysTable contains mapping txNum -> key1+key2, while index table `key -> {txnums}` is omitted.

	// dirtyFiles - list of ALL files - including: un-indexed-yet, garbage, merged-into-bigger-one, ...
	// thread-safe, but maybe need 1 RWLock for all trees in Aggregator
	//
	// _visibleFiles derivative from field `file`, but without garbage:
	//  - no files with `canDelete=true`
	//  - no overlaps
	//  - no un-indexed files (`power-off` may happen between .ef and .efi creation)
	//
	// BeginRo() using _visibleFiles in zero-copy way
	dirtyFiles *btree2.BTreeG[*filesItem]

	// _visibleFiles - underscore in name means: don't use this field directly, use BeginFilesRo()
	// underlying array is immutable - means it's ready for zero-copy use
	_visibleFiles []visibleFile

	indexList idxList

	// Schema:
	//  .v - list of values
	//  .vi - txNum+key -> offset in .v

	historyValsTable string // key1+key2+txnNum -> oldValue , stores values BEFORE change

	compressCfg seg.Cfg
	compression seg.FileCompression

	//TODO: re-visit this check - maybe we don't need it. It's about kill in the middle of merge
	integrityCheck func(fromStep, toStep uint64) bool

	// not large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2 -> txNum + value (DupSort)
	// large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2+txNum -> value (not DupSort)
	historyLargeValues bool // can't use DupSort optimization (aka. prefix-compression) if values size > 4kb

	snapshotsDisabled bool   // don't produce .v and .ef files, keep in db table. old data will be pruned anyway.
	historyDisabled   bool   // skip all write operations to this History (even in DB)
	keepRecentTxnInDB uint64 // When dontProduceHistoryFiles=true, keepRecentTxInDB is used to keep this amount of txn in db before pruning
}

type histCfg struct {
	iiCfg       iiCfg
	compression seg.FileCompression

	//historyLargeValues: used to store values > 2kb (pageSize/2)
	//small values - can be stored in more compact ways in db (DupSort feature)
	//historyLargeValues=true - doesn't support keys of various length (all keys must have same length)
	historyLargeValues bool

	withLocalityIndex  bool
	withExistenceIndex bool // move to iiCfg

	snapshotsDisabled bool   // don't produce .v and .ef files. old data will be pruned anyway.
	keepTxInDB        uint64 // When dontProduceHistoryFiles=true, keepTxInDB is used to keep this amount of txn in db before pruning
}

func NewHistory(cfg histCfg, aggregationStep uint64, filenameBase, indexKeysTable, indexTable, historyValsTable string, integrityCheck func(fromStep, toStep uint64) bool, logger log.Logger) (*History, error) {
	compressCfg := seg.DefaultCfg
	compressCfg.Workers = 1
	h := History{
		dirtyFiles:         btree2.NewBTreeGOptions[*filesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		historyValsTable:   historyValsTable,
		compression:        cfg.compression,
		compressCfg:        compressCfg,
		indexList:          withHashMap,
		integrityCheck:     integrityCheck,
		historyLargeValues: cfg.historyLargeValues,
		snapshotsDisabled:  cfg.snapshotsDisabled,
		keepRecentTxnInDB:  cfg.keepTxInDB,
	}
	h._visibleFiles = []visibleFile{}
	var err error
	h.InvertedIndex, err = NewInvertedIndex(cfg.iiCfg, aggregationStep, filenameBase, indexKeysTable, indexTable, func(fromStep, toStep uint64) bool {
		exists, err := dir.FileExist(h.vFilePath(fromStep, toStep))
		if err != nil {
			panic(err)
		}
		return exists
	}, logger)
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

// openList - main method to open list of files.
// It's ok if some files was open earlier.
// If some file already open: noop.
// If some file already open but not in provided list: close and remove from `files` field.
func (h *History) openList(idxFiles, histNames []string) error {
	if err := h.InvertedIndex.openList(idxFiles); err != nil {
		return err
	}

	h.closeWhatNotInList(histNames)
	h.scanDirtyFiles(histNames)
	if err := h.openDirtyFiles(); err != nil {
		return fmt.Errorf("History(%s).openList: %w", h.filenameBase, err)
	}
	return nil
}

func (h *History) openFolder() error {
	idxFiles, histFiles, _, err := h.fileNamesOnDisk()
	if err != nil {
		return err
	}
	return h.openList(idxFiles, histFiles)
}

// scanDirtyFiles
// returns `uselessFiles` where file "is useless" means: it's subset of frozen file. such files can be safely deleted. subset of non-frozen file may be useful
func (h *History) scanDirtyFiles(fNames []string) (garbageFiles []*filesItem) {
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

		if _, has := h.dirtyFiles.Get(newFile); has {
			continue
		}
		h.dirtyFiles.Set(newFile)
	}
	return garbageFiles
}

func (h *History) openDirtyFiles() error {
	invalidFilesMu := sync.Mutex{}
	invalidFileItems := make([]*filesItem, 0)
	h.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			fromStep, toStep := item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep
			if item.decompressor == nil {
				fPath := h.vFilePath(fromStep, toStep)
				exists, err := dir.FileExist(fPath)
				if err != nil {
					_, fName := filepath.Split(fPath)
					h.logger.Debug("[agg] History.openDirtyFiles: FileExist", "f", fName, "err", err)
					invalidFilesMu.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFilesMu.Unlock()
					continue
				}
				if !exists {
					_, fName := filepath.Split(fPath)
					h.logger.Debug("[agg] History.openDirtyFiles: file does not exists", "f", fName)
					invalidFilesMu.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFilesMu.Unlock()
					continue
				}
				if item.decompressor, err = seg.NewDecompressor(fPath); err != nil {
					_, fName := filepath.Split(fPath)
					if errors.Is(err, &seg.ErrCompressedFileCorrupted{}) {
						h.logger.Debug("[agg] History.openDirtyFiles", "err", err, "f", fName)
						// TODO we do not restore those files so we could just remove them along with indices. Same for domains/indices.
						//      Those files will keep space on disk and closed automatically as corrupted. So better to remove them, and maybe remove downloading prohibiter to allow downloading them again?
						//
						// itemPaths := []string{
						// 	fPath,
						// 	h.vAccessorFilePath(fromStep, toStep),
						// }
						// for _, fp := range itemPaths {
						// 	err = os.Remove(fp)
						// 	if err != nil {
						// 		h.logger.Warn("[agg] History.openDirtyFiles cannot remove corrupted file", "err", err, "f", fp)
						// 	}
						// }
					} else {
						h.logger.Warn("[agg] History.openDirtyFiles", "err", err, "f", fName)
					}
					invalidFilesMu.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFilesMu.Unlock()
					// don't interrupt on error. other files may be good. but skip indices open.
					continue
				}
			}

			if item.index == nil {
				fPath := h.vAccessorFilePath(fromStep, toStep)
				exists, err := dir.FileExist(fPath)
				if err != nil {
					_, fName := filepath.Split(fPath)
					h.logger.Warn("[agg] History.openDirtyFiles", "err", err, "f", fName)
				}
				if exists {
					if item.index, err = recsplit.OpenIndex(fPath); err != nil {
						_, fName := filepath.Split(fPath)
						h.logger.Warn("[agg] History.openDirtyFiles", "err", err, "f", fName)
						// don't interrupt on error. other files may be good
					}
				}
			}
		}
		return true
	})
	for _, item := range invalidFileItems {
		item.closeFiles()
		h.dirtyFiles.Delete(item)
	}

	return nil
}

func (h *History) closeWhatNotInList(fNames []string) {
	protectFiles := make(map[string]struct{}, len(fNames))
	for _, f := range fNames {
		protectFiles[f] = struct{}{}
	}
	var toClose []*filesItem
	h.dirtyFiles.Walk(func(items []*filesItem) bool {
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
		h.dirtyFiles.Delete(item)
	}
}

func (h *History) Close() {
	if h == nil {
		return
	}
	h.InvertedIndex.Close()
	h.closeWhatNotInList([]string{})
}

func (ht *HistoryRoTx) Files() (res []string) {
	for _, item := range ht.files {
		if item.src.decompressor != nil {
			res = append(res, item.src.decompressor.FileName())
		}
	}
	return append(res, ht.iit.Files()...)
}

func (h *History) missedAccessors() (l []*filesItem) {
	h.dirtyFiles.Walk(func(items []*filesItem) bool { // don't run slow logic while iterating on btree
		for _, item := range items {
			fromStep, toStep := item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep
			exists, err := dir.FileExist(h.vAccessorFilePath(fromStep, toStep))
			if err != nil {
				_, fName := filepath.Split(h.vAccessorFilePath(fromStep, toStep))
				h.logger.Warn("[agg] History.missedAccessors", "err", err, "f", fName)
			}
			if !exists {
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
	iiItem, ok := h.InvertedIndex.dirtyFiles.Get(search)
	if !ok {
		return nil
	}

	if iiItem.decompressor == nil {
		return fmt.Errorf("buildVI: got iiItem with nil decompressor %s %d-%d", h.filenameBase, item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep)
	}
	fromStep, toStep := item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep
	idxPath := h.vAccessorFilePath(fromStep, toStep)

	_, err = h.buildVI(ctx, idxPath, item.decompressor, iiItem.decompressor, ps)
	if err != nil {
		return fmt.Errorf("buildVI: %w", err)
	}
	return nil
}

func (h *History) buildVI(ctx context.Context, historyIdxPath string, hist, efHist *seg.Decompressor, ps *background.ProgressSet) (string, error) {
	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   hist.Count(),
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     h.dirs.Tmp,
		IndexFile:  historyIdxPath,
		Salt:       h.salt,
		NoFsync:    h.noFsync,
	}, h.logger)
	if err != nil {
		return "", fmt.Errorf("create recsplit: %w", err)
	}
	defer rs.Close()
	rs.LogLvl(log.LvlTrace)

	var historyKey []byte
	var txKey [8]byte
	var valOffset uint64

	_, fName := filepath.Split(historyIdxPath)
	p := ps.AddNew(fName, uint64(hist.Count()))
	defer ps.Delete(p)

	defer hist.EnableReadAhead().DisableReadAhead()
	defer efHist.EnableReadAhead().DisableReadAhead()

	var keyBuf, valBuf []byte
	histReader := seg.NewReader(hist.MakeGetter(), h.compression)
	efHistReader := seg.NewReader(efHist.MakeGetter(), h.InvertedIndex.compression)

	for {
		histReader.Reset(0)
		efHistReader.Reset(0)

		valOffset = 0
		for efHistReader.HasNext() {
			keyBuf, _ = efHistReader.Next(nil)
			valBuf, _ = efHistReader.Next(nil)

			// fmt.Printf("ef key %x\n", keyBuf)

			ef, _ := eliasfano32.ReadEliasFano(valBuf)
			efIt := ef.Iterator()
			for efIt.HasNext() {
				txNum, err := efIt.Next()
				if err != nil {
					return "", err
				}
				binary.BigEndian.PutUint64(txKey[:], txNum)
				historyKey = append(append(historyKey[:0], txKey[:]...), keyBuf...)
				if err = rs.AddKey(historyKey, valOffset); err != nil {
					return "", err
				}
				valOffset, _ = histReader.Skip()
				p.Processed.Add(1)
			}

			select {
			case <-ctx.Done():
				return "", ctx.Err()
			default:
			}
		}

		if err = rs.Build(ctx); err != nil {
			if rs.Collision() {
				log.Info("Building recsplit. Collision happened. It's ok. Restarting...")
				rs.ResetNextSalt()
			} else {
				return "", fmt.Errorf("build idx: %w", err)
			}
		} else {
			break
		}
	}
	return historyIdxPath, nil
}

func (h *History) BuildMissedAccessors(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet) {
	h.InvertedIndex.BuildMissedAccessors(ctx, g, ps)
	missedFiles := h.missedAccessors()
	for _, item := range missedFiles {
		item := item
		g.Go(func() error {
			return h.buildVi(ctx, item, ps)
		})
	}
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

func (ht *HistoryRoTx) NewWriter() *historyBufferedWriter {
	return ht.newWriter(ht.h.dirs.Tmp, false)
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

func (ht *HistoryRoTx) newWriter(tmpdir string, discard bool) *historyBufferedWriter {
	w := &historyBufferedWriter{
		discard: discard,

		historyKey:       make([]byte, 128),
		largeValues:      ht.h.historyLargeValues,
		historyValsTable: ht.h.historyValsTable,
		historyVals:      etl.NewCollector(ht.h.filenameBase+".flush.hist", tmpdir, etl.NewSortableBuffer(WALCollectorRAM), ht.h.logger).LogLvl(log.LvlTrace),

		ii: ht.iit.newWriter(tmpdir, discard),
	}
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
	historyComp   *seg.Writer
	efHistoryComp *seg.Writer
	historyPath   string
	efHistoryPath string
	historyCount  int // same as historyComp.Count()
}

func (c HistoryCollation) Close() {
	if c.historyComp != nil {
		c.historyComp.Close()
	}
	if c.efHistoryComp != nil {
		c.efHistoryComp.Close()
	}
}

// [txFrom; txTo)
func (h *History) collate(ctx context.Context, step, txFrom, txTo uint64, roTx kv.Tx) (HistoryCollation, error) {
	if h.snapshotsDisabled {
		return HistoryCollation{}, nil
	}

	var (
		historyComp   *seg.Writer
		efHistoryComp *seg.Writer
		txKey         [8]byte
		err           error

		historyPath   = h.vFilePath(step, step+1)
		efHistoryPath = h.efFilePath(step, step+1)
		startAt       = time.Now()
		closeComp     = true
	)
	defer func() {
		mxCollateTookHistory.ObserveDuration(startAt)
		if closeComp {
			if historyComp != nil {
				historyComp.Close()
			}
			if efHistoryComp != nil {
				efHistoryComp.Close()
			}
		}
	}()

	comp, err := seg.NewCompressor(ctx, "collate hist "+h.filenameBase, historyPath, h.dirs.Tmp, h.compressCfg, log.LvlTrace, h.logger)
	if err != nil {
		return HistoryCollation{}, fmt.Errorf("create %s history compressor: %w", h.filenameBase, err)
	}
	historyComp = seg.NewWriter(comp, h.compression)

	keysCursor, err := roTx.CursorDupSort(h.indexKeysTable)
	if err != nil {
		return HistoryCollation{}, fmt.Errorf("create %s history cursor: %w", h.filenameBase, err)
	}
	defer keysCursor.Close()

	binary.BigEndian.PutUint64(txKey[:], txFrom)
	collector := etl.NewCollector(h.filenameBase+".collate.hist", h.iiCfg.dirs.Tmp, etl.NewSortableBuffer(CollateETLRAM), h.logger).LogLvl(log.LvlTrace)
	defer collector.Close()

	for txnmb, k, err := keysCursor.Seek(txKey[:]); txnmb != nil; txnmb, k, err = keysCursor.Next() {
		if err != nil {
			return HistoryCollation{}, fmt.Errorf("iterate over %s history cursor: %w", h.filenameBase, err)
		}
		txNum := binary.BigEndian.Uint64(txnmb)
		if txNum >= txTo { // [txFrom; txTo)
			break
		}
		if err := collector.Collect(k, txnmb); err != nil {
			return HistoryCollation{}, fmt.Errorf("collect %s history key [%x]=>txn %d [%x]: %w", h.filenameBase, k, txNum, txnmb, err)
		}

		select {
		case <-ctx.Done():
			return HistoryCollation{}, ctx.Err()
		default:
		}
	}

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

	efComp, err := seg.NewCompressor(ctx, "collate idx "+h.filenameBase, efHistoryPath, h.dirs.Tmp, h.compressCfg, log.LvlTrace, h.logger)
	if err != nil {
		return HistoryCollation{}, fmt.Errorf("create %s ef history compressor: %w", h.filenameBase, err)
	}
	if h.noFsync {
		efComp.DisableFsync()
	}

	var (
		keyBuf      = make([]byte, 0, 256)
		numBuf      = make([]byte, 8)
		bitmap      = bitmapdb.NewBitmap64()
		prevEf      []byte
		prevKey     []byte
		initialized bool
	)
	efHistoryComp = seg.NewWriter(efComp, seg.CompressNone) // coll+build must be fast - no compression
	collector.SortAndFlushInBackground(true)
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

		ef := eliasfano32.NewEliasFano(bitmap.GetCardinality(), bitmap.Maximum())
		it := bitmap.Iterator()

		for it.HasNext() {
			vTxNum := it.Next()
			binary.BigEndian.PutUint64(numBuf, vTxNum)
			if h.historyLargeValues {
				keyBuf = append(append(keyBuf[:0], prevKey...), numBuf...)
				key, val, err := c.SeekExact(keyBuf)
				if err != nil {
					return fmt.Errorf("seekExact %s history val [%x]: %w", h.filenameBase, key, err)
				}
				if len(val) == 0 {
					val = nil
				}
				if err = historyComp.AddWord(val); err != nil {
					return fmt.Errorf("add %s history val [%x]=>[%x]: %w", h.filenameBase, key, val, err)
				}
			} else {
				val, err := cd.SeekBothRange(prevKey, numBuf)
				if err != nil {
					return fmt.Errorf("seekBothRange %s history val [%x]: %w", h.filenameBase, prevKey, err)
				}
				if val != nil && binary.BigEndian.Uint64(val) == vTxNum {
					val = val[8:]
				} else {
					val = nil
				}
				if err = historyComp.AddWord(val); err != nil {
					return fmt.Errorf("add %s history val [%x]=>[%x]: %w", h.filenameBase, prevKey, val, err)
				}
			}

			ef.AddOffset(vTxNum)
		}
		bitmap.Clear()
		ef.Build()

		prevEf = ef.AppendBytes(prevEf[:0])

		if err = efHistoryComp.AddWord(prevKey); err != nil {
			return fmt.Errorf("add %s ef history key [%x]: %w", h.filenameBase, prevKey, err)
		}
		if err = efHistoryComp.AddWord(prevEf); err != nil {
			return fmt.Errorf("add %s ef history val: %w", h.filenameBase, err)
		}

		prevKey = append(prevKey[:0], k...)
		txNum = binary.BigEndian.Uint64(v)
		bitmap.Add(txNum)

		return nil
	}

	err = collector.Load(nil, "", loadBitmapsFunc, etl.TransformArgs{Quit: ctx.Done()})
	if err != nil {
		return HistoryCollation{}, err
	}
	if !bitmap.IsEmpty() {
		if err = loadBitmapsFunc(nil, make([]byte, 8), nil, nil); err != nil {
			return HistoryCollation{}, err
		}
	}

	closeComp = false
	mxCollationSizeHist.SetUint64(uint64(historyComp.Count()))

	return HistoryCollation{
		efHistoryComp: efHistoryComp,
		efHistoryPath: efHistoryPath,
		historyPath:   historyPath,
		historyComp:   historyComp,
		historyCount:  historyComp.Count(),
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
func (h *History) reCalcVisibleFiles(toTxNum uint64) {
	h._visibleFiles = calcVisibleFiles(h.dirtyFiles, h.indexList, false, toTxNum)
	h.InvertedIndex.reCalcVisibleFiles(toTxNum)
}

// buildFiles performs potentially resource intensive operations of creating
// static files and their indices
func (h *History) buildFiles(ctx context.Context, step uint64, collation HistoryCollation, ps *background.ProgressSet) (HistoryFiles, error) {
	if h.snapshotsDisabled {
		return HistoryFiles{}, nil
	}
	var (
		historyDecomp, efHistoryDecomp *seg.Decompressor
		historyIdx, efHistoryIdx       *recsplit.Index

		efExistence *ExistenceFilter
		closeComp   = true
		err         error
	)

	defer func() {
		if closeComp {
			collation.Close()

			if historyDecomp != nil {
				historyDecomp.Close()
			}
			if historyIdx != nil {
				historyIdx.Close()
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
		}
	}()

	if h.noFsync {
		collation.historyComp.DisableFsync()
		collation.efHistoryComp.DisableFsync()
	}

	{
		ps := background.NewProgressSet()
		_, efHistoryFileName := filepath.Split(collation.efHistoryPath)
		p := ps.AddNew(efHistoryFileName, 1)
		defer ps.Delete(p)

		if err = collation.efHistoryComp.Compress(); err != nil {
			return HistoryFiles{}, fmt.Errorf("compress %s .ef history: %w", h.filenameBase, err)
		}
		ps.Delete(p)
	}
	{
		_, historyFileName := filepath.Split(collation.historyPath)
		p := ps.AddNew(historyFileName, 1)
		defer ps.Delete(p)
		if err = collation.historyComp.Compress(); err != nil {
			return HistoryFiles{}, fmt.Errorf("compress %s .v history: %w", h.filenameBase, err)
		}
		ps.Delete(p)
	}
	collation.Close()

	efHistoryDecomp, err = seg.NewDecompressor(collation.efHistoryPath)
	if err != nil {
		return HistoryFiles{}, fmt.Errorf("open %s .ef history decompressor: %w", h.filenameBase, err)
	}
	{
		if err := h.InvertedIndex.buildMapAccessor(ctx, step, step+1, efHistoryDecomp, ps); err != nil {
			return HistoryFiles{}, fmt.Errorf("build %s .ef history idx: %w", h.filenameBase, err)
		}
		if efHistoryIdx, err = recsplit.OpenIndex(h.InvertedIndex.efAccessorFilePath(step, step+1)); err != nil {
			return HistoryFiles{}, err
		}
	}

	historyDecomp, err = seg.NewDecompressor(collation.historyPath)
	if err != nil {
		return HistoryFiles{}, fmt.Errorf("open %s v history decompressor: %w", h.filenameBase, err)
	}

	historyIdxPath := h.vAccessorFilePath(step, step+1)
	historyIdxPath, err = h.buildVI(ctx, historyIdxPath, historyDecomp, efHistoryDecomp, ps)
	if err != nil {
		return HistoryFiles{}, fmt.Errorf("build %s .vi: %w", h.filenameBase, err)
	}

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

func (h *History) integrateDirtyFiles(sf HistoryFiles, txNumFrom, txNumTo uint64) {
	if h.snapshotsDisabled {
		return
	}

	h.InvertedIndex.integrateDirtyFiles(InvertedFiles{
		decomp:    sf.efHistoryDecomp,
		index:     sf.efHistoryIdx,
		existence: sf.efExistence,
	}, txNumFrom, txNumTo)

	fi := newFilesItem(txNumFrom, txNumTo, h.aggregationStep)
	fi.decompressor = sf.historyDecomp
	fi.index = sf.historyIdx
	h.dirtyFiles.Set(fi)
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

type HistoryRoTx struct {
	h   *History
	iit *InvertedIndexRoTx

	files   visibleFiles // have no garbage (canDelete=true, overlaps, etc...)
	getters []*seg.Reader
	readers []*recsplit.IndexReader

	trace bool

	valsC    kv.Cursor
	valsCDup kv.CursorDupSort

	_bufTs []byte
}

func (h *History) BeginFilesRo() *HistoryRoTx {
	files := h._visibleFiles
	for i := 0; i < len(files); i++ {
		if !files[i].src.frozen {
			files[i].src.refcount.Add(1)
		}
	}

	return &HistoryRoTx{
		h:     h,
		iit:   h.InvertedIndex.BeginFilesRo(),
		files: files,
		trace: false,
	}
}

func (ht *HistoryRoTx) statelessGetter(i int) *seg.Reader {
	if ht.getters == nil {
		ht.getters = make([]*seg.Reader, len(ht.files))
	}
	r := ht.getters[i]
	if r == nil {
		g := ht.files[i].src.decompressor.MakeGetter()
		r = seg.NewReader(g, ht.h.compression)
		ht.getters[i] = r
	}
	return r
}
func (ht *HistoryRoTx) statelessIdxReader(i int) *recsplit.IndexReader {
	if ht.readers == nil {
		ht.readers = make([]*recsplit.IndexReader, len(ht.files))
	}
	{
		//assert
		for _, f := range ht.files {
			if f.src.index == nil {
				panic("assert: file has nil index " + f.src.decompressor.FileName())
			}
		}
	}
	r := ht.readers[i]
	if r == nil {
		r = ht.files[i].src.index.GetReaderFromPool()
		ht.readers[i] = r
	}
	return r
}

func (ht *HistoryRoTx) canPruneUntil(tx kv.Tx, untilTx uint64) (can bool, txTo uint64) {
	minIdxTx, maxIdxTx := ht.iit.ii.minTxNumInDB(tx), ht.iit.ii.maxTxNumInDB(tx)
	//defer func() {
	//	fmt.Printf("CanPrune[%s]Until(%d) noFiles=%t txTo %d idxTx [%d-%d] keepRecentTxInDB=%d; result %t\n",
	//		ht.h.filenameBase, untilTx, ht.h.dontProduceHistoryFiles, txTo, minIdxTx, maxIdxTx, ht.h.keepRecentTxInDB, minIdxTx < txTo)
	//}()

	if ht.h.snapshotsDisabled {
		if ht.h.keepRecentTxnInDB >= maxIdxTx {
			return false, 0
		}
		txTo = min(maxIdxTx-ht.h.keepRecentTxnInDB, untilTx) // bound pruning
	} else {
		canPruneIdx := ht.iit.CanPrune(tx)
		if !canPruneIdx {
			return false, 0
		}
		txTo = min(ht.files.EndTxNum(), ht.iit.files.EndTxNum(), untilTx)
	}

	switch ht.h.filenameBase {
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

// Prune [txFrom; txTo)
// `force` flag to prune even if canPruneUntil returns false (when Unwind is needed, canPruneUntil always returns false)
// `useProgress` flag to restore and update prune progress.
//   - E.g. Unwind can't use progress, because it's not linear
//     and will wrongly update progress of steps cleaning and could end up with inconsistent history.
func (ht *HistoryRoTx) Prune(ctx context.Context, rwTx kv.RwTx, txFrom, txTo, limit uint64, forced bool, logEvery *time.Ticker) (*InvertedIndexPruneStat, error) {
	//fmt.Printf(" pruneH[%s] %t, %d-%d\n", ht.h.filenameBase, ht.CanPruneUntil(rwTx), txFrom, txTo)
	if !forced {
		var can bool
		can, txTo = ht.canPruneUntil(rwTx, txTo)
		if !can {
			return nil, nil
		}
	}
	defer func(t time.Time) { mxPruneTookHistory.ObserveDuration(t) }(time.Now())

	var (
		seek     = make([]byte, 8, 256)
		valsCDup kv.RwCursorDupSort
		valsC    kv.RwCursor
		err      error
	)

	if !ht.h.historyLargeValues {
		valsCDup, err = rwTx.RwCursorDupSort(ht.h.historyValsTable)
		if err != nil {
			return nil, err
		}
		defer valsCDup.Close()
	} else {
		valsC, err = rwTx.RwCursor(ht.h.historyValsTable)
		if err != nil {
			return nil, err
		}
		defer valsC.Close()
	}

	var pruned int
	pruneValue := func(k, txnm []byte) error {
		txNum := binary.BigEndian.Uint64(txnm)
		if txNum >= txTo || txNum < txFrom { //[txFrom; txTo), but in this case idx record
			return fmt.Errorf("history pruneValue: txNum %d not in pruning range [%d,%d)", txNum, txFrom, txTo)
		}

		if ht.h.historyLargeValues {
			seek = append(append(seek[:0], k...), txnm...)
			if err := valsC.Delete(seek); err != nil {
				return err
			}
		} else {
			vv, err := valsCDup.SeekBothRange(k, txnm)
			if err != nil {
				return err
			}
			if vtx := binary.BigEndian.Uint64(vv); vtx != txNum {
				return fmt.Errorf("prune history %s got invalid txNum: found %d != %d wanted", ht.h.filenameBase, vtx, txNum)
			}
			if err = valsCDup.DeleteCurrent(); err != nil {
				return err
			}
		}

		pruned++
		return nil
	}
	mxPruneSizeHistory.AddInt(pruned)

	if !forced && ht.h.snapshotsDisabled {
		forced = true // or index.CanPrune will return false cuz no snapshots made
	}

	return ht.iit.Prune(ctx, rwTx, txFrom, txTo, limit, logEvery, forced, pruneValue)
}

func (ht *HistoryRoTx) Close() {
	if ht.files == nil { // invariant: it's safe to call Close multiple times
		return
	}
	files := ht.files
	ht.files = nil
	for i := 0; i < len(files); i++ {
		src := files[i].src
		if src == nil || src.frozen {
			continue
		}
		refCnt := src.refcount.Add(-1)
		//GC: last reader responsible to remove useles files: close it and delete
		if refCnt == 0 && src.canDelete.Load() {
			if traceFileLife != "" && ht.h.filenameBase == traceFileLife {
				ht.h.logger.Warn("[agg.dbg] real remove at HistoryRoTx.Close", "file", src.decompressor.FileName())
			}
			src.closeFilesAndRemove()
		}
	}
	for _, r := range ht.readers {
		r.Close()
	}

	ht.iit.Close()
}

func (ht *HistoryRoTx) getFileDeprecated(from, to uint64) (it visibleFile, ok bool) {
	for i := 0; i < len(ht.files); i++ {
		if ht.files[i].startTxNum == from && ht.files[i].endTxNum == to {
			return ht.files[i], true
		}
	}
	return it, false
}
func (ht *HistoryRoTx) getFile(txNum uint64) (it visibleFile, ok bool) {
	for i := 0; i < len(ht.files); i++ {
		if ht.files[i].startTxNum <= txNum && ht.files[i].endTxNum > txNum {
			return ht.files[i], true
		}
	}
	return it, false
}

func (ht *HistoryRoTx) historySeekInFiles(key []byte, txNum uint64) ([]byte, bool, error) {
	// Files list of II and History is different
	// it means II can't return index of file, but can return TxNum which History will use to find own file
	ok, histTxNum := ht.iit.seekInFiles(key, txNum)
	if !ok {
		return nil, false, nil
	}
	historyItem, ok := ht.getFile(histTxNum)
	if !ok {
		return nil, false, fmt.Errorf("hist file not found: key=%x, %s.%d-%d", key, ht.h.filenameBase, histTxNum/ht.h.aggregationStep, histTxNum/ht.h.aggregationStep)
	}
	reader := ht.statelessIdxReader(historyItem.i)
	if reader.Empty() {
		return nil, false, nil
	}
	offset, ok := reader.Lookup(ht.encodeTs(histTxNum, key))
	if !ok {
		return nil, false, nil
	}
	g := ht.statelessGetter(historyItem.i)
	g.Reset(offset)

	v, _ := g.Next(nil)
	if traceGetAsOf == ht.h.filenameBase {
		fmt.Printf("GetAsOf(%s, %x, %d) -> %s, histTxNum=%d, isNil(v)=%t\n", ht.h.filenameBase, key, txNum, g.FileName(), histTxNum, v == nil)
	}
	return v, true, nil
}

func (hs *HistoryStep) GetNoState(key []byte, txNum uint64) ([]byte, bool, uint64) {
	//fmt.Printf("historySeekInFiles [%x] %d\n", key, txNum)
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

func (ht *HistoryRoTx) encodeTs(txNum uint64, key []byte) []byte {
	if ht._bufTs == nil {
		ht._bufTs = make([]byte, 8+len(key))
	}
	binary.BigEndian.PutUint64(ht._bufTs, txNum)
	ht._bufTs = append(ht._bufTs[:8], key...)
	return ht._bufTs[:8+len(key)]
}

// HistorySeek searches history for a value of specified key before txNum
// second return value is true if the value is found in the history (even if it is nil)
func (ht *HistoryRoTx) HistorySeek(key []byte, txNum uint64, roTx kv.Tx) ([]byte, bool, error) {
	v, ok, err := ht.historySeekInFiles(key, txNum)
	if err != nil {
		return nil, ok, err
	}
	if ok {
		return v, true, nil
	}

	return ht.historySeekInDB(key, txNum, roTx)
}

func (ht *HistoryRoTx) valsCursor(tx kv.Tx) (c kv.Cursor, err error) {
	if ht.valsC != nil {
		return ht.valsC, nil
	}
	ht.valsC, err = tx.Cursor(ht.h.historyValsTable)
	if err != nil {
		return nil, err
	}
	return ht.valsC, nil
}
func (ht *HistoryRoTx) valsCursorDup(tx kv.Tx) (c kv.CursorDupSort, err error) {
	if ht.valsCDup != nil {
		return ht.valsCDup, nil
	}
	ht.valsCDup, err = tx.CursorDupSort(ht.h.historyValsTable)
	if err != nil {
		return nil, err
	}
	return ht.valsCDup, nil
}

func (ht *HistoryRoTx) historySeekInDB(key []byte, txNum uint64, tx kv.Tx) ([]byte, bool, error) {
	if ht.h.historyLargeValues {
		c, err := ht.valsCursor(tx)
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
	c, err := ht.valsCursorDup(tx)
	if err != nil {
		return nil, false, err
	}
	val, err := c.SeekBothRange(key, ht.encodeTs(txNum, nil))
	if err != nil {
		return nil, false, err
	}
	if val == nil {
		return nil, false, nil
	}
	// `val == []byte{}` means key was created in this txNum and doesn't exist before.
	return val[8:], true, nil
}
func (ht *HistoryRoTx) WalkAsOf(ctx context.Context, startTxNum uint64, from, to []byte, roTx kv.Tx, limit int) (stream.KV, error) {
	hi := &StateAsOfIterF{
		from: from, to: to, limit: limit,

		hc:         ht,
		startTxNum: startTxNum,

		ctx: ctx,
	}
	for _, item := range ht.iit.files {
		if item.endTxNum <= startTxNum {
			continue
		}
		// TODO: seek(from)
		g := seg.NewReader(item.src.decompressor.MakeGetter(), ht.h.compression)
		g.Reset(0)
		if g.HasNext() {
			key, offset := g.Next(nil)
			heap.Push(&hi.h, &ReconItem{g: g, key: key, startTxNum: item.startTxNum, endTxNum: item.endTxNum, txNum: item.endTxNum, startOffset: offset, lastOffset: offset})
		}
	}
	binary.BigEndian.PutUint64(hi.startTxKey[:], startTxNum)
	if err := hi.advanceInFiles(); err != nil {
		hi.Close() //it's responsibility of constructor (our) to close resource on error
		return nil, err
	}

	dbit := &StateAsOfIterDB{
		largeValues: ht.h.historyLargeValues,
		roTx:        roTx,
		valsTable:   ht.h.historyValsTable,
		from:        from, to: to, limit: limit,

		startTxNum: startTxNum,

		ctx: ctx,
	}
	binary.BigEndian.PutUint64(dbit.startTxKey[:], startTxNum)
	if err := dbit.advance(); err != nil {
		dbit.Close() //it's responsibility of constructor (our) to close resource on error
		return nil, err
	}
	return stream.UnionKV(hi, dbit, limit), nil
}

// StateAsOfIter - returns state range at given time in history
type StateAsOfIterF struct {
	hc    *HistoryRoTx
	limit int

	from, to []byte
	nextVal  []byte
	nextKey  []byte

	h          ReconHeap
	startTxNum uint64
	startTxKey [8]byte
	txnKey     [8]byte

	k, v, kBackup, vBackup []byte

	ctx context.Context
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

		if hi.from != nil && bytes.Compare(key, hi.from) < 0 { //TODO: replace by seekInFiles()
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
	select {
	case <-hi.ctx.Done():
		return nil, nil, hi.ctx.Err()
	default:
	}

	hi.limit--
	hi.k, hi.v = append(hi.k[:0], hi.nextKey...), append(hi.v[:0], hi.nextVal...)

	// Satisfy stream.Duo Invariant 2
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

	ctx context.Context
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
	select {
	case <-hi.ctx.Done():
		return nil, nil, hi.ctx.Err()
	default:
	}

	if hi.err != nil {
		return nil, nil, hi.err
	}
	hi.limit--
	hi.k, hi.v = hi.nextKey, hi.nextVal

	// Satisfy stream.Duo Invariant 2
	hi.k, hi.kBackup, hi.v, hi.vBackup = hi.kBackup, hi.k, hi.vBackup, hi.v
	if err := hi.advance(); err != nil {
		return nil, nil, err
	}
	return hi.kBackup, hi.vBackup, nil
}

func (ht *HistoryRoTx) iterateChangedFrozen(fromTxNum, toTxNum int, asc order.By, limit int) (stream.KV, error) {
	if asc == false {
		panic("not supported yet")
	}
	if len(ht.iit.files) == 0 {
		return stream.EmptyKV, nil
	}

	if fromTxNum >= 0 && ht.iit.files.EndTxNum() <= uint64(fromTxNum) {
		return stream.EmptyKV, nil
	}

	s := &HistoryChangesIterFiles{
		hc:         ht,
		startTxNum: max(0, uint64(fromTxNum)),
		endTxNum:   toTxNum,
		limit:      limit,
	}
	if fromTxNum >= 0 {
		binary.BigEndian.PutUint64(s.startTxKey[:], uint64(fromTxNum))
	}
	for _, item := range ht.iit.files {
		if fromTxNum >= 0 && item.endTxNum <= uint64(fromTxNum) {
			continue
		}
		if toTxNum >= 0 && item.startTxNum >= uint64(toTxNum) {
			break
		}
		g := seg.NewReader(item.src.decompressor.MakeGetter(), ht.h.compression)
		g.Reset(0)
		if g.HasNext() {
			key, offset := g.Next(nil)
			heap.Push(&s.h, &ReconItem{g: g, key: key, startTxNum: item.startTxNum, endTxNum: item.endTxNum, txNum: item.endTxNum, startOffset: offset, lastOffset: offset})
		}
	}
	if err := s.advance(); err != nil {
		s.Close() //it's responsibility of constructor (our) to close resource on error
		return nil, err
	}
	return s, nil
}

func (ht *HistoryRoTx) iterateChangedRecent(fromTxNum, toTxNum int, asc order.By, limit int, roTx kv.Tx) (stream.KVS, error) {
	if asc == order.Desc {
		panic("not supported yet")
	}
	rangeIsInFiles := toTxNum >= 0 && len(ht.iit.files) > 0 && ht.iit.files.EndTxNum() >= uint64(toTxNum)
	if rangeIsInFiles {
		return stream.EmptyKVS, nil
	}
	s := &HistoryChangesIterDB{
		endTxNum:    toTxNum,
		roTx:        roTx,
		largeValues: ht.h.historyLargeValues,
		valsTable:   ht.h.historyValsTable,
		limit:       limit,
	}
	if fromTxNum >= 0 {
		binary.BigEndian.PutUint64(s.startTxKey[:], uint64(fromTxNum))
	}
	if err := s.advance(); err != nil {
		s.Close() //it's responsibility of constructor (our) to close resource on error
		return nil, err
	}
	return s, nil
}

func (ht *HistoryRoTx) HistoryRange(fromTxNum, toTxNum int, asc order.By, limit int, roTx kv.Tx) (stream.KVS, error) {
	if asc == order.Desc {
		panic("not supported yet")
	}
	itOnFiles, err := ht.iterateChangedFrozen(fromTxNum, toTxNum, asc, limit)
	if err != nil {
		return nil, err
	}
	itOnDB, err := ht.iterateChangedRecent(fromTxNum, toTxNum, asc, limit, roTx)
	if err != nil {
		return nil, err
	}
	return stream.MergeKVS(itOnDB, itOnFiles, limit), nil
}

func (ht *HistoryRoTx) idxRangeRecent(key []byte, startTxNum, endTxNum int, asc order.By, limit int, roTx kv.Tx) (stream.U64, error) {
	var dbIt stream.U64
	if ht.h.historyLargeValues {
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
		var it stream.KV
		var err error
		if asc {
			it, err = roTx.RangeAscend(ht.h.historyValsTable, from, to, limit)
		} else {
			it, err = roTx.RangeDescend(ht.h.historyValsTable, from, to, limit)
		}
		if err != nil {
			return nil, err
		}
		dbIt = stream.TransformKV2U64(it, func(k, v []byte) (uint64, error) {
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
		it, err := roTx.RangeDupSort(ht.h.historyValsTable, key, from, to, asc, limit)
		if err != nil {
			return nil, err
		}
		dbIt = stream.TransformKV2U64(it, func(k, v []byte) (uint64, error) {
			if len(v) < 8 {
				return 0, fmt.Errorf("unexpected small value length %d", len(v))
			}
			return binary.BigEndian.Uint64(v), nil
		})
	}

	return dbIt, nil
}
func (ht *HistoryRoTx) IdxRange(key []byte, startTxNum, endTxNum int, asc order.By, limit int, roTx kv.Tx) (stream.U64, error) {
	frozenIt, err := ht.iit.iterateRangeFrozen(key, startTxNum, endTxNum, asc, limit)
	if err != nil {
		return nil, err
	}
	recentIt, err := ht.idxRangeRecent(key, startTxNum, endTxNum, asc, limit, roTx)
	if err != nil {
		return nil, err
	}
	return stream.Union[uint64](frozenIt, recentIt, asc, limit), nil
}

type HistoryChangesIterFiles struct {
	hc         *HistoryRoTx
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

	// Satisfy iter.Duo Invariant 2
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
	indexFile    visibleFile
	historyItem  *filesItem
	historyFile  visibleFile
}

// MakeSteps [0, toTxNum)
func (h *History) MakeSteps(toTxNum uint64) []*HistoryStep {
	var steps []*HistoryStep
	h.InvertedIndex.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.index == nil || !item.frozen || item.startTxNum >= toTxNum {
				continue
			}

			step := &HistoryStep{
				compressVals: h.compression&seg.CompressVals != 0,
				indexItem:    item,
				indexFile: visibleFile{
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
	h.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.index == nil || !item.frozen || item.startTxNum >= toTxNum {
				continue
			}
			steps[i].historyItem = item
			steps[i].historyFile = visibleFile{
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
		indexFile: visibleFile{
			startTxNum: hs.indexFile.startTxNum,
			endTxNum:   hs.indexFile.endTxNum,
			getter:     hs.indexItem.decompressor.MakeGetter(),
			reader:     recsplit.NewIndexReader(hs.indexItem.index),
		},
		historyItem: hs.historyItem,
		historyFile: visibleFile{
			startTxNum: hs.historyFile.startTxNum,
			endTxNum:   hs.historyFile.endTxNum,
			getter:     hs.historyItem.decompressor.MakeGetter(),
			reader:     recsplit.NewIndexReader(hs.historyItem.index),
		},
	}
}
