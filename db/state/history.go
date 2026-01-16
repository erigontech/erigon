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
	"io"
	"math"
	"path/filepath"
	"strings"
	"time"

	btree2 "github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
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
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

type History struct {
	statecfg.HistCfg // keep higher than embedded InvertedIndexis to correctly shadow it's exposed variables
	*InvertedIndex   // KeysTable contains mapping txNum -> key1+key2, while index table `key -> {txnums}` is omitted.

	// Schema:
	//  .v - list of values
	//  .vi - txNum+key -> offset in .v

	// dirtyFiles - list of ALL files - including: un-indexed-yet, garbage, merged-into-bigger-one, ...
	// thread-safe, but maybe need 1 RWLock for all trees in Aggregator
	//
	// _visibleFiles derivative from field `file`, but without garbage:
	//  - no files with `canDelete=true`
	//  - no overlaps
	//  - no un-indexed files (`power-off` may happen between .ef and .efi creation)
	//
	// BeginRo() using _visibleFiles in zero-copy way
	dirtyFiles *btree2.BTreeG[*FilesItem]

	// _visibleFiles - underscore in name means: don't use this field directly, use BeginFilesRo()
	// underlying array is immutable - means it's ready for zero-copy use
	_visibleFiles []visibleFile
}

func NewHistory(cfg statecfg.HistCfg, stepSize, stepsInFrozenFile uint64, dirs datadir.Dirs, logger log.Logger) (*History, error) {
	//if cfg.compressorCfg.MaxDictPatterns == 0 && cfg.compressorCfg.MaxPatternLen == 0 {
	if cfg.Accessors == 0 {
		cfg.Accessors = statecfg.AccessorHashMap
	}

	h := History{
		HistCfg:       cfg,
		dirtyFiles:    btree2.NewBTreeGOptions(filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		_visibleFiles: []visibleFile{},
	}

	// Validate configuration consistency
	if cfg.DBValuesOnCompressedPage > 0 && !cfg.HistoryLargeValues {
		panic(fmt.Errorf("assert: DBValuesOnCompressedPage=%d requires HistoryLargeValues=true for %s (page-level compression produces large values)",
			cfg.DBValuesOnCompressedPage, cfg.IiCfg.FilenameBase))
	}

	var err error
	h.InvertedIndex, err = NewInvertedIndex(cfg.IiCfg, stepSize, stepsInFrozenFile, dirs, logger)
	if err != nil {
		return nil, fmt.Errorf("NewHistory: %s, %w", cfg.IiCfg.FilenameBase, err)
	}

	if h.FileVersion.DataV.IsZero() {
		panic(fmt.Errorf("assert: forgot to set version of %s", h.Name))
	}
	if h.FileVersion.AccessorVI.IsZero() {
		panic(fmt.Errorf("assert: forgot to set version of %s", h.Name))
	}
	h.InvertedIndex.Name = h.HistoryIdx

	return &h, nil
}

func (h *History) vFileName(fromStep, toStep kv.Step) string {
	return fmt.Sprintf("%s-%s.%d-%d.v", h.FileVersion.DataV.String(), h.FilenameBase, fromStep, toStep)
}
func (h *History) vNewFilePath(fromStep, toStep kv.Step) string {
	return filepath.Join(h.dirs.SnapHistory, h.vFileName(fromStep, toStep))
}
func (h *History) vAccessorNewFilePath(fromStep, toStep kv.Step) string {
	return filepath.Join(h.dirs.SnapAccessors, fmt.Sprintf("%s-%s.%d-%d.vi", h.FileVersion.AccessorVI.String(), h.FilenameBase, fromStep, toStep))
}

func (h *History) vFileNameMask(fromStep, toStep kv.Step) string {
	return fmt.Sprintf("*-%s.%d-%d.v", h.FilenameBase, fromStep, toStep)
}
func (h *History) vFilePathMask(fromStep, toStep kv.Step) string {
	return filepath.Join(h.dirs.SnapHistory, h.vFileNameMask(fromStep, toStep))
}
func (h *History) vAccessorFilePathMask(fromStep, toStep kv.Step) string {
	return filepath.Join(h.dirs.SnapAccessors, fmt.Sprintf("*-%s.%d-%d.vi", h.FilenameBase, fromStep, toStep))
}

func (h *History) openHashMapAccessor(fPath string) (*recsplit.Index, error) {
	accessor, err := recsplit.OpenIndex(fPath)
	if err != nil {
		return nil, err
	}
	return accessor, nil
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
		return fmt.Errorf("History(%s).openList: %w", h.FilenameBase, err)
	}
	return nil
}

func (h *History) openFolder(scanDirsRes *ScanDirsResult) error {
	return h.openList(scanDirsRes.iiFiles, scanDirsRes.historyFiles)
}

func (h *History) scanDirtyFiles(fileNames []string) {
	if h.FilenameBase == "" {
		panic("assert: empty `filenameBase`")
	}
	if h.stepSize == 0 {
		panic("assert: empty `stepSize`")
	}
	for _, dirtyFile := range filterDirtyFiles(fileNames, h.stepSize, h.stepsInFrozenFile, h.FilenameBase, "v", h.logger) {
		if _, has := h.dirtyFiles.Get(dirtyFile); !has {
			h.dirtyFiles.Set(dirtyFile)
		}
	}
}

func (h *History) closeWhatNotInList(fNames []string) {
	closeWhatNotInList(h.dirtyFiles, fNames)
}

func (h *History) Tables() []string { return append(h.InvertedIndex.Tables(), h.ValuesTable) }

func (h *History) Close() {
	if h == nil {
		return
	}
	h.InvertedIndex.Close()
	h.closeWhatNotInList([]string{})
}

func (ht *HistoryRoTx) Files() (res VisibleFiles) {
	for _, item := range ht.files {
		if item.src.decompressor != nil {
			res = append(res, item)
		}
	}
	return append(res, ht.iit.Files()...)
}

func (h *History) MissedMapAccessors() (l []*FilesItem) {
	return h.missedMapAccessors(h.dirtyFiles.Items())
}

func (h *History) missedMapAccessors(source []*FilesItem) (l []*FilesItem) {
	if !h.Accessors.Has(statecfg.AccessorHashMap) {
		return nil
	}
	return fileItemsWithMissedAccessors(source, h.stepSize, func(fromStep, toStep kv.Step) []string {
		fPath, _, _, err := version.FindFilesWithVersionsByPattern(h.vAccessorFilePathMask(fromStep, toStep))
		if err != nil {
			panic(err)
		}
		return []string{fPath}
	})
}

func (h *History) buildVi(ctx context.Context, item *FilesItem, ps *background.ProgressSet) (err error) {
	if item.decompressor == nil {
		fromStep, toStep := item.StepRange(h.stepSize)
		return fmt.Errorf("buildVI: passed item with nil decompressor %s %d-%d", h.FilenameBase, fromStep, toStep)
	}

	search := &FilesItem{startTxNum: item.startTxNum, endTxNum: item.endTxNum}
	iiItem, ok := h.InvertedIndex.dirtyFiles.Get(search)
	if !ok {
		return nil
	}

	if iiItem.decompressor == nil {
		fromStep, toStep := item.StepRange(h.stepSize)
		return fmt.Errorf("buildVI: got iiItem with nil decompressor %s %d-%d", h.FilenameBase, fromStep, toStep)
	}
	idxPath := h.vAccessorNewFilePath(item.StepRange(h.stepSize))

	err = h.buildVI(ctx, idxPath, item.decompressor, iiItem.decompressor, iiItem.startTxNum, ps)
	if err != nil {
		return fmt.Errorf("buildVI: %w", err)
	}
	return nil
}

func (h *History) buildVI(ctx context.Context, historyIdxPath string, hist, efHist *seg.Decompressor, efBaseTxNum uint64, ps *background.ProgressSet) error {
	var histKey []byte
	var valOffset uint64

	defer hist.MadvSequential().DisableReadAhead()
	defer efHist.MadvSequential().DisableReadAhead()

	iiReader := h.InvertedIndex.dataReader(efHist)

	var keyBuf, valBuf []byte
	cnt := uint64(0)
	for iiReader.HasNext() {
		keyBuf, _ = iiReader.Next(keyBuf[:0]) // skip key
		valBuf, _ = iiReader.Next(valBuf[:0])
		cnt += multiencseq.Count(efBaseTxNum, valBuf)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	histReader := h.dataReader(hist)

	_, fName := filepath.Split(historyIdxPath)
	p := ps.AddNew(fName, uint64(efHist.Count())/2)
	defer ps.Delete(p)
	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   int(cnt),
		Enums:      false,
		BucketSize: recsplit.DefaultBucketSize,
		LeafSize:   recsplit.DefaultLeafSize,
		TmpDir:     h.dirs.Tmp,
		IndexFile:  historyIdxPath,
		Salt:       h.salt.Load(),
		NoFsync:    h.noFsync,
	}, h.logger)
	if err != nil {
		return fmt.Errorf("create recsplit: %w", err)
	}
	defer rs.Close()
	rs.LogLvl(log.LvlTrace)

	seq := &multiencseq.SequenceReader{}

	i := 0
	for {
		histReader.Reset(0)
		iiReader.Reset(0)

		valOffset = 0
		for iiReader.HasNext() {
			keyBuf, _ = iiReader.Next(keyBuf[:0])
			valBuf, _ = iiReader.Next(valBuf[:0])
			p.Processed.Add(1)

			// fmt.Printf("ef key %x\n", keyBuf)

			seq.Reset(efBaseTxNum, valBuf)
			it := seq.Iterator(0)
			for it.HasNext() {
				txNum, err := it.Next()
				if err != nil {
					return err
				}
				histKey = historyKey(txNum, keyBuf, histKey[:0])
				if err = rs.AddKey(histKey, valOffset); err != nil {
					return err
				}

				if h.CompressorCfg.ValuesOnCompressedPage == 0 {
					valOffset, _ = histReader.Skip()
				} else {
					i++
					if i%h.CompressorCfg.ValuesOnCompressedPage == 0 {
						valOffset, _ = histReader.Skip()
					}
				}
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}

		if err = rs.Build(ctx); err != nil {
			if rs.Collision() {
				log.Info("Building recsplit. Collision happened. It's ok. Restarting...")
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

func (h *History) BuildMissedAccessors(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet, historyFiles *MissedAccessorHistoryFiles) {
	h.InvertedIndex.BuildMissedAccessors(ctx, g, ps, historyFiles.ii)
	for _, item := range historyFiles.missedMapAccessors() {
		g.Go(func() error {
			return h.buildVi(ctx, item, ps)
		})
	}
}

func (h *History) Scan(toTxNum uint64) error {
	scanResult, err := scanDirs(h.dirs)
	if err != nil {
		return err
	}

	if err := h.openFolder(scanResult); err != nil {
		return err
	}

	h.reCalcVisibleFiles(toTxNum)

	salt, err := GetStateIndicesSalt(h.dirs, false, h.logger)
	if err != nil {
		return err
	}

	h.salt.Store(salt)
	return nil
}

func (w *historyBufferedWriter) AddPrevValue(k []byte, txNum uint64, original []byte) (err error) {
	if w.discard {
		return nil
	}

	if original == nil {
		original = []byte{}
	}
	binary.BigEndian.PutUint64(w.ii.txNumBytes[:], txNum)

	//defer func() {
	//	fmt.Printf("addPrevValue [%p;tx=%d] '%x' -> '%x'\n", w, w.ii.txNum, key1, original)
	//}()

	if w.largeValues {
		lk := len(k)

		w.historyKey = append(append(w.historyKey[:0], k...), w.ii.txNumBytes[:]...)
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

	lk := len(k)
	w.historyKey = append(append(append(w.historyKey[:0], k...), w.ii.txNumBytes[:]...), original...)
	historyKey := w.historyKey[:lk+8+len(original)]
	historyKey1 := historyKey[:lk]
	historyVal := historyKey[lk:]
	invIdxVal := historyKey[:lk]

	if len(original) > 2048 {
		log.Error("History value is too large while largeValues=false", "h", w.historyValsTable, "histo", string(w.historyKey[:lk]), "len", len(original), "max", len(w.historyKey)-8-len(k))
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

	// dbPageSize - number of values to compress together when storing in DB
	// if > 0 then page level compression is enabled for DB storage
	dbPageSize int

	ii *InvertedIndexBufferedWriter
}

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
		largeValues:      ht.h.HistoryLargeValues,
		historyValsTable: ht.h.ValuesTable,
		dbPageSize:       ht.h.DBValuesOnCompressedPage,

		ii: ht.iit.newWriter(tmpdir, discard),
	}
	if !discard {
		w.historyVals = etl.NewCollectorWithAllocator(w.ii.filenameBase+".flush.hist", tmpdir, etl.SmallSortableBuffers, ht.h.logger).
			LogLvl(log.LvlTrace).SortAndFlushInBackground(true)
	}
	return w
}

func (w *historyBufferedWriter) init() {

}

func (w *historyBufferedWriter) Flush(ctx context.Context, tx kv.RwTx) error {
	if w.discard {
		return nil
	}
	if err := w.ii.Flush(ctx, tx); err != nil {
		return err
	}

	// Use page compression if enabled
	if w.dbPageSize > 0 {
		if err := w.flushWithPageCompression(ctx, tx); err != nil {
			return err
		}
	} else {
		if err := w.historyVals.Load(tx, w.historyValsTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
			return err
		}
	}
	w.close()
	return nil
}

// flushWithPageCompression writes history values to DB using page-level compression
// Values for the same key are grouped into compressed pages
func (w *historyBufferedWriter) flushWithPageCompression(ctx context.Context, tx kv.RwTx) error {
	pageWriter := seg.NewPagedWriterStandalone(w.dbPageSize, true)

	var prevKey []byte // for small values, this is just the key (k); for large values, this is k+txNum

	// getKeyPrefix returns the key prefix used to group values into pages
	// for small values: the key (k)
	// for large values: the key (k) without the txNum suffix
	getKeyPrefix := func(k []byte) []byte {
		if w.largeValues {
			// for large values, k = key + txNum (8 bytes)
			if len(k) >= 8 {
				return k[:len(k)-8]
			}
			return k
		}
		// for small values, k is already just the key
		return k
	}

	writePageToDB := func() error {
		if pageWriter.IsEmpty() {
			return nil
		}
		pageKey := pageWriter.PageKey()
		pageData := pageWriter.BuildPage()
		pageWriter.Reset()

		if w.largeValues {
			// for large values: store with first key (k+txNum) as key
			return tx.Put(w.historyValsTable, pageKey, pageData)
		}
		// for small values: store with key (k) and page as value
		// note: this changes from DupSort to regular Put
		return tx.Put(w.historyValsTable, pageKey, pageData)
	}

	loadFn := func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		keyPrefix := getKeyPrefix(k)

		// Check if key changed (need to start a new page for different keys)
		keyChanged := prevKey != nil && !bytes.Equal(prevKey, keyPrefix)
		if keyChanged || pageWriter.IsFull() {
			if err := writePageToDB(); err != nil {
				return err
			}
		}

		if err := pageWriter.Add(k, v); err != nil {
			return err
		}
		prevKey = common.Copy(keyPrefix)
		return nil
	}

	if err := w.historyVals.Load(tx, w.historyValsTable, loadFn, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	// Flush remaining buffered values
	return writePageToDB()
}

type HistoryCollation struct {
	historyComp   *seg.PagedWriter
	efHistoryComp *seg.Writer
	historyPath   string
	efHistoryPath string
	efBaseTxNum   uint64
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
func (h *History) collate(ctx context.Context, step kv.Step, txFrom, txTo uint64, roTx kv.Tx) (HistoryCollation, error) {
	if h.SnapshotsDisabled {
		return HistoryCollation{}, nil
	}

	var (
		_histComp *seg.Compressor
		_efComp   *seg.Compressor
		txKey     [8]byte
		err       error

		historyPath   = h.vNewFilePath(step, step+1)
		efHistoryPath = h.efNewFilePath(step, step+1)
		startAt       = time.Now()
		closeComp     = true
	)
	defer func() {
		mxCollateTookHistory.ObserveDuration(startAt)
		if closeComp {
			if _histComp != nil {
				_histComp.Close()
			}
			if _efComp != nil {
				_efComp.Close()
			}
		}
	}()

	_histComp, err = seg.NewCompressor(ctx, "collate hist "+h.FilenameBase, historyPath, h.dirs.Tmp, h.CompressorCfg, log.LvlTrace, h.logger)
	if err != nil {
		return HistoryCollation{}, fmt.Errorf("create %s history compressor: %w", h.FilenameBase, err)
	}
	if h.noFsync {
		_histComp.DisableFsync()
	}
	historyWriter := h.dataWriter(_histComp)

	_efComp, err = seg.NewCompressor(ctx, "collate idx "+h.FilenameBase, efHistoryPath, h.dirs.Tmp, h.CompressorCfg, log.LvlTrace, h.logger)
	if err != nil {
		return HistoryCollation{}, fmt.Errorf("create %s ef history compressor: %w", h.FilenameBase, err)
	}
	if h.noFsync {
		_efComp.DisableFsync()
	}
	invIndexWriter := h.InvertedIndex.dataWriter(_efComp, true) // coll+build must be fast - no Compression

	keysCursor, err := roTx.CursorDupSort(h.KeysTable)
	if err != nil {
		return HistoryCollation{}, fmt.Errorf("create %s history cursor: %w", h.FilenameBase, err)
	}
	defer keysCursor.Close()

	binary.BigEndian.PutUint64(txKey[:], txFrom)
	collector := etl.NewCollectorWithAllocator(h.FilenameBase+".collate.hist", h.dirs.Tmp, etl.SmallSortableBuffers, h.logger).LogLvl(log.LvlTrace)
	defer collector.Close()
	collector.SortAndFlushInBackground(false)

	for txnmb, k, err := keysCursor.Seek(txKey[:]); txnmb != nil; txnmb, k, err = keysCursor.Next() {
		if err != nil {
			return HistoryCollation{}, fmt.Errorf("iterate over %s history cursor: %w", h.FilenameBase, err)
		}
		txNum := binary.BigEndian.Uint64(txnmb)
		if txNum >= txTo { // [txFrom; txTo)
			break
		}
		if err := collector.Collect(k, txnmb); err != nil {
			return HistoryCollation{}, fmt.Errorf("collect %s history key [%x]=>txn %d [%x]: %w", h.FilenameBase, k, txNum, txnmb, err)
		}

		select {
		case <-ctx.Done():
			return HistoryCollation{}, ctx.Err()
		default:
		}
	}

	var c kv.Cursor
	var cd kv.CursorDupSort
	if h.HistoryLargeValues {
		c, err = roTx.Cursor(h.ValuesTable)
		if err != nil {
			return HistoryCollation{}, err
		}
		defer c.Close()
	} else {
		cd, err = roTx.CursorDupSort(h.ValuesTable)
		if err != nil {
			return HistoryCollation{}, err
		}
		defer cd.Close()
	}

	var (
		keyBuf  = make([]byte, 0, 256)
		numBuf  = make([]byte, 8)
		bitmap  = bitmapdb.NewBitmap64()
		prevEf  []byte
		prevKey []byte

		initialized bool

		// For DB page compression support
		snappyReadBuf []byte
	)
	defer bitmapdb.ReturnToPool64(bitmap)

	baseTxNum := uint64(step) * h.stepSize
	cnt := 0
	var histKeyBuf []byte
	//log.Warn("[dbg] collate", "name", h.filenameBase, "sampling", h.historyValuesOnCompressedPage)

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

		seqBuilder := multiencseq.NewBuilder(baseTxNum, bitmap.GetCardinality(), bitmap.Maximum())
		it := bitmap.Iterator()

		for it.HasNext() {
			cnt++
			vTxNum := it.Next()
			seqBuilder.AddOffset(vTxNum)

			binary.BigEndian.PutUint64(numBuf, vTxNum)
			var val []byte
			if !h.HistoryLargeValues {
				// Small values path - page compression not supported for small values
				dbVal, err := cd.SeekBothRange(prevKey, numBuf)
				if err != nil {
					return fmt.Errorf("seekBothRange %s history val [%x]: %w", h.FilenameBase, prevKey, err)
				}
				if dbVal != nil && binary.BigEndian.Uint64(dbVal) == vTxNum {
					val = dbVal[8:]
				} else {
					val = nil
				}
			} else {
				// Large values path - may use page compression
				keyBuf = append(append(keyBuf[:0], prevKey...), numBuf...)

				if h.DBValuesOnCompressedPage > 0 {
					// DB page compression enabled - need to extract value from compressed page
					// For large values, pages are stored with key = k + firstTxNum
					// Pages group entries for the same key, so we need to:
					// 1. Seek to the key prefix (prevKey) to find first page for this key
					// 2. Iterate through all pages for this key
					// 3. Extract the specific value we're looking for using GetFromPage

					// Seek to the key prefix to find the first page for this key
					kAndTxNum, compressedPage, err := c.Seek(prevKey)
					if err != nil {
						return fmt.Errorf("seek %s history val [%x]: %w", h.FilenameBase, prevKey, err)
					}

					// Check if we found any pages for this key
					if kAndTxNum == nil || len(kAndTxNum) < 8 || !bytes.Equal(kAndTxNum[:len(kAndTxNum)-8], prevKey) {
						val = nil
						goto addToFile
					}

					// Search through all pages for this key to find the one containing our txNum
					val = nil
					for kAndTxNum != nil && len(kAndTxNum) >= 8 && bytes.Equal(kAndTxNum[:len(kAndTxNum)-8], prevKey) {
						// Try to find the value in this page using the full key (prevKey + vTxNum)
						v, buf := seg.GetFromPage(keyBuf, compressedPage, snappyReadBuf, true)
						snappyReadBuf = buf
						if v != nil {
							val = v
							break
						}
						// Move to next page for this key
						kAndTxNum, compressedPage, err = c.Next()
						if err != nil {
							return fmt.Errorf("next %s history val: %w", h.FilenameBase, err)
						}
					}
				} else {
					// No page compression - direct value lookup
					key, dbVal, err := c.SeekExact(keyBuf)
					if err != nil {
						return fmt.Errorf("seekExact %s history val [%x]: %w", h.FilenameBase, key, err)
					}
					if len(dbVal) == 0 {
						val = nil
					} else {
						val = dbVal
					}
				}
			}

		addToFile:

			histKeyBuf = historyKey(vTxNum, prevKey, histKeyBuf)
			if err := historyWriter.Add(histKeyBuf, val); err != nil {
				return fmt.Errorf("add %s history val [%x]: %w", h.FilenameBase, prevKey, err)
			}
		}
		bitmap.Clear()
		seqBuilder.Build()

		prevEf = seqBuilder.AppendBytes(prevEf[:0])

		if _, err = invIndexWriter.Write(prevKey); err != nil {
			return fmt.Errorf("add %s ef history key [%x]: %w", h.FilenameBase, prevKey, err)
		}
		if _, err = invIndexWriter.Write(prevEf); err != nil {
			return fmt.Errorf("add %s ef history val: %w", h.FilenameBase, err)
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
	if err = historyWriter.Flush(); err != nil {
		return HistoryCollation{}, fmt.Errorf("add %s history val: %w", h.FilenameBase, err)
	}
	closeComp = false
	mxCollationSizeHist.SetUint64(uint64(historyWriter.Count()))

	return HistoryCollation{
		efHistoryComp: invIndexWriter,
		efHistoryPath: efHistoryPath,
		efBaseTxNum:   uint64(step) * h.stepSize,
		historyPath:   historyPath,
		historyComp:   historyWriter,
	}, nil
}

type HistoryFiles struct {
	historyDecomp   *seg.Decompressor
	historyIdx      *recsplit.Index
	efHistoryDecomp *seg.Decompressor
	efHistoryIdx    *recsplit.Index
	efExistence     *existence.Filter
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
	h._visibleFiles = calcVisibleFiles(h.dirtyFiles, h.Accessors, nil, false, toTxNum)
	h.InvertedIndex.reCalcVisibleFiles(toTxNum)
}

// buildFiles performs potentially resource intensive operations of creating
// static files and their indices
func (h *History) buildFiles(ctx context.Context, step kv.Step, collation HistoryCollation, ps *background.ProgressSet) (HistoryFiles, error) {
	if h.SnapshotsDisabled {
		return HistoryFiles{}, nil
	}
	var (
		historyDecomp, efHistoryDecomp *seg.Decompressor
		historyIdx, efHistoryIdx       *recsplit.Index

		efExistence *existence.Filter
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
			return HistoryFiles{}, fmt.Errorf("compress %s .ef history: %w", h.FilenameBase, err)
		}
		ps.Delete(p)
	}
	{
		_, historyFileName := filepath.Split(collation.historyPath)
		p := ps.AddNew(historyFileName, 1)
		defer ps.Delete(p)
		if err = collation.historyComp.Compress(); err != nil {
			return HistoryFiles{}, fmt.Errorf("compress %s .v history: %w", h.FilenameBase, err)
		}
		ps.Delete(p)
	}
	collation.Close()

	efHistoryDecomp, err = seg.NewDecompressor(collation.efHistoryPath)
	if err != nil {
		return HistoryFiles{}, fmt.Errorf("open %s .ef history decompressor: %w", h.FilenameBase, err)
	}
	{
		if err := h.InvertedIndex.buildMapAccessor(ctx, step, step+1, h.InvertedIndex.dataReader(efHistoryDecomp), ps); err != nil {
			return HistoryFiles{}, fmt.Errorf("build %s .ef history idx: %w", h.FilenameBase, err)
		}
		if efHistoryIdx, err = h.InvertedIndex.openHashMapAccessor(h.InvertedIndex.efAccessorNewFilePath(step, step+1)); err != nil {
			return HistoryFiles{}, err
		}
	}

	historyDecomp, err = seg.NewDecompressor(collation.historyPath)
	if err != nil {
		return HistoryFiles{}, fmt.Errorf("open %s v history decompressor: %w", h.FilenameBase, err)
	}

	historyIdxPath := h.vAccessorNewFilePath(step, step+1)
	err = h.buildVI(ctx, historyIdxPath, historyDecomp, efHistoryDecomp, collation.efBaseTxNum, ps)
	if err != nil {
		return HistoryFiles{}, fmt.Errorf("build %s .vi: %w", h.FilenameBase, err)
	}

	if historyIdx, err = h.openHashMapAccessor(historyIdxPath); err != nil {
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
	if h.SnapshotsDisabled {
		return
	}
	if txNumFrom == txNumTo {
		panic(fmt.Sprintf("assert: txNumFrom(%d) == txNumTo(%d)", txNumFrom, txNumTo))
	}

	h.InvertedIndex.integrateDirtyFiles(InvertedFiles{
		decomp:    sf.efHistoryDecomp,
		index:     sf.efHistoryIdx,
		existence: sf.efExistence,
	}, txNumFrom, txNumTo)

	fi := newFilesItem(txNumFrom, txNumTo, h.stepSize, h.stepsInFrozenFile)
	fi.decompressor = sf.historyDecomp
	fi.index = sf.historyIdx
	h.dirtyFiles.Set(fi)
}

func (h *History) dataReader(f *seg.Decompressor) *seg.Reader {
	if !strings.Contains(f.FileName(), ".v") {
		panic("assert: miss-use " + f.FileName())
	}
	return seg.NewReader(f.MakeGetter(), h.Compression)
}
func (h *History) dataWriter(f *seg.Compressor) *seg.PagedWriter {
	if !strings.Contains(f.FileName(), ".v") {
		panic("assert: miss-use " + f.FileName())
	}
	return seg.NewPagedWriter(seg.NewWriter(f, h.Compression), f.GetValuesOnCompressedPage() > 0)
}
func (ht *HistoryRoTx) dataReader(f *seg.Decompressor) *seg.Reader { return ht.h.dataReader(f) }
func (ht *HistoryRoTx) datarWriter(f *seg.Compressor) *seg.PagedWriter {
	return ht.h.dataWriter(f)
}

func (h *History) isEmpty(tx kv.Tx) (bool, error) {
	k, err := kv.FirstKey(tx, h.ValuesTable)
	if err != nil {
		return false, err
	}
	k2, err := kv.FirstKey(tx, h.KeysTable)
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

	files             visibleFiles // have no garbage (canDelete=true, overlaps, etc...)
	getters           []*seg.Reader
	readers           []*recsplit.IndexReader
	stepSize          uint64
	stepsInFrozenFile uint64

	trace bool

	valsC    kv.Cursor
	valsCDup kv.CursorDupSort

	_bufTs           []byte
	snappyReadBuffer []byte
	dbPage           *seg.Page // reusable page for DB page compression reads
}

func (h *History) BeginFilesRo() *HistoryRoTx {
	files := h._visibleFiles
	for i := 0; i < len(files); i++ {
		if !files[i].src.frozen {
			files[i].src.refcount.Add(1)
		}
	}

	return &HistoryRoTx{
		h:                 h,
		iit:               h.InvertedIndex.BeginFilesRo(),
		files:             files,
		stepSize:          h.stepSize,
		stepsInFrozenFile: h.stepsInFrozenFile,
		trace:             false,
	}
}

func (ht *HistoryRoTx) statelessGetter(i int) *seg.Reader {
	if ht.getters == nil {
		ht.getters = make([]*seg.Reader, len(ht.files))
	}
	if ht.getters[i] == nil {
		ht.getters[i] = ht.dataReader(ht.files[i].src.decompressor)
	}
	return ht.getters[i]
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

	if ht.h.SnapshotsDisabled {
		if ht.h.KeepRecentTxnInDB >= maxIdxTx {
			return false, 0
		}
		txTo = min(maxIdxTx-ht.h.KeepRecentTxnInDB, untilTx) // bound pruning
	} else {
		canPruneIdx := ht.iit.CanPrune(tx)
		if !canPruneIdx {
			return false, 0
		}
		txTo = min(ht.files.EndTxNum(), ht.iit.files.EndTxNum(), untilTx)
	}

	switch ht.h.FilenameBase {
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
func (ht *HistoryRoTx) Prune(ctx context.Context, tx kv.RwTx, txFrom, txTo, limit uint64, forced bool, logEvery *time.Ticker) (*InvertedIndexPruneStat, error) {
	if !forced {
		if ht.files.EndTxNum() > 0 {
			txTo = min(txTo, ht.files.EndTxNum())
		}
		var can bool
		can, txTo = ht.canPruneUntil(tx, txTo)
		if !can {
			return nil, nil
		}
	}
	return ht.prune(ctx, tx, txFrom, txTo, limit, forced, logEvery)
}

func (ht *HistoryRoTx) prune(ctx context.Context, rwTx kv.RwTx, txFrom, txTo, limit uint64, forced bool, logEvery *time.Ticker) (*InvertedIndexPruneStat, error) {
	//fmt.Printf(" pruneH[%s] %t, %d-%d\n", ht.h.filenameBase, ht.CanPruneUntil(rwTx), txFrom, txTo)
	defer func(t time.Time) { mxPruneTookHistory.ObserveDuration(t) }(time.Now())

	var (
		seek     = make([]byte, 8, 256)
		valsCDup kv.RwCursorDupSort
		valsC    kv.RwCursor
		err      error
	)

	if !ht.h.HistoryLargeValues {
		valsCDup, err = rwTx.RwCursorDupSort(ht.h.ValuesTable)
		if err != nil {
			return nil, err
		}
		defer valsCDup.Close()
	} else {
		valsC, err = rwTx.RwCursor(ht.h.ValuesTable)
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

		if ht.h.HistoryLargeValues {
			seek = append(bytes.Clone(k), txnm...)
			if err := valsC.Delete(seek); err != nil {
				return err
			}
		} else {
			vv, err := valsCDup.SeekBothRange(k, txnm)
			if err != nil {
				return err
			}
			if len(vv) < 8 {
				return fmt.Errorf("prune history %s got invalid value length: %d < 8", ht.h.FilenameBase, len(vv))
			}
			if vtx := binary.BigEndian.Uint64(vv); vtx != txNum {
				return fmt.Errorf("prune history %s got invalid txNum: found %d != %d wanted", ht.h.FilenameBase, vtx, txNum)
			}
			if err = valsCDup.DeleteCurrent(); err != nil {
				return err
			}
		}

		pruned++
		return nil
	}
	mxPruneSizeHistory.AddInt(pruned)

	if !forced && ht.h.SnapshotsDisabled {
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
			if traceFileLife != "" && ht.h.FilenameBase == traceFileLife {
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
	ok, histTxNum, err := ht.iit.seekInFiles(key, txNum)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	historyItem, ok := ht.getFile(histTxNum)
	if !ok {
		log.Warn("historySeekInFiles: file not found", "key", key, "txNum", txNum, "histTxNum", histTxNum, "ssize", ht.h.stepSize)
		return nil, false, fmt.Errorf("hist file not found: key=%x, %s.%d-%d", key, ht.h.FilenameBase, histTxNum/ht.h.stepSize, histTxNum/ht.h.stepSize)
	}
	reader := ht.statelessIdxReader(historyItem.i)
	if reader.Empty() {
		return nil, false, nil
	}
	historyKey := ht.encodeTs(histTxNum, key)
	offset, ok := reader.Lookup(historyKey)
	if !ok {
		return nil, false, nil
	}
	g := ht.statelessGetter(historyItem.i)
	g.Reset(offset)
	//fmt.Printf("[dbg] hist.seek: offset=%d\n", offset)
	v, _ := g.Next(nil)
	if traceGetAsOf == ht.h.FilenameBase {
		fmt.Printf("DomainGetAsOf(%s, %x, %d) -> %s, histTxNum=%d, isNil(v)=%t\n", ht.h.FilenameBase, key, txNum, g.FileName(), histTxNum, v == nil)
	}

	compressedPageValuesCount := historyItem.src.decompressor.CompressedPageValuesCount()

	if historyItem.src.decompressor.CompressionFormatVersion() == seg.FileCompressionFormatV0 {
		compressedPageValuesCount = ht.h.HistoryValuesOnCompressedPage
	}

	if compressedPageValuesCount > 1 {
		v, ht.snappyReadBuffer = seg.GetFromPage(historyKey, v, ht.snappyReadBuffer, true)
	}
	return v, true, nil
}

func historyKey(txNum uint64, key []byte, buf []byte) []byte {
	if buf == nil || cap(buf) < 8+len(key) {
		buf = make([]byte, 8+len(key))
	}
	buf = buf[:8+len(key)]
	binary.BigEndian.PutUint64(buf, txNum)
	copy(buf[8:], key)
	return buf
}

func (ht *HistoryRoTx) encodeTs(txNum uint64, key []byte) []byte {
	ht._bufTs = historyKey(txNum, key, ht._bufTs)
	return ht._bufTs
}

// HistorySeek searches history for a value of specified key before txNum
// second return value is true if the value is found in the history (even if it is nil)
func (ht *HistoryRoTx) HistorySeek(key []byte, txNum uint64, roTx kv.Tx) ([]byte, bool, error) {
	if ht.h.Disable {
		return nil, false, nil
	}

	v, ok, err := ht.historySeekInFiles(key, txNum)
	if err != nil {
		return nil, false, err
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
	ht.valsC, err = tx.Cursor(ht.h.ValuesTable) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	return ht.valsC, nil
}
func (ht *HistoryRoTx) valsCursorDup(tx kv.Tx) (c kv.CursorDupSort, err error) {
	if ht.valsCDup != nil {
		return ht.valsCDup, nil
	}
	ht.valsCDup, err = tx.CursorDupSort(ht.h.ValuesTable) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	return ht.valsCDup, nil
}

func (ht *HistoryRoTx) historySeekInDB(key []byte, txNum uint64, tx kv.Tx) ([]byte, bool, error) {
	// Check if page compression is enabled for DB
	if ht.h.DBValuesOnCompressedPage > 0 {
		return ht.historySeekInDBPaged(key, txNum, tx)
	}

	if ht.h.HistoryLargeValues {
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
	v := val[8:]
	return v, true, nil
}

// historySeekInDBPaged handles seeking in DB when page compression is enabled
// Uses seg.Page and seg.GetFromPage for reading compressed pages
func (ht *HistoryRoTx) historySeekInDBPaged(key []byte, txNum uint64, tx kv.Tx) ([]byte, bool, error) {
	c, err := ht.valsCursor(tx)
	if err != nil {
		return nil, false, err
	}

	// Lazily initialize the reusable page
	if ht.dbPage == nil {
		ht.dbPage = &seg.Page{}
	}

	if ht.h.HistoryLargeValues {
		// For large values with page compression:
		// Pages are stored with key = k + firstTxNum
		// We need to find the page that might contain our txNum
		seek := make([]byte, len(key)+8)
		copy(seek, key)
		binary.BigEndian.PutUint64(seek[len(key):], txNum)

		// Seek to find a page that might contain this txNum
		kAndTxNum, compressedPage, err := c.Seek(seek)
		if err != nil {
			return nil, false, err
		}

		// Check if we found a page for this key
		if kAndTxNum == nil || len(kAndTxNum) < 8 || !bytes.Equal(kAndTxNum[:len(kAndTxNum)-8], key) {
			// Try seeking backwards - the page might start before our txNum
			// Go back to find pages for this key
			kAndTxNum, compressedPage, err = c.Seek(key)
			if err != nil {
				return nil, false, err
			}
			if kAndTxNum == nil || len(kAndTxNum) < 8 || !bytes.Equal(kAndTxNum[:len(kAndTxNum)-8], key) {
				return nil, false, nil
			}
		}

		// Build the full search key (k + txNum) for lookup
		searchKey := make([]byte, len(key)+8)
		copy(searchKey, key)
		binary.BigEndian.PutUint64(searchKey[len(key):], txNum)

		// Search through pages for this key to find the one containing our txNum
		for kAndTxNum != nil && len(kAndTxNum) >= 8 && bytes.Equal(kAndTxNum[:len(kAndTxNum)-8], key) {
			// Try to find the value in this page using seg.GetFromPage
			val, buf := seg.GetFromPage(searchKey, compressedPage, ht.snappyReadBuffer, true)
			ht.snappyReadBuffer = buf
			if val != nil {
				return val, true, nil
			}

			// Move to next page for this key
			kAndTxNum, compressedPage, err = c.Next()
			if err != nil {
				return nil, false, err
			}
		}
		return nil, false, nil
	}

	// For small values with page compression:
	// Pages are stored with key = k (the original key, not k+txNum)
	// Each page contains multiple (key, txNum+value) pairs
	_, compressedPage, err := c.Seek(key)
	if err != nil {
		return nil, false, err
	}
	if compressedPage == nil {
		return nil, false, nil
	}

	// Search for the txNum in the page using seg.Page
	val, found := seg.SearchPageForTxNum(txNum, compressedPage, ht.dbPage)
	if found {
		return val, true, nil
	}
	return nil, false, nil
}

func (ht *HistoryRoTx) RangeAsOf(ctx context.Context, startTxNum uint64, from, to []byte, asc order.By, limit int, roTx kv.Tx) (stream.KV, error) {
	if !asc {
		panic("implement me")
	}
	hi := &HistoryRangeAsOfFiles{
		from: from, toPrefix: to, limit: kv.Unlim, orderAscend: asc,

		hc:         ht,
		startTxNum: startTxNum,
		ctx:        ctx, logger: ht.h.logger,
	}
	if err := hi.init(ht.iit.files); err != nil {
		hi.Close() //it's responsibility of constructor (our) to close resource on error
		return nil, err
	}

	dbit := &HistoryRangeAsOfDB{
		largeValues: ht.h.HistoryLargeValues,
		roTx:        roTx,
		valsTable:   ht.h.ValuesTable,
		from:        from, toPrefix: to, limit: kv.Unlim, orderAscend: asc,

		startTxNum: startTxNum,

		ctx: ctx, logger: ht.h.logger,
	}
	binary.BigEndian.PutUint64(dbit.startTxKey[:], startTxNum)
	if err := dbit.advance(); err != nil {
		dbit.Close() //it's responsibility of constructor (our) to close resource on error
		return nil, err
	}

	return stream.UnionKV(hi, dbit, limit), nil
}

func (ht *HistoryRoTx) iterateChangedFrozen(fromTxNum, toTxNum int, asc order.By, limit int) (stream.KV, error) {
	if asc == order.Desc {
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
		g := ht.iit.dataReader(item.src.decompressor)
		g.Reset(0)
		wrapper := NewSegReaderWrapper(g)
		if wrapper.HasNext() {
			key, val, err := wrapper.Next()
			if err != nil {
				s.Close()
				return nil, err
			}
			heap.Push(&s.h, &ReconItem{g: wrapper, key: key, val: val, startTxNum: item.startTxNum, endTxNum: item.endTxNum, txNum: item.endTxNum})
		}
	}
	if err := s.advance(); err != nil {
		s.Close() //it's responsibility of constructor (our) to close resource on error
		return nil, err
	}
	return s, nil
}

func (ht *HistoryRoTx) iterateChangedRecent(fromTxNum, toTxNum int, asc order.By, limit int, roTx kv.Tx) (stream.KV, error) {
	if asc == order.Desc {
		panic("not supported yet")
	}
	rangeIsInFiles := toTxNum >= 0 && len(ht.iit.files) > 0 && ht.iit.files.EndTxNum() >= uint64(toTxNum)
	if rangeIsInFiles {
		return stream.EmptyKV, nil
	}
	s := &HistoryChangesIterDB{
		endTxNum:    toTxNum,
		roTx:        roTx,
		largeValues: ht.h.HistoryLargeValues,
		valsTable:   ht.h.ValuesTable,
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

// HistoryRange producing state-patch for Unwind - return state-patch for Unwind: "what keys changed between `[from, to)` and what was their value BEFORE txNum"
func (ht *HistoryRoTx) HistoryRange(fromTxNum, toTxNum int, asc order.By, limit int, roTx kv.Tx) (stream.KV, error) {
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
	return stream.UnionKV(itOnFiles, itOnDB, limit), nil
}

func (ht *HistoryRoTx) HistoryDump(fromTxNum, toTxNum int, dumpTo io.Writer) error {
	if len(ht.iit.files) == 0 {
		return nil
	}

	if fromTxNum >= 0 && ht.iit.files.EndTxNum() <= uint64(fromTxNum) {
		return nil
	}

	for _, item := range ht.iit.files {
		if fromTxNum >= 0 && item.endTxNum <= uint64(fromTxNum) {
			continue
		}
		if toTxNum >= 0 && item.startTxNum >= uint64(toTxNum) {
			break
		}

		efGetter := ht.iit.dataReader(item.src.decompressor)
		efGetter.Reset(0)

		for efGetter.HasNext() {
			key, _ := efGetter.Next(nil)
			val, _ := efGetter.Next(nil) // encoded EF sequence

			seq := multiencseq.ReadMultiEncSeq(item.startTxNum, val)
			ss := seq.Iterator(0)

			for ss.HasNext() {
				txNum, _ := ss.Next()

				var txNumKey [8]byte
				binary.BigEndian.PutUint64(txNumKey[:], txNum)

				viFile, ok := ht.getFile(txNum)
				if !ok {
					return fmt.Errorf("HistoryDump: no .vi %s file found for [%x]", ht.iit.name, txNum)
				}

				viReader := ht.statelessIdxReader(viFile.i)
				vOffset, ok := viReader.Lookup2(txNumKey[:], key)
				if !ok {
					return fmt.Errorf("HistoryDump: failed to resolve offset in .vi %s file for key [%x]", viFile.Fullpath(), key)
				}

				compressedPageValuesCount := viFile.src.decompressor.CompressedPageValuesCount()

				if viFile.src.decompressor.CompressionFormatVersion() == seg.FileCompressionFormatV0 {
					compressedPageValuesCount = ht.h.HistoryValuesOnCompressedPage
				}

				vGetter := seg.NewPagedReader(
					ht.statelessGetter(viFile.i),
					compressedPageValuesCount,
					true,
				)

				vGetter.Reset(vOffset)
				val, _ := vGetter.Next(nil)

				fmt.Fprintf(dumpTo, "key: %x, txn: %d, val: %x\n", key, txNum, val)
			}
		}
	}

	return nil
}

// CompactRange rebuilds the history files within the specified transaction range by performing a forced self-merge.
// If the range contains existing static files, the method collects all files belonging to that span and merges them.
func (ht *HistoryRoTx) CompactRange(ctx context.Context, fromTxNum, toTxNum uint64) error {
	if len(ht.iit.files) == 0 {
		return nil
	}

	mergeRange := NewHistoryRanges(
		*NewMergeRange("", true, fromTxNum, toTxNum),
		*NewMergeRange("", true, fromTxNum, toTxNum),
	)

	efFiles, vFiles, err := ht.staticFilesInRange(mergeRange)
	if err != nil {
		return err
	}

	return ht.deduplicateFiles(ctx, efFiles, vFiles, mergeRange, background.NewProgressSet())
}

func (ht *HistoryRoTx) idxRangeOnDB(key []byte, startTxNum, endTxNum int, asc order.By, limit int, roTx kv.Tx) (stream.U64, error) {
	if ht.h.HistoryLargeValues {
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
		it, err := roTx.Range(ht.h.ValuesTable, from, to, asc, limit)
		if err != nil {
			return nil, err
		}
		return stream.TransformKV2U64(it, func(k, v []byte) (uint64, error) {
			if len(k) < 8 {
				return 0, fmt.Errorf("unexpected large key length %d", len(k))
			}
			return binary.BigEndian.Uint64(k[len(k)-8:]), nil
		}), nil
	}

	var from, to []byte
	if startTxNum >= 0 {
		from = make([]byte, 8)
		binary.BigEndian.PutUint64(from, uint64(startTxNum))
	}
	if endTxNum >= 0 {
		to = make([]byte, 8)
		binary.BigEndian.PutUint64(to, uint64(endTxNum))
	}
	it, err := roTx.RangeDupSort(ht.h.ValuesTable, key, from, to, asc, limit)
	if err != nil {
		return nil, err
	}
	return stream.TransformKV2U64(it, func(k, v []byte) (uint64, error) {
		if len(v) < 8 {
			return 0, fmt.Errorf("unexpected small value length %d", len(v))
		}
		return binary.BigEndian.Uint64(v), nil
	}), nil
}

func (ht *HistoryRoTx) IdxRange(key []byte, startTxNum, endTxNum int, asc order.By, limit int, roTx kv.Tx) (stream.U64, error) {
	frozenIt, err := ht.iit.iterateRangeOnFiles(key, startTxNum, endTxNum, asc, limit)
	if err != nil {
		return nil, err
	}
	recentIt, err := ht.idxRangeOnDB(key, startTxNum, endTxNum, asc, limit, roTx)
	if err != nil {
		return nil, err
	}
	return stream.Union(frozenIt, recentIt, asc, limit), nil
}

func (ht *HistoryRoTx) DebugHistoryTraceKey(ctx context.Context, key []byte, fromTxNum, toTxNum uint64, roTx kv.Tx) (stream.U64V, error) {
	files := HistoryTraceKeyFiles{
		hc:        ht,
		fromTxNum: fromTxNum,
		toTxNum:   toTxNum,
		key:       key,
		logger:    ht.h.logger,
		ctx:       ctx,
	}
	if err := files.init(); err != nil {
		files.Close()
		return nil, err
	}
	db := HistoryTraceKeyDB{
		largeValues: ht.h.HistoryLargeValues,
		roTx:        roTx,
		valsTable:   ht.h.ValuesTable,
		fromTxNum:   fromTxNum,
		toTxNum:     toTxNum,
		key:         key,

		logger: ht.h.logger,
		ctx:    ctx,
	}
	if err := db.init(); err != nil {
		db.Close()
		return nil, err
	}
	return stream.Union2(&files, &db, order.Asc, kv.Unlim), nil

}
