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
	"path/filepath"
	"slices"
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
		dirtyFiles:    btree2.NewBTreeGOptions[*FilesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		_visibleFiles: []visibleFile{},
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
	protectFiles := make(map[string]struct{}, len(fNames))
	for _, f := range fNames {
		protectFiles[f] = struct{}{}
	}
	var toClose []*FilesItem
	h.dirtyFiles.Walk(func(items []*FilesItem) bool {
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

				histKey = historyKeyInFiles(keyBuf, txNum, histKey[:0])
				if err = rs.AddKey(histKey, valOffset); err != nil {
					return err
				}
				if h.HistoryValuesOnCompressedPage == 0 {
					valOffset, _ = histReader.Skip()
				} else {
					i++
					if i%h.HistoryValuesOnCompressedPage == 0 {
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
		w.historyKey = historyKeyInDB(txNum, k, w.ii.stepSize, w.historyKey) // ^step + key + txNum -> value
		if err := w.historyVals.Collect(w.historyKey, original); err != nil {
			return err
		}

		if !w.ii.discard {
			if err := w.ii.indexKeys.Collect(w.ii.txNumBytes[:], k); err != nil {
				return err
			}
		}
		return nil
	}

	if len(original) > 2048 {
		log.Error("History value is too large while largeValues=false", "h", w.historyValsTable, "histo", string(k), "len", len(original), "max", 2048)
		panic("History value is too large while largeValues=false")
	}

	step := txNum / w.ii.stepSize
	w.historyKey = historyKeyPrefixInDB(step, k, w.historyKey[:0])
	histKey := w.historyKey

	// Value is txNum + original for non-large values
	histVal := make([]byte, 8+len(original))
	binary.BigEndian.PutUint64(histVal[:8], txNum)
	copy(histVal[8:], original)
	if err := w.historyVals.Collect(histKey, histVal); err != nil {
		return err
	}
	if !w.ii.discard {
		if err := w.ii.indexKeys.Collect(w.ii.txNumBytes[:], k); err != nil {
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
	if err := w.historyVals.Load(tx, w.historyValsTable, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	w.close()
	return nil
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
	collector.SortAndFlushInBackground(true)

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
			if !h.HistoryLargeValues {
				// ^step + key -> txNum + v
				stepKey := historyKeyPrefixInDB(vTxNum/h.stepSize, prevKey, nil)
				val, err := cd.SeekBothRange(stepKey, numBuf)
				if err != nil {
					return fmt.Errorf("SeekBothRange %s step-prefixed values [%x]: %w", h.FilenameBase, stepKey, err)
				}
				if val != nil && binary.BigEndian.Uint64(val) == vTxNum {
					val = val[8:]
				} else {
					val = nil
				}

				histKeyBuf = historyKeyInFiles(prevKey, vTxNum, histKeyBuf)
				if err := historyWriter.Add(histKeyBuf, val); err != nil {
					return fmt.Errorf("add %s history val [%x]: %w", h.FilenameBase, prevKey, err)
				}
				continue
			}

			// ^step + addr + txNum -> v
			keyBuf = historyKeyInDB(vTxNum, prevKey, h.stepSize, keyBuf[:0])
			_, val, err := c.SeekExact(keyBuf)
			if err != nil {
				return fmt.Errorf("seekExact %s step-prefixed key [%x]: %w", h.FilenameBase, keyBuf, err)
			}
			if len(val) == 0 {
				val = nil
			}

			histKeyBuf = historyKeyInFiles(prevKey, vTxNum, histKeyBuf)
			if err := historyWriter.Add(histKeyBuf, val); err != nil {
				return fmt.Errorf("add %s history val [%x]: %w", h.FilenameBase, keyBuf, err)
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
	return seg.NewPagedWriter(seg.NewWriter(f, h.Compression), h.HistoryValuesOnCompressedPage, true)
}
func (ht *HistoryRoTx) dataReader(f *seg.Decompressor) *seg.Reader     { return ht.h.dataReader(f) }
func (ht *HistoryRoTx) datarWriter(f *seg.Compressor) *seg.PagedWriter { return ht.h.dataWriter(f) }

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

	_bufTs           [8]byte
	_bufKey          []byte
	snappyReadBuffer []byte
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

	var txFromBytes [8]byte
	var pruned int
	pruneValue := func(k []byte, minTxNum, maxTxNum uint64) error {
		binary.BigEndian.PutUint64(txFromBytes[:], minTxNum)
		if ht.h.HistoryLargeValues {
			// For large values with step-prefixed keys: [^step][addr][txNum] -> value
			// Create step+addr prefix and iterate through all matching keys
			minStep := minTxNum / ht.stepSize
			maxStep := maxTxNum / ht.stepSize

			for step := minStep; step <= maxStep; step++ {
				invertedStep := ^step
				stepAddrPrefix := make([]byte, 8+len(k)+8)
				binary.BigEndian.PutUint64(stepAddrPrefix[:8], invertedStep)
				copy(stepAddrPrefix[8:], k)
				binary.BigEndian.PutUint64(stepAddrPrefix[8+len(k):], minTxNum)

				for k, _, err := valsC.Seek(stepAddrPrefix); k != nil; k, _, err = valsC.Next() {
					if err != nil {
						return fmt.Errorf("iterate over %s values cursor: %w", ht.h.FilenameBase, err)
					}
					txNum := binary.BigEndian.Uint64(k[len(k)-8:])
					if txNum > maxTxNum {
						break
					}
					if !bytes.HasPrefix(k, stepAddrPrefix[:len(stepAddrPrefix)-8]) {
						break
					}
					if err := valsC.DeleteCurrent(); err != nil {
						return err
					}
					pruned++
				}
			}
		} else {
			// For non-large values with step-prefixed keys: [^step][addr] -> txNum+value
			// Create step+addr k and iterate through all values
			minStep := minTxNum / ht.stepSize
			maxStep := maxTxNum / ht.stepSize

			for step := minStep; step <= maxStep; step++ {
				invertedStep := ^step
				stepKey := make([]byte, 8+len(k))
				binary.BigEndian.PutUint64(stepKey[:8], invertedStep)
				copy(stepKey[8:], k)

				for v, err := valsCDup.SeekBothRange(stepKey, txFromBytes[:]); v != nil; _, v, err = valsCDup.NextDup() {
					if err != nil {
						return fmt.Errorf("iterate over %s values cursor: %w", ht.h.FilenameBase, err)
					}
					txNum := binary.BigEndian.Uint64(v)
					if txNum > maxTxNum {
						break
					}
					if err := valsCDup.DeleteCurrent(); err != nil {
						return err
					}
					pruned++
				}
			}
		}
		return nil
	}

	if !forced && ht.h.SnapshotsDisabled {
		forced = true // or index.CanPrune will return false cuz no snapshots made
	}
	st, err := ht.iit.Prune(ctx, rwTx, txFrom, txTo, limit, logEvery, forced, pruneValue)
	if err != nil {
		return nil, err
	}

	mxPruneSizeHistory.AddInt(pruned)
	st.PruneCountValues = uint64(pruned)
	return st, err
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
	historyKey := ht.keyInFiles(histTxNum, key)
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

	if ht.h.HistoryValuesOnCompressedPage > 1 {
		v, ht.snappyReadBuffer = seg.GetFromPage(historyKey, v, ht.snappyReadBuffer, true)
	}
	return v, true, nil
}

// historyKeyInDB ^step + key + txNum
func historyKeyInDB(txNum uint64, key []byte, stepSize uint64, buf []byte) []byte {
	l := 8 + len(key) + 8
	buf = slices.Grow(buf, l)[:l]
	binary.BigEndian.PutUint64(buf[:8], ^(txNum / stepSize))
	copy(buf[8:8+len(key)], key)
	binary.BigEndian.PutUint64(buf[8+len(key):], txNum)
	return buf
}

// historyKeyPrefixInDB  ^step + key
func historyKeyPrefixInDB(step uint64, key []byte, buf []byte) []byte {
	l := 8 + len(key)
	buf = slices.Grow(buf, l)[:l]
	binary.BigEndian.PutUint64(buf[:8], ^step)
	copy(buf[8:], key)
	return buf
}

// historyKeyInFiles key + txNum -> v
func historyKeyInFiles(key []byte, txNum uint64, buf []byte) []byte {
	l := 8 + len(key)
	buf = slices.Grow(buf, l)[:l]
	binary.BigEndian.PutUint64(buf, txNum)
	copy(buf[8:], key)
	return buf
}

// encodeTs - timestamp encode
func (ht *HistoryRoTx) encodeTs(txNum uint64) []byte {
	binary.BigEndian.PutUint64(ht._bufTs[:], txNum)
	return ht._bufTs[:]
}

// keyInFiles key + txNum
func (ht *HistoryRoTx) keyInFiles(txNum uint64, key []byte) []byte {
	ht._bufKey = historyKeyInFiles(key, txNum, ht._bufKey)
	return ht._bufKey
}

// keyInDB ^step + key + txNum
func (ht *HistoryRoTx) keyInDB(key []byte, txNum uint64) []byte {
	ht._bufKey = historyKeyInDB(txNum, key, ht.stepSize, ht._bufKey)
	return ht._bufKey
}

// keyPrefixInDB ^step + key
func (ht *HistoryRoTx) keyPrefixInDB(step uint64, key []byte) []byte {
	ht._bufKey = historyKeyPrefixInDB(step, key, ht._bufKey)
	return ht._bufKey
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
	if ht.h.HistoryLargeValues {
		c, err := ht.valsCursor(tx)
		if err != nil {
			return nil, false, err
		}

		seekKey := ht.keyInDB(key, txNum) // ^step + key + txNum
		k, val, err := c.Seek(seekKey)
		if err != nil {
			return nil, false, err
		}
		if k == nil || !bytes.HasPrefix(k, seekKey[:len(seekKey)-8]) {
			return nil, false, nil
		}
		// val == []byte{}, means key was created in this txNum and doesn't exist before.
		return val, true, nil
	}
	c, err := ht.valsCursorDup(tx)
	if err != nil {
		return nil, false, err
	}

	step := txNum / ht.stepSize
	_, maxStep := ht.stepsRangeInDB(tx)
	for searchStep := step; searchStep <= uint64(maxStep); searchStep++ {
		seekTxNum := txNum
		if searchStep > step {
			seekTxNum = searchStep * ht.stepSize // Start of the step
		}

		seek := ht.keyPrefixInDB(searchStep, key)
		val, err := c.SeekBothRange(seek, ht.encodeTs(seekTxNum))
		if err != nil {
			return nil, false, err
		}
		if val == nil {
			continue
		}
		v := val[8:] // `v == []byte{}` means key was created in this txNum and doesn't exist before.
		return v, true, nil
	}
	return nil, false, nil
}

func (ht *HistoryRoTx) stepsRangeInDB(tx kv.Tx) (from, to float64) {
	// For step-prefixed keys, get range from valuesTable
	var fst, lst []byte

	fst, _ = kv.FirstKey(tx, ht.h.ValuesTable)
	lst, _ = kv.LastKey(tx, ht.h.ValuesTable)

	if len(fst) >= 8 {
		invertedStep := binary.BigEndian.Uint64(fst[:8])
		step := ^invertedStep
		from = float64(step)
	}
	if len(lst) >= 8 {
		invertedStep := binary.BigEndian.Uint64(lst[:8])
		step := ^invertedStep
		to = float64(step)
	}
	// With inverted steps, first key has highest step, last key has lowest step
	// So we need to swap from and to
	if from > to {
		from, to = to, from
	}
	if to == 0 {
		to = from
	}
	return from, to
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

	// Create step-based DB iterators
	dbIter, err := ht.rangeAsOfDBStepBased(ctx, startTxNum, from, to, asc, limit, roTx)
	if err != nil {
		hi.Close()
		return nil, err
	}

	return stream.UnionKV(hi, dbIter, limit), nil
}

func (ht *HistoryRoTx) rangeAsOfDBStepBased(ctx context.Context, startTxNum uint64, from, to []byte, asc order.By, limit int, roTx kv.Tx) (stream.KV, error) {
	fromStepF, toStepF := ht.stepsRangeInDB(roTx)
	minStep := uint64(fromStepF)
	maxStep := uint64(toStepF)

	var iterators []stream.KV
	for step := minStep; step <= maxStep; step++ {
		dbit := &HistoryRangeAsOfDB{
			largeValues: ht.h.HistoryLargeValues,
			roTx:        roTx,
			valsTable:   ht.h.ValuesTable,
			from:        from, toPrefix: to, limit: kv.Unlim, orderAscend: asc,
			startTxNum: startTxNum,
			step:       step,
			ctx:        ctx, logger: ht.h.logger,
		}
		binary.BigEndian.PutUint64(dbit.startTxKey[:], startTxNum)

		if err := dbit.advance(); err != nil {
			dbit.Close()
			for _, it := range iterators {
				it.Close()
			}
			return nil, err
		}

		if dbit.HasNext() {
			iterators = append(iterators, dbit)
		} else {
			dbit.Close()
		}

		if step == ^uint64(0) {
			break
		}
	}

	if len(iterators) == 0 {
		return stream.EmptyKV, nil
	}

	result := iterators[0]
	for i := 1; i < len(iterators); i++ {
		result = stream.UnionKV(result, iterators[i], limit)
	}
	return result, nil
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

	if fromTxNum < 0 {
		fromTxNum = 0
	}

	// Check for empty or invalid range - allow negative toTxNum for unlimited upper bound
	if toTxNum >= 0 && fromTxNum >= toTxNum {
		return stream.EmptyKV, nil
	}

	// Handle limit values
	if limit == 0 {
		return stream.EmptyKV, nil // Zero limit means no results
	}
	if limit < 0 {
		limit = 1000000 // Negative limit means unlimited, but use reasonable default
	}

	rangeIsInFiles := toTxNum >= 0 && len(ht.iit.files) > 0 && ht.iit.files.EndTxNum() >= uint64(toTxNum)
	if rangeIsInFiles {
		return stream.EmptyKV, nil
	}

	// Calculate step range for the given txNum range
	var fromStep, toStep uint64
	fromStep = uint64(fromTxNum) / ht.stepSize

	if toTxNum >= 0 {
		toStep = uint64(toTxNum) / ht.stepSize
	} else {
		// For unlimited upper bound, scan a reasonable number of recent steps
		toStep = fromStep + 100 // Scan up to 100 steps for efficiency
	}

	// Create one iterator per step within the range
	var iterators []stream.KV

	// Iterate through steps from fromStep to toStep
	for step := fromStep; step <= toStep && step != ^uint64(0); step++ {
		// Create step-specific iterator
		s := &HistoryChangesIterDB{
			endTxNum:    toTxNum,
			roTx:        roTx,
			largeValues: ht.h.HistoryLargeValues,
			valsTable:   ht.h.ValuesTable,
			limit:       limit,
			step:        step,
			stepSize:    ht.stepSize,
		}
		binary.BigEndian.PutUint64(s.startTxNumBytes[:], uint64(fromTxNum))

		if err := s.advance(); err != nil {
			s.Close()
			return nil, err
		}

		// Only add iterator if it has data
		if s.HasNext() {
			iterators = append(iterators, s)
		} else {
			s.Close()
		}

		// Break if we've reached the max step to avoid infinite loop
		if step == ^uint64(0)-1 {
			break
		}
	}

	if len(iterators) == 0 {
		return stream.EmptyKV, nil
	}

	// stream.UnionKV handles deduplication
	result := iterators[0]
	for i := 1; i < len(iterators); i++ {
		result = stream.UnionKV(result, iterators[i], limit)
	}
	return result, nil
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
	return stream.UnionKV(itOnDB, itOnFiles, limit), nil
}

func (ht *HistoryRoTx) idxRangeOnDBForStep(key []byte, step uint64, startTxNum, endTxNum int, asc order.By, limit int, roTx kv.Tx) (stream.U64, error) {
	if ht.h.HistoryLargeValues {
		var fromTxNum uint64
		if startTxNum >= 0 {
			fromTxNum = uint64(startTxNum)
		}
		toTxNum := uint64(math.MaxUint64)
		if endTxNum >= 0 {
			toTxNum = uint64(endTxNum)
		}

		from := make([]byte, 8+len(key)+8)
		binary.BigEndian.PutUint64(from, ^step)
		copy(from[8:], key)
		binary.BigEndian.PutUint64(from[8+len(key):], fromTxNum)

		to := common.Copy(from)
		binary.BigEndian.PutUint64(to[8+len(key):], toTxNum)
		it, err := roTx.Range(ht.h.ValuesTable, from, to, asc, limit) // [from, to)
		if err != nil {
			return nil, err
		}
		return stream.TransformKV2U64(it, func(k, v []byte) (uint64, error) {
			if len(k) < 8+len(key)+8 {
				return 0, fmt.Errorf("unexpected key length %d", len(k))
			}
			txNum := binary.BigEndian.Uint64(k[len(k)-8:])
			return txNum, nil
		}), nil
	}

	var from, to []byte
	if startTxNum >= 0 {
		from = make([]byte, 8)
		binary.BigEndian.PutUint64(from, uint64(startTxNum))
	}
	if endTxNum >= 0 {
		to = make([]byte, 8)
		binary.BigEndian.PutUint64(to, uint64(endTxNum)) // exclusive upper bound
	}

	stepKey := ht.keyPrefixInDB(step, key)
	it, err := roTx.RangeDupSort(ht.h.ValuesTable, stepKey, from, to, asc, limit)
	if err != nil {
		return nil, err
	}

	return stream.TransformKV2U64(it, func(k, v []byte) (uint64, error) {
		if len(v) < 8 {
			return 0, fmt.Errorf("unexpected small value length %d", len(v))
		}
		txNum := binary.BigEndian.Uint64(v[:8])
		return txNum, nil
	}), nil
}

func (ht *HistoryRoTx) idxRangeOnDB(key []byte, startTxNum, endTxNum int, asc order.By, limit int, roTx kv.Tx) (stream.U64, error) {
	if roTx == nil {
		return stream.EmptyU64, nil
	}
	if limit == 0 {
		return stream.EmptyU64, nil
	}

	var fromStep, toStep uint64
	if startTxNum >= 0 {
		fromStep = uint64(startTxNum) / ht.stepSize
	} else {
		fromStep = 0
	}
	if endTxNum >= 0 {
		toStep = uint64(endTxNum) / ht.stepSize
	} else {
		_, maxStepFloat := ht.stepsRangeInDB(roTx)
		toStep = uint64(maxStepFloat) + 1
	}

	var iterators []stream.U64

	if asc {
		for step := fromStep; step <= toStep; step++ {
			stepIt, err := ht.idxRangeOnDBForStep(key, step, startTxNum, endTxNum, asc, limit, roTx)
			if err != nil {
				return nil, err
			}
			if stepIt != nil {
				iterators = append(iterators, stepIt)
			}
		}
	} else {
		maxStep := toStep
		minStep := fromStep
		if fromStep > toStep {
			maxStep = fromStep
			minStep = toStep
		}
		for step := maxStep; step >= minStep; step-- {
			stepIt, err := ht.idxRangeOnDBForStep(key, step, startTxNum, endTxNum, asc, limit, roTx)
			if err != nil {
				return nil, err
			}
			if stepIt != nil {
				iterators = append(iterators, stepIt)
			}
			if step == 0 {
				break
			}
		}
	}

	if len(iterators) == 0 {
		return stream.EmptyU64, nil
	}

	result := iterators[0]
	for i := 1; i < len(iterators); i++ {
		result = stream.Union[uint64](result, iterators[i], asc, limit)
	}
	return result, nil
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
	return stream.Union[uint64](frozenIt, recentIt, asc, limit), nil
}
