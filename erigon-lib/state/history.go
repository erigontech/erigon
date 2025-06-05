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
	"sync"
	"time"

	btree2 "github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/page"
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
	"github.com/erigontech/erigon-lib/version"
)

type History struct {
	histCfg        // keep higher than embedded InvertedIndexis to correctly shadow it's exposed variables
	*InvertedIndex // keysTable contains mapping txNum -> key1+key2, while index table `key -> {txnums}` is omitted.

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
	dirtyFiles *btree2.BTreeG[*filesItem]

	// _visibleFiles - underscore in name means: don't use this field directly, use BeginFilesRo()
	// underlying array is immutable - means it's ready for zero-copy use
	_visibleFiles []visibleFile
}

type histCfg struct {
	iiCfg iiCfg

	valuesTable string // bucket for history values; key1+key2+txnNum -> oldValue , stores values BEFORE change

	keepRecentTxnInDB uint64 // When snapshotsDisabled=true, keepRecentTxnInDB is used to keep this amount of txn in db before pruning

	// historyLargeValues: used to store values > 2kb (pageSize/2)
	// small values - can be stored in more compact ways in db (DupSort feature)
	// can't use DupSort optimization (aka. prefix-compression) if values size > 4kb

	// historyLargeValues=true - doesn't support keys of various length (all keys must have same length)
	// not large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2 -> txNum + value (DupSort)
	// large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2+txNum -> value (not DupSort)
	historyLargeValues bool
	snapshotsDisabled  bool // don't produce .v and .ef files, keep in db table. old data will be pruned anyway.
	historyDisabled    bool // skip all write operations to this History (even in DB)

	historyValuesOnCompressedPage int // when collating .v files: concat 16 values and snappy them

	Accessors     Accessors
	CompressorCfg seg.Cfg             // Compression settings for history files
	Compression   seg.FileCompression // defines type of Compression for history files
	historyIdx    kv.InvertedIdx

	version HistVersionTypes
}

func (h histCfg) GetVersions() VersionTypes {
	return VersionTypes{
		Hist: &h.version,
		II:   &h.iiCfg.version,
	}
}

func NewHistory(cfg histCfg, aggStep uint64, logger log.Logger) (*History, error) {
	//if cfg.compressorCfg.MaxDictPatterns == 0 && cfg.compressorCfg.MaxPatternLen == 0 {
	if cfg.Accessors == 0 {
		cfg.Accessors = AccessorHashMap
	}

	h := History{
		histCfg:       cfg,
		dirtyFiles:    btree2.NewBTreeGOptions(filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		_visibleFiles: []visibleFile{},
	}

	var err error
	h.InvertedIndex, err = NewInvertedIndex(cfg.iiCfg, aggStep, logger)
	if err != nil {
		return nil, fmt.Errorf("NewHistory: %s, %w", cfg.iiCfg.filenameBase, err)
	}

	if h.version.DataV.IsZero() {
		panic(fmt.Errorf("assert: forgot to set version of %s", h.name))
	}
	if h.version.AccessorVI.IsZero() {
		panic(fmt.Errorf("assert: forgot to set version of %s", h.name))
	}
	h.InvertedIndex.name = h.name

	return &h, nil
}

func (h *History) vFileName(fromStep, toStep uint64) string {
	return fmt.Sprintf("%s-%s.%d-%d.v", h.version.DataV.String(), h.filenameBase, fromStep, toStep)
}
func (h *History) vFilePath(fromStep, toStep uint64) string {
	return filepath.Join(h.dirs.SnapHistory, h.vFileName(fromStep, toStep))
}
func (h *History) vAccessorFilePath(fromStep, toStep uint64) string {
	return filepath.Join(h.dirs.SnapAccessors, fmt.Sprintf("%s-%s.%d-%d.vi", h.version.AccessorVI.String(), h.filenameBase, fromStep, toStep))
}

func (h *History) vFileNameMask(fromStep, toStep uint64) string {
	return fmt.Sprintf("*-%s.%d-%d.v", h.filenameBase, fromStep, toStep)
}
func (h *History) vFilePathMask(fromStep, toStep uint64) string {
	return filepath.Join(h.dirs.SnapHistory, h.vFileNameMask(fromStep, toStep))
}
func (h *History) vAccessorFilePathMask(fromStep, toStep uint64) string {
	return filepath.Join(h.dirs.SnapAccessors, fmt.Sprintf("*-%s.%d-%d.vi", h.filenameBase, fromStep, toStep))
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

func (h *History) scanDirtyFiles(fileNames []string) {
	if h.filenameBase == "" {
		panic("assert: empty `filenameBase`")
	}
	if h.aggregationStep == 0 {
		panic("assert: empty `aggregationStep`")
	}
	for _, dirtyFile := range scanDirtyFiles(fileNames, h.aggregationStep, h.filenameBase, "v", h.logger) {
		if _, has := h.dirtyFiles.Get(dirtyFile); !has {
			h.dirtyFiles.Set(dirtyFile)
		}
	}
}

func (h *History) openDirtyFiles() error {
	invalidFilesMu := sync.Mutex{}
	invalidFileItems := make([]*filesItem, 0)
	h.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			fromStep, toStep := item.startTxNum/h.aggregationStep, item.endTxNum/h.aggregationStep
			if item.decompressor == nil {
				fPathMask := h.vFilePathMask(fromStep, toStep)
				fPath, fileVer, ok, err := version.FindFilesWithVersionsByPattern(fPathMask)
				if err != nil {
					_, fName := filepath.Split(fPath)
					h.logger.Debug("[agg] History.openDirtyFiles: FileExist", "f", fName, "err", err)
					invalidFilesMu.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFilesMu.Unlock()
					continue
				}
				if !ok {
					_, fName := filepath.Split(fPath)
					h.logger.Debug("[agg] History.openDirtyFiles: file does not exists", "f", fName)
					invalidFilesMu.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFilesMu.Unlock()
					continue
				}
				if !fileVer.Eq(h.version.DataV.Current) {
					if !fileVer.Less(h.version.DataV.MinSupported) {
						h.version.DataV.Current = fileVer
					} else {
						panic("Version is too low, try to rm v history snapshots")
						//return false
					}
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
				fPathMask := h.vAccessorFilePathMask(fromStep, toStep)
				fPath, fileVer, ok, err := version.FindFilesWithVersionsByPattern(fPathMask)
				if err != nil {
					_, fName := filepath.Split(fPath)
					h.logger.Warn("[agg] History.openDirtyFiles", "err", err, "f", fName)
				}
				if ok {
					if !fileVer.Eq(h.version.AccessorVI.Current) {
						if !fileVer.Less(h.version.AccessorVI.MinSupported) {
							h.version.AccessorVI.Current = fileVer
						} else {
							panic("Version is too low, try to rm vi history snapshots")
							//return false
						}
					}
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

func (h *History) Tables() []string { return append(h.InvertedIndex.Tables(), h.valuesTable) }

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

func (h *History) MissedMapAccessors() (l []*filesItem) {
	return h.missedMapAccessors(h.dirtyFiles.Items())
}

func (h *History) missedMapAccessors(source []*filesItem) (l []*filesItem) {
	if !h.Accessors.Has(AccessorHashMap) {
		return nil
	}
	return fileItemsWithMissedAccessors(source, h.aggregationStep, func(fromStep, toStep uint64) []string {
		return []string{
			h.vAccessorFilePath(fromStep, toStep),
		}
	})
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

	iiReader := seg.NewReader(efHist.MakeGetter(), h.InvertedIndex.Compression)

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

	histReader := seg.NewReader(hist.MakeGetter(), h.Compression)

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
				if h.historyValuesOnCompressedPage == 0 {
					valOffset, _ = histReader.Skip()
				} else {
					i++
					if i%h.historyValuesOnCompressedPage == 0 {
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
		item := item
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
		largeValues:      ht.h.historyLargeValues,
		historyValsTable: ht.h.valuesTable,

		ii:          ht.iit.newWriter(tmpdir, discard),
		historyVals: etl.NewCollectorWithAllocator(ht.h.filenameBase+".flush.hist", tmpdir, etl.SmallSortableBuffers, ht.h.logger).LogLvl(log.LvlTrace),
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

	comp, err := seg.NewCompressor(ctx, "collate hist "+h.filenameBase, historyPath, h.dirs.Tmp, h.CompressorCfg, log.LvlTrace, h.logger)
	if err != nil {
		return HistoryCollation{}, fmt.Errorf("create %s history compressor: %w", h.filenameBase, err)
	}
	historyComp = seg.NewWriter(comp, h.Compression)

	keysCursor, err := roTx.CursorDupSort(h.keysTable)
	if err != nil {
		return HistoryCollation{}, fmt.Errorf("create %s history cursor: %w", h.filenameBase, err)
	}
	defer keysCursor.Close()

	binary.BigEndian.PutUint64(txKey[:], txFrom)
	collector := etl.NewCollectorWithAllocator(h.filenameBase+".collate.hist", h.dirs.Tmp, etl.SmallSortableBuffers, h.logger).LogLvl(log.LvlTrace)
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
		c, err = roTx.Cursor(h.valuesTable)
		if err != nil {
			return HistoryCollation{}, err
		}
		defer c.Close()
	} else {
		cd, err = roTx.CursorDupSort(h.valuesTable)
		if err != nil {
			return HistoryCollation{}, err
		}
		defer cd.Close()
	}

	efComp, err := seg.NewCompressor(ctx, "collate idx "+h.filenameBase, efHistoryPath, h.dirs.Tmp, h.CompressorCfg, log.LvlTrace, h.logger)
	if err != nil {
		return HistoryCollation{}, fmt.Errorf("create %s ef history compressor: %w", h.filenameBase, err)
	}
	if h.noFsync {
		efComp.DisableFsync()
	}

	var (
		keyBuf  = make([]byte, 0, 256)
		numBuf  = make([]byte, 8)
		bitmap  = bitmapdb.NewBitmap64()
		prevEf  []byte
		prevKey []byte

		initialized bool
	)
	efHistoryComp = seg.NewWriter(efComp, seg.CompressNone) // coll+build must be fast - no Compression
	collector.SortAndFlushInBackground(true)
	defer bitmapdb.ReturnToPool64(bitmap)

	baseTxNum := step * h.aggregationStep
	cnt := 0
	var histKeyBuf []byte
	//log.Warn("[dbg] collate", "name", h.filenameBase, "sampling", h.historyValuesOnCompressedPage)
	historyWriter := page.NewWriter(historyComp, h.historyValuesOnCompressedPage, true)
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
			if !h.historyLargeValues {
				val, err := cd.SeekBothRange(prevKey, numBuf)
				if err != nil {
					return fmt.Errorf("seekBothRange %s history val [%x]: %w", h.filenameBase, prevKey, err)
				}
				if val != nil && binary.BigEndian.Uint64(val) == vTxNum {
					val = val[8:]
				} else {
					val = nil
				}

				histKeyBuf = historyKey(vTxNum, prevKey, histKeyBuf)
				if err := historyWriter.Add(histKeyBuf, val); err != nil {
					return fmt.Errorf("add %s history val [%x]: %w", h.filenameBase, prevKey, err)
				}
				continue
			}
			keyBuf = append(append(keyBuf[:0], prevKey...), numBuf...)
			key, val, err := c.SeekExact(keyBuf)
			if err != nil {
				return fmt.Errorf("seekExact %s history val [%x]: %w", h.filenameBase, key, err)
			}
			if len(val) == 0 {
				val = nil
			}

			histKeyBuf = historyKey(vTxNum, prevKey, histKeyBuf)
			if err := historyWriter.Add(histKeyBuf, val); err != nil {
				return fmt.Errorf("add %s history val [%x]: %w", h.filenameBase, key, err)
			}
		}
		bitmap.Clear()
		seqBuilder.Build()

		prevEf = seqBuilder.AppendBytes(prevEf[:0])

		if _, err = efHistoryComp.Write(prevKey); err != nil {
			return fmt.Errorf("add %s ef history key [%x]: %w", h.filenameBase, prevKey, err)
		}
		if _, err = efHistoryComp.Write(prevEf); err != nil {
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
	if err = historyWriter.Flush(); err != nil {
		return HistoryCollation{}, fmt.Errorf("add %s history val: %w", h.filenameBase, err)
	}
	closeComp = false
	mxCollationSizeHist.SetUint64(uint64(historyComp.Count()))

	return HistoryCollation{
		efHistoryComp: efHistoryComp,
		efHistoryPath: efHistoryPath,
		efBaseTxNum:   step * h.aggregationStep,
		historyPath:   historyPath,
		historyComp:   historyComp,
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
func (h *History) buildFiles(ctx context.Context, step uint64, collation HistoryCollation, ps *background.ProgressSet) (HistoryFiles, error) {
	if h.snapshotsDisabled {
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
	err = h.buildVI(ctx, historyIdxPath, historyDecomp, efHistoryDecomp, collation.efBaseTxNum, ps)
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
	k, err := kv.FirstKey(tx, h.valuesTable)
	if err != nil {
		return false, err
	}
	k2, err := kv.FirstKey(tx, h.keysTable)
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

	_bufTs           []byte
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
		r = seg.NewReader(g, ht.h.Compression)
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

	if !ht.h.historyLargeValues {
		valsCDup, err = rwTx.RwCursorDupSort(ht.h.valuesTable)
		if err != nil {
			return nil, err
		}
		defer valsCDup.Close()
	} else {
		valsC, err = rwTx.RwCursor(ht.h.valuesTable)
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
			seek = append(bytes.Clone(k), txnm...)
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
	ok, histTxNum, err := ht.iit.seekInFiles(key, txNum)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	historyItem, ok := ht.getFile(histTxNum)
	if !ok {
		log.Warn("historySeekInFiles: file not found", "key", key, "txNum", txNum, "histTxNum", histTxNum, "ssize", ht.h.aggregationStep)
		return nil, false, fmt.Errorf("hist file not found: key=%x, %s.%d-%d", key, ht.h.filenameBase, histTxNum/ht.h.aggregationStep, histTxNum/ht.h.aggregationStep)
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
	if traceGetAsOf == ht.h.filenameBase {
		fmt.Printf("DomainGetAsOf(%s, %x, %d) -> %s, histTxNum=%d, isNil(v)=%t\n", ht.h.filenameBase, key, txNum, g.FileName(), histTxNum, v == nil)
	}

	if ht.h.historyValuesOnCompressedPage > 1 {
		v, ht.snappyReadBuffer = page.Get(historyKey, v, ht.snappyReadBuffer, true)
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
	if ht.h.disable {
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
	ht.valsC, err = tx.Cursor(ht.h.valuesTable) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	return ht.valsC, nil
}
func (ht *HistoryRoTx) valsCursorDup(tx kv.Tx) (c kv.CursorDupSort, err error) {
	if ht.valsCDup != nil {
		return ht.valsCDup, nil
	}
	ht.valsCDup, err = tx.CursorDupSort(ht.h.valuesTable) //nolint:gocritic
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
	v := val[8:]
	return v, true, nil
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
		largeValues: ht.h.historyLargeValues,
		roTx:        roTx,
		valsTable:   ht.h.valuesTable,
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
		g := seg.NewReader(item.src.decompressor.MakeGetter(), ht.h.Compression)
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
		valsTable:   ht.h.valuesTable,
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

func (ht *HistoryRoTx) idxRangeOnDB(key []byte, startTxNum, endTxNum int, asc order.By, limit int, roTx kv.Tx) (stream.U64, error) {
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
		it, err := roTx.Range(ht.h.valuesTable, from, to, asc, limit)
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
	it, err := roTx.RangeDupSort(ht.h.valuesTable, key, from, to, asc, limit)
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
	return stream.Union[uint64](frozenIt, recentIt, asc, limit), nil
}
