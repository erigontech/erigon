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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/common"

	btree2 "github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common/assert"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
)

// Appendable - data type allows store data for different blockchain forks.
// - It assign new AutoIncrementID to each entity. Example: receipts, logs.
// - Each record key has `AutoIncrementID` format.
// - Use external table to refer it.
// - Only data which belongs to `canonical` block moving from DB to files.
// - It doesn't need Unwind - because `AutoIncrementID` always-growing
type Appendable struct {
	cfg AppendableCfg

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

	table           string // txnNum_u64 -> key (k+auto_increment)
	filenameBase    string
	aggregationStep uint64

	//TODO: re-visit this check - maybe we don't need it. It's abot kill in the middle of merge
	integrityCheck func(fromStep, toStep uint64) bool

	// fields for history write
	logger log.Logger

	noFsync bool // fsync is enabled by default, but tests can manually disable

	compressCfg seg.Cfg
	compression seg.FileCompression
	indexList   idxList
}

type AppendableCfg struct {
	Salt *uint32
	Dirs datadir.Dirs
	DB   kv.RoDB // global db pointer. mostly for background warmup.

	iters CanonicalsReader
}

func NewAppendable(cfg AppendableCfg, aggregationStep uint64, filenameBase, table string, integrityCheck func(fromStep uint64, toStep uint64) bool, logger log.Logger) (*Appendable, error) {
	if cfg.Dirs.SnapHistory == "" {
		panic("empty `dirs` varialbe")
	}
	compressCfg := seg.DefaultCfg
	compressCfg.Workers = 1
	ap := Appendable{
		cfg:             cfg,
		dirtyFiles:      btree2.NewBTreeGOptions[*filesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		aggregationStep: aggregationStep,
		filenameBase:    filenameBase,
		table:           table,
		compressCfg:     compressCfg,
		//compression:     seg.CompressNone, //CompressKeys | CompressVals,
		compression: seg.CompressKeys | seg.CompressVals,

		integrityCheck: integrityCheck,
		logger:         logger,
	}
	ap.indexList = withHashMap
	ap._visibleFiles = []visibleFile{}

	return &ap, nil
}

func (ap *Appendable) accessorFilePath(fromStep, toStep uint64) string {
	return filepath.Join(ap.cfg.Dirs.SnapAccessors, fmt.Sprintf("v1-%s.%d-%d.api", ap.filenameBase, fromStep, toStep))
}
func (ap *Appendable) apFilePath(fromStep, toStep uint64) string {
	return filepath.Join(ap.cfg.Dirs.SnapHistory, fmt.Sprintf("v1-%s.%d-%d.ap", ap.filenameBase, fromStep, toStep))
}

func (ap *Appendable) fileNamesOnDisk() ([]string, error) {
	return filesFromDir(ap.cfg.Dirs.SnapHistory)
}

func (ap *Appendable) openList(fNames []string) error {
	ap.closeWhatNotInList(fNames)
	ap.scanDirtyFiles(fNames)
	if err := ap.openDirtyFiles(); err != nil {
		return fmt.Errorf("NewHistory.openDirtyFiles: %w, %s", err, ap.filenameBase)
	}
	return nil
}

func (ap *Appendable) openFolder() error {
	files, err := ap.fileNamesOnDisk()
	if err != nil {
		return err
	}
	return ap.openList(files)
}

func (ap *Appendable) scanDirtyFiles(fileNames []string) (garbageFiles []*filesItem) {
	re := regexp.MustCompile("^v([0-9]+)-" + ap.filenameBase + ".([0-9]+)-([0-9]+).ap$")
	var err error
	for _, name := range fileNames {
		subs := re.FindStringSubmatch(name)
		if len(subs) != 4 {
			if len(subs) != 0 {
				ap.logger.Warn("File ignored by inverted index scan, more than 3 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startStep, endStep uint64
		if startStep, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			ap.logger.Warn("File ignored by inverted index scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endStep, err = strconv.ParseUint(subs[3], 10, 64); err != nil {
			ap.logger.Warn("File ignored by inverted index scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startStep > endStep {
			ap.logger.Warn("File ignored by inverted index scan, startTxNum > endTxNum", "name", name)
			continue
		}

		startTxNum, endTxNum := startStep*ap.aggregationStep, endStep*ap.aggregationStep
		var newFile = newFilesItem(startTxNum, endTxNum, ap.aggregationStep)

		if ap.integrityCheck != nil && !ap.integrityCheck(startStep, endStep) {
			continue
		}

		if _, has := ap.dirtyFiles.Get(newFile); has {
			continue
		}

		ap.dirtyFiles.Set(newFile)
	}
	return garbageFiles
}

func (ap *Appendable) reCalcVisibleFiles(toTxNum uint64) {
	ap._visibleFiles = calcVisibleFiles(ap.dirtyFiles, ap.indexList, false, toTxNum)
}

func (ap *Appendable) missedAccessors() (l []*filesItem) {
	ap.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			fromStep, toStep := item.startTxNum/ap.aggregationStep, item.endTxNum/ap.aggregationStep
			exists, err := dir.FileExist(ap.accessorFilePath(fromStep, toStep))
			if err != nil {
				panic(err)
			}
			if !exists {
				l = append(l, item)
			}
		}
		return true
	})
	return l
}

func (ap *Appendable) buildAccessor(ctx context.Context, fromStep, toStep uint64, d *seg.Decompressor, ps *background.ProgressSet) error {
	if d == nil {
		return fmt.Errorf("buildAccessor: passed item with nil decompressor %s %d-%d", ap.filenameBase, fromStep, toStep)
	}
	idxPath := ap.accessorFilePath(fromStep, toStep)
	cfg := recsplit.RecSplitArgs{
		Enums: true,

		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     ap.cfg.Dirs.Tmp,
		IndexFile:  idxPath,
		Salt:       ap.cfg.Salt,
		NoFsync:    ap.noFsync,

		KeyCount: d.Count(),
	}
	_, fileName := filepath.Split(idxPath)
	count := d.Count()
	p := ps.AddNew(fileName, uint64(count))
	defer ps.Delete(p)

	num := make([]byte, binary.MaxVarintLen64)
	return buildSimpleMapAccessor(ctx, d, ap.compression, cfg, ap.logger, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		if p != nil {
			p.Processed.Add(1)
		}
		n := binary.PutUvarint(num, i)
		if err := idx.AddKey(num[:n], offset); err != nil {
			return err
		}
		return nil
	})
}

func (ap *Appendable) BuildMissedAccessors(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet) {
	for _, item := range ap.missedAccessors() {
		item := item
		g.Go(func() error {
			fromStep, toStep := item.startTxNum/ap.aggregationStep, item.endTxNum/ap.aggregationStep
			return ap.buildAccessor(ctx, fromStep, toStep, item.decompressor, ps)
		})
	}
}

func (ap *Appendable) openDirtyFiles() error {
	var invalidFileItems []*filesItem
	invalidFileItemsLock := sync.Mutex{}
	ap.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			item := item
			fromStep, toStep := item.startTxNum/ap.aggregationStep, item.endTxNum/ap.aggregationStep
			if item.decompressor == nil {
				fPath := ap.apFilePath(fromStep, toStep)
				exists, err := dir.FileExist(fPath)
				if err != nil {
					_, fName := filepath.Split(fPath)
					ap.logger.Debug("[agg] Appendable.openDirtyFiles", "err", err, "f", fName)
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					continue
				}
				if !exists {
					_, fName := filepath.Split(fPath)
					ap.logger.Debug("[agg] Appendable.openDirtyFiles: file does not exists", "f", fName)
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					continue
				}

				if item.decompressor, err = seg.NewDecompressor(fPath); err != nil {
					_, fName := filepath.Split(fPath)
					if errors.Is(err, &seg.ErrCompressedFileCorrupted{}) {
						ap.logger.Debug("[agg] Appendable.openDirtyFiles", "err", err, "f", fName)
					} else {
						ap.logger.Warn("[agg] Appendable.openDirtyFiles", "err", err, "f", fName)
					}
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					// don't interrupt on error. other files may be good. but skip indices open.
					continue
				}
			}

			if item.index == nil {
				fPath := ap.accessorFilePath(fromStep, toStep)
				exists, err := dir.FileExist(fPath)
				if err != nil {
					_, fName := filepath.Split(fPath)
					ap.logger.Warn("[agg] Appendable.openDirtyFiles", "err", err, "f", fName)
				}
				if exists {
					if item.index, err = recsplit.OpenIndex(fPath); err != nil {
						_, fName := filepath.Split(fPath)
						ap.logger.Warn("[agg] Appendable.openDirtyFiles", "err", err, "f", fName)
						// don't interrupt on error. other files may be good
					}
				}
			}
		}

		return true
	})
	for _, item := range invalidFileItems {
		item.closeFiles()
		ap.dirtyFiles.Delete(item)
	}

	return nil
}

func (ap *Appendable) closeWhatNotInList(fNames []string) {
	var toClose []*filesItem
	ap.dirtyFiles.Walk(func(items []*filesItem) bool {
	Loop1:
		for _, item := range items {
			for _, protectName := range fNames {
				if item.decompressor != nil && item.decompressor.FileName() == protectName {
					continue Loop1
				}
			}
			toClose = append(toClose, item)
		}
		return true
	})
	for _, item := range toClose {
		item.closeFiles()
		ap.dirtyFiles.Delete(item)
	}
}

func (ap *Appendable) Close() {
	if ap == nil {
		return
	}
	ap.closeWhatNotInList([]string{})
}

// DisableFsync - just for tests
func (ap *Appendable) DisableFsync() { ap.noFsync = true }

func (tx *AppendableRoTx) Files() (res []string) {
	for _, item := range tx.files {
		if item.src.decompressor != nil {
			res = append(res, item.src.decompressor.FileName())
		}
	}
	return res
}

func (tx *AppendableRoTx) Get(txnID kv.TxnId, dbtx kv.Tx) (v []byte, ok bool, err error) {
	v, ok = tx.getFromFiles(uint64(txnID))
	if ok {
		return v, true, nil
	}
	v, ok, err = tx.ap.getFromDBByTs(uint64(txnID), dbtx)
	if err != nil {
		return nil, false, err
	}
	if ok {
		return v, true, nil
	}
	return nil, false, nil
}
func (tx *AppendableRoTx) Append(txnID kv.TxnId, v []byte, dbtx kv.RwTx) error {
	return dbtx.Put(tx.ap.table, hexutility.EncodeTs(uint64(txnID)), v)
}

func (tx *AppendableRoTx) getFromFiles(ts uint64) (v []byte, ok bool) {
	i, ok := tx.fileByTS(ts)
	if !ok {
		return nil, false
	}

	baseTxNum := tx.files[i].startTxNum // we are very lucky: each txNum has 1 appendable
	lookup := ts - baseTxNum
	accessor := tx.files[i].src.index
	if accessor.KeyCount() <= lookup {
		return nil, false
	}
	offset := accessor.OrdinalLookup(lookup)
	g := tx.statelessGetter(i)
	g.Reset(offset)
	k, _ := g.Next(nil)
	return k, true
}

func (tx *AppendableRoTx) fileByTS(ts uint64) (i int, ok bool) {
	for i = 0; i < len(tx.files); i++ {
		if tx.files[i].hasTS(ts) {
			return i, true
		}
	}
	return 0, false
}

func (ap *Appendable) getFromDBByTs(ts uint64, dbtx kv.Tx) ([]byte, bool, error) {
	return ap.getFromDB(hexutility.EncodeTs(ts), dbtx)
}
func (ap *Appendable) getFromDB(k []byte, dbtx kv.Tx) ([]byte, bool, error) {
	v, err := dbtx.GetOne(ap.table, k)
	if err != nil {
		return nil, false, err
	}
	return v, v != nil, err
}

// Add - !NotThreadSafe. Must use WalRLock/BatchHistoryWriteEnd
func (w *appendableBufferedWriter) Append(ts kv.TxnId, v []byte) error {
	if w.discard {
		return nil
	}
	if err := w.tableCollector.Collect(hexutility.EncodeTs(uint64(ts)), v); err != nil {
		return err
	}
	return nil
}

func (tx *AppendableRoTx) NewWriter() *appendableBufferedWriter {
	return tx.newWriter(tx.ap.cfg.Dirs.Tmp, false)
}

type appendableBufferedWriter struct {
	tableCollector *etl.Collector
	tmpdir         string
	discard        bool
	filenameBase   string

	table string

	aggregationStep uint64
}

func (w *appendableBufferedWriter) Flush(ctx context.Context, tx kv.RwTx) error {
	if w.discard {
		return nil
	}
	if err := w.tableCollector.Load(tx, w.table, loadFunc, etl.TransformArgs{Quit: ctx.Done(), EmptyVals: true}); err != nil {
		return err
	}
	w.close()
	return nil
}

func (w *appendableBufferedWriter) close() {
	if w == nil {
		return
	}
	if w.tableCollector != nil {
		w.tableCollector.Close()
	}
}

func (tx *AppendableRoTx) newWriter(tmpdir string, discard bool) *appendableBufferedWriter {
	w := &appendableBufferedWriter{
		discard:         discard,
		tmpdir:          tmpdir,
		filenameBase:    tx.ap.filenameBase,
		aggregationStep: tx.ap.aggregationStep,

		table: tx.ap.table,
		// etl collector doesn't fsync: means if have enough ram, all files produced by all collectors will be in ram
		tableCollector: etl.NewCollector("flush "+tx.ap.table, tmpdir, etl.NewSortableBuffer(WALCollectorRAM), tx.ap.logger),
	}
	w.tableCollector.LogLvl(log.LvlTrace)
	w.tableCollector.SortAndFlushInBackground(true)
	return w
}

func (ap *Appendable) BeginFilesRo() *AppendableRoTx {
	files := ap._visibleFiles
	for i := 0; i < len(files); i++ {
		if !files[i].src.frozen {
			files[i].src.refcount.Add(1)
		}
	}

	return &AppendableRoTx{
		ap:    ap,
		files: files,
	}
}

func (tx *AppendableRoTx) Close() {
	if tx.files == nil { // invariant: it's safe to call Close multiple times
		return
	}
	files := tx.files
	tx.files = nil
	for i := range files {
		src := files[i].src
		if src == nil || src.frozen {
			continue
		}
		refCnt := src.refcount.Add(-1)
		//GC: last reader responsible to remove useles files: close it and delete
		if refCnt == 0 && src.canDelete.Load() {
			if traceFileLife != "" && tx.ap.filenameBase == traceFileLife {
				tx.ap.logger.Warn("[agg.dbg] real remove at AppendableRoTx.Close", "file", src.decompressor.FileName())
			}
			src.closeFilesAndRemove()
		}
	}

	for _, r := range tx.readers {
		r.Close()
	}
}

type AppendableRoTx struct {
	ap      *Appendable
	files   visibleFiles // have no garbage (overlaps, etc...)
	getters []*seg.Reader
	readers []*recsplit.IndexReader
}

func (tx *AppendableRoTx) statelessGetter(i int) *seg.Reader {
	if tx.getters == nil {
		tx.getters = make([]*seg.Reader, len(tx.files))
	}
	r := tx.getters[i]
	if r == nil {
		g := tx.files[i].src.decompressor.MakeGetter()
		r = seg.NewReader(g, tx.ap.compression)
		tx.getters[i] = r
	}
	return r
}

func (tx *AppendableRoTx) maxTxnIDInDB(dbtx kv.Tx) (txNum kv.TxnId, ok bool) {
	k, _ := kv.LastKey(dbtx, tx.ap.table)
	if len(k) == 0 {
		return 0, false
	}
	return kv.TxnId(binary.BigEndian.Uint64(k)), true
}

func (tx *AppendableRoTx) minTxnIDInDB(dbtx kv.Tx) (kv.TxnId, bool) {
	k, _ := kv.FirstKey(dbtx, tx.ap.table)
	if len(k) == 0 {
		return 0, false
	}
	return kv.TxnId(binary.BigEndian.Uint64(k)), true
}

func (tx *AppendableRoTx) CanPrune(dbtx kv.Tx) bool {
	txnIDInDB, ok := tx.minTxnIDInDB(dbtx)
	if !ok {
		return false
	}
	_, txnIDInFiles, ok, _ := tx.ap.cfg.iters.TxNum2ID(dbtx, tx.files.EndTxNum())
	if !ok {
		return false
	}
	return txnIDInDB < txnIDInFiles
}

func (tx *AppendableRoTx) canBuild(dbtx kv.Tx) bool { //nolint
	//TODO: support "keep in db" parameter
	//TODO: what if all files are pruned?
	maxStepInFiles := tx.files.EndTxNum() / tx.ap.aggregationStep
	txNumOfNextStep := (maxStepInFiles + 1) * tx.ap.aggregationStep
	_, expectingTxnID, ok, _ := tx.ap.cfg.iters.TxNum2ID(dbtx, txNumOfNextStep)
	if !ok {
		return false
	}
	maxInDB, ok := tx.maxTxnIDInDB(dbtx)
	if !ok {
		return false
	}
	return expectingTxnID <= maxInDB
}

type AppendablePruneStat struct {
	MinTxNum     uint64
	MaxTxNum     uint64
	PruneCountTx uint64
}

func (is *AppendablePruneStat) String() string {
	if is.MinTxNum == math.MaxUint64 && is.PruneCountTx == 0 {
		return ""
	}
	return fmt.Sprintf("ap %d txs in %s-%s", is.PruneCountTx, common.PrettyCounter(is.MinTxNum), common.PrettyCounter(is.MaxTxNum))
}

func (is *AppendablePruneStat) Accumulate(other *AppendablePruneStat) {
	if other == nil {
		return
	}
	is.MinTxNum = min(is.MinTxNum, other.MinTxNum)
	is.MaxTxNum = max(is.MaxTxNum, other.MaxTxNum)
	is.PruneCountTx += other.PruneCountTx
}

// [txFrom; txTo)
// forced - prune even if CanPrune returns false, so its true only when we do Unwind.
func (tx *AppendableRoTx) Prune(ctx context.Context, rwTx kv.RwTx, txFrom, txTo, limit uint64, logEvery *time.Ticker, forced bool, fn func(key []byte, txnum []byte) error) (stat *AppendablePruneStat, err error) {
	stat = &AppendablePruneStat{MinTxNum: math.MaxUint64}
	if !forced && !tx.CanPrune(rwTx) {
		return stat, nil
	}

	mxPruneInProgress.Inc()
	defer mxPruneInProgress.Dec()
	defer func(t time.Time) { mxPruneTookIndex.ObserveDuration(t) }(time.Now())

	if limit == 0 {
		limit = math.MaxUint64
	}

	fromID, toID, ok, err := tx.txNum2id(rwTx, txFrom, txTo)
	if err != nil {
		return nil, err
	}
	if !ok {
		panic(ok)
	}
	// [from:to)
	r, err := rwTx.Range(tx.ap.table, hexutility.EncodeTs(fromID), hexutility.EncodeTs(toID))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	for r.HasNext() {
		k, _, err := r.Next()
		if err != nil {
			return nil, err
		}
		limit--
		if limit == 0 {
			break
		}
		if err = rwTx.Delete(tx.ap.table, k); err != nil {
			return nil, err
		}
	}

	return stat, err
}
func (tx *AppendableRoTx) txNum2id(rwTx kv.RwTx, txFrom, txTo uint64) (fromID, toID uint64, ok bool, err error) {
	var found1, found2 bool
	it, err := tx.ap.cfg.iters.TxnIdsOfCanonicalBlocks(rwTx, int(txFrom), -1, order.Asc, 1)
	if err != nil {
		return fromID, toID, ok, err
	}
	defer it.Close()
	if it.HasNext() {
		fromID, err = it.Next()
		if err != nil {
			return fromID, toID, ok, err
		}
		found1 = true
	}
	it.Close()

	it, err = tx.ap.cfg.iters.TxnIdsOfCanonicalBlocks(rwTx, int(txTo), -1, order.Asc, 1)
	if err != nil {
		return fromID, toID, ok, err
	}
	defer it.Close()
	if it.HasNext() {
		toID, err = it.Next()
		if err != nil {
			return fromID, toID, ok, err
		}
		found2 = true
	}

	return fromID, toID, found1 && found2, nil
}

func (ap *Appendable) collate(ctx context.Context, step uint64, roTx kv.Tx) (AppendableCollation, error) {
	stepTo := step + 1
	fromTxNum, toTxNum := step*ap.aggregationStep, stepTo*ap.aggregationStep
	start := time.Now()
	defer mxCollateTookIndex.ObserveDuration(start)

	var (
		coll = AppendableCollation{
			iiPath: ap.apFilePath(step, stepTo),
		}
		closeComp bool
	)
	defer func() {
		if closeComp {
			coll.Close()
		}
	}()

	comp, err := seg.NewCompressor(ctx, "collate "+ap.filenameBase, coll.iiPath, ap.cfg.Dirs.Tmp, ap.compressCfg, log.LvlTrace, ap.logger)
	if err != nil {
		return coll, fmt.Errorf("create %s compressor: %w", ap.filenameBase, err)
	}
	coll.writer = seg.NewWriter(comp, seg.CompressNone)

	it, err := ap.cfg.iters.TxnIdsOfCanonicalBlocks(roTx, int(fromTxNum), int(toTxNum), order.Asc, -1)
	if err != nil {
		return coll, fmt.Errorf("collate %s: %w", ap.filenameBase, err)
	}
	defer it.Close()

	for it.HasNext() {
		k, err := it.Next()
		if err != nil {
			return coll, fmt.Errorf("collate %s: %w", ap.filenameBase, err)
		}
		v, ok, err := ap.getFromDBByTs(k, roTx)
		if err != nil {
			return coll, fmt.Errorf("collate %s: %w", ap.filenameBase, err)
		}
		if !ok {
			continue
		}
		if err = coll.writer.AddWord(v); err != nil {
			return coll, fmt.Errorf("collate %s: %w", ap.filenameBase, err)
		}
	}
	closeComp = false
	return coll, nil
}

func (ap *Appendable) stepsRangeInDB(tx kv.Tx) (from, to float64) {
	fst, _ := kv.FirstKey(tx, ap.table)
	if len(fst) > 0 {
		from = float64(binary.BigEndian.Uint64(fst)) / float64(ap.aggregationStep)
	}
	lst, _ := kv.LastKey(tx, ap.table)
	if len(lst) > 0 {
		to = float64(binary.BigEndian.Uint64(lst)) / float64(ap.aggregationStep)
	}
	if to == 0 {
		to = from
	}
	return from, to
}

type AppendableFiles struct {
	decomp *seg.Decompressor
	index  *recsplit.Index
}

func (sf AppendableFiles) CleanupOnError() {
	if sf.decomp != nil {
		sf.decomp.Close()
	}
	if sf.index != nil {
		sf.index.Close()
	}
}

type AppendableCollation struct {
	iiPath string
	writer *seg.Writer
}

func (collation AppendableCollation) Close() {
	if collation.writer != nil {
		collation.writer.Close()
		collation.writer = nil //nolint
	}
}

// buildFiles - `step=N` means build file `[N:N+1)` which is equal to [N:N+1)
func (ap *Appendable) buildFiles(ctx context.Context, step uint64, coll AppendableCollation, ps *background.ProgressSet) (AppendableFiles, error) {
	var (
		decomp *seg.Decompressor
		index  *recsplit.Index
		err    error
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
		}
	}()

	if assert.Enable {
		if coll.iiPath == "" && reflect.ValueOf(coll.writer).IsNil() {
			panic("assert: collation is not initialized " + ap.filenameBase)
		}
	}

	{
		p := ps.AddNew(path.Base(coll.iiPath), 1)
		if err = coll.writer.Compress(); err != nil {
			ps.Delete(p)
			return AppendableFiles{}, fmt.Errorf("compress %s: %w", ap.filenameBase, err)
		}
		coll.Close()
		ps.Delete(p)
	}

	if decomp, err = seg.NewDecompressor(coll.iiPath); err != nil {
		return AppendableFiles{}, fmt.Errorf("open %s decompressor: %w", ap.filenameBase, err)
	}

	if err := ap.buildAccessor(ctx, step, step+1, decomp, ps); err != nil {
		return AppendableFiles{}, fmt.Errorf("build %s api: %w", ap.filenameBase, err)
	}
	if index, err = recsplit.OpenIndex(ap.accessorFilePath(step, step+1)); err != nil {
		return AppendableFiles{}, err
	}

	closeComp = false
	return AppendableFiles{decomp: decomp, index: index}, nil
}

func (ap *Appendable) integrateDirtyFiles(sf AppendableFiles, txNumFrom, txNumTo uint64) {
	fi := newFilesItem(txNumFrom, txNumTo, ap.aggregationStep)
	fi.decompressor = sf.decomp
	fi.index = sf.index
	ap.dirtyFiles.Set(fi)
}

func (tx *AppendableRoTx) Unwind(ctx context.Context, rwTx kv.RwTx, txFrom, txTo, limit uint64, logEvery *time.Ticker, forced bool, fn func(key []byte, txnum []byte) error) error {
	return nil //Appendable type is unwind-less. See docs of Appendable type.
}
