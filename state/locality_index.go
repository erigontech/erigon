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
	"path/filepath"
	"regexp"
	"strconv"
	"sync/atomic"

	_ "github.com/FastFilter/xorfilter"
	bloomfilter "github.com/holiman/bloomfilter/v2"
	"github.com/ledgerwatch/erigon-lib/common/assert"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/log/v3"
)

const LocalityIndexUint64Limit = 64 //bitmap spend 1 bit per file, stored as uint64

// LocalityIndex - has info in which .ef or .kv files exists given key
// Format: key -> bitmap(step_number_list)
// step_number_list is list of .ef files where exists given key
type LocalityIndex struct {
	filenameBase    string
	dir, tmpdir     string // Directory where static files are created
	aggregationStep uint64 // immutable

	// preferSmallerFiles forcing files like `32-40.l` have higher priority than `0-40.l`.
	// It's used by "warm data indexing": new small "warm index" created after old data
	// merged and indexed by "cold index"
	preferSmallerFiles bool

	file *filesItem

	roFiles atomic.Pointer[ctxItem]
	logger  log.Logger

	noFsync bool // fsync is enabled by default, but tests can manually disable
}

func NewLocalityIndex(preferSmallerFiles bool, dir, filenameBase string, aggregationStep uint64, tmpdir string, logger log.Logger) *LocalityIndex {
	return &LocalityIndex{
		preferSmallerFiles: preferSmallerFiles,
		dir:                dir,
		tmpdir:             tmpdir,
		aggregationStep:    aggregationStep,
		filenameBase:       filenameBase,
		logger:             logger,
	}
}
func (li *LocalityIndex) closeWhatNotInList(fNames []string) {
	if li == nil || li.file == nil {
		return
	}

	for _, protectName := range fNames {
		if li.file.bm.FileName() == protectName {
			return
		}
	}
	li.closeFiles()
}

func (li *LocalityIndex) OpenList(fNames []string) error {
	if li == nil {
		return nil
	}
	li.closeWhatNotInList(fNames)
	_ = li.scanStateFiles(fNames)
	if err := li.openFiles(); err != nil {
		return fmt.Errorf("LocalityIndex.openFiles: %s, %w", li.filenameBase, err)
	}
	return nil
}

func (li *LocalityIndex) scanStateFiles(fNames []string) (uselessFiles []*filesItem) {
	if li == nil {
		return nil
	}

	re := regexp.MustCompile("^" + li.filenameBase + ".([0-9]+)-([0-9]+).l$")
	var err error
	for _, name := range fNames {
		subs := re.FindStringSubmatch(name)
		if len(subs) != 3 {
			if len(subs) != 0 {
				li.logger.Warn("File ignored by inverted index scan, more than 3 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startStep, endStep uint64
		if startStep, err = strconv.ParseUint(subs[1], 10, 64); err != nil {
			li.logger.Warn("File ignored by inverted index scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endStep, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			li.logger.Warn("File ignored by inverted index scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startStep > endStep {
			li.logger.Warn("File ignored by inverted index scan, startTxNum > endTxNum", "name", name)
			continue
		}

		if endStep-startStep > StepsInColdFile*LocalityIndexUint64Limit {
			li.logger.Warn("LocalityIndex does store bitmaps as uint64, means it can't handle > 2048 steps. But it's possible to implement")
			continue
		}

		startTxNum, endTxNum := startStep*li.aggregationStep, endStep*li.aggregationStep
		useThisFile := li.file == nil ||
			(li.file.endTxNum < endTxNum) || // newer
			(li.preferSmallerFiles && li.file.endTxNum == endTxNum && li.file.startTxNum < startTxNum) ||
			(!li.preferSmallerFiles && li.file.startTxNum == startTxNum && li.file.endTxNum < endTxNum)
		if useThisFile {
			li.file = newFilesItem(startTxNum, endTxNum, li.aggregationStep)
			li.file.frozen = false // LocalityIndex files are never frozen
		}
	}
	return uselessFiles
}

func (li *LocalityIndex) openFiles() (err error) {
	if li == nil || li.file == nil {
		return nil
	}

	fromStep, toStep := li.file.startTxNum/li.aggregationStep, li.file.endTxNum/li.aggregationStep
	if li.file.bm == nil {
		dataPath := filepath.Join(li.dir, fmt.Sprintf("%s.%d-%d.l", li.filenameBase, fromStep, toStep))
		if dir.FileExist(dataPath) {
			li.file.bm, err = bitmapdb.OpenFixedSizeBitmaps(dataPath)
			if err != nil {
				return err
			}
		}
	}
	if li.file.index == nil {
		idxPath := filepath.Join(li.dir, fmt.Sprintf("%s.%d-%d.li", li.filenameBase, fromStep, toStep))
		if dir.FileExist(idxPath) {
			li.file.index, err = recsplit.OpenIndex(idxPath)
			if err != nil {
				return fmt.Errorf("LocalityIndex.openFiles: %w, %s", err, idxPath)
			}
		}
	}
	if li.file.bloom == nil {
		idxPath := filepath.Join(li.dir, fmt.Sprintf("%s.%d-%d.li.lb", li.filenameBase, fromStep, toStep))
		if dir.FileExist(idxPath) {
			li.file.bloom, _, err = bloomfilter.ReadFile(idxPath)
			if err != nil {
				return fmt.Errorf("LocalityIndex.openFiles: %w, %s", err, idxPath)
			}
		}
	}
	li.reCalcRoFiles()
	return nil
}

func (li *LocalityIndex) closeFiles() {
	if li == nil || li.file == nil {
		return
	}
	if li.file.index != nil {
		li.file.index.Close()
		li.file.index = nil
	}
	if li.file.bm != nil {
		li.file.bm.Close()
		li.file.bm = nil
	}
	if li.file.bloom != nil {
		li.file.bloom = nil
	}
}
func (li *LocalityIndex) reCalcRoFiles() {
	if li == nil {
		return
	}

	if li.file == nil {
		li.roFiles.Store(nil)
		return
	}
	li.roFiles.Store(&ctxItem{
		startTxNum: li.file.startTxNum,
		endTxNum:   li.file.endTxNum,
		i:          0,
		src:        li.file,
	})
}

func (li *LocalityIndex) MakeContext() *ctxLocalityIdx {
	if li == nil {
		return nil
	}
	x := &ctxLocalityIdx{
		file:            li.roFiles.Load(),
		aggregationStep: li.aggregationStep,
	}
	if x.file != nil && x.file.src != nil {
		x.file.src.refcount.Add(1)
	}
	return x
}

func (lc *ctxLocalityIdx) Close() {
	if lc == nil || lc.file == nil || lc.file.src == nil { // invariant: it's safe to call Close multiple times
		return
	}
	refCnt := lc.file.src.refcount.Add(-1)
	if refCnt == 0 && lc.file.src.canDelete.Load() {
		closeLocalityIndexFilesAndRemove(lc)
	}
	lc.file = nil
}

func closeLocalityIndexFilesAndRemove(i *ctxLocalityIdx) {
	if i.file == nil || i.file.src == nil {
		return
	}
	i.file.src.closeFilesAndRemove()
	i.file.src = nil
}

func (li *LocalityIndex) Close() {
	li.closeWhatNotInList([]string{})
	li.reCalcRoFiles()
}
func (li *LocalityIndex) Files() (res []string) { return res }
func (li *LocalityIndex) NewIdxReader() *recsplit.IndexReader {
	if li != nil && li.file != nil && li.file.index != nil {
		return recsplit.NewIndexReader(li.file.index)
	}
	return nil
}

// LocalityIndex return exactly 2 file (step)
// prevents searching key in many files
func (lc *ctxLocalityIdx) lookupIdxFiles(key []byte, fromTxNum uint64) (exactShard1, exactShard2 uint64, lastIndexedTxNum uint64, ok1, ok2 bool) {
	if lc == nil || lc.file == nil {
		return 0, 0, 0, false, false
	}
	if lc.reader == nil {
		lc.reader = recsplit.NewIndexReader(lc.file.src.index)
	}

	if fromTxNum >= lc.file.endTxNum {
		return 0, 0, fromTxNum, false, false
	}

	fromFileNum := fromTxNum / lc.aggregationStep / StepsInColdFile
	fn1, fn2, ok1, ok2, err := lc.file.src.bm.First2At(lc.reader.Lookup(key), fromFileNum)
	if err != nil {
		panic(err)
	}
	return fn1 * StepsInColdFile, fn2 * StepsInColdFile, lc.file.endTxNum, ok1, ok2
}

// indexedTo - [from, to)
func (lc *ctxLocalityIdx) indexedTo() uint64 {
	if lc == nil || lc.file == nil {
		return 0
	}
	return lc.file.endTxNum
}
func (lc *ctxLocalityIdx) indexedFrom() (uint64, bool) {
	if lc == nil || lc.file == nil {
		return 0, false
	}
	return lc.file.startTxNum, true
}

// lookupLatest return latest file (step)
// prevents searching key in many files
func (lc *ctxLocalityIdx) lookupLatest(key []byte) (latestShard uint64, ok bool, err error) {
	if lc == nil || lc.file == nil || lc.file.src.index == nil {
		return 0, false, nil
	}
	if lc.reader == nil {
		lc.reader = recsplit.NewIndexReader(lc.file.src.index)
	}
	if lc.reader.Empty() {
		return 0, false, nil
	}

	if !lc.file.src.bloom.ContainsHash(localityHash(key)) {
		return 0, false, nil
	}

	//if bytes.HasPrefix(key, common.FromHex("f29a")) {
	//	res, _ := lc.file.src.bm.At(lc.reader.Lookup(key))
	//	l, _, _ := lc.file.src.bm.LastAt(lc.reader.Lookup(key))
	//	fmt.Printf("idx: %x, %d, last: %d\n", key, res, l)
	//}
	return lc.file.src.bm.LastAt(lc.reader.Lookup(key))
}

func (li *LocalityIndex) exists(fromStep, toStep uint64) bool {
	return dir.FileExist(filepath.Join(li.dir, fmt.Sprintf("%s.%d-%d.li", li.filenameBase, fromStep, toStep)))
}
func (li *LocalityIndex) missedIdxFiles(ii *HistoryContext) (toStep uint64, idxExists bool) {
	if len(ii.files) == 0 {
		return 0, true
	}
	var item *ctxItem
	for i := len(ii.files) - 1; i >= 0; i-- {
		if ii.files[i].src.frozen {
			item = &ii.files[i]
			break
		}
	}
	if item != nil {
		toStep = item.endTxNum / li.aggregationStep
	}
	fName := fmt.Sprintf("%s.%d-%d.li", li.filenameBase, 0, toStep)
	return toStep, dir.FileExist(filepath.Join(li.dir, fName))
}

// newStateBloomWithSize creates a brand new state bloom for state generation.
// The bloom filter will be created by the passing bloom filter size. According
// to the https://hur.st/bloomfilter/?n=600000000&p=&m=2048MB&k=4, the parameters
// are picked so that the false-positive rate for mainnet is low enough.
func newColdBloomWithSize(megabytes uint64) (*bloomfilter.Filter, error) {
	return bloomfilter.New(megabytes*1024*1024*8, 4)
}

func (li *LocalityIndex) buildFiles(ctx context.Context, fromStep, toStep uint64, convertStepsToFileNums bool, ps *background.ProgressSet, makeIter func() *LocalityIterator) (files *LocalityIndexFiles, err error) {
	if toStep < fromStep {
		return nil, fmt.Errorf("LocalityIndex.buildFiles: fromStep(%d) < toStep(%d)", fromStep, toStep)
	}

	fName := fmt.Sprintf("%s.%d-%d.li", li.filenameBase, fromStep, toStep)
	idxPath := filepath.Join(li.dir, fName)
	filePath := filepath.Join(li.dir, fmt.Sprintf("%s.%d-%d.l", li.filenameBase, fromStep, toStep))

	p := ps.AddNew(fName, uint64(1))
	defer ps.Delete(p)

	count := 0
	it := makeIter()
	defer it.Close()
	//if it.FilesAmount() == 1 { // optimization: no reason to create LocalityIndex for 1 file
	//	return nil, nil
	//}

	for it.HasNext() {
		_, _, _ = it.Next()
		count++
	}
	it.Close()

	p.Total.Store(uint64(count))

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   count,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     li.tmpdir,
		IndexFile:  idxPath,
	}, li.logger)
	if err != nil {
		return nil, fmt.Errorf("create recsplit: %w", err)
	}
	defer rs.Close()
	rs.LogLvl(log.LvlTrace)
	if li.noFsync {
		rs.DisableFsync()
	}
	for {
		p.Processed.Store(0)
		i := uint64(0)
		maxPossibleValue := int(toStep - fromStep)
		baseDataID := fromStep
		if convertStepsToFileNums {
			maxPossibleValue = int(it.FilesAmount())
			baseDataID = uint64(0)
		}
		dense, err := bitmapdb.NewFixedSizeBitmapsWriter(filePath, maxPossibleValue, baseDataID, uint64(count), li.logger)
		if err != nil {
			return nil, err
		}
		defer dense.Close()
		if li.noFsync {
			dense.DisableFsync()
		}

		//bloom, err := newColdBloomWithSize(128)
		bloom, err := bloomfilter.NewOptimal(uint64(count), 0.01)
		if err != nil {
			return nil, err
		}

		it = makeIter()
		defer it.Close()
		for it.HasNext() {
			k, inSteps, err := it.Next()
			if err != nil {
				return nil, err
			}
			//if bytes.HasPrefix(k, common.FromHex("5e7d")) {
			//	fmt.Printf("build: %x, %d\n", k, inSteps)
			//}

			if convertStepsToFileNums {
				for j := range inSteps {
					inSteps[j] = inSteps[j] / StepsInColdFile
				}
			}

			bloom.AddHash(localityHash(k))
			//wrintf("buld: %x, %d, %d\n", k, i, inFiles)
			if err := dense.AddArray(i, inSteps); err != nil {
				return nil, err
			}
			if err = rs.AddKey(k, i); err != nil {
				return nil, err
			}
			i++
			p.Processed.Add(1)
		}
		it.Close()

		fmt.Printf("bloom: %s, keys=%dk, size=%smb, probability=%f\n", fName, bloom.N()/1000, bloom.M()/8/1024/1024, bloom.FalsePosititveProbability())
		bloom.WriteFile(idxPath + ".lb")

		if err := dense.Build(); err != nil {
			return nil, err
		}

		if err = rs.Build(); err != nil {
			if rs.Collision() {
				li.logger.Debug("Building recsplit. Collision happened. It's ok. Restarting...")
				rs.ResetNextSalt()
			} else {
				return nil, fmt.Errorf("build idx: %w", err)
			}
		} else {
			break
		}
	}

	idx, err := recsplit.OpenIndex(idxPath)
	if err != nil {
		return nil, err
	}
	bm, err := bitmapdb.OpenFixedSizeBitmaps(filePath)
	if err != nil {
		return nil, err
	}
	bloom, _, err := bloomfilter.ReadFile(idxPath + ".lb")
	if err != nil {
		return nil, err
	}
	return &LocalityIndexFiles{index: idx, bm: bm, bloom: bloom, fromStep: fromStep, toStep: toStep}, nil
}

func localityHash(k []byte) uint64 {
	if len(k) <= 20 {
		return binary.BigEndian.Uint64(k)
	}
	lo := binary.BigEndian.Uint32(k[20:])
	if lo == 0 {
		lo = binary.BigEndian.Uint32(k[len(k)-4:])
	}
	return uint64(binary.BigEndian.Uint32(k))<<32 | uint64(lo)
}

func (li *LocalityIndex) integrateFiles(sf *LocalityIndexFiles) {
	if li == nil {
		return
	}
	if li.file != nil {
		li.file.canDelete.Store(true)
	}
	if sf == nil {
		return //TODO: support non-indexing of single file
		//li.file = nil
		//li.bm = nil
	} else {
		li.file = &filesItem{
			startTxNum: sf.fromStep * li.aggregationStep,
			endTxNum:   sf.toStep * li.aggregationStep,
			index:      sf.index,
			bm:         sf.bm,
			bloom:      sf.bloom,
			frozen:     false,
		}
	}
	li.reCalcRoFiles()
}

func (li *LocalityIndex) BuildMissedIndices(ctx context.Context, fromStep, toStep uint64, convertStepsToFileNums bool, ps *background.ProgressSet, makeIter func() *LocalityIterator) error {
	f, err := li.buildFiles(ctx, fromStep, toStep, convertStepsToFileNums, ps, makeIter)
	if err != nil {
		return err
	}
	li.integrateFiles(f)
	return nil
}

type LocalityIndexFiles struct {
	index *recsplit.Index
	bm    *bitmapdb.FixedSizeBitmaps
	bloom *bloomfilter.Filter

	fromStep, toStep uint64
}

func (sf LocalityIndexFiles) Close() {
	if sf.index != nil {
		sf.index.Close()
	}
	if sf.bm != nil {
		sf.bm.Close()
	}
	if sf.bloom != nil {
		sf.bloom = nil
	}
}

type LocalityIterator struct {
	aggStep           uint64
	compressVals      bool
	h                 ReconHeapOlderFirst
	v, nextV, vBackup []uint64
	k, nextK, kBackup []byte
	progress          uint64

	totalOffsets, filesAmount uint64
	involvedFiles             []*compress.Decompressor //used in destructor to disable read-ahead
	ctx                       context.Context
}

func (si *LocalityIterator) advance() {
	for si.h.Len() > 0 {
		top := heap.Pop(&si.h).(*ReconItem)
		key := top.key
		var offset uint64
		if si.compressVals {
			offset = top.g.Skip()
		} else {
			offset = top.g.SkipUncompressed()
		}
		si.progress += offset - top.lastOffset
		top.lastOffset = offset
		inStep := top.startTxNum / si.aggStep
		if top.g.HasNext() {
			top.key, _ = top.g.NextUncompressed()
			heap.Push(&si.h, top)
		}

		if si.k == nil {
			si.k = key
			si.v = append(si.v, inStep)
			continue
		}

		if !bytes.Equal(key, si.k) {
			si.nextV, si.v = si.v, si.nextV[:0]
			si.nextK = si.k

			si.v = append(si.v, inStep)
			si.k = key
			return
		}
		si.v = append(si.v, inStep)
	}
	si.nextV, si.v = si.v, si.nextV[:0]
	si.nextK = si.k
	si.k = nil
}

func (si *LocalityIterator) HasNext() bool { return si.nextK != nil }
func (si *LocalityIterator) Progress() float64 {
	return (float64(si.progress) / float64(si.totalOffsets)) * 100
}
func (si *LocalityIterator) FilesAmount() uint64 { return si.filesAmount }

func (si *LocalityIterator) Next() ([]byte, []uint64, error) {
	select {
	case <-si.ctx.Done():
		return nil, nil, si.ctx.Err()
	default:
	}

	//if hi.err != nil {
	//	return nil, nil, hi.err
	//}
	//hi.limit--

	// Satisfy iter.Dual Invariant 2
	si.nextK, si.kBackup, si.nextV, si.vBackup = si.kBackup, si.nextK, si.vBackup, si.nextV
	si.advance()
	return si.kBackup, si.vBackup, nil
}

// Close - safe to call multiple times
func (si *LocalityIterator) Close() {
	for _, f := range si.involvedFiles {
		f.DisableReadAhead()
	}
	si.involvedFiles = nil
}

// iterateKeysLocality [from, to)
func (ic *InvertedIndexContext) iterateKeysLocality(ctx context.Context, fromStep, toStep uint64, last *compress.Decompressor) *LocalityIterator {
	fromTxNum, toTxNum := fromStep*ic.ii.aggregationStep, toStep*ic.ii.aggregationStep
	si := &LocalityIterator{ctx: ctx, aggStep: ic.ii.aggregationStep, compressVals: false}

	for _, item := range ic.files {
		if item.endTxNum <= fromTxNum || item.startTxNum >= toTxNum {
			continue
		}
		if assert.Enable {
			if (item.endTxNum-item.startTxNum)/si.aggStep != StepsInColdFile {
				panic(fmt.Errorf("frozen file of small size: %s", item.src.decompressor.FileName()))
			}
		}
		item.src.decompressor.EnableReadAhead() // disable in destructor of iterator
		si.involvedFiles = append(si.involvedFiles, item.src.decompressor)

		g := item.src.decompressor.MakeGetter()
		if g.HasNext() {
			key, offset := g.NextUncompressed()

			heapItem := &ReconItem{startTxNum: item.startTxNum, endTxNum: item.endTxNum, g: g, txNum: ^item.endTxNum, key: key, startOffset: offset, lastOffset: offset}
			heap.Push(&si.h, heapItem)
		}
		si.totalOffsets += uint64(g.Size())
		si.filesAmount++
	}

	if last != nil {
		//add last one
		last.EnableReadAhead() // disable in destructor of iterator
		si.involvedFiles = append(si.involvedFiles, last)
		g := last.MakeGetter()
		if g.HasNext() {
			key, offset := g.NextUncompressed()

			startTxNum, endTxNum := (toStep-1)*ic.ii.aggregationStep, toStep*ic.ii.aggregationStep
			heapItem := &ReconItem{startTxNum: startTxNum, endTxNum: endTxNum, g: g, txNum: ^endTxNum, key: key, startOffset: offset, lastOffset: offset}
			heap.Push(&si.h, heapItem)
		}
		si.totalOffsets += uint64(g.Size())
		si.filesAmount++
	}

	si.advance()
	return si
}
