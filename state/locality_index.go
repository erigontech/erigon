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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/log/v3"
)

const LocalityIndexUint64Limit = 64 //bitmap spend 1 bit per file, stored as uint64

// LocalityIndex - has info in which .ef files exists given key
// Format: key -> bitmap(step_number_list)
// step_number_list is list of .ef files where exists given key
type LocalityIndex struct {
	//file         *filesItem
	filenameBase    string
	dir             string // Directory where static files are created
	tmpdir          string // Directory where static files are created
	aggregationStep uint64 // Directory where static files are created

	file *filesItem
	bm   *bitmapdb.FixedSizeBitmaps
}

func NewLocalityIndex(
	dir, tmpdir string,
	aggregationStep uint64,
	filenameBase string,
) (*LocalityIndex, error) {
	li := &LocalityIndex{
		dir:             dir,
		tmpdir:          tmpdir,
		aggregationStep: aggregationStep,
		filenameBase:    filenameBase,
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("NewInvertedIndex: %s, %w", filenameBase, err)
	}
	uselessFiles := li.scanStateFiles(files)
	for _, f := range uselessFiles {
		_ = os.Remove(filepath.Join(li.dir, f))
	}
	if err = li.openFiles(); err != nil {
		return nil, fmt.Errorf("NewInvertedIndex: %s, %w", filenameBase, err)
	}
	return li, nil
}

func (li *LocalityIndex) scanStateFiles(files []fs.DirEntry) (uselessFiles []string) {
	re := regexp.MustCompile("^" + li.filenameBase + ".([0-9]+)-([0-9]+).li$")
	var err error
	for _, f := range files {
		if !f.Type().IsRegular() {
			continue
		}

		name := f.Name()
		subs := re.FindStringSubmatch(name)
		if len(subs) != 3 {
			if len(subs) != 0 {
				log.Warn("File ignored by inverted index scan, more than 3 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startStep, endStep uint64
		if startStep, err = strconv.ParseUint(subs[1], 10, 64); err != nil {
			log.Warn("File ignored by inverted index scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endStep, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			log.Warn("File ignored by inverted index scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startStep > endStep {
			log.Warn("File ignored by inverted index scan, startTxNum > endTxNum", "name", name)
			continue
		}

		if startStep != 0 {
			log.Warn("LocalityIndex must always starts from step 0")
			continue
		}
		if endStep > StepsInBiggestFile*LocalityIndexUint64Limit {
			log.Warn("LocalityIndex does store bitmaps as uint64, means it can't handle > 2048 steps. But it's possible to implement")
			continue
		}

		startTxNum, endTxNum := startStep*li.aggregationStep, endStep*li.aggregationStep
		if li.file == nil {
			li.file = &filesItem{startTxNum: startTxNum, endTxNum: endTxNum}
		} else if li.file.endTxNum < endTxNum {
			uselessFiles = append(uselessFiles,
				fmt.Sprintf("%s.%d-%d.li", li.filenameBase, li.file.startTxNum/li.aggregationStep, li.file.endTxNum/li.aggregationStep),
			)
			li.file = &filesItem{startTxNum: startTxNum, endTxNum: endTxNum}
		}
	}
	return uselessFiles
}

func (li *LocalityIndex) openFiles() (err error) {
	if li.file == nil {
		return nil
	}
	fromStep, toStep := li.file.startTxNum/li.aggregationStep, li.file.endTxNum/li.aggregationStep
	idxPath := filepath.Join(li.dir, fmt.Sprintf("%s.%d-%d.li", li.filenameBase, fromStep, toStep))
	li.file.index, err = recsplit.OpenIndex(idxPath)
	if err != nil {
		return fmt.Errorf("LocalityIndex.openFiles: %w, %s", err, idxPath)
	}
	dataPath := filepath.Join(li.dir, fmt.Sprintf("%s.%d-%d.l", li.filenameBase, fromStep, toStep))
	li.bm, err = bitmapdb.OpenFixedSizeBitmaps(dataPath, int((toStep-fromStep)/StepsInBiggestFile))
	if err != nil {
		return err
	}
	return nil
}

func (li *LocalityIndex) closeFiles() {
	if li.file.index != nil {
		li.file.index.Close()
	}
	if li.bm != nil {
		li.bm.Close()
	}
}

func (li *LocalityIndex) Close() {
	li.closeFiles()
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
func (li *LocalityIndex) lookupIdxFiles(r *recsplit.IndexReader, bm *bitmapdb.FixedSizeBitmaps, key []byte, fromTxNum uint64) (exactShard1, exactShard2 uint64, lastIndexedTxNum uint64, ok1, ok2 bool) {
	if li == nil || r == nil || bm == nil {
		return 0, 0, 0, false, false
	}
	if fromTxNum >= li.file.endTxNum {
		return 0, 0, fromTxNum, false, false
	}

	fromFileNum := fromTxNum / li.aggregationStep / StepsInBiggestFile
	fn1, fn2, ok1, ok2, err := bm.First2At(r.Lookup(key), fromFileNum)
	if err != nil {
		panic(err)
	}
	return fn1 * StepsInBiggestFile, fn2 * StepsInBiggestFile, li.file.endTxNum, ok1, ok2
}

func (li *LocalityIndex) missedIdxFiles(ii *InvertedIndex) (toStep uint64, idxExists bool) {
	ii.files.Descend(func(item *filesItem) bool {
		if item.endTxNum-item.startTxNum == StepsInBiggestFile*li.aggregationStep {
			toStep = item.endTxNum / li.aggregationStep
			return false
		}
		return true
	})
	fName := fmt.Sprintf("%s.%d-%d.li", li.filenameBase, 0, toStep)
	return toStep, dir.FileExist(filepath.Join(li.dir, fName))
}
func (li *LocalityIndex) buildFiles(ctx context.Context, ii *InvertedIndex, toStep uint64) (files *LocalityIndexFiles, err error) {
	defer ii.EnableMadvNormalReadAhead().DisableReadAhead()

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	fromStep := uint64(0)

	count := 0
	it := ii.MakeContext().iterateKeysLocality(toStep * li.aggregationStep)
	for it.HasNext() {
		_, _ = it.Next()
		count++
		//select {
		//case <-ctx.Done():
		//	return nil, ctx.Err()
		//case <-logEvery.C:
		//	log.Info("[LocalityIndex] build", "name", li.filenameBase, "progress", fmt.Sprintf("%.2f%%", it.Progress()/2))
		//default:
		//}
	}

	fName := fmt.Sprintf("%s.%d-%d.li", li.filenameBase, fromStep, toStep)
	idxPath := filepath.Join(li.dir, fName)
	filePath := filepath.Join(li.dir, fmt.Sprintf("%s.%d-%d.l", li.filenameBase, fromStep, toStep))

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   count,
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     li.tmpdir,
		IndexFile:  idxPath,
	})
	if err != nil {
		return nil, fmt.Errorf("create recsplit: %w", err)
	}
	defer rs.Close()
	rs.LogLvl(log.LvlTrace)

	i := uint64(0)
	for {
		dense, err := bitmapdb.NewFixedSizeBitmapsWriter(filePath, int(it.FilesAmount()), uint64(count))
		if err != nil {
			return nil, err
		}
		defer dense.Close()

		it = ii.MakeContext().iterateKeysLocality(toStep * li.aggregationStep)
		for it.HasNext() {
			k, inFiles := it.Next()
			if err := dense.AddArray(i, inFiles); err != nil {
				return nil, err
			}
			if err = rs.AddKey(k, 0); err != nil {
				return nil, err
			}
			i++

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-logEvery.C:
				log.Debug("[LocalityIndex] build", "name", li.filenameBase, "progress", fmt.Sprintf("%.2f%%", 50+it.Progress()/2))
			default:
			}
		}

		if err := dense.Build(); err != nil {
			return nil, err
		}

		if err = rs.Build(); err != nil {
			if rs.Collision() {
				log.Debug("Building recsplit. Collision happened. It's ok. Restarting...")
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
	bm, err := bitmapdb.OpenFixedSizeBitmaps(filePath, int(it.FilesAmount()))
	if err != nil {
		return nil, err
	}
	return &LocalityIndexFiles{index: idx, bm: bm}, nil
}

func (li *LocalityIndex) integrateFiles(sf LocalityIndexFiles, txNumFrom, txNumTo uint64) {
	li.file = &filesItem{
		startTxNum: txNumFrom,
		endTxNum:   txNumTo,
		index:      sf.index,
	}
	li.bm = sf.bm
}

func (li *LocalityIndex) BuildMissedIndices(ctx context.Context, ii *InvertedIndex) error {
	if li == nil {
		return nil
	}
	toStep, idxExists := li.missedIdxFiles(ii)
	if idxExists {
		return nil
	}
	if toStep == 0 {
		return nil
	}
	fromStep := uint64(0)
	f, err := li.buildFiles(ctx, ii, toStep)
	if err != nil {
		return err
	}
	var oldFile *filesItem
	if li.file != nil {
		oldFile = li.file
	}
	li.integrateFiles(*f, fromStep*li.aggregationStep, toStep*li.aggregationStep)
	if err = li.deleteFiles(oldFile); err != nil {
		return err
	}
	return nil
}

type LocalityIndexFiles struct {
	index *recsplit.Index
	bm    *bitmapdb.FixedSizeBitmaps
}

func (sf LocalityIndexFiles) Close() {
	if sf.index != nil {
		sf.index.Close()
	}
	if sf.bm != nil {
		sf.bm.Close()
	}
}

type LocalityIterator struct {
	hc               *InvertedIndexContext
	h                ReconHeapOlderFirst
	files, nextFiles []uint64
	key, nextKey     []byte
	progress         uint64
	hasNext          bool

	totalOffsets, filesAmount uint64
}

func (si *LocalityIterator) advance() {
	for si.h.Len() > 0 {
		top := heap.Pop(&si.h).(*ReconItem)
		key := top.key
		_, offset := top.g.NextUncompressed()
		si.progress += offset - top.lastOffset
		top.lastOffset = offset
		inStep := uint32(top.startTxNum / si.hc.ii.aggregationStep)
		if top.g.HasNext() {
			top.key, _ = top.g.NextUncompressed()
			heap.Push(&si.h, top)
		}

		inFile := inStep / StepsInBiggestFile
		if inFile > 64 {
			panic("this index supports only up to 64 files")
		}

		if !bytes.Equal(key, si.key) {
			if si.key == nil {
				si.key = key
				si.files = append(si.files, uint64(inFile))
				continue
			}

			si.nextFiles, si.files = si.files, si.nextFiles[:0]
			si.nextKey = si.key

			si.files = append(si.files, uint64(inFile))
			si.key = key
			si.hasNext = true
			return
		}
		si.files = append(si.files, uint64(inFile))
	}
	si.nextFiles, si.files = si.files, si.nextFiles[:0]
	si.nextKey = si.key
	si.hasNext = false
}

func (si *LocalityIterator) HasNext() bool { return si.hasNext }
func (si *LocalityIterator) Progress() float64 {
	return ((float64(si.progress) / float64(si.totalOffsets)) * 100) / 2
}
func (si *LocalityIterator) FilesAmount() uint64 { return si.filesAmount }

func (si *LocalityIterator) Next() ([]byte, []uint64) {
	si.advance()
	return si.nextKey, si.nextFiles
}

func (ic *InvertedIndexContext) iterateKeysLocality(uptoTxNum uint64) *LocalityIterator {
	si := &LocalityIterator{hc: ic}
	ic.files.Ascend(func(item ctxItem) bool {
		if (item.endTxNum-item.startTxNum)/ic.ii.aggregationStep != StepsInBiggestFile {
			return false
		}
		if item.startTxNum > uptoTxNum {
			return false
		}
		g := item.getter
		if g.HasNext() {
			key, offset := g.NextUncompressed()

			heapItem := &ReconItem{startTxNum: item.startTxNum, endTxNum: item.endTxNum, g: g, txNum: ^item.endTxNum, key: key, startOffset: offset, lastOffset: offset}
			heap.Push(&si.h, heapItem)
		}
		si.totalOffsets += uint64(item.getter.Size())
		si.filesAmount++
		return true
	})
	si.advance()
	return si
}
