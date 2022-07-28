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
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/google/btree"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
)

type InvertedIndex struct {
	dir             string // Directory where static files are created
	aggregationStep uint64
	filenameBase    string
	indexKeysTable  string
	indexTable      string // Needs to be table with DupSort
	tx              kv.RwTx
	txNum           uint64
	files           *btree.BTreeG[*filesItem]
}

func NewInvertedIndex(
	dir string,
	aggregationStep uint64,
	filenameBase string,
	indexKeysTable string,
	indexTable string,
) (*InvertedIndex, error) {
	ii := InvertedIndex{
		dir:             dir,
		aggregationStep: aggregationStep,
		filenameBase:    filenameBase,
		indexKeysTable:  indexKeysTable,
		indexTable:      indexTable,
	}
	ii.files = btree.NewG[*filesItem](32, filesItemLess)
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	ii.scanStateFiles(files)
	if err = ii.openFiles(); err != nil {
		return nil, err
	}
	return &ii, nil
}

func (ii *InvertedIndex) scanStateFiles(files []fs.DirEntry) {
	re := regexp.MustCompile(ii.filenameBase + ".([0-9]+)-([0-9]+).(ef|efi)")
	var err error
	for _, f := range files {
		name := f.Name()
		subs := re.FindStringSubmatch(name)
		if len(subs) != 4 {
			if len(subs) != 0 {
				log.Warn("File ignored by inverted index scan, more than 4 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startTxNum, endTxNum uint64
		if startTxNum, err = strconv.ParseUint(subs[1], 10, 64); err != nil {
			log.Warn("File ignored by inverted index scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endTxNum, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			log.Warn("File ignored by inverted index scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startTxNum > endTxNum {
			log.Warn("File ignored by inverted index scan, startTxNum > endTxNum", "name", name)
			continue
		}
		var item = &filesItem{startTxNum: startTxNum * ii.aggregationStep, endTxNum: endTxNum * ii.aggregationStep}
		var foundI *filesItem
		ii.files.AscendGreaterOrEqual(&filesItem{startTxNum: endTxNum * ii.aggregationStep, endTxNum: endTxNum * ii.aggregationStep}, func(it *filesItem) bool {
			if it.endTxNum == endTxNum {
				foundI = it
			}
			return false
		})
		if foundI == nil || foundI.startTxNum > startTxNum {
			//log.Info("Load state file", "name", name, "startTxNum", startTxNum*ii.aggregationStep, "endTxNum", endTxNum*ii.aggregationStep)
			ii.files.ReplaceOrInsert(item)
		}
	}
}

func (ii *InvertedIndex) openFiles() error {
	var err error
	var totalKeys uint64
	ii.files.Ascend(func(item *filesItem) bool {
		datPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.ef", ii.filenameBase, item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep))
		if item.decompressor, err = compress.NewDecompressor(datPath); err != nil {
			return false
		}
		idxPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep))
		if item.index, err = recsplit.OpenIndex(idxPath); err != nil {
			return false
		}
		totalKeys += item.index.KeyCount()
		return true
	})
	if err != nil {
		return err
	}
	return nil
}

func (ii *InvertedIndex) closeFiles() {
	ii.files.Ascend(func(item *filesItem) bool {
		if item.decompressor != nil {
			item.decompressor.Close()
		}
		if item.index != nil {
			item.index.Close()
		}
		return true
	})
}

func (ii *InvertedIndex) Close() {
	ii.closeFiles()
}

func (ii *InvertedIndex) SetTx(tx kv.RwTx) {
	ii.tx = tx
}

func (ii *InvertedIndex) SetTxNum(txNum uint64) {
	ii.txNum = txNum
}

func (ii *InvertedIndex) add(key, indexKey []byte) error {
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], ii.txNum)
	if err := ii.tx.Put(ii.indexKeysTable, txKey[:], key); err != nil {
		return err
	}
	if err := ii.tx.Put(ii.indexTable, indexKey, txKey[:]); err != nil {
		return err
	}
	return nil
}

func (ii *InvertedIndex) Add(key []byte) error {
	return ii.add(key, key)
}

// InvertedIterator allows iteration over range of tx numbers
// Iteration is not implmented via callback function, because there is often
// a requirement for interators to be composable (for example, to implement AND and OR for indices)
// InvertedIterator must be closed after use to prevent leaking of resources like cursor
type InvertedIterator struct {
	key                  []byte
	startTxNum, endTxNum uint64
	stack                []*ctxItem
	efIt                 *eliasfano32.EliasFanoIter
	next                 uint64
	hasNextInFiles       bool
	hasNextInDb          bool
	roTx                 kv.Tx
	indexTable           string
	cursor               kv.CursorDupSort
}

func (it *InvertedIterator) Close() {
	if it.cursor != nil {
		it.cursor.Close()
	}
}

func (it *InvertedIterator) advanceInFiles() {
	for {
		for it.efIt == nil {
			if len(it.stack) == 0 {
				it.hasNextInFiles = false
				return
			}
			item := it.stack[len(it.stack)-1]
			it.stack = it.stack[:len(it.stack)-1]
			offset := item.reader.Lookup(it.key)
			g := item.getter
			g.Reset(offset)
			if k, _ := g.NextUncompressed(); bytes.Equal(k, it.key) {
				eliasVal, _ := g.NextUncompressed()
				ef, _ := eliasfano32.ReadEliasFano(eliasVal)
				it.efIt = ef.Iterator()
			}
		}
		for it.efIt.HasNext() {
			n := it.efIt.Next()
			if n >= it.endTxNum {
				it.hasNextInFiles = false
				return
			}
			if n >= it.startTxNum {
				it.hasNextInFiles = true
				it.next = n
				return
			}
		}
		it.efIt = nil // Exhausted this iterator
	}
}

func (it *InvertedIterator) advanceInDb() {
	var v []byte
	var err error
	if it.cursor == nil {
		if it.cursor, err = it.roTx.CursorDupSort(it.indexTable); err != nil {
			// TODO pass error properly around
			panic(err)
		}
		var k []byte
		if k, v, err = it.cursor.Seek(it.key); err != nil {
			// TODO pass error properly around
			panic(err)
		}
		if !bytes.Equal(k, it.key) {
			it.cursor.Close()
			it.hasNextInDb = false
			return
		}
	} else {
		_, v, err = it.cursor.NextDup()
	}
	for ; err == nil && v != nil; _, v, err = it.cursor.NextDup() {
		n := binary.BigEndian.Uint64(v)
		if n >= it.endTxNum {
			it.cursor.Close()
			it.hasNextInDb = false
			return
		}
		if n >= it.startTxNum {
			it.hasNextInDb = true
			it.next = n
			return
		}
	}
	if err != nil {
		// TODO pass error properly around
		panic(err)
	}
	it.cursor.Close()
	it.hasNextInDb = false
}

func (it *InvertedIterator) advance() {
	if it.hasNextInFiles {
		it.advanceInFiles()
	}
	if it.hasNextInDb && !it.hasNextInFiles {
		it.advanceInDb()
	}
}

func (it *InvertedIterator) HasNext() bool {
	return it.hasNextInFiles || it.hasNextInDb
}

func (it *InvertedIterator) Next() uint64 {
	n := it.next
	it.advance()
	return n
}

type InvertedIndexContext struct {
	ii    *InvertedIndex
	files *btree.BTreeG[*ctxItem]
}

func (ii *InvertedIndex) MakeContext() *InvertedIndexContext {
	var ic = InvertedIndexContext{ii: ii}
	ic.files = btree.NewG[*ctxItem](32, ctxItemLess)
	ii.files.Ascend(func(item *filesItem) bool {
		ic.files.ReplaceOrInsert(&ctxItem{
			startTxNum: item.startTxNum,
			endTxNum:   item.endTxNum,
			getter:     item.decompressor.MakeGetter(),
			reader:     recsplit.NewIndexReader(item.index),
		})
		return true
	})
	return &ic
}

// IterateRange is to be used in public API, therefore it relies on read-only transaction
// so that iteration can be done even when the inverted index is being updated.
// [startTxNum; endNumTx)
func (ic *InvertedIndexContext) IterateRange(key []byte, startTxNum, endTxNum uint64, roTx kv.Tx) InvertedIterator {
	it := InvertedIterator{
		key:        key,
		startTxNum: startTxNum,
		endTxNum:   endTxNum,
		indexTable: ic.ii.indexTable,
		roTx:       roTx,
	}
	var search ctxItem
	it.hasNextInDb = true
	search.startTxNum = 0
	search.endTxNum = startTxNum
	ic.files.DescendGreaterThan(&search, func(item *ctxItem) bool {
		if item.startTxNum < endTxNum {
			it.stack = append(it.stack, item)
			it.hasNextInFiles = true
		}
		if item.endTxNum >= endTxNum {
			it.hasNextInDb = false
		}
		return true
	})
	it.advance()
	return it
}

func (ii *InvertedIndex) collate(txFrom, txTo uint64, roTx kv.Tx) (map[string]*roaring64.Bitmap, error) {
	keysCursor, err := roTx.CursorDupSort(ii.indexKeysTable)
	if err != nil {
		return nil, fmt.Errorf("create %s keys cursor: %w", ii.filenameBase, err)
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
			bitmap = roaring64.New()
			indexBitmaps[string(v)] = bitmap
		}
		bitmap.Add(txNum)
	}
	if err != nil {
		return nil, fmt.Errorf("iterate over %s keys cursor: %w", ii.filenameBase, err)
	}
	return indexBitmaps, nil
}

type InvertedFiles struct {
	decomp *compress.Decompressor
	index  *recsplit.Index
}

func (sf InvertedFiles) Close() {
	if sf.decomp != nil {
		sf.decomp.Close()
	}
	if sf.index != nil {
		sf.index.Close()
	}
}

func (ii *InvertedIndex) buildFiles(step uint64, bitmaps map[string]*roaring64.Bitmap) (InvertedFiles, error) {
	var decomp *compress.Decompressor
	var index *recsplit.Index
	var comp *compress.Compressor
	var err error
	closeComp := true
	defer func() {
		if closeComp {
			if comp != nil {
				comp.Close()
			}
			if decomp != nil {
				decomp.Close()
			}
			if index != nil {
				index.Close()
			}
		}
	}()
	txNumFrom := step * ii.aggregationStep
	txNumTo := (step + 1) * ii.aggregationStep
	datPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.ef", ii.filenameBase, txNumFrom/ii.aggregationStep, txNumTo/ii.aggregationStep))
	comp, err = compress.NewCompressor(context.Background(), "ef", datPath, ii.dir, compress.MinPatternScore, 1, log.LvlDebug)
	if err != nil {
		return InvertedFiles{}, fmt.Errorf("create %s compressor: %w", ii.filenameBase, err)
	}
	var buf []byte
	keys := make([]string, 0, len(bitmaps))
	for key := range bitmaps {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if err = comp.AddUncompressedWord([]byte(key)); err != nil {
			return InvertedFiles{}, fmt.Errorf("add %s key [%x]: %w", ii.filenameBase, key, err)
		}
		bitmap := bitmaps[key]
		ef := eliasfano32.NewEliasFano(bitmap.GetCardinality(), bitmap.Maximum())
		it := bitmap.Iterator()
		for it.HasNext() {
			ef.AddOffset(it.Next())
		}
		ef.Build()
		buf = ef.AppendBytes(buf[:0])
		if err = comp.AddUncompressedWord(buf); err != nil {
			return InvertedFiles{}, fmt.Errorf("add %s val: %w", ii.filenameBase, err)
		}
	}
	if err = comp.Compress(); err != nil {
		return InvertedFiles{}, fmt.Errorf("compress %s: %w", ii.filenameBase, err)
	}
	comp.Close()
	comp = nil
	if decomp, err = compress.NewDecompressor(datPath); err != nil {
		return InvertedFiles{}, fmt.Errorf("open %s decompressor: %w", ii.filenameBase, err)
	}
	idxPath := filepath.Join(ii.dir, fmt.Sprintf("%s.%d-%d.efi", ii.filenameBase, txNumFrom/ii.aggregationStep, txNumTo/ii.aggregationStep))
	if index, err = buildIndex(decomp, idxPath, ii.dir, len(keys), false /* values */); err != nil {
		return InvertedFiles{}, fmt.Errorf("build %s efi: %w", ii.filenameBase, err)
	}
	closeComp = false
	return InvertedFiles{decomp: decomp, index: index}, nil
}

func (ii *InvertedIndex) integrateFiles(sf InvertedFiles, txNumFrom, txNumTo uint64) {
	ii.files.ReplaceOrInsert(&filesItem{
		startTxNum:   txNumFrom,
		endTxNum:     txNumTo,
		decompressor: sf.decomp,
		index:        sf.index,
	})
}

// [txFrom; txTo)
func (ii *InvertedIndex) prune(txFrom, txTo uint64) error {
	keysCursor, err := ii.tx.RwCursorDupSort(ii.indexKeysTable)
	if err != nil {
		return fmt.Errorf("create %s keys cursor: %w", ii.filenameBase, err)
	}
	defer keysCursor.Close()
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)
	var k, v []byte
	idxC, err := ii.tx.RwCursorDupSort(ii.indexTable)
	if err != nil {
		return err
	}
	defer idxC.Close()
	for k, v, err = keysCursor.Seek(txKey[:]); err == nil && k != nil; k, v, err = keysCursor.Next() {
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo {
			break
		}
		if err = idxC.DeleteExact(v, k); err != nil {
			return err
		}
		// This DeleteCurrent needs to the the last in the loop iteration, because it invalidates k and v
		if err = keysCursor.DeleteCurrent(); err != nil {
			return err
		}
	}
	if err != nil {
		return fmt.Errorf("iterate over %s keys: %w", ii.filenameBase, err)
	}
	return nil
}
