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

package aggregator

import (
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/btree"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/log/v3"
)

// History is a utility class that allows reading history of state
// from state files, history files, and bitmap files produced by an Aggregator
type History struct {
	diffDir         string // Directory where the state diff files are stored
	files           [NumberOfTypes]*btree.BTreeG[*byEndBlockItem]
	aggregationStep uint64
}

func NewHistory(diffDir string, blockTo uint64, aggregationStep uint64) (*History, error) {
	h := &History{
		diffDir:         diffDir,
		aggregationStep: aggregationStep,
	}
	for fType := FirstType; fType < NumberOfTypes; fType++ {
		h.files[fType] = btree.NewG(32, ByEndBlockItemLess)
	}
	var closeStateFiles = true // It will be set to false in case of success at the end of the function
	defer func() {
		// Clean up all decompressor and indices upon error
		if closeStateFiles {
			h.Close()
		}
	}()
	// Scan the diff directory and create the mapping of end blocks to files
	files, err := os.ReadDir(diffDir)
	if err != nil {
		return nil, err
	}
	h.scanStateFiles(files, blockTo)
	for fType := FirstType; fType < NumberOfTypes; fType++ {
		if err := h.openFiles(fType); err != nil {
			return nil, fmt.Errorf("opening %s state files: %w", fType.String(), err)
		}
	}
	closeStateFiles = false
	return h, nil
}

func (h *History) scanStateFiles(files []fs.DirEntry, blockTo uint64) {
	typeStrings := make([]string, NumberOfTypes)
	for fType := FileType(0); fType < NumberOfTypes; fType++ {
		typeStrings[fType] = fType.String()
	}
	re := regexp.MustCompile("^(" + strings.Join(typeStrings, "|") + ").([0-9]+)-([0-9]+).(dat|idx)$")
	var err error
	for _, f := range files {
		name := f.Name()
		subs := re.FindStringSubmatch(name)
		if len(subs) != 5 {
			if len(subs) != 0 {
				log.Warn("File ignored by history, more than 4 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startBlock, endBlock uint64
		if startBlock, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			log.Warn("File ignored by history, parsing startBlock", "error", err, "name", name)
			continue
		}
		if endBlock, err = strconv.ParseUint(subs[3], 10, 64); err != nil {
			log.Warn("File ignored by history, parsing endBlock", "error", err, "name", name)
			continue
		}
		if startBlock > endBlock {
			log.Warn("File ignored by history, startBlock > endBlock", "name", name)
			continue
		}
		if endBlock > blockTo {
			// Only load files up to specified block
			continue
		}
		fType, ok := ParseFileType(subs[1])
		if !ok {
			log.Warn("File ignored by history, type unknown", "type", subs[1])
		}
		var item = &byEndBlockItem{startBlock: startBlock, endBlock: endBlock}
		var foundI *byEndBlockItem
		h.files[fType].AscendGreaterOrEqual(&byEndBlockItem{startBlock: endBlock, endBlock: endBlock}, func(it *byEndBlockItem) bool {
			if it.endBlock == endBlock {
				foundI = it
			}
			return false
		})
		if foundI == nil || foundI.startBlock > startBlock {
			h.files[fType].ReplaceOrInsert(item)
			log.Info("Load file", "name", name, "type", fType.String(), "endBlock", item.endBlock)
		}
	}
}

func (h *History) openFiles(fType FileType) error {
	var err error
	h.files[fType].Ascend(func(item *byEndBlockItem) bool {
		if item.decompressor, err = compress.NewDecompressor(path.Join(h.diffDir, fmt.Sprintf("%s.%d-%d.dat", fType.String(), item.startBlock, item.endBlock))); err != nil {
			return false
		}
		if item.index, err = recsplit.OpenIndex(path.Join(h.diffDir, fmt.Sprintf("%s.%d-%d.idx", fType.String(), item.startBlock, item.endBlock))); err != nil {
			return false
		}
		item.getter = item.decompressor.MakeGetter()
		item.getterMerge = item.decompressor.MakeGetter()
		item.indexReader = recsplit.NewIndexReader(item.index)
		item.readerMerge = recsplit.NewIndexReader(item.index)
		return true
	})
	return err
}

func (h *History) closeFiles(fType FileType) {
	h.files[fType].Ascend(func(item *byEndBlockItem) bool {
		if item.decompressor != nil {
			item.decompressor.Close()
		}
		if item.index != nil {
			item.index.Close()
		}
		return true
	})
}

func (h *History) Close() {
	// Closing state files only after background aggregation goroutine is finished
	for fType := FirstType; fType < NumberOfTypes; fType++ {
		h.closeFiles(fType)
	}
}

func (h *History) MakeHistoryReader() *HistoryReader {
	r := &HistoryReader{
		h: h,
	}
	return r
}

type HistoryReader struct {
	h        *History
	search   byEndBlockItem
	blockNum uint64
	txNum    uint64
	lastTx   bool // Whether it is the last transaction in the block
}

func (hr *HistoryReader) SetNums(blockNum, txNum uint64, lastTx bool) {
	hr.blockNum = blockNum
	hr.txNum = txNum
	hr.lastTx = lastTx
}

func (hr *HistoryReader) searchInHistory(bitmapType, historyType FileType, key []byte, trace bool) (bool, []byte, error) {
	if trace {
		fmt.Printf("searchInHistory %s %s [%x] blockNum %d, txNum %d\n", bitmapType.String(), historyType.String(), key, hr.blockNum, hr.txNum)
	}
	searchBlock := hr.blockNum
	if hr.lastTx {
		searchBlock++
	}
	searchTx := hr.txNum
	hr.search.endBlock = searchBlock
	hr.search.startBlock = searchBlock - (searchBlock % 500_000)
	var eliasVal []byte
	var err error
	var found bool
	var foundTxNum uint64
	var foundEndBlock uint64
	hr.h.files[bitmapType].AscendGreaterOrEqual(&hr.search, func(item *byEndBlockItem) bool {
		offset := item.indexReader.Lookup(key)
		g := item.getter
		g.Reset(offset)
		if keyMatch, _ := g.Match(key); keyMatch {
			if trace {
				fmt.Printf("Found bitmap for [%x] in %s.[%d-%d]\n", key, bitmapType.String(), item.startBlock, item.endBlock)
			}
			eliasVal, _ = g.NextUncompressed()
			ef, _ := eliasfano32.ReadEliasFano(eliasVal)
			it := ef.Iterator()
			if trace {
				for it.HasNext() {
					fmt.Printf(" %d", it.Next())
				}
				fmt.Printf("\n")
			}
			foundTxNum, found = ef.Search(searchTx)
			if found {
				foundEndBlock = item.endBlock
				return false
			}
		}
		// Not found, next
		return true
	})
	if err != nil {
		return false, nil, err
	}
	if !found {
		return false, nil, nil
	}
	if trace {
		fmt.Printf("found in tx %d, endBlock %d\n", foundTxNum, foundEndBlock)
	}
	var lookupKey = make([]byte, len(key)+8)
	binary.BigEndian.PutUint64(lookupKey, foundTxNum)
	copy(lookupKey[8:], key)
	var historyItem *byEndBlockItem
	hr.search.endBlock = foundEndBlock
	hr.search.startBlock = foundEndBlock - 499_999
	var ok bool
	historyItem, ok = hr.h.files[historyType].Get(&hr.search)
	if !ok || historyItem == nil {
		return false, nil, fmt.Errorf("no %s file found for %d", historyType.String(), foundEndBlock)
	}
	offset := historyItem.indexReader.Lookup(lookupKey)
	if trace {
		fmt.Printf("Lookup [%x] in %s.[%d-%d].idx = %d\n", lookupKey, historyType.String(), historyItem.startBlock, historyItem.endBlock, offset)
	}
	historyItem.getter.Reset(offset)
	v, _ := historyItem.getter.Next(nil)
	return true, v, nil
}

func (hr *HistoryReader) ReadAccountData(addr []byte, trace bool) ([]byte, error) {
	// Look in the history first
	hOk, v, err := hr.searchInHistory(AccountBitmap, AccountHistory, addr, trace)
	if err != nil {
		return nil, err
	}
	if hOk {
		if trace {
			fmt.Printf("ReadAccountData %x, found in history [%x]\n", addr, v)
		}
		return v, nil
	}
	if trace {
		fmt.Printf("ReadAccountData %x, not found in history, get from the state\n", addr)
	}
	// Not found in history - look in the state files
	return hr.h.readFromFiles(Account, addr, trace), nil
}

func (hr *HistoryReader) ReadAccountStorage(addr []byte, loc []byte, trace bool) (*uint256.Int, error) {
	// Look in the history first
	dbkey := make([]byte, len(addr)+len(loc))
	copy(dbkey[0:], addr)
	copy(dbkey[len(addr):], loc)
	hOk, v, err := hr.searchInHistory(StorageBitmap, StorageHistory, dbkey, trace)
	if err != nil {
		return nil, err
	}
	if hOk {
		return new(uint256.Int).SetBytes(v), nil
	}
	// Not found in history, look in the state files
	v = hr.h.readFromFiles(Storage, dbkey, trace)
	if v != nil {
		return new(uint256.Int).SetBytes(v), nil
	}
	return nil, nil
}

func (hr *HistoryReader) ReadAccountCode(addr []byte, trace bool) ([]byte, error) {
	// Look in the history first
	hOk, v, err := hr.searchInHistory(CodeBitmap, CodeHistory, addr, false)
	if err != nil {
		return nil, err
	}
	if hOk {
		return v, err
	}
	// Not found in history, look in the history files
	return hr.h.readFromFiles(Code, addr, trace), nil
}

func (hr *HistoryReader) ReadAccountCodeSize(addr []byte, trace bool) (int, error) {
	// Look in the history first
	hOk, v, err := hr.searchInHistory(CodeBitmap, CodeHistory, addr, false)
	if err != nil {
		return 0, err
	}
	if hOk {
		return len(v), err
	}
	// Not found in history, look in the history files
	return len(hr.h.readFromFiles(Code, addr, trace)), nil
}

func (h *History) readFromFiles(fType FileType, filekey []byte, trace bool) []byte {
	var val []byte
	h.files[fType].Descend(func(item *byEndBlockItem) bool {
		if trace {
			fmt.Printf("read %s %x: search in file [%d-%d]\n", fType.String(), filekey, item.startBlock, item.endBlock)
		}
		if item.tree != nil {
			ai, ok := item.tree.Get(&AggregateItem{k: filekey})
			if !ok || ai == nil {
				return true
			}
			val = ai.v
			return false
		}
		if item.index.Empty() {
			return true
		}
		offset := item.indexReader.Lookup(filekey)
		g := item.getter
		g.Reset(offset)
		if g.HasNext() {
			if keyMatch, _ := g.Match(filekey); keyMatch {
				val, _ = g.Next(nil)
				if trace {
					fmt.Printf("read %s %x: found [%x] in file [%d-%d]\n", fType.String(), filekey, val, item.startBlock, item.endBlock)
				}
				return false
			}
		}
		return true
	})
	return val
}
