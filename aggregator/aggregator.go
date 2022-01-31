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
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/crypto/sha3"
)

// Aggregator of multiple state files to support state reader and state writer
// The convension for the file names are as follows
// State is composed of three types of files:
// 1. Accounts. keys are addresses (20 bytes), values are encoding of accounts
// 2. Contract storage. Keys are concatenation of addresses (20 bytes) and storage locations (32 bytes), values have their leading zeroes removed
// 3. Contract codes. Keys are addresses (20 bytes), values are bycodes
// Within each type, any file can cover an interval of block numbers, for example, `accounts.1-16` represents changes in accounts
// that were effected by the blocks from 1 to 16, inclusively. The second component of the interval will be called "end block" for the file.
// Finally, for each type and interval, there are two files - one with the compressed data (extension `dat`),
// and another with the index (extension `idx`) consisting of the minimal perfect hash table mapping keys to the offsets of corresponding keys
// in the data file
// Aggregator consists (apart from the file it is aggregating) of the 4 parts:
// 1. Persistent table of expiration time for each of the files. Key - name of the file, value - timestamp, at which the file can be removed
// 2. Transient (in-memory) mapping the "end block" of each file to the objects required for accessing the file (compress.Decompressor and resplit.Index)
// 3. Persistent tables (one for accounts, one for contract storage, and one for contract code) summarising all the 1-block state diff files
//    that were not yet merged together to form larger files. In these tables, keys are the same as keys in the state diff files, but values are also
//    augemented by the number of state diff files this key is present. This number gets decremented every time when a 1-block state diff files is removed
//    from the summary table (due to being merged). And when this number gets to 0, the record is deleted from the summary table.
//    This number is encoded into first 4 bytes of the value
// 4. Aggregating persistent hash table. Maps state keys to the block numbers for the use in the part 2 (which is not necessarily the block number where
//    the item last changed, but it is guaranteed to find correct element in the Transient mapping of part 2

type Aggregator struct {
	diffDir           string       // Directory where the state diff files are stored
	accountsFiles     *btree.BTree // tree of account files, sorted by endBlock of each file
	accountsFilesLock sync.RWMutex
	codeFiles         *btree.BTree // tree of code files, sorted by endBlock of each file
	codeFilesLock     sync.RWMutex
	storageFiles      *btree.BTree // tree of storage files, sorted by endBlock of each file
	storageFilesLock  sync.RWMutex
	commitmentFiles   *btree.BTree // tree of commitment files, sorted by endBlock of each file
	commFilesLock     sync.RWMutex
	unwindLimit       uint64              // How far the chain may unwind
	aggregationStep   uint64              // How many items (block, but later perhaps txs or changes) are required to form one state diff file
	changesBtree      *btree.BTree        // btree of ChangesItem
	trace             bool                // Turns on tracing for specific accounts and locations
	tracedKeys        map[string]struct{} // Set of keys being traced during aggregations
	hph               *commitment.HexPatriciaHashed
	keccak            hash.Hash
	changesets        bool // Whether to generate changesets (off by default)
	aggChannel        chan AggregationTask
	aggBackCh         chan struct{} // Channel for acknoledgement of AggregationTask
	aggError          chan error
	aggWg             sync.WaitGroup
	mergeChannel      chan struct{}
	mergeError        chan error
	mergeWg           sync.WaitGroup
}

type ChangeFile struct {
	dir         string
	step        uint64
	namebase    string
	path        string
	file        *os.File
	w           *bufio.Writer
	r           *bufio.Reader
	numBuf      [8]byte
	sizeCounter uint64
	txPos       int64 // Position of the last block iterated upon
	txNum       uint64
	txSize      uint64
	txRemaining uint64 // Remaining number of bytes to read in the current transaction
	words       []byte // Words pending for the next block record, in the same slice
	wordOffsets []int  // Offsets of words in the `words` slice
}

func (cf *ChangeFile) closeFile() error {
	if len(cf.wordOffsets) > 0 {
		return fmt.Errorf("closeFile without finish")
	}
	if cf.w != nil {
		if err := cf.w.Flush(); err != nil {
			return err
		}
		cf.w = nil
	}
	if cf.file != nil {
		if err := cf.file.Close(); err != nil {
			return err
		}
		cf.file = nil
	}
	return nil
}

func (cf *ChangeFile) openFile(blockNum uint64, write bool) error {
	if len(cf.wordOffsets) > 0 {
		return fmt.Errorf("openFile without finish")
	}
	rem := blockNum % cf.step
	startBlock := blockNum - rem
	endBlock := startBlock + cf.step - 1
	if cf.w == nil {
		cf.path = path.Join(cf.dir, fmt.Sprintf("%s.%d-%d.chg", cf.namebase, startBlock, endBlock))
		var err error
		if write {
			if cf.file, err = os.OpenFile(cf.path, os.O_RDWR|os.O_CREATE, 0755); err != nil {
				return err
			}
		} else {
			if cf.file, err = os.Open(cf.path); err != nil {
				return err
			}
		}
		if cf.txPos, err = cf.file.Seek(0, 2 /* relative to the end of the file */); err != nil {
			return err
		}
		if write {
			cf.w = bufio.NewWriter(cf.file)
		}
		cf.r = bufio.NewReader(cf.file)
	}
	return nil
}

func (cf *ChangeFile) rewind() error {
	var err error
	if cf.txPos, err = cf.file.Seek(0, 2 /* relative to the end of the file */); err != nil {
		return err
	}
	cf.r = bufio.NewReader(cf.file)
	return nil
}

func (cf *ChangeFile) add(word []byte) {
	cf.words = append(cf.words, word...)
	cf.wordOffsets = append(cf.wordOffsets, len(cf.words))
}

func (cf *ChangeFile) update(word []byte) {
	cf.words = append(cf.words, 0)
	cf.add(word)
}

func (cf *ChangeFile) insert(word []byte) {
	cf.words = append(cf.words, 1)
	cf.add(word)
}

func (cf *ChangeFile) finish(txNum uint64) error {
	// Write out words
	lastOffset := 0
	for _, offset := range cf.wordOffsets {
		word := cf.words[lastOffset:offset]
		n := binary.PutUvarint(cf.numBuf[:], uint64(len(word)))
		if _, err := cf.w.Write(cf.numBuf[:n]); err != nil {
			return err
		}
		if len(word) > 0 {
			if _, err := cf.w.Write(word); err != nil {
				return err
			}
		}
		cf.sizeCounter += uint64(n + len(word))
		lastOffset = offset
	}
	cf.words = cf.words[:0]
	cf.wordOffsets = cf.wordOffsets[:0]
	// Write out tx number and then size of changes in this block
	binary.BigEndian.PutUint64(cf.numBuf[:], txNum)
	if _, err := cf.w.Write(cf.numBuf[:]); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(cf.numBuf[:], cf.sizeCounter)
	if _, err := cf.w.Write(cf.numBuf[:]); err != nil {
		return err
	}
	cf.sizeCounter = 0
	return nil
}

// prevTx positions the reader to the beginning
// of the transaction
func (cf *ChangeFile) prevTx() (bool, error) {
	if cf.txPos == 0 {
		return false, nil
	}
	// Move back 16 bytes to read tx number and tx size
	pos, err := cf.file.Seek(cf.txPos-16, 0 /* relative to the beginning */)
	if err != nil {
		return false, err
	}
	cf.r.Reset(cf.file)
	if _, err = io.ReadFull(cf.r, cf.numBuf[:8]); err != nil {
		return false, err
	}
	cf.txNum = binary.BigEndian.Uint64(cf.numBuf[:])
	if _, err = io.ReadFull(cf.r, cf.numBuf[:8]); err != nil {
		return false, err
	}
	cf.txSize = binary.BigEndian.Uint64(cf.numBuf[:])
	cf.txRemaining = cf.txSize
	cf.txPos, err = cf.file.Seek(pos-int64(cf.txSize), 0)
	if err != nil {
		return false, err
	}
	cf.r.Reset(cf.file)
	return true, nil
}

func (cf *ChangeFile) nextWord(wordBuf []byte) ([]byte, bool, error) {
	if cf.txRemaining == 0 {
		return wordBuf, false, nil
	}
	ws, err := binary.ReadUvarint(cf.r)
	if err != nil {
		return wordBuf, false, fmt.Errorf("word size: %w", err)
	}
	var buf []byte
	if total := len(wordBuf) + int(ws); cap(wordBuf) >= total {
		buf = wordBuf[:total] // Reuse the space in wordBuf, is it has enough capacity
	} else {
		buf = make([]byte, total)
		copy(buf, wordBuf)
	}
	if _, err = io.ReadFull(cf.r, buf[len(wordBuf):]); err != nil {
		return wordBuf, false, fmt.Errorf("read word (%d %d): %w", ws, len(buf[len(wordBuf):]), err)
	}
	n := binary.PutUvarint(cf.numBuf[:], ws)
	cf.txRemaining -= uint64(n) + ws
	return buf, true, nil
}

func (cf *ChangeFile) deleteFile() error {
	return os.Remove(cf.path)
}

type Changes struct {
	namebase string
	keys     ChangeFile
	before   ChangeFile
	after    ChangeFile
	step     uint64
	dir      string
	beforeOn bool
}

func (c *Changes) Init(namebase string, step uint64, dir string, beforeOn bool) {
	c.namebase = namebase
	c.step = step
	c.dir = dir
	c.keys.namebase = namebase + ".keys"
	c.keys.dir = dir
	c.keys.step = step
	c.before.namebase = namebase + ".before"
	c.before.dir = dir
	c.before.step = step
	c.after.namebase = namebase + ".after"
	c.after.dir = dir
	c.after.step = step
	c.beforeOn = beforeOn
}

func (c *Changes) closeFiles() error {
	if err := c.keys.closeFile(); err != nil {
		return err
	}
	if c.beforeOn {
		if err := c.before.closeFile(); err != nil {
			return err
		}
	}
	if err := c.after.closeFile(); err != nil {
		return err
	}
	return nil
}

func (c *Changes) openFiles(blockNum uint64, write bool) error {
	if err := c.keys.openFile(blockNum, write); err != nil {
		return err
	}
	if c.beforeOn {
		if err := c.before.openFile(blockNum, write); err != nil {
			return err
		}
	}
	if err := c.after.openFile(blockNum, write); err != nil {
		return err
	}
	return nil
}

func (c *Changes) insert(key, after []byte) {
	c.keys.add(key)
	if c.beforeOn {
		c.before.add(nil)
	}
	c.after.insert(after)
}

func (c *Changes) update(key, before, after []byte) {
	c.keys.add(key)
	if c.beforeOn {
		c.before.add(before)
	}
	c.after.update(after)
}

func (c *Changes) delete(key, before []byte) {
	c.keys.add(key)
	if c.beforeOn {
		c.before.add(before)
	}
	c.after.add(nil)
}

func (c *Changes) finish(txNum uint64) error {
	if err := c.keys.finish(txNum); err != nil {
		return err
	}
	if c.beforeOn {
		if err := c.before.finish(txNum); err != nil {
			return err
		}
	}
	if err := c.after.finish(txNum); err != nil {
		return err
	}
	return nil
}

func (c *Changes) prevTx() (bool, uint64, error) {
	bkeys, err := c.keys.prevTx()
	if err != nil {
		return false, 0, err
	}
	var bbefore, bafter bool
	if c.beforeOn {
		if bbefore, err = c.before.prevTx(); err != nil {
			return false, 0, err
		}
	}
	if bafter, err = c.after.prevTx(); err != nil {
		return false, 0, err
	}
	if c.beforeOn && bkeys != bbefore {
		return false, 0, fmt.Errorf("inconsistent tx iteration")
	}
	if bkeys != bafter {
		return false, 0, fmt.Errorf("inconsistent tx iteration")
	}
	txNum := c.keys.txNum
	if c.beforeOn {
		if txNum != c.before.txNum {
			return false, 0, fmt.Errorf("inconsistent txNum, keys: %d, before: %d", txNum, c.before.txNum)
		}
	}
	if txNum != c.after.txNum {
		return false, 0, fmt.Errorf("inconsistent txNum, keys: %d, after: %d", txNum, c.after.txNum)
	}
	return bkeys, txNum, nil
}

func (c *Changes) rewind() error {
	if err := c.keys.rewind(); err != nil {
		return err
	}
	if c.beforeOn {
		if err := c.before.rewind(); err != nil {
			return err
		}
	}
	if err := c.after.rewind(); err != nil {
		return err
	}
	return nil
}

func (c *Changes) nextTriple(keyBuf, beforeBuf []byte, afterBuf []byte) ([]byte, []byte, []byte, bool, error) {
	key, bkeys, err := c.keys.nextWord(keyBuf)
	if err != nil {
		return keyBuf, beforeBuf, afterBuf, false, fmt.Errorf("next key: %w", err)
	}
	var before, after []byte
	var bbefore, bafter bool
	if c.beforeOn {
		if before, bbefore, err = c.before.nextWord(beforeBuf); err != nil {
			return keyBuf, beforeBuf, afterBuf, false, fmt.Errorf("next before: %w", err)
		}
	}
	if c.beforeOn && bkeys != bbefore {
		return keyBuf, beforeBuf, afterBuf, false, fmt.Errorf("inconsistent word iteration")
	}
	if after, bafter, err = c.after.nextWord(afterBuf); err != nil {
		return keyBuf, beforeBuf, afterBuf, false, fmt.Errorf("next after: %w", err)
	}
	if bkeys != bafter {
		return keyBuf, beforeBuf, afterBuf, false, fmt.Errorf("inconsistent word iteration")
	}
	return key, before, after, bkeys, nil
}

func (c *Changes) deleteFiles() error {
	if err := c.keys.deleteFile(); err != nil {
		return err
	}
	if c.beforeOn {
		if err := c.before.deleteFile(); err != nil {
			return err
		}
	}
	if err := c.after.deleteFile(); err != nil {
		return err
	}
	return nil
}

func buildIndex(datPath, idxPath, tmpDir string, count int) (*compress.Decompressor, *recsplit.Index, error) {
	d, err := compress.NewDecompressor(datPath)
	if err != nil {
		return nil, nil, err
	}
	var rs *recsplit.RecSplit
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   count,
		Enums:      false,
		BucketSize: 2000,
		Salt:       0,
		LeafSize:   8,
		TmpDir:     tmpDir,
		StartSeed: []uint64{0x106393c187cae21a, 0x6453cec3f7376937, 0x643e521ddbd2be98, 0x3740c6412f6572cb, 0x717d47562f1ce470, 0x4cd6eb4c63befb7c, 0x9bfd8c5e18c8da73,
			0x082f20e10092a9a3, 0x2ada2ce68d21defc, 0xe33cb4f3e7c6466b, 0x3980be458c509c59, 0xc466fd9584828e8c, 0x45f0aabe1a61ede6, 0xf6e7b8b33ad9b98d,
			0x4ef95e25f4b4983d, 0x81175195173b92d3, 0x4e50927d8dd15978, 0x1ea2099d1fafae7f, 0x425c8a06fbaaa815, 0xcd4216006c74052a},
		IndexFile: idxPath,
	}); err != nil {
		return nil, nil, err
	}
	word := make([]byte, 0, 256)
	var pos uint64
	for {
		g := d.MakeGetter()
		for g.HasNext() {
			word, _ = g.Next(word[:0])
			if err = rs.AddKey(word, pos); err != nil {
				return nil, nil, err
			}
			// Skip value
			pos = g.Skip()
		}
		if err = rs.Build(); err != nil {
			return nil, nil, err
		}
		if rs.Collision() {
			log.Info("Building recsplit. Collision happened. It's ok. Restarting...")
			rs.ResetNextSalt()
		} else {
			break
		}
	}
	var idx *recsplit.Index
	if idx, err = recsplit.OpenIndex(idxPath); err != nil {
		return nil, nil, err
	}
	return d, idx, nil
}

// aggregate gathers changes from the changefiles into a B-tree, and "removes" them from the database
// This function is time-critical because it needs to be run in the same go-routine (thread) as the general
// execution (due to read-write tx). After that, we can optimistically execute the rest in the background
func (c *Changes) aggregate(blockFrom, blockTo uint64, prefixLen int, tx kv.RwTx, table string) (*btree.BTree, error) {
	if err := c.openFiles(blockTo, false /* write */); err != nil {
		return nil, fmt.Errorf("open files: %w", err)
	}
	bt := btree.New(32)
	err := c.aggregateToBtree(bt, prefixLen)
	if err != nil {
		return nil, fmt.Errorf("aggregateToBtree: %w", err)
	}
	// Clean up the DB table
	var e error
	bt.Ascend(func(i btree.Item) bool {
		item := i.(*AggregateItem)
		if item.count == 0 {
			return true
		}
		dbPrefix := item.k
		prevV, err := tx.GetOne(table, dbPrefix)
		if err != nil {
			e = err
			return false
		}
		if prevV == nil {
			e = fmt.Errorf("record not found in db for %s key %x", table, dbPrefix)
			return false
		}
		prevNum := binary.BigEndian.Uint32(prevV[:4])
		if prevNum < item.count {
			e = fmt.Errorf("record count too low for %s key %s count %d, subtracting %d", table, dbPrefix, prevNum, item.count)
			return false
		}
		if prevNum == item.count {
			if e = tx.Delete(table, dbPrefix, nil); e != nil {
				return false
			}
		} else {
			v := common.Copy(prevV)
			binary.BigEndian.PutUint32(v[:4], prevNum-item.count)
			if e = tx.Put(table, dbPrefix, v); e != nil {
				return false
			}
		}
		return true
	})
	if e != nil {
		return nil, fmt.Errorf("clean up table %s after aggregation: %w", table, e)
	}
	return bt, nil
}

type AggregateItem struct {
	k, v  []byte
	count uint32
}

func (i *AggregateItem) Less(than btree.Item) bool {
	return bytes.Compare(i.k, than.(*AggregateItem).k) < 0
}

func (c *Changes) produceChangeSets(datPath, idxPath string) error {
	comp, err := compress.NewCompressor(context.Background(), AggregatorPrefix, datPath, c.dir, compress.MinPatternScore, 1)
	if err != nil {
		return fmt.Errorf("produceChangeSets NewCompressor: %w", err)
	}
	defer comp.Close()
	var totalRecords int
	var b bool
	var e error
	var key, before, after []byte
	if err = c.rewind(); err != nil {
		return fmt.Errorf("produceChangeSets rewind: %w", err)
	}
	for b, _, e = c.prevTx(); b && e == nil; b, _, e = c.prevTx() {
		for key, before, after, b, e = c.nextTriple(key[:0], before[:0], after[:0]); b && e == nil; key, before, after, b, e = c.nextTriple(key[:0], before[:0], after[:0]) {
			totalRecords++
			if err = comp.AddWord(before); err != nil {
				return fmt.Errorf("produceChangeSets AddWord: %w", err)
			}
		}
		if e != nil {
			return fmt.Errorf("produceChangeSets nextTriple: %w", e)
		}
	}
	if e != nil {
		return fmt.Errorf("produceChangeSets prevTx: %w", e)
	}
	if err = comp.Compress(); err != nil {
		return fmt.Errorf("produceChangeSets Compress: %w", err)
	}
	var d *compress.Decompressor
	if d, err = compress.NewDecompressor(datPath); err != nil {
		return fmt.Errorf("produceChangeSets NewDecompressor: %w", err)
	}
	defer d.Close()
	var rs *recsplit.RecSplit
	if rs, err = recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   totalRecords,
		Enums:      true,
		BucketSize: 2000,
		Salt:       0,
		LeafSize:   8,
		TmpDir:     c.dir,
		StartSeed: []uint64{0x106393c187cae21a, 0x6453cec3f7376937, 0x643e521ddbd2be98, 0x3740c6412f6572cb, 0x717d47562f1ce470, 0x4cd6eb4c63befb7c, 0x9bfd8c5e18c8da73,
			0x082f20e10092a9a3, 0x2ada2ce68d21defc, 0xe33cb4f3e7c6466b, 0x3980be458c509c59, 0xc466fd9584828e8c, 0x45f0aabe1a61ede6, 0xf6e7b8b33ad9b98d,
			0x4ef95e25f4b4983d, 0x81175195173b92d3, 0x4e50927d8dd15978, 0x1ea2099d1fafae7f, 0x425c8a06fbaaa815, 0xcd4216006c74052a},
		IndexFile: idxPath,
	}); err != nil {
		return fmt.Errorf("produceChangeSets NewRecSplit: %w", err)
	}
	for {
		if err = c.rewind(); err != nil {
			return fmt.Errorf("produceChangeSets rewind2: %w", err)
		}
		var txKey = make([]byte, 8, 60)
		var pos, prevPos uint64
		var txNum uint64
		g := d.MakeGetter()
		for b, txNum, e = c.prevTx(); b && e == nil; b, txNum, e = c.prevTx() {
			binary.BigEndian.PutUint64(txKey[:8], txNum)
			for key, before, after, b, e = c.nextTriple(key[:0], before[:0], after[:0]); b && e == nil; key, before, after, b, e = c.nextTriple(key[:0], before[:0], after[:0]) {
				txKey = append(txKey[:8], key...)
				pos = g.Skip()
				if err = rs.AddKey(txKey, prevPos); err != nil {
					return fmt.Errorf("produceChangeSets AddKey: %w", e)
				}
				prevPos = pos
			}
			if e != nil {
				return fmt.Errorf("produceChangeSets nextTriple2: %w", e)
			}
		}
		if e != nil {
			return fmt.Errorf("produceChangeSets prevTx2: %w", e)
		}
		if err = rs.Build(); err != nil {
			return fmt.Errorf("produceChangeSets Build: %w", err)
		}
		if rs.Collision() {
			log.Info("Building produceChangeSets. Collision happened. It's ok. Restarting...")
			rs.ResetNextSalt()
		} else {
			break
		}
	}
	return nil
}

// aggregateToBtree iterates over all available changes in the change files covered by this instance `c`
// (there are 3 of them, one for "keys", one for values "before" every change, and one for values "after" every change)
// and create a B-tree where each key is only represented once, with the value corresponding to the "after" value
// of the latest change. Also, the first byte of value in the B-tree indicates whether the change has occurred from
// non-existent (zero) value. In such cases, the fist byte is set to 1 (insertion), otherwise it is 0 (update).
func (c *Changes) aggregateToBtree(bt *btree.BTree, prefixLen int) error {
	var b bool
	var e error
	var key, before, after []byte
	var ai AggregateItem
	var prefix []byte
	// Note that the following loop iterates over transactions backwards, therefore it does not replace entries in the B-tree,
	// but instead just updates their "change count" and the first byte of the value (insertion vs update flag)
	for b, _, e = c.prevTx(); b && e == nil; b, _, e = c.prevTx() {
		// Within each transaction, keys are unique, but they can appear in any order
		for key, before, after, b, e = c.nextTriple(key[:0], before[:0], after[:0]); b && e == nil; key, before, after, b, e = c.nextTriple(key[:0], before[:0], after[:0]) {
			if prefixLen > 0 && !bytes.Equal(prefix, key[:prefixLen]) {
				prefix = common.Copy(key[:prefixLen])
				item := &AggregateItem{k: prefix, count: 0}
				bt.ReplaceOrInsert(item)
			}
			ai.k = key
			i := bt.Get(&ai)
			if i == nil {
				item := &AggregateItem{k: common.Copy(key), count: 1, v: common.Copy(after)}
				bt.ReplaceOrInsert(item)
			} else {
				item := i.(*AggregateItem)
				if len(item.v) > 0 && len(after) > 0 {
					item.v[0] = after[0]
				}
				item.count++
			}
		}
		if e != nil {
			return fmt.Errorf("aggregateToBtree nextTriple: %w", e)
		}
	}
	if e != nil {
		return fmt.Errorf("aggregateToBtree prevTx: %w", e)
	}
	return nil
}

const AggregatorPrefix = "aggregator"

func btreeToFile(bt *btree.BTree, datPath string, tmpdir string, trace bool, workers int) (int, error) {
	comp, err := compress.NewCompressor(context.Background(), AggregatorPrefix, datPath, tmpdir, compress.MinPatternScore, workers)
	if err != nil {
		return 0, err
	}
	defer comp.Close()
	comp.SetTrace(trace)
	count := 0
	bt.Ascend(func(i btree.Item) bool {
		item := i.(*AggregateItem)
		if err = comp.AddWord(item.k); err != nil {
			return false
		}
		count++ // Only counting keys, not values
		if err = comp.AddWord(item.v); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	if err = comp.Compress(); err != nil {
		return 0, err
	}
	return count, nil
}

type ChangesItem struct {
	endBlock   uint64
	startBlock uint64
	fileCount  int
}

func (i *ChangesItem) Less(than btree.Item) bool {
	if i.endBlock == than.(*ChangesItem).endBlock {
		// Larger intevals will come last
		return i.startBlock > than.(*ChangesItem).startBlock
	}
	return i.endBlock < than.(*ChangesItem).endBlock
}

type byEndBlockItem struct {
	startBlock   uint64
	endBlock     uint64
	decompressor *compress.Decompressor
	index        *recsplit.Index
	tree         *btree.BTree // Substitute for decompressor+index combination
}

func (i *byEndBlockItem) Less(than btree.Item) bool {
	if i.endBlock == than.(*byEndBlockItem).endBlock {
		return i.startBlock > than.(*byEndBlockItem).startBlock
	}
	return i.endBlock < than.(*byEndBlockItem).endBlock
}

func NewAggregator(diffDir string, unwindLimit uint64, aggregationStep uint64) (*Aggregator, error) {
	a := &Aggregator{
		diffDir:         diffDir,
		unwindLimit:     unwindLimit,
		aggregationStep: aggregationStep,
		tracedKeys:      map[string]struct{}{},
		keccak:          sha3.NewLegacyKeccak256(),
		hph:             commitment.NewHexPatriciaHashed(length.Addr, nil, nil, nil, nil, nil),
		accountsFiles:   btree.New(32),
		codeFiles:       btree.New(32),
		storageFiles:    btree.New(32),
		commitmentFiles: btree.New(32),
		aggChannel:      make(chan AggregationTask),
		aggBackCh:       make(chan struct{}),
		aggError:        make(chan error, 1),
		mergeChannel:    make(chan struct{}, 1),
		mergeError:      make(chan error, 1),
	}
	var closeStateFiles = true // It will be set to false in case of success at the end of the function
	defer func() {
		// Clean up all decompressor and indices upon error
		if closeStateFiles {
			a.Close()
		}
	}()
	// Scan the diff directory and create the mapping of end blocks to files
	files, err := os.ReadDir(diffDir)
	if err != nil {
		return nil, err
	}
	re := regexp.MustCompile(`(accounts|storage|code|commitment).([0-9]+)-([0-9]+).(dat|idx)`)
	for _, f := range files {
		name := f.Name()
		subs := re.FindStringSubmatch(name)
		if len(subs) != 5 {
			if len(subs) != 0 {
				log.Warn("File ignored by aggregator, more than 4 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startBlock, endBlock uint64
		if startBlock, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			log.Warn("File ignored by aggregator, parsing startBlock", "error", err, "name", name)
			continue
		}
		if endBlock, err = strconv.ParseUint(subs[3], 10, 64); err != nil {
			log.Warn("File ignored by aggregator, parsing endBlock", "error", err, "name", name)
			continue
		}
		if startBlock > endBlock {
			log.Warn("File ignored by aggregator, startBlock > endBlock", "name", name)
			continue
		}
		var tree *btree.BTree
		switch subs[1] {
		case "accounts":
			tree = a.accountsFiles
		case "storage":
			tree = a.storageFiles
		case "code":
			tree = a.codeFiles
		case "commitment":
			tree = a.commitmentFiles
		}
		var item = &byEndBlockItem{startBlock: startBlock, endBlock: endBlock}
		var foundI *byEndBlockItem
		tree.AscendGreaterOrEqual(&byEndBlockItem{startBlock: endBlock, endBlock: endBlock}, func(i btree.Item) bool {
			it := i.(*byEndBlockItem)
			if it.endBlock == endBlock {
				foundI = it
			}
			return false
		})
		if foundI == nil || foundI.startBlock > startBlock {
			tree.ReplaceOrInsert(item)
		}
	}
	// Check for overlaps and holes
	if err := checkOverlaps("accounts", a.accountsFiles); err != nil {
		return nil, err
	}
	if err := checkOverlaps("storage", a.storageFiles); err != nil {
		return nil, err
	}
	if err := checkOverlaps("code", a.codeFiles); err != nil {
		return nil, err
	}
	if err := checkOverlaps("commitment", a.commitmentFiles); err != nil {
		return nil, err
	}
	// Open decompressor and index files for all items in all trees
	if err := openFiles("accounts", diffDir, a.accountsFiles); err != nil {
		return nil, fmt.Errorf("opening accounts state files: %w", err)
	}
	if err := openFiles("storage", diffDir, a.storageFiles); err != nil {
		return nil, fmt.Errorf("opening storage state files: %w", err)
	}
	if err := openFiles("code", diffDir, a.codeFiles); err != nil {
		return nil, fmt.Errorf("opening code state files: %w", err)
	}
	if err := openFiles("commitment", diffDir, a.commitmentFiles); err != nil {
		return nil, fmt.Errorf("opening commitment state files: %w", err)
	}
	a.changesBtree = btree.New(32)
	re = regexp.MustCompile(`(accounts|storage|code|commitment).(keys|before|after).([0-9]+)-([0-9]+).chg`)
	for _, f := range files {
		name := f.Name()
		subs := re.FindStringSubmatch(name)
		if len(subs) != 5 {
			if len(subs) != 0 {
				log.Warn("File ignored by changes scan, more than 4 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startBlock, endBlock uint64
		if startBlock, err = strconv.ParseUint(subs[3], 10, 64); err != nil {
			log.Warn("File ignored by changes scan, parsing startBlock", "error", err, "name", name)
			continue
		}
		if endBlock, err = strconv.ParseUint(subs[4], 10, 64); err != nil {
			log.Warn("File ignored by changes scan, parsing endBlock", "error", err, "name", name)
			continue
		}
		if startBlock > endBlock {
			log.Warn("File ignored by changes scan, startBlock > endBlock", "name", name)
			continue
		}
		if endBlock != startBlock+aggregationStep-1 {
			log.Warn("File ignored by changes scan, endBlock != startBlock+aggregationStep-1", "name", name)
			continue
		}
		var item = &ChangesItem{fileCount: 1, startBlock: startBlock, endBlock: endBlock}
		i := a.changesBtree.Get(item)
		if i == nil {
			a.changesBtree.ReplaceOrInsert(item)
		} else {
			item = i.(*ChangesItem)
			if item.startBlock == startBlock {
				item.fileCount++
			} else {
				return nil, fmt.Errorf("change files overlap [%d-%d] with [%d-%d]", item.startBlock, item.endBlock, startBlock, endBlock)
			}
		}
	}
	// Check for holes in change files
	minStart := uint64(math.MaxUint64)
	a.changesBtree.Descend(func(i btree.Item) bool {
		item := i.(*ChangesItem)
		if item.startBlock < minStart {
			if item.endBlock >= minStart {
				err = fmt.Errorf("overlap of change files [%d-%d] with %d", item.startBlock, item.endBlock, minStart)
				return false
			}
			if minStart != math.MaxUint64 && item.endBlock+1 != minStart {
				err = fmt.Errorf("whole in change files [%d-%d]", item.endBlock, minStart)
				return false
			}
			if item.fileCount != 12 && item.fileCount != 8 {
				err = fmt.Errorf("missing change files for interval [%d-%d]", item.startBlock, item.endBlock)
				return false
			}
			minStart = item.startBlock
		} else {
			err = fmt.Errorf("overlap of change files [%d-%d] with %d", item.startBlock, item.endBlock, minStart)
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if err = checkOverlapWithMinStart("accounts", a.accountsFiles, minStart); err != nil {
		return nil, err
	}
	if err = checkOverlapWithMinStart("storage", a.storageFiles, minStart); err != nil {
		return nil, err
	}
	if err = checkOverlapWithMinStart("code", a.codeFiles, minStart); err != nil {
		return nil, err
	}
	if err = checkOverlapWithMinStart("commitment", a.commitmentFiles, minStart); err != nil {
		return nil, err
	}
	closeStateFiles = false
	a.aggWg.Add(1)
	go a.backgroundAggregation()
	a.mergeWg.Add(1)
	go a.backgroundMerge()
	return a, nil
}

type AggregationTask struct {
	accountChanges *Changes
	accountsBt     *btree.BTree
	codeChanges    *Changes
	codeBt         *btree.BTree
	storageChanges *Changes
	storageBt      *btree.BTree
	commChanges    *Changes
	commitmentBt   *btree.BTree
	blockFrom      uint64
	blockTo        uint64
}

func removeLocked(tree **btree.BTree, lock sync.Locker, toRemove []*byEndBlockItem, toAdd *byEndBlockItem) {
	lock.Lock()
	defer lock.Unlock()
	for _, ag := range toRemove {
		(*tree).Delete(ag)
	}
	(*tree).ReplaceOrInsert(toAdd)
}

func removeFiles(treeName string, diffDir string, toRemove []*byEndBlockItem) error {
	// Close all the memory maps etc
	for _, ag := range toRemove {
		if err := ag.index.Close(); err != nil {
			return fmt.Errorf("close index: %w", err)
		}
		if err := ag.decompressor.Close(); err != nil {
			return fmt.Errorf("close decompressor: %w", err)
		}
	}
	// Delete files
	// TODO: in a non-test version, this is delayed to allow other participants to roll over to the next file
	for _, ag := range toRemove {
		if err := os.Remove(path.Join(diffDir, fmt.Sprintf("%s.%d-%d.dat", treeName, ag.startBlock, ag.endBlock))); err != nil {
			return fmt.Errorf("remove decompressor file %s.%d-%d.dat: %w", treeName, ag.startBlock, ag.endBlock, err)
		}
		if err := os.Remove(path.Join(diffDir, fmt.Sprintf("%s.%d-%d.idx", treeName, ag.startBlock, ag.endBlock))); err != nil {
			return fmt.Errorf("remove index file %s.%d-%d.idx: %w", treeName, ag.startBlock, ag.endBlock, err)
		}
	}
	return nil
}

// backgroundAggregation is the functin that runs in a background go-routine and performs creation of initial state files
// allowing the main goroutine to proceed
func (a *Aggregator) backgroundAggregation() {
	defer a.aggWg.Done()
	for aggTask := range a.aggChannel {
		addLocked(&a.accountsFiles, &byEndBlockItem{startBlock: aggTask.blockFrom, endBlock: aggTask.blockTo, tree: aggTask.accountsBt}, &a.accountsFilesLock)
		addLocked(&a.codeFiles, &byEndBlockItem{startBlock: aggTask.blockFrom, endBlock: aggTask.blockTo, tree: aggTask.codeBt}, &a.codeFilesLock)
		addLocked(&a.storageFiles, &byEndBlockItem{startBlock: aggTask.blockFrom, endBlock: aggTask.blockTo, tree: aggTask.storageBt}, &a.storageFilesLock)
		addLocked(&a.commitmentFiles, &byEndBlockItem{startBlock: aggTask.blockFrom, endBlock: aggTask.blockTo, tree: aggTask.commitmentBt}, &a.commFilesLock)
		a.aggBackCh <- struct{}{}
		var err error
		if a.changesets && aggTask.accountsBt.Len() > 0 { // No need to produce changeset files if there were no changes
			chsetDatPath := path.Join(a.diffDir, fmt.Sprintf("chsets.%s.%d-%d.dat", "accounts", aggTask.blockFrom, aggTask.blockTo))
			chsetIdxPath := path.Join(a.diffDir, fmt.Sprintf("chsets.%s.%d-%d.idx", "accounts", aggTask.blockFrom, aggTask.blockTo))
			if err = aggTask.accountChanges.produceChangeSets(chsetDatPath, chsetIdxPath); err != nil {
				a.aggError <- fmt.Errorf("produceChangeSets accounts: %w", err)
				return
			}
		}
		if err = aggTask.accountChanges.closeFiles(); err != nil {
			a.aggError <- fmt.Errorf("close accountChanges: %w", err)
			return
		}
		var accountsItem = &byEndBlockItem{startBlock: aggTask.blockFrom, endBlock: aggTask.blockTo}
		if accountsItem.decompressor, accountsItem.index, err = createDatAndIndex("accounts", a.diffDir, aggTask.accountsBt, aggTask.blockFrom, aggTask.blockTo); err != nil {
			a.aggError <- fmt.Errorf("createDatAndIndex accounts: %w", err)
			return
		}
		if err = aggTask.accountChanges.deleteFiles(); err != nil {
			a.aggError <- fmt.Errorf("delete accountChanges: %w", err)
			return
		}
		addLocked(&a.accountsFiles, accountsItem, &a.accountsFilesLock)
		if a.changesets && aggTask.codeBt.Len() > 0 { // No need to produce changeset files if there were no changes
			chsetDatPath := path.Join(a.diffDir, fmt.Sprintf("chsets.%s.%d-%d.dat", "code", aggTask.blockFrom, aggTask.blockTo))
			chsetIdxPath := path.Join(a.diffDir, fmt.Sprintf("chsets.%s.%d-%d.idx", "code", aggTask.blockFrom, aggTask.blockTo))
			if err = aggTask.codeChanges.produceChangeSets(chsetDatPath, chsetIdxPath); err != nil {
				a.aggError <- fmt.Errorf("produceChangeSets code: %w", err)
				return
			}
		}
		if err = aggTask.codeChanges.closeFiles(); err != nil {
			a.aggError <- fmt.Errorf("close codeChanges: %w", err)
			return
		}
		var codeItem = &byEndBlockItem{startBlock: aggTask.blockFrom, endBlock: aggTask.blockTo}
		if codeItem.decompressor, codeItem.index, err = createDatAndIndex("code", a.diffDir, aggTask.codeBt, aggTask.blockFrom, aggTask.blockTo); err != nil {
			a.aggError <- fmt.Errorf("createDatAndIndex code: %w", err)
			return
		}
		if err = aggTask.codeChanges.deleteFiles(); err != nil {
			a.aggError <- fmt.Errorf("delete codeChanges: %w", err)
			return
		}
		addLocked(&a.codeFiles, codeItem, &a.codeFilesLock)
		if a.changesets && aggTask.storageBt.Len() > 0 { // No need to produce changeset files if there were no changes
			chsetDatPath := path.Join(a.diffDir, fmt.Sprintf("chsets.%s.%d-%d.dat", "storage", aggTask.blockFrom, aggTask.blockTo))
			chsetIdxPath := path.Join(a.diffDir, fmt.Sprintf("chsets.%s.%d-%d.idx", "storage", aggTask.blockFrom, aggTask.blockTo))
			if err = aggTask.storageChanges.produceChangeSets(chsetDatPath, chsetIdxPath); err != nil {
				a.aggError <- fmt.Errorf("produceChangeSets storage: %w", err)
				return
			}
		}
		if err = aggTask.storageChanges.closeFiles(); err != nil {
			a.aggError <- fmt.Errorf("close storageChanges: %w", err)
			return
		}
		var storageItem = &byEndBlockItem{startBlock: aggTask.blockFrom, endBlock: aggTask.blockTo}
		if storageItem.decompressor, storageItem.index, err = createDatAndIndex("storage", a.diffDir, aggTask.storageBt, aggTask.blockFrom, aggTask.blockTo); err != nil {
			a.aggError <- fmt.Errorf("createDatAndIndex storage: %w", err)
			return
		}
		if err = aggTask.storageChanges.deleteFiles(); err != nil {
			a.aggError <- fmt.Errorf("delete storageChanges: %w", err)
			return
		}
		addLocked(&a.storageFiles, storageItem, &a.storageFilesLock)
		if err = aggTask.commChanges.closeFiles(); err != nil {
			a.aggError <- fmt.Errorf("close commChanges: %w", err)
			return
		}
		var commitmentItem = &byEndBlockItem{startBlock: aggTask.blockFrom, endBlock: aggTask.blockTo}
		if commitmentItem.decompressor, commitmentItem.index, err = createDatAndIndex("commitment", a.diffDir, aggTask.commitmentBt, aggTask.blockFrom, aggTask.blockTo); err != nil {
			a.aggError <- fmt.Errorf("createDatAndIndex commitment: %w", err)
			return
		}
		if err = aggTask.commChanges.deleteFiles(); err != nil {
			a.aggError <- fmt.Errorf("delete commChanges: %w", err)
			return
		}
		addLocked(&a.commitmentFiles, commitmentItem, &a.commFilesLock)
		// At this point, 3 new state files (containing latest changes) has been created for accounts, code, and storage
		// Corresponding items has been added to the registy of state files, and B-tree are not necessary anymore, change files can be removed
		// What follows can be performed by the 2nd background goroutine
		select {
		case a.mergeChannel <- struct{}{}:
		default:
		}
	}
}

type CommitmentValTransform struct {
	numBuf       [binary.MaxVarintLen64]byte // Buffer for encoding varint numbers without extra allocations
	preAccounts  []*byEndBlockItem           // List of account state files before the merge
	preCode      []*byEndBlockItem           // List of code state files before the merge
	preStorage   []*byEndBlockItem           // List of storage state files before the merge
	postAccounts []*byEndBlockItem           // List of account state files after the merge
	postCode     []*byEndBlockItem           // List of code state files after the merge
	postStorage  []*byEndBlockItem           // List of storage state files after the merge
}

func decodeU64(from []byte) uint64 {
	var i uint64
	for _, b := range from {
		i = (i << 8) | uint64(b)
	}
	return i
}

func encodeU64(i uint64, to []byte) []byte {
	// writes i to b in big endian byte order, using the least number of bytes needed to represent i.
	switch {
	case i < (1 << 8):
		return append(to, byte(i))
	case i < (1 << 16):
		return append(to, byte(i>>8), byte(i))
	case i < (1 << 24):
		return append(to, byte(i>>16), byte(i>>8), byte(i))
	case i < (1 << 32):
		return append(to, byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
	case i < (1 << 40):
		return append(to, byte(i>>32), byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
	case i < (1 << 48):
		return append(to, byte(i>>40), byte(i>>32), byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
	case i < (1 << 56):
		return append(to, byte(i>>48), byte(i>>40), byte(i>>32), byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
	default:
		return append(to, byte(i>>56), byte(i>>48), byte(i>>40), byte(i>>32), byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
	}
}

// commitmentValTransform parses the value of of the commitment record to extract references
// to accounts and storage items, then looks them up in the new, merged files, and replaces them with
// the updated references
func (cvt *CommitmentValTransform) commitmentValTransform(val []byte, transValBuf []byte) ([]byte, error) {
	if len(val) == 0 {
		return transValBuf, nil
	}
	accountPlainKeys, storagePlainKeys, err := commitment.ExtractPlainKeys(val[1:])
	if err != nil {
		return nil, err
	}
	var transAccountPks = make([][]byte, len(accountPlainKeys))
	var transStoragePks = make([][]byte, len(storagePlainKeys))
	var apkBuf, spkBuf []byte
	for i, apk := range accountPlainKeys {
		if len(apk) == length.Addr {
			// Non-optimised key originating from a database record
			apkBuf = append(apkBuf[:0], apk...)
		} else {
			// Optimised key referencing a state file record (file number and offset within the file)
			fileI := int(apk[0])
			offset := decodeU64(apk[1:])
			g := cvt.preAccounts[fileI].decompressor.MakeGetter() // TODO Cache in the reader
			g.Reset(offset)
			apkBuf, _ = g.Next(apkBuf[:0])
		}
		// Look up apkBuf in the post account files
		for j := len(cvt.postAccounts); j > 0; j-- {
			item := cvt.postAccounts[j-1]
			if item.index.Empty() {
				continue
			}
			reader := recsplit.NewIndexReader(item.index)
			offset := reader.Lookup(apkBuf)
			g := item.decompressor.MakeGetter() // TODO Cache in the reader
			g.Reset(offset)
			if g.HasNext() {
				if keyMatch, _ := g.Match(apkBuf); keyMatch {
					apk = encodeU64(offset, []byte{byte(j - 1)})
					//fmt.Fprintf(os.Stderr, "encoding apkBuf [%x] into fileI %d, offset %d = [%x], file [%d-%d]\n", apkBuf, j-1, offset, apk, item.startBlock, item.endBlock)
					break
				}
			}
		}
		transAccountPks[i] = apk
	}
	for i, spk := range storagePlainKeys {
		if len(spk) == length.Addr+length.Hash {
			// Non-optimised key originating from a database record
			spkBuf = append(spkBuf[:0], spk...)
		} else {
			// Optimised key referencing a state file record (file number and offset within the file)
			fileI := int(spk[0])
			offset := decodeU64(spk[1:])
			g := cvt.preStorage[fileI].decompressor.MakeGetter() // TODO Cache in the reader
			g.Reset(offset)
			spkBuf, _ = g.Next(spkBuf[:0])
		}
		// Lookup spkBuf in the post storage files
		for j := len(cvt.postStorage); j > 0; j-- {
			item := cvt.postStorage[j-1]
			if item.index.Empty() {
				continue
			}
			reader := recsplit.NewIndexReader(item.index)
			offset := reader.Lookup(spkBuf)
			g := item.decompressor.MakeGetter() // TODO Cache in the reader
			g.Reset(offset)
			if g.HasNext() {
				if keyMatch, _ := g.Match(spkBuf); keyMatch {
					spk = encodeU64(offset, []byte{byte(j - 1)})
					//fmt.Fprintf(os.Stderr, "encoding spkBuf [%x] into fileI %d, offset %d = [%x], file [%d-%d]\n", spkBuf, j-1, offset, spk, item.startBlock, item.endBlock)
					break
				}
			}
		}
		transStoragePks[i] = spk
	}
	transValBuf = append(transValBuf, val[0])
	if transValBuf, err = commitment.ReplacePlainKeys(val[1:], transAccountPks, transStoragePks, cvt.numBuf[:], transValBuf); err != nil {
		return nil, err
	}
	return transValBuf, nil
}

func (a *Aggregator) backgroundMerge() {
	defer a.mergeWg.Done()
	for range a.mergeChannel {
		t := time.Now()
		var err error
		var accountsToRemove []*byEndBlockItem
		var accountsFrom, accountsTo uint64
		var cvt CommitmentValTransform
		commitmentToRemove, _, _, blockFrom, blockTo := findLargestMerge(&a.commitmentFiles, a.commFilesLock.RLocker(), uint64(math.MaxUint64))
		accountsToRemove, cvt.preAccounts, cvt.postAccounts, accountsFrom, accountsTo = findLargestMerge(&a.accountsFiles, a.accountsFilesLock.RLocker(), blockTo)
		if accountsFrom != blockFrom {
			a.mergeError <- fmt.Errorf("accountsFrom %d != blockFrom %d", accountsFrom, blockFrom)
			return
		}
		if accountsTo != blockTo {
			a.mergeError <- fmt.Errorf("accountsTo %d != blockTo %d", accountsTo, blockTo)
			return
		}
		var newAccountsItem *byEndBlockItem
		if len(accountsToRemove) > 1 {
			if newAccountsItem, err = a.computeAggregation("accounts", accountsToRemove, accountsFrom, accountsTo, nil /* valTransform */); err != nil {
				a.mergeError <- fmt.Errorf("computeAggreation accounts: %w", err)
				return
			}
			cvt.postAccounts = append(cvt.postAccounts, newAccountsItem)
		}
		var codeToRemove []*byEndBlockItem
		var codeFrom, codeTo uint64
		codeToRemove, cvt.preCode, cvt.postCode, codeFrom, codeTo = findLargestMerge(&a.codeFiles, a.codeFilesLock.RLocker(), blockTo)
		if codeFrom != blockFrom {
			a.mergeError <- fmt.Errorf("codeFrom %d != blockFrom %d", codeFrom, blockFrom)
			return
		}
		if codeTo != blockTo {
			a.mergeError <- fmt.Errorf("codeTo %d != blockTo %d", codeTo, blockTo)
			return
		}
		var newCodeItem *byEndBlockItem
		if len(codeToRemove) > 1 {
			if newCodeItem, err = a.computeAggregation("code", codeToRemove, codeFrom, codeTo, nil /* valTransform */); err != nil {
				a.mergeError <- fmt.Errorf("computeAggreation code: %w", err)
				return
			}
			cvt.postCode = append(cvt.postCode, newCodeItem)
		}
		var storageToRemove []*byEndBlockItem
		var storageFrom, storageTo uint64
		storageToRemove, cvt.preStorage, cvt.postStorage, storageFrom, storageTo = findLargestMerge(&a.storageFiles, a.storageFilesLock.RLocker(), blockTo)
		if storageFrom != blockFrom {
			a.mergeError <- fmt.Errorf("storageFrom %d != blockFrom %d", storageFrom, blockFrom)
			return
		}
		if storageTo != blockTo {
			a.mergeError <- fmt.Errorf("storageTo %d != blockTo %d", storageTo, blockTo)
			return
		}
		var newStorageItem *byEndBlockItem
		if len(storageToRemove) > 1 {
			if newStorageItem, err = a.computeAggregation("storage", storageToRemove, storageFrom, storageTo, nil /* valTransform */); err != nil {
				a.mergeError <- fmt.Errorf("computeAggreation storage: %w", err)
				return
			}
			cvt.postStorage = append(cvt.postStorage, newStorageItem)
		}
		var newCommitmentItem *byEndBlockItem
		if len(commitmentToRemove) > 1 {
			if newCommitmentItem, err = a.computeAggregation("commitment", commitmentToRemove, blockFrom, blockTo, cvt.commitmentValTransform); err != nil {
				a.mergeError <- fmt.Errorf("computeAggreation commitment: %w", err)
				return
			}
		}
		// Switch aggregator to new state files, close and remove old files
		if len(accountsToRemove) > 1 {
			removeLocked(&a.accountsFiles, &a.accountsFilesLock, accountsToRemove, newAccountsItem)
			removeFiles("accounts", a.diffDir, accountsToRemove)
		}
		if len(codeToRemove) > 1 {
			removeLocked(&a.codeFiles, &a.codeFilesLock, codeToRemove, newCodeItem)
			removeFiles("code", a.diffDir, codeToRemove)
		}
		if len(storageToRemove) > 1 {
			removeLocked(&a.storageFiles, &a.storageFilesLock, storageToRemove, newStorageItem)
			removeFiles("storage", a.diffDir, storageToRemove)
		}
		if len(commitmentToRemove) > 1 {
			removeLocked(&a.commitmentFiles, &a.commFilesLock, commitmentToRemove, newCommitmentItem)
			removeFiles("commitment", a.diffDir, commitmentToRemove)
		}
		if len(accountsToRemove) > 1 {
			mergeTime := time.Since(t)
			if mergeTime > time.Minute {
				log.Info("Long merge", "from", blockFrom, "to", blockTo, "files", len(accountsToRemove), "time", time.Since(t))
			}
		}
	}
}

func (a *Aggregator) GenerateChangesets(on bool) {
	a.changesets = on
}

// checkOverlaps does not lock tree, because it is only called from the constructor of aggregator
func checkOverlaps(treeName string, tree *btree.BTree) error {
	var minStart uint64 = math.MaxUint64
	var err error
	tree.Descend(func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if item.startBlock < minStart {
			if item.endBlock >= minStart {
				err = fmt.Errorf("overlap of %s state files [%d-%d] with %d", treeName, item.startBlock, item.endBlock, minStart)
				return false
			}
			if minStart != math.MaxUint64 && item.endBlock+1 != minStart {
				err = fmt.Errorf("hole in %s state files [%d-%d]", treeName, item.endBlock, minStart)
				return false
			}
			minStart = item.startBlock
		}
		return true
	})
	return err
}

func openFiles(treeName string, diffDir string, tree *btree.BTree) error {
	var err error
	tree.Ascend(func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if item.decompressor, err = compress.NewDecompressor(path.Join(diffDir, fmt.Sprintf("%s.%d-%d.dat", treeName, item.startBlock, item.endBlock))); err != nil {
			return false
		}
		if item.index, err = recsplit.OpenIndex(path.Join(diffDir, fmt.Sprintf("%s.%d-%d.idx", treeName, item.startBlock, item.endBlock))); err != nil {
			return false
		}
		return true
	})
	return err
}

func closeFiles(tree **btree.BTree, lock sync.Locker) {
	lock.Lock()
	defer lock.Unlock()
	(*tree).Ascend(func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if item.decompressor != nil {
			item.decompressor.Close()
		}
		if item.index != nil {
			item.index.Close()
		}
		return true
	})
}

func (a *Aggregator) Close() {
	close(a.aggChannel)
	a.aggWg.Wait()
	close(a.mergeChannel)
	a.mergeWg.Wait()
	// Closing state files only after background aggregation goroutine is finished
	closeFiles(&a.accountsFiles, &a.accountsFilesLock)
	closeFiles(&a.codeFiles, &a.codeFilesLock)
	closeFiles(&a.storageFiles, &a.storageFilesLock)
	closeFiles(&a.commitmentFiles, &a.commFilesLock)
}

// checkOverlapWithMinStart does not need to lock tree lock, because it is only used in the constructor of Aggregator
func checkOverlapWithMinStart(treeName string, tree *btree.BTree, minStart uint64) error {
	if lastStateI := tree.Max(); lastStateI != nil {
		item := lastStateI.(*byEndBlockItem)
		if minStart != math.MaxUint64 && item.endBlock+1 != minStart {
			return fmt.Errorf("hole or overlap between %s state files and change files [%d-%d]", treeName, item.endBlock, minStart)
		}
	}
	return nil
}

func readFromFiles(treeName string, tree **btree.BTree, lock sync.Locker, blockNum uint64, filekey []byte, trace bool) []byte {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}
	var val []byte
	(*tree).DescendLessOrEqual(&byEndBlockItem{endBlock: blockNum}, func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if trace {
			fmt.Printf("read %s %x: search in file [%d-%d]\n", treeName, filekey, item.startBlock, item.endBlock)
		}
		if item.tree != nil {
			ai := item.tree.Get(&AggregateItem{k: filekey})
			if ai == nil {
				return true
			}
			val = ai.(*AggregateItem).v
			return false
		}
		if item.index.Empty() {
			return true
		}
		reader := recsplit.NewIndexReader(item.index)
		offset := reader.Lookup(filekey)
		g := item.decompressor.MakeGetter() // TODO Cache in the reader
		g.Reset(offset)
		if g.HasNext() {
			if keyMatch, _ := g.Match(filekey); keyMatch {
				val, _ = g.Next(nil)
				if trace {
					fmt.Printf("read %s %x: found [%x] in file [%d-%d]\n", treeName, filekey, val, item.startBlock, item.endBlock)
				}
				return false
			}
		}
		return true
	})
	if len(val) > 0 {
		return val[1:]
	}
	return nil
}

// readByOffset is assumed to be invoked under a read lock
func readByOffset(treeName string, tree **btree.BTree, fileI int, offset uint64) ([]byte, []byte) {
	var key, val []byte
	fi := 0
	(*tree).Ascend(func(i btree.Item) bool {
		if fi < fileI {
			fi++
			return true
		}
		item := i.(*byEndBlockItem)
		//fmt.Fprintf(os.Stderr, "found in file [%d-%d]\n", item.startBlock, item.endBlock)
		g := item.decompressor.MakeGetter() // TODO Cache in the reader
		g.Reset(offset)
		key, _ = g.Next(nil)
		val, _ = g.Next(nil)
		return false
	})
	if len(val) > 0 {
		return key, val[1:]
	}
	return key, nil
}

func (a *Aggregator) MakeStateReader(tx kv.Getter, blockNum uint64) *Reader {
	r := &Reader{
		a:        a,
		tx:       tx,
		blockNum: blockNum,
	}
	return r
}

type Reader struct {
	a        *Aggregator
	tx       kv.Getter
	blockNum uint64
}

func (r *Reader) ReadAccountData(addr []byte, trace bool) ([]byte, error) {
	// Look in the summary table first
	v, err := r.tx.GetOne(kv.StateAccounts, addr)
	if err != nil {
		return nil, err
	}
	if v != nil {
		// First 4 bytes is the number of 1-block state diffs containing the key
		if trace {
			fmt.Printf("ReadAccountData %x, found in DB: %x, number of diffs: %d\n", addr, v[4:], binary.BigEndian.Uint32(v[:4]))
		}
		return v[4:], nil
	}
	// Look in the files
	val := readFromFiles("accounts", &r.a.accountsFiles, r.a.accountsFilesLock.RLocker(), r.blockNum, addr, trace)
	return val, nil
}

func (r *Reader) ReadAccountStorage(addr []byte, loc []byte, trace bool) (*uint256.Int, error) {
	// Look in the summary table first
	dbkey := make([]byte, len(addr)+len(loc))
	copy(dbkey[0:], addr)
	copy(dbkey[len(addr):], loc)
	v, err := r.tx.GetOne(kv.StateStorage, dbkey)
	if err != nil {
		return nil, err
	}
	if v != nil {
		if trace {
			fmt.Printf("ReadAccountStorage %x %x, found in DB: %x, number of diffs: %d\n", addr, loc, v[4:], binary.BigEndian.Uint32(v[:4]))
		}
		if len(v) == 4 {
			return nil, nil
		}
		// First 4 bytes is the number of 1-block state diffs containing the key
		return new(uint256.Int).SetBytes(v[4:]), nil
	}
	// Look in the files
	val := readFromFiles("storage", &r.a.storageFiles, r.a.storageFilesLock.RLocker(), r.blockNum, dbkey, trace)
	if val != nil {
		return new(uint256.Int).SetBytes(val), nil
	}
	return nil, nil
}

func (r *Reader) ReadAccountCode(addr []byte, trace bool) ([]byte, error) {
	// Look in the summary table first
	v, err := r.tx.GetOne(kv.StateCode, addr)
	if err != nil {
		return nil, err
	}
	if v != nil {
		// First 4 bytes is the number of 1-block state diffs containing the key
		if trace {
			fmt.Printf("ReadAccountCode %x, found in DB: %x, number of diffs: %d\n", addr, v[4:], binary.BigEndian.Uint32(v[:4]))
		}
		return v[4:], nil
	}
	// Look in the files
	val := readFromFiles("code", &r.a.codeFiles, r.a.codeFilesLock.RLocker(), r.blockNum, addr, trace)
	return val, nil
}

type Writer struct {
	a              *Aggregator
	tx             kv.RwTx
	blockNum       uint64
	changeFileNum  uint64       // Block number associated with the current change files. It is the last block number whose changes will go into that file
	accountChanges Changes      // Change files for accounts
	codeChanges    Changes      // Change files for contract code
	storageChanges Changes      // Change files for contract storage
	commChanges    Changes      // Change files for commitment
	commTree       *btree.BTree // BTree used for gathering commitment data
}

func (a *Aggregator) MakeStateWriter(beforeOn bool) *Writer {
	w := &Writer{
		a:        a,
		commTree: btree.New(32),
	}
	w.accountChanges.Init("accounts", a.aggregationStep, a.diffDir, beforeOn)
	w.codeChanges.Init("code", a.aggregationStep, a.diffDir, beforeOn)
	w.storageChanges.Init("storage", a.aggregationStep, a.diffDir, beforeOn)
	w.commChanges.Init("commitment", a.aggregationStep, a.diffDir, false /* we do not unwind commitment */)
	return w
}

func (w *Writer) Close() {
	w.accountChanges.closeFiles()
	w.codeChanges.closeFiles()
	w.storageChanges.closeFiles()
	w.commChanges.closeFiles()
}

func (w *Writer) Reset(tx kv.RwTx, blockNum uint64) error {
	w.tx = tx
	w.blockNum = blockNum
	if blockNum > w.changeFileNum {
		if err := w.accountChanges.closeFiles(); err != nil {
			return err
		}
		if err := w.codeChanges.closeFiles(); err != nil {
			return err
		}
		if err := w.storageChanges.closeFiles(); err != nil {
			return err
		}
		if err := w.commChanges.closeFiles(); err != nil {
			return err
		}
		if w.changeFileNum != 0 {
			w.a.changesBtree.ReplaceOrInsert(&ChangesItem{startBlock: w.changeFileNum + 1 - w.a.aggregationStep, endBlock: w.changeFileNum, fileCount: 12})
		}
	}
	if w.changeFileNum == 0 || blockNum > w.changeFileNum {
		if err := w.accountChanges.openFiles(blockNum, true /* write */); err != nil {
			return err
		}
		if err := w.codeChanges.openFiles(blockNum, true /* write */); err != nil {
			return err
		}
		if err := w.storageChanges.openFiles(blockNum, true /* write */); err != nil {
			return err
		}
		if err := w.commChanges.openFiles(blockNum, true /* write */); err != nil {
			return err
		}
		w.changeFileNum = blockNum - (blockNum % w.a.aggregationStep) + w.a.aggregationStep - 1
	}
	return nil
}

type CommitmentItem struct {
	plainKey  []byte
	hashedKey []byte
	u         commitment.Update
}

func (i *CommitmentItem) Less(than btree.Item) bool {
	return bytes.Compare(i.hashedKey, than.(*CommitmentItem).hashedKey) < 0
}

func (w *Writer) lockFn() {
	w.a.accountsFilesLock.RLock()
	w.a.storageFilesLock.RLock()
	w.a.commFilesLock.RLock()
}

func (w *Writer) unlockFn() {
	w.a.commFilesLock.RUnlock()
	w.a.storageFilesLock.RUnlock()
	w.a.accountsFilesLock.RUnlock()
}

func (w *Writer) branchFn(prefix []byte) ([]byte, error) {
	// Look in the summary table first
	dbPrefix := prefix
	v, err := w.tx.GetOne(kv.StateCommitment, dbPrefix)
	if err != nil {
		return nil, err
	}
	if v != nil {
		// First 4 bytes is the number of 1-block state diffs containing the key
		return v[4:], nil
	}
	// Look in the files
	val := readFromFiles("commitment", &w.a.commitmentFiles, nil /* lock */, w.blockNum, prefix, false /* trace */)
	return val, nil
}

func bytesToUint64(buf []byte) (x uint64) {
	for i, b := range buf {
		x = x<<8 + uint64(b)
		if i == 7 {
			return
		}
	}
	return
}

func (w *Writer) accountFn(plainKey []byte, cell *commitment.Cell) ([]byte, error) {
	var enc []byte
	var v []byte
	var err error
	if len(plainKey) != length.Addr {
		fileI := int(plainKey[0])
		offset := decodeU64(plainKey[1:])
		//fmt.Fprintf(os.Stderr, "accountFn, plainKey [%x], fileI %d, offset %d\n", plainKey, fileI, offset)
		plainKey, _ = readByOffset("accounts", &w.a.accountsFiles, fileI, offset)
		//fmt.Fprintf(os.Stderr, "retrived [%x]\n", plainKey)
	}
	// Look in the summary table first
	if v, err = w.tx.GetOne(kv.StateAccounts, plainKey); err != nil {
		return nil, err
	}
	if v != nil {
		// First 4 bytes is the number of 1-block state diffs containing the key
		enc = v[4:]
	} else {
		// Look in the files
		enc = readFromFiles("accounts", &w.a.accountsFiles, nil /* lock */, w.blockNum, plainKey, false /* trace */)
	}
	cell.Nonce = 0
	cell.Balance.Clear()
	copy(cell.CodeHash[:], commitment.EmptyCodeHash)

	if len(enc) > 0 {
		pos := 0
		nonceBytes := int(enc[pos])
		pos++
		if nonceBytes > 0 {
			cell.Nonce = bytesToUint64(enc[pos : pos+nonceBytes])
			pos += nonceBytes
		}
		balanceBytes := int(enc[pos])
		pos++
		if balanceBytes > 0 {
			cell.Balance.SetBytes(enc[pos : pos+balanceBytes])
		}
	}

	v, err = w.tx.GetOne(kv.StateCode, plainKey)
	if err != nil {
		return nil, err
	}
	if v != nil {
		// First 4 bytes is the number of 1-block state diffs containing the key
		enc = v[4:]
	} else {
		// Look in the files
		enc = readFromFiles("code", &w.a.codeFiles, w.a.codeFilesLock.RLocker(), w.blockNum, plainKey, false /* trace */)
	}
	if len(enc) > 0 {
		w.a.keccak.Reset()
		w.a.keccak.Write(enc)
		w.a.keccak.(io.Reader).Read(cell.CodeHash[:])
	}
	return plainKey, nil
}

func (w *Writer) storageFn(plainKey []byte, cell *commitment.Cell) ([]byte, error) {
	var enc []byte
	var v []byte
	var err error
	if len(plainKey) != length.Addr+length.Hash {
		fileI := int(plainKey[0])
		offset := decodeU64(plainKey[1:])
		//fmt.Fprintf(os.Stderr, "storageFn, plainKey [%x], fileI %d, offset %d\n", plainKey, fileI, offset)
		plainKey, _ = readByOffset("storage", &w.a.storageFiles, fileI, offset)
		//fmt.Fprintf(os.Stderr, "retrived [%x]\n", plainKey)
	}
	// Look in the summary table first
	if v, err = w.tx.GetOne(kv.StateStorage, plainKey); err != nil {
		return nil, err
	}
	if v != nil {
		// First 4 bytes is the number of 1-block state diffs containing the key
		enc = v[4:]
	} else {
		// Look in the files
		enc = readFromFiles("storage", &w.a.storageFiles, nil /* lock */, w.blockNum, plainKey, false /* trace */)
	}
	cell.StorageLen = len(enc)
	copy(cell.Storage[:], enc)
	return plainKey, nil
}

func (w *Writer) captureCommitmentData(trace bool) {
	if trace {
		fmt.Printf("captureCommitmentData start w.commTree.Len()=%d\n", w.commTree.Len())
	}
	lastOffsetKey := 0
	lastOffsetVal := 0
	for i, offsetKey := range w.codeChanges.keys.wordOffsets {
		offsetVal := w.codeChanges.after.wordOffsets[i]
		key := w.codeChanges.keys.words[lastOffsetKey:offsetKey]
		val := w.codeChanges.after.words[lastOffsetVal:offsetVal]
		if len(val) > 0 {
			val = val[1:]
		}
		if trace {
			fmt.Printf("captureCommitmentData cod [%x]=>[%x]\n", key, val)
		}
		w.a.keccak.Reset()
		w.a.keccak.Write(key)
		hashedKey := w.a.keccak.Sum(nil)
		var c = &CommitmentItem{plainKey: common.Copy(key), hashedKey: make([]byte, len(hashedKey)*2)}
		for i, b := range hashedKey {
			c.hashedKey[i*2] = (b >> 4) & 0xf
			c.hashedKey[i*2+1] = b & 0xf
		}
		c.u.Flags = commitment.CODE_UPDATE
		item := w.commTree.Get(&CommitmentItem{hashedKey: c.hashedKey})
		if item != nil {
			itemC := item.(*CommitmentItem)
			if itemC.u.Flags&commitment.BALANCE_UPDATE != 0 {
				c.u.Flags |= commitment.BALANCE_UPDATE
				c.u.Balance.Set(&itemC.u.Balance)
			}
			if itemC.u.Flags&commitment.NONCE_UPDATE != 0 {
				c.u.Flags |= commitment.NONCE_UPDATE
				c.u.Nonce = itemC.u.Nonce
			}
			if itemC.u.Flags == commitment.DELETE_UPDATE && len(val) == 0 {
				c.u.Flags = commitment.DELETE_UPDATE
			} else {
				w.a.keccak.Reset()
				w.a.keccak.Write(val)
				w.a.keccak.(io.Reader).Read(c.u.CodeHashOrStorage[:])
			}
		} else {
			w.a.keccak.Reset()
			w.a.keccak.Write(val)
			w.a.keccak.(io.Reader).Read(c.u.CodeHashOrStorage[:])
		}
		w.commTree.ReplaceOrInsert(c)
		lastOffsetKey = offsetKey
		lastOffsetVal = offsetVal
	}
	lastOffsetKey = 0
	lastOffsetVal = 0
	for i, offsetKey := range w.accountChanges.keys.wordOffsets {
		offsetVal := w.accountChanges.after.wordOffsets[i]
		key := w.accountChanges.keys.words[lastOffsetKey:offsetKey]
		val := w.accountChanges.after.words[lastOffsetVal:offsetVal]
		if len(val) > 0 {
			val = val[1:]
		}
		if trace {
			fmt.Printf("captureCommitmentData acc [%x]=>[%x]\n", key, val)
		}
		w.a.keccak.Reset()
		w.a.keccak.Write(key)
		hashedKey := w.a.keccak.Sum(nil)
		var c = &CommitmentItem{plainKey: common.Copy(key), hashedKey: make([]byte, len(hashedKey)*2)}
		for i, b := range hashedKey {
			c.hashedKey[i*2] = (b >> 4) & 0xf
			c.hashedKey[i*2+1] = b & 0xf
		}
		if len(val) == 0 {
			c.u.Flags = commitment.DELETE_UPDATE
		} else {
			c.u.DecodeForStorage(val)
			c.u.Flags = commitment.BALANCE_UPDATE | commitment.NONCE_UPDATE
			item := w.commTree.Get(&CommitmentItem{hashedKey: c.hashedKey})
			if item != nil {
				itemC := item.(*CommitmentItem)
				if itemC.u.Flags&commitment.CODE_UPDATE != 0 {
					c.u.Flags |= commitment.CODE_UPDATE
					copy(c.u.CodeHashOrStorage[:], itemC.u.CodeHashOrStorage[:])
				}
			}
		}
		w.commTree.ReplaceOrInsert(c)
		lastOffsetKey = offsetKey
		lastOffsetVal = offsetVal
	}
	lastOffsetKey = 0
	lastOffsetVal = 0
	for i, offsetKey := range w.storageChanges.keys.wordOffsets {
		offsetVal := w.storageChanges.after.wordOffsets[i]
		key := w.storageChanges.keys.words[lastOffsetKey:offsetKey]
		val := w.storageChanges.after.words[lastOffsetVal:offsetVal]
		if len(val) > 0 {
			val = val[1:]
		}
		if trace {
			fmt.Printf("captureCommitmentData str [%x]=>[%x]\n", key, val)
		}
		hashedKey := make([]byte, 2*length.Hash)
		w.a.keccak.Reset()
		w.a.keccak.Write(key[:length.Addr])
		w.a.keccak.(io.Reader).Read(hashedKey[:length.Hash])
		w.a.keccak.Reset()
		w.a.keccak.Write(key[length.Addr:])
		w.a.keccak.(io.Reader).Read(hashedKey[length.Hash:])
		var c = &CommitmentItem{plainKey: common.Copy(key), hashedKey: make([]byte, len(hashedKey)*2)}
		for i, b := range hashedKey {
			c.hashedKey[i*2] = (b >> 4) & 0xf
			c.hashedKey[i*2+1] = b & 0xf
		}
		c.u.ValLength = len(val)
		if len(val) > 0 {
			copy(c.u.CodeHashOrStorage[:], val)
		}
		if len(val) == 0 {
			c.u.Flags = commitment.DELETE_UPDATE
		} else {
			c.u.Flags = commitment.STORAGE_UPDATE
		}
		w.commTree.ReplaceOrInsert(c)
		lastOffsetKey = offsetKey
		lastOffsetVal = offsetVal
	}
	if trace {
		fmt.Printf("captureCommitmentData end w.commTree.Len()=%d\n", w.commTree.Len())
	}
}

// computeCommitment is computing the commitment to the state after
// the change would have been applied.
// It assumes that the state accessible via the aggregator has already been
// modified with the new values
// At the moment, it is specific version for hex merkle patricia tree commitment
// but it will be extended to support other types of commitments
func (w *Writer) computeCommitment(trace bool) ([]byte, error) {
	if trace {
		fmt.Printf("computeCommitment w.commTree.Len()=%d\n", w.commTree.Len())
	}
	plainKeys := make([][]byte, w.commTree.Len())
	hashedKeys := make([][]byte, w.commTree.Len())
	updates := make([]commitment.Update, w.commTree.Len())
	j := 0
	w.commTree.Ascend(func(i btree.Item) bool {
		item := i.(*CommitmentItem)
		plainKeys[j] = item.plainKey
		hashedKeys[j] = item.hashedKey
		updates[j] = item.u
		j++
		return true
	})
	w.a.hph.Reset()
	w.a.hph.ResetFns(w.branchFn, w.accountFn, w.storageFn, w.lockFn, w.unlockFn)
	w.a.hph.SetTrace(trace)
	branchNodeUpdates, err := w.a.hph.ProcessUpdates(plainKeys, hashedKeys, updates)
	if err != nil {
		return nil, err
	}
	for prefixStr, branchNodeUpdate := range branchNodeUpdates {
		prefix := []byte(prefixStr)
		dbPrefix := prefix
		prevV, err := w.tx.GetOne(kv.StateCommitment, dbPrefix)
		if err != nil {
			return nil, err
		}
		var prevNum uint32
		var original []byte
		if prevV == nil {
			original = readFromFiles("commitment", &w.a.commitmentFiles, w.a.commFilesLock.RLocker(), w.blockNum, prefix, false)
		} else {
			prevNum = binary.BigEndian.Uint32(prevV[:4])
		}
		v := make([]byte, 4+len(branchNodeUpdate))
		binary.BigEndian.PutUint32(v[:4], prevNum+1)
		copy(v[4:], branchNodeUpdate)
		if err = w.tx.Put(kv.StateCommitment, dbPrefix, v); err != nil {
			return nil, err
		}
		if len(branchNodeUpdate) == 0 {
			w.commChanges.delete(prefix, original)
		} else {
			if prevV == nil && original == nil {
				w.commChanges.insert(prefix, branchNodeUpdate)
			} else {
				if original == nil {
					original = prevV[4:]
				}
				w.commChanges.update(prefix, original, branchNodeUpdate)
			}
		}
	}
	var rootHash []byte
	if rootHash, err = w.a.hph.RootHash(); err != nil {
		return nil, err
	}
	return rootHash, nil
}

func (w *Writer) FinishTx(txNum uint64, trace bool) error {
	w.captureCommitmentData(trace)
	var err error
	if err = w.accountChanges.finish(txNum); err != nil {
		return fmt.Errorf("finish accountChanges: %w", err)
	}
	if err = w.codeChanges.finish(txNum); err != nil {
		return fmt.Errorf("finish codeChanges: %w", err)
	}
	if err = w.storageChanges.finish(txNum); err != nil {
		return fmt.Errorf("finish storageChanges: %w", err)
	}
	return nil
}

func (w *Writer) ComputeCommitment(trace bool) ([]byte, error) {
	comm, err := w.computeCommitment(trace)
	if err != nil {
		return nil, fmt.Errorf("compute commitment: %w", err)
	}
	w.commTree.Clear(true)
	if err = w.commChanges.finish(w.blockNum); err != nil {
		return nil, fmt.Errorf("finish commChanges: %w", err)
	}
	return comm, nil
}

// Aggegate should be called to check if the aggregation is required, and
// if it is required, perform it
func (w *Writer) Aggregate(trace bool) error {
	if w.blockNum < w.a.unwindLimit+w.a.aggregationStep-1 {
		return nil
	}
	diff := w.blockNum - w.a.unwindLimit
	if (diff+1)%w.a.aggregationStep != 0 {
		return nil
	}
	if err := w.aggregateUpto(diff+1-w.a.aggregationStep, diff); err != nil {
		return fmt.Errorf("aggregateUpto(%d, %d): %w", diff+1-w.a.aggregationStep, diff, err)
	}
	return nil
}

func (w *Writer) UpdateAccountData(addr []byte, account []byte, trace bool) error {
	prevV, err := w.tx.GetOne(kv.StateAccounts, addr)
	if err != nil {
		return err
	}
	var prevNum uint32
	var original []byte
	if prevV == nil {
		original = readFromFiles("accounts", &w.a.accountsFiles, w.a.accountsFilesLock.RLocker(), w.blockNum, addr, trace)
	} else {
		prevNum = binary.BigEndian.Uint32(prevV[:4])
		original = prevV[4:]
	}
	if bytes.Equal(account, original) {
		// No change
		return nil
	}
	v := make([]byte, 4+len(account))
	binary.BigEndian.PutUint32(v[:4], prevNum+1)
	copy(v[4:], account)
	if err = w.tx.Put(kv.StateAccounts, addr, v); err != nil {
		return err
	}
	if prevV == nil && len(original) == 0 {
		w.accountChanges.insert(addr, account)
	} else {
		w.accountChanges.update(addr, original, account)
	}
	if trace {
		w.a.trace = true
		w.a.tracedKeys[string(addr)] = struct{}{}
	}
	return nil
}

func (w *Writer) UpdateAccountCode(addr []byte, code []byte, trace bool) error {
	prevV, err := w.tx.GetOne(kv.StateCode, addr)
	if err != nil {
		return err
	}
	var prevNum uint32
	var original []byte
	if prevV == nil {
		original = readFromFiles("code", &w.a.codeFiles, w.a.codeFilesLock.RLocker(), w.blockNum, addr, trace)
	} else {
		prevNum = binary.BigEndian.Uint32(prevV[:4])
	}
	v := make([]byte, 4+len(code))
	binary.BigEndian.PutUint32(v[:4], prevNum+1)
	copy(v[4:], code)
	if err = w.tx.Put(kv.StateCode, addr, v); err != nil {
		return err
	}
	if prevV == nil && original == nil {
		w.codeChanges.insert(addr, code)
	} else {
		if original == nil {
			original = prevV[4:]
		}
		w.codeChanges.update(addr, original, code)
	}
	if trace {
		w.a.trace = true
		w.a.tracedKeys[string(addr)] = struct{}{}
	}
	return nil
}

type CursorType uint8

const (
	FILE_CURSOR CursorType = iota
	DB_CURSOR
	TREE_CURSOR
)

// CursorItem is the item in the priority queue used to do merge interation
// over storage of a given account
type CursorItem struct {
	t        CursorType // Whether this item represents state file or DB record, or tree
	endBlock uint64
	key, val []byte
	dg       *compress.Getter
	c        kv.Cursor
	tree     *btree.BTree
}

type CursorHeap []*CursorItem

func (ch CursorHeap) Len() int {
	return len(ch)
}

func (ch CursorHeap) Less(i, j int) bool {
	cmp := bytes.Compare(ch[i].key, ch[j].key)
	if cmp == 0 {
		// when keys match, the items with later blocks are preferred
		return ch[i].endBlock > ch[j].endBlock
	}
	return cmp < 0
}

func (ch *CursorHeap) Swap(i, j int) {
	(*ch)[i], (*ch)[j] = (*ch)[j], (*ch)[i]
}

func (ch *CursorHeap) Push(x interface{}) {
	*ch = append(*ch, x.(*CursorItem))
}

func (ch *CursorHeap) Pop() interface{} {
	old := *ch
	n := len(old)
	x := old[n-1]
	*ch = old[0 : n-1]
	return x
}

func (w *Writer) deleteAccount(addr []byte, trace bool) (bool, error) {
	prevV, err := w.tx.GetOne(kv.StateAccounts, addr)
	if err != nil {
		return false, err
	}
	var prevNum uint32
	var original []byte
	if prevV == nil {
		original = readFromFiles("accounts", &w.a.accountsFiles, w.a.accountsFilesLock.RLocker(), w.blockNum, addr, trace)
		if original == nil {
			return false, nil
		}
	} else {
		prevNum = binary.BigEndian.Uint32(prevV[:4])
		original = prevV[4:]
	}
	v := make([]byte, 4)
	binary.BigEndian.PutUint32(v[:4], prevNum+1)
	if err = w.tx.Put(kv.StateAccounts, addr, v); err != nil {
		return false, err
	}
	w.accountChanges.delete(addr, original)
	return true, nil
}

func (w *Writer) deleteCode(addr []byte, trace bool) error {
	prevV, err := w.tx.GetOne(kv.StateCode, addr)
	if err != nil {
		return err
	}
	var prevNum uint32
	var original []byte
	if prevV == nil {
		original = readFromFiles("code", &w.a.codeFiles, w.a.codeFilesLock.RLocker(), w.blockNum, addr, trace)
		if original == nil {
			// Nothing to do
			return nil
		}
	} else {
		prevNum = binary.BigEndian.Uint32(prevV[:4])
		original = prevV[4:]
	}
	v := make([]byte, 4)
	binary.BigEndian.PutUint32(v[:4], prevNum+1)
	if err = w.tx.Put(kv.StateCode, addr, v); err != nil {
		return err
	}
	w.codeChanges.delete(addr, original)
	return nil
}

func (w *Writer) DeleteAccount(addr []byte, trace bool) error {
	if deleted, err := w.deleteAccount(addr, trace); err != nil {
		return err
	} else if !deleted {
		return nil
	}
	if err := w.deleteCode(addr, trace); err != nil {
		return err
	}
	// Find all storage items for this address
	var cp CursorHeap
	heap.Init(&cp)
	c, err := w.tx.Cursor(kv.StateStorage)
	if err != nil {
		return err
	}
	var k, v []byte
	if k, v, err = c.Seek(addr); err != nil {
		return err
	}
	if k != nil && bytes.HasPrefix(k, addr) {
		heap.Push(&cp, &CursorItem{t: DB_CURSOR, key: common.Copy(k), val: common.Copy(v), c: c, endBlock: w.blockNum})
	}
	w.a.storageFiles.Ascend(func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if item.tree != nil {
			item.tree.AscendGreaterOrEqual(&AggregateItem{k: addr}, func(ai btree.Item) bool {
				aitem := ai.(*AggregateItem)
				if !bytes.HasPrefix(aitem.k, addr) {
					return false
				}
				if len(aitem.k) == len(addr) {
					return true
				}
				heap.Push(&cp, &CursorItem{t: TREE_CURSOR, key: aitem.k, val: aitem.v, tree: item.tree, endBlock: item.endBlock})
				return false
			})
			return true
		}
		if item.index.Empty() {
			return true
		}
		reader := recsplit.NewIndexReader(item.index)
		offset := reader.Lookup(addr)
		g := item.decompressor.MakeGetter() // TODO Cache in the reader
		g.Reset(offset)
		if g.HasNext() {
			if keyMatch, _ := g.Match(addr); !keyMatch {
				return true
			}
			g.Skip()
		}
		if g.HasNext() {
			key, _ := g.Next(nil)
			if bytes.HasPrefix(key, addr) {
				val, _ := g.Next(nil)
				heap.Push(&cp, &CursorItem{t: FILE_CURSOR, key: key, val: val, dg: g, endBlock: item.endBlock})
			}
		}
		return true
	})
	for cp.Len() > 0 {
		lastKey := common.Copy(cp[0].key)
		lastVal := common.Copy(cp[0].val)
		// Advance all the items that have this key (including the top)
		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
			ci1 := cp[0]
			switch ci1.t {
			case FILE_CURSOR:
				if ci1.dg.HasNext() {
					ci1.key, _ = ci1.dg.Next(ci1.key[:0])
					if bytes.HasPrefix(ci1.key, addr) {
						ci1.val, _ = ci1.dg.Next(ci1.val[:0])
						heap.Fix(&cp, 0)
					} else {
						heap.Pop(&cp)
					}
				} else {
					heap.Pop(&cp)
				}
			case DB_CURSOR:
				k, v, err = ci1.c.Next()
				if err != nil {
					return err
				}
				if k != nil && bytes.HasPrefix(k, addr) {
					ci1.key = common.Copy(k)
					ci1.val = common.Copy(v)
					heap.Fix(&cp, 0)
				} else {
					heap.Pop(&cp)
				}
			case TREE_CURSOR:
				skip := true
				var aitem *AggregateItem
				ci1.tree.AscendGreaterOrEqual(&AggregateItem{k: ci1.key}, func(ai btree.Item) bool {
					if skip {
						skip = false
						return true
					}
					aitem = ai.(*AggregateItem)
					return false
				})
				if aitem != nil && bytes.HasPrefix(aitem.k, addr) {
					ci1.key = aitem.k
					ci1.val = aitem.v
					heap.Fix(&cp, 0)
				} else {
					heap.Pop(&cp)
				}
			}
		}
		var prevV []byte
		prevV, err = w.tx.GetOne(kv.StateStorage, lastKey)
		if err != nil {
			return err
		}
		var prevNum uint32
		if prevV != nil {
			prevNum = binary.BigEndian.Uint32(prevV[:4])
		}
		v = make([]byte, 4)
		binary.BigEndian.PutUint32(v[:4], prevNum+1)
		if err = w.tx.Put(kv.StateStorage, lastKey, v); err != nil {
			return err
		}
		w.storageChanges.delete(lastKey, lastVal)
	}
	if trace {
		w.a.trace = true
		w.a.tracedKeys[string(addr)] = struct{}{}
	}
	return nil
}

func (w *Writer) WriteAccountStorage(addr []byte, loc []byte, value *uint256.Int, trace bool) error {
	dbkey := make([]byte, len(addr)+len(loc))
	copy(dbkey[0:], addr)
	copy(dbkey[len(addr):], loc)
	prevV, err := w.tx.GetOne(kv.StateStorage, dbkey)
	if err != nil {
		return err
	}
	var prevNum uint32
	var original []byte
	if prevV == nil {
		original = readFromFiles("storage", &w.a.storageFiles, w.a.storageFilesLock.RLocker(), w.blockNum, dbkey, trace)
	} else {
		prevNum = binary.BigEndian.Uint32(prevV[:4])
		original = prevV[4:]
	}
	vLen := value.ByteLen()
	v := make([]byte, 4+vLen)
	binary.BigEndian.PutUint32(v[:4], prevNum+1)
	value.WriteToSlice(v[4:])
	if bytes.Equal(v[4:], original) {
		// No change
		return nil
	}
	if err = w.tx.Put(kv.StateStorage, dbkey, v); err != nil {
		return err
	}
	if prevV == nil && len(original) == 0 {
		w.storageChanges.insert(dbkey, v[4:])
	} else {
		w.storageChanges.update(dbkey, original, v[4:])
	}
	if trace {
		w.a.trace = true
		w.a.tracedKeys[string(dbkey)] = struct{}{}
	}
	return nil
}

func findLargestMerge(tree **btree.BTree, lock sync.Locker, maxTo uint64) (toAggregate []*byEndBlockItem, pre []*byEndBlockItem, post []*byEndBlockItem, aggFrom uint64, aggTo uint64) {
	lock.Lock()
	defer lock.Unlock()
	var maxEndBlock uint64
	(*tree).DescendLessOrEqual(&byEndBlockItem{endBlock: maxTo}, func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if item.decompressor == nil {
			return true
		}
		maxEndBlock = item.endBlock
		return false
	})
	if maxEndBlock == 0 {
		return
	}
	(*tree).Ascend(func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if item.decompressor == nil {
			return true // Skip B-tree based items
		}
		pre = append(pre, item)
		if aggTo == 0 {
			doubleEnd := item.endBlock + (item.endBlock - item.startBlock) + 1
			if doubleEnd <= maxEndBlock {
				aggFrom = item.startBlock
				aggTo = doubleEnd
			} else {
				post = append(post, item)
				return true
			}
		}
		toAggregate = append(toAggregate, item)
		return item.endBlock < aggTo
	})
	return
}

func (a *Aggregator) computeAggregation(treeName string, toAggregate []*byEndBlockItem, aggFrom uint64, aggTo uint64, valTransform func(val []byte, transValBuf []byte) ([]byte, error)) (*byEndBlockItem, error) {
	var item2 = &byEndBlockItem{startBlock: aggFrom, endBlock: aggTo}
	var cp CursorHeap
	heap.Init(&cp)
	for _, ag := range toAggregate {
		g := ag.decompressor.MakeGetter()
		if g.HasNext() {
			key, _ := g.Next(nil)
			val, _ := g.Next(nil)
			heap.Push(&cp, &CursorItem{t: FILE_CURSOR, dg: g, key: key, val: val, endBlock: ag.endBlock})
		}
	}
	var err error
	if item2.decompressor, item2.index, err = a.mergeIntoStateFile(&cp, 0, treeName, aggFrom, aggTo, a.diffDir, valTransform); err != nil {
		return nil, fmt.Errorf("mergeIntoStateFile %s [%d-%d]: %w", treeName, aggFrom, aggTo, err)
	}
	return item2, nil
}

func createDatAndIndex(treeName string, diffDir string, bt *btree.BTree, blockFrom uint64, blockTo uint64) (*compress.Decompressor, *recsplit.Index, error) {
	datPath := path.Join(diffDir, fmt.Sprintf("%s.%d-%d.dat", treeName, blockFrom, blockTo))
	idxPath := path.Join(diffDir, fmt.Sprintf("%s.%d-%d.idx", treeName, blockFrom, blockTo))
	count, err := btreeToFile(bt, datPath, diffDir, false /* trace */, 1 /* workers */)
	if err != nil {
		return nil, nil, fmt.Errorf("btreeToFile: %w", err)
	}
	return buildIndex(datPath, idxPath, diffDir, count)
}

func addLocked(tree **btree.BTree, item *byEndBlockItem, lock sync.Locker) {
	lock.Lock()
	defer lock.Unlock()
	(*tree).ReplaceOrInsert(item)
}

func (w *Writer) aggregateUpto(blockFrom, blockTo uint64) error {
	// React on any previous error of aggregation or merge
	select {
	case err := <-w.a.aggError:
		return err
	case err := <-w.a.mergeError:
		return err
	default:
	}
	t := time.Now()
	i := w.a.changesBtree.Get(&ChangesItem{startBlock: blockFrom, endBlock: blockTo})
	if i == nil {
		return fmt.Errorf("did not find change files for [%d-%d], w.a.changesBtree.Len() = %d", blockFrom, blockTo, w.a.changesBtree.Len())
	}
	item := i.(*ChangesItem)
	if item.startBlock != blockFrom {
		return fmt.Errorf("expected change files[%d-%d], got [%d-%d]", blockFrom, blockTo, item.startBlock, item.endBlock)
	}
	w.a.changesBtree.Delete(i)
	var accountChanges, codeChanges, storageChanges, commChanges Changes
	accountChanges.Init("accounts", w.a.aggregationStep, w.a.diffDir, false /* beforeOn */)
	codeChanges.Init("code", w.a.aggregationStep, w.a.diffDir, false /* beforeOn */)
	storageChanges.Init("storage", w.a.aggregationStep, w.a.diffDir, false /* beforeOn */)
	commChanges.Init("commitment", w.a.aggregationStep, w.a.diffDir, false /* beforeOn */)
	var err error
	var accountsBt *btree.BTree
	if accountsBt, err = accountChanges.aggregate(blockFrom, blockTo, 0, w.tx, kv.StateAccounts); err != nil {
		return fmt.Errorf("aggregate accountsChanges: %w", err)
	}
	var codeBt *btree.BTree
	if codeBt, err = codeChanges.aggregate(blockFrom, blockTo, 0, w.tx, kv.StateCode); err != nil {
		return fmt.Errorf("aggregate codeChanges: %w", err)
	}
	var storageBt *btree.BTree
	if storageBt, err = storageChanges.aggregate(blockFrom, blockTo, 20, w.tx, kv.StateStorage); err != nil {
		return fmt.Errorf("aggregate storageChanges: %w", err)
	}
	var commitmentBt *btree.BTree
	if commitmentBt, err = commChanges.aggregate(blockFrom, blockTo, 0, w.tx, kv.StateCommitment); err != nil {
		return fmt.Errorf("aggregate commitmentChanges: %w", err)
	}
	aggTime := time.Since(t)
	t = time.Now()
	// At this point, all the changes are gathered in 4 B-trees (accounts, code, storage and commitment) and removed from the database
	// What follows can be done in the 1st background goroutine (TODO)
	w.a.aggChannel <- AggregationTask{
		accountChanges: &accountChanges,
		accountsBt:     accountsBt,
		codeChanges:    &codeChanges,
		codeBt:         codeBt,
		storageChanges: &storageChanges,
		storageBt:      storageBt,
		commChanges:    &commChanges,
		commitmentBt:   commitmentBt,
		blockFrom:      blockFrom,
		blockTo:        blockTo,
	}
	<-w.a.aggBackCh // Waiting for the B-tree based items have been added
	handoverTime := time.Since(t)
	if handoverTime > time.Millisecond {
		log.Info("Long handover to background aggregation", "from", blockFrom, "to", blockTo, "composition", aggTime, "handover", time.Since(t))
	}
	return nil
}

// mergeIntoStateFile assumes that all entries in the cp heap have type FILE_CURSOR
func (a *Aggregator) mergeIntoStateFile(cp *CursorHeap, prefixLen int, basename string, startBlock, endBlock uint64, dir string, valTransform func(val []byte, transValBuf []byte) ([]byte, error)) (*compress.Decompressor, *recsplit.Index, error) {
	datPath := path.Join(dir, fmt.Sprintf("%s.%d-%d.dat", basename, startBlock, endBlock))
	idxPath := path.Join(dir, fmt.Sprintf("%s.%d-%d.idx", basename, startBlock, endBlock))
	comp, err := compress.NewCompressor(context.Background(), AggregatorPrefix, datPath, dir, compress.MinPatternScore, 1)
	if err != nil {
		return nil, nil, fmt.Errorf("compressor %s: %w", datPath, err)
	}
	defer comp.Close()
	count := 0
	var keyBuf, valBuf, transValBuf []byte
	for cp.Len() > 0 {
		lastKey := common.Copy((*cp)[0].key)
		lastVal := common.Copy((*cp)[0].val)
		if a.trace {
			if _, ok := a.tracedKeys[string(lastKey)]; ok {
				fmt.Printf("looking at key %x val [%x] endBlock %d to merge into [%d-%d]\n", lastKey, lastVal, (*cp)[0].endBlock, startBlock, endBlock)
			}
		}
		var first, firstDelete, firstInsert bool
		// Advance all the items that have this key (including the top)
		for cp.Len() > 0 && bytes.Equal((*cp)[0].key, lastKey) {
			ci1 := (*cp)[0]
			if a.trace {
				if _, ok := a.tracedKeys[string(ci1.key)]; ok {
					fmt.Printf("skipping same key %x val [%x] endBlock %d to merge into [%d-%d]\n", ci1.key, ci1.val, ci1.endBlock, startBlock, endBlock)
				}
			}
			if ci1.t != FILE_CURSOR {
				return nil, nil, fmt.Errorf("mergeIntoStateFile: cursor of unexpected type: %d", ci1.t)
			}
			first = true
			firstDelete = len(ci1.val) == 0
			firstInsert = !firstDelete && ci1.val[0] != 0
			if ci1.dg.HasNext() {
				ci1.key, _ = ci1.dg.Next(ci1.key[:0])
				ci1.val, _ = ci1.dg.Next(ci1.val[:0])
				heap.Fix(cp, 0)
			} else {
				heap.Pop(cp)
			}
		}
		lastDelete := len(lastVal) == 0
		lastInsert := !lastDelete && lastVal[0] != 0
		var skip bool
		if first {
			if firstInsert {
				if lastDelete {
					// Insert => Delete
					skip = true
				}
			} else if firstDelete {
				if lastInsert {
					// Delete => Insert equivalent to Update
					lastVal[0] = 0
				}
			} else {
				if lastInsert {
					// Update => Insert equivalent to Update
					lastVal[0] = 0
				}
			}
		}
		if startBlock != 0 || !skip {
			if keyBuf != nil && (prefixLen == 0 || len(keyBuf) != prefixLen || bytes.HasPrefix(lastKey, keyBuf)) {
				if err = comp.AddWord(keyBuf); err != nil {
					return nil, nil, err
				}
				if a.trace {
					if _, ok := a.tracedKeys[string(keyBuf)]; ok {
						fmt.Printf("merge key %x val [%x] into [%d-%d]\n", keyBuf, valBuf, startBlock, endBlock)
					}
				}
				count++ // Only counting keys, not values
				if valTransform != nil {
					if transValBuf, err = valTransform(valBuf, transValBuf[:0]); err != nil {
						return nil, nil, fmt.Errorf("mergeIntoStateFile valTransform [%x]: %w", valBuf, err)
					}
					if err = comp.AddWord(transValBuf); err != nil {
						return nil, nil, err
					}
				} else if err = comp.AddWord(valBuf); err != nil {
					return nil, nil, err
				}
			}
			keyBuf = append(keyBuf[:0], lastKey...)
			valBuf = append(valBuf[:0], lastVal...)
		} else if a.trace {
			if _, ok := a.tracedKeys[string(keyBuf)]; ok {
				fmt.Printf("skipped key %x for [%d-%d]\n", keyBuf, startBlock, endBlock)
			}
		}
	}
	if keyBuf != nil {
		if err = comp.AddWord(keyBuf); err != nil {
			return nil, nil, err
		}
		if a.trace {
			if _, ok := a.tracedKeys[string(keyBuf)]; ok {
				fmt.Printf("merge key %x val [%x] into [%d-%d]\n", keyBuf, valBuf, startBlock, endBlock)
			}
		}
		count++ // Only counting keys, not values
		if valTransform != nil {
			if transValBuf, err = valTransform(valBuf, transValBuf[:0]); err != nil {
				return nil, nil, fmt.Errorf("mergeIntoStateFile valTransform [%x]: %w", valBuf, err)
			}
			if err = comp.AddWord(transValBuf); err != nil {
				return nil, nil, err
			}
		} else if err = comp.AddWord(valBuf); err != nil {
			return nil, nil, err
		}
	}
	if err = comp.Compress(); err != nil {
		return nil, nil, err
	}
	var d *compress.Decompressor
	var idx *recsplit.Index
	if d, idx, err = buildIndex(datPath, idxPath, dir, count); err != nil {
		return nil, nil, fmt.Errorf("build index: %w", err)
	}
	return d, idx, nil
}

func stats(tree **btree.BTree, lock sync.Locker) (count int, datSize, idxSize int64) {
	lock.Lock()
	defer lock.Unlock()
	count = 0
	datSize = 0
	idxSize = 0
	(*tree).Ascend(func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if item.decompressor != nil {
			count++
			datSize += item.decompressor.Size()
			count++
			idxSize += item.index.Size()
		}
		return true
	})
	return
}

type FilesStats struct {
	AccountsCount     int
	AccountsDatSize   int64
	AccountsIdxSize   int64
	CodeCount         int
	CodeDatSize       int64
	CodeIdxSize       int64
	StorageCount      int
	StorageDatSize    int64
	StorageIdxSize    int64
	CommitmentCount   int
	CommitmentDatSize int64
	CommitmentIdxSize int64
}

func (a *Aggregator) Stats() FilesStats {
	var fs FilesStats
	fs.AccountsCount, fs.AccountsDatSize, fs.AccountsIdxSize = stats(&a.accountsFiles, a.accountsFilesLock.RLocker())
	fs.CodeCount, fs.CodeDatSize, fs.CodeIdxSize = stats(&a.codeFiles, a.codeFilesLock.RLocker())
	fs.StorageCount, fs.StorageDatSize, fs.StorageIdxSize = stats(&a.storageFiles, a.storageFilesLock.RLocker())
	fs.CommitmentCount, fs.CommitmentDatSize, fs.CommitmentIdxSize = stats(&a.commitmentFiles, a.commFilesLock.RLocker())
	return fs
}
