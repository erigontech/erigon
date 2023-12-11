/*
   Copyright 2021 Erigon contributors

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
	"encoding/binary"
	"fmt"

	"github.com/google/btree"
	"golang.org/x/crypto/sha3"
	"golang.org/x/exp/slices"

	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cryptozerocopy"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types"
)

// Defines how to evaluate commitments
type CommitmentMode uint

const (
	CommitmentModeDisabled CommitmentMode = 0
	CommitmentModeDirect   CommitmentMode = 1
	CommitmentModeUpdate   CommitmentMode = 2
)

func (m CommitmentMode) String() string {
	switch m {
	case CommitmentModeDisabled:
		return "disabled"
	case CommitmentModeDirect:
		return "direct"
	case CommitmentModeUpdate:
		return "update"
	default:
		return "unknown"
	}
}

func ParseCommitmentMode(s string) CommitmentMode {
	var mode CommitmentMode
	switch s {
	case "off":
		mode = CommitmentModeDisabled
	case "update":
		mode = CommitmentModeUpdate
	default:
		mode = CommitmentModeDirect
	}
	return mode
}

type ValueMerger func(prev, current []byte) (merged []byte, err error)

type UpdateTree struct {
	tree   *btree.BTreeG[*commitmentItem]
	keccak cryptozerocopy.KeccakState
	keys   map[string]struct{}
	mode   CommitmentMode
}

func NewUpdateTree(m CommitmentMode) *UpdateTree {
	return &UpdateTree{
		tree:   btree.NewG[*commitmentItem](64, commitmentItemLessPlain),
		keccak: sha3.NewLegacyKeccak256().(cryptozerocopy.KeccakState),
		keys:   map[string]struct{}{},
		mode:   m,
	}
}

func (t *UpdateTree) get(key []byte) (*commitmentItem, bool) {
	c := &commitmentItem{plainKey: key, update: commitment.Update{CodeHashOrStorage: commitment.EmptyCodeHashArray}}
	el, ok := t.tree.Get(c)
	if ok {
		return el, true
	}
	c.plainKey = common.Copy(c.plainKey)
	return c, false
}

// TouchPlainKey marks plainKey as updated and applies different fn for different key types
// (different behaviour for Code, Account and Storage key modifications).
func (t *UpdateTree) TouchPlainKey(key string, val []byte, fn func(c *commitmentItem, val []byte)) {
	switch t.mode {
	case CommitmentModeUpdate:
		item, _ := t.get([]byte(key))
		fn(item, val)
		t.tree.ReplaceOrInsert(item)
	case CommitmentModeDirect:
		t.keys[key] = struct{}{}
	default:
	}
}

func (t *UpdateTree) Size() uint64 {
	return uint64(len(t.keys))
}

func (t *UpdateTree) TouchAccount(c *commitmentItem, val []byte) {
	if len(val) == 0 {
		c.update.Flags = commitment.DeleteUpdate
		return
	}
	if c.update.Flags&commitment.DeleteUpdate != 0 {
		c.update.Flags ^= commitment.DeleteUpdate
	}
	nonce, balance, chash := types.DecodeAccountBytesV3(val)
	if c.update.Nonce != nonce {
		c.update.Nonce = nonce
		c.update.Flags |= commitment.NonceUpdate
	}
	if !c.update.Balance.Eq(balance) {
		c.update.Balance.Set(balance)
		c.update.Flags |= commitment.BalanceUpdate
	}
	if !bytes.Equal(chash, c.update.CodeHashOrStorage[:]) {
		if len(chash) == 0 {
			c.update.ValLength = length.Hash
			copy(c.update.CodeHashOrStorage[:], commitment.EmptyCodeHash)
		} else {
			copy(c.update.CodeHashOrStorage[:], chash)
			c.update.ValLength = length.Hash
			c.update.Flags |= commitment.CodeUpdate
		}
	}
}

func (t *UpdateTree) UpdatePrefix(prefix, val []byte, fn func(c *commitmentItem, val []byte)) {
	t.tree.AscendGreaterOrEqual(&commitmentItem{}, func(item *commitmentItem) bool {
		if !bytes.HasPrefix(item.plainKey, prefix) {
			return false
		}
		fn(item, val)
		return true
	})
}

func (t *UpdateTree) TouchStorage(c *commitmentItem, val []byte) {
	c.update.ValLength = len(val)
	if len(val) == 0 {
		c.update.Flags = commitment.DeleteUpdate
	} else {
		c.update.Flags |= commitment.StorageUpdate
		copy(c.update.CodeHashOrStorage[:], val)
	}
}

func (t *UpdateTree) TouchCode(c *commitmentItem, val []byte) {
	t.keccak.Reset()
	t.keccak.Write(val)
	t.keccak.Read(c.update.CodeHashOrStorage[:])
	if c.update.Flags == commitment.DeleteUpdate && len(val) == 0 {
		c.update.Flags = commitment.DeleteUpdate
		c.update.ValLength = 0
		return
	}
	c.update.ValLength = length.Hash
	if len(val) != 0 {
		c.update.Flags |= commitment.CodeUpdate
	}
}

// Returns list of both plain and hashed keys. If .mode is CommitmentModeUpdate, updates also returned.
// No ordering guarantees is provided.
func (t *UpdateTree) List(clear bool) ([][]byte, []commitment.Update) {
	switch t.mode {
	case CommitmentModeDirect:
		plainKeys := make([][]byte, len(t.keys))
		i := 0
		for key := range t.keys {
			plainKeys[i] = []byte(key)
			i++
		}
		slices.SortFunc(plainKeys, func(i, j []byte) int { return bytes.Compare(i, j) })
		if clear {
			t.keys = make(map[string]struct{}, len(t.keys)/8)
		}

		return plainKeys, nil
	case CommitmentModeUpdate:
		plainKeys := make([][]byte, t.tree.Len())
		updates := make([]commitment.Update, t.tree.Len())
		i := 0
		t.tree.Ascend(func(item *commitmentItem) bool {
			plainKeys[i], updates[i] = item.plainKey, item.update
			i++
			return true
		})
		if clear {
			t.tree.Clear(true)
		}
		return plainKeys, updates
	default:
		return nil, nil
	}
}

type commitmentState struct {
	txNum     uint64
	blockNum  uint64
	trieState []byte
}

func (cs *commitmentState) Decode(buf []byte) error {
	if len(buf) < 10 {
		return fmt.Errorf("ivalid commitment state buffer size %d, expected at least 10b", len(buf))
	}
	pos := 0
	cs.txNum = binary.BigEndian.Uint64(buf[pos : pos+8])
	pos += 8
	cs.blockNum = binary.BigEndian.Uint64(buf[pos : pos+8])
	pos += 8
	cs.trieState = make([]byte, binary.BigEndian.Uint16(buf[pos:pos+2]))
	pos += 2
	if len(cs.trieState) == 0 && len(buf) == 10 {
		return nil
	}
	copy(cs.trieState, buf[pos:pos+len(cs.trieState)])
	return nil
}

func (cs *commitmentState) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	var v [18]byte
	binary.BigEndian.PutUint64(v[:], cs.txNum)
	binary.BigEndian.PutUint64(v[8:16], cs.blockNum)
	binary.BigEndian.PutUint16(v[16:18], uint16(len(cs.trieState)))
	if _, err := buf.Write(v[:]); err != nil {
		return nil, err
	}
	if _, err := buf.Write(cs.trieState); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// nolint
func decodeU64(from []byte) uint64 {
	var i uint64
	for _, b := range from {
		i = (i << 8) | uint64(b)
	}
	return i
}

// nolint
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

// Optimised key referencing a state file record (file number and offset within the file)
// nolint
func shortenedKey(apk []byte) (step uint16, offset uint64) {
	step = binary.BigEndian.Uint16(apk[:2])
	return step, decodeU64(apk[1:])
}

// nolint
func encodeShortenedKey(buf []byte, step uint16, offset uint64) []byte {
	binary.BigEndian.PutUint16(buf[:2], step)
	encodeU64(offset, buf[2:])
	return buf
}

type commitmentItem struct {
	plainKey []byte
	update   commitment.Update
}

func commitmentItemLessPlain(i, j *commitmentItem) bool {
	return bytes.Compare(i.plainKey, j.plainKey) < 0
}

//type DomainCommitted struct {
//	*Domain
//	trace        bool
//	shortenKeys  bool
//	updates      *UpdateTree
//	mode         CommitmentMode
//	patriciaTrie commitment.Trie
//	justRestored atomic.Bool
//	discard      bool
//}

// nolint
//
//	func (d *DomainCommitted) findShortenKey(fullKey []byte, list ...*filesItem) (shortened []byte, found bool) {
//		shortened = make([]byte, 2, 10)
//
//		//dc := d.MakeContext()
//		//defer dc.Close()
//
//		for _, item := range list {
//			g := NewArchiveGetter(item.decompressor.MakeGetter(), d.compression)
//			//index := recsplit.NewIndexReader(item.index) // TODO is support recsplt is needed?
//			// TODO: existence filter existence should be checked for domain which filesItem list is provided, not in commitmnet
//			//if d.withExistenceIndex && item.existence != nil {
//			//	hi, _ := dc.hc.ic.hashKey(fullKey)
//			//	if !item.existence.ContainsHash(hi) {
//			//		continue
//			//		//return nil, false, nil
//			//	}
//			//}
//
//			cur, err := item.bindex.Seek(g, fullKey)
//			if err != nil {
//				d.logger.Warn("commitment branch key replacement seek failed", "key", fmt.Sprintf("%x", fullKey), "err", err, "file", item.decompressor.FileName())
//				continue
//			}
//			if cur == nil {
//				continue
//			}
//			step := uint16(item.endTxNum / d.aggregationStep)
//			shortened = encodeShortenedKey(shortened[:], step, cur.Di())
//			if d.trace {
//				fmt.Printf("replacing [%x] => {%x} step=%d, di=%d file=%s\n", fullKey, shortened, step, cur.Di(), item.decompressor.FileName())
//			}
//			found = true
//			break
//		}
//		//if !found {
//		//	d.logger.Warn("failed to find key reference", "key", fmt.Sprintf("%x", fullKey))
//		//}
//		return shortened, found
//	}
//
// // nolint
//
//	func (d *DomainCommitted) lookupByShortenedKey(shortKey []byte, list []*filesItem) (fullKey []byte, found bool) {
//		fileStep, offset := shortenedKey(shortKey)
//		expected := uint64(fileStep) * d.aggregationStep
//
//		for _, item := range list {
//			if item.startTxNum > expected || item.endTxNum < expected {
//				continue
//			}
//
//			g := NewArchiveGetter(item.decompressor.MakeGetter(), d.compression)
//			fullKey, _, err := item.bindex.dataLookup(offset, g)
//			if err != nil {
//				return nil, false
//			}
//			if d.trace {
//				fmt.Printf("shortenedKey [%x]=>{%x} step=%d offset=%d, file=%s\n", shortKey, fullKey, fileStep, offset, item.decompressor.FileName())
//			}
//			found = true
//			break
//		}
//		return fullKey, found
//	}
//
// // commitmentValTransform parses the value of the commitment record to extract references
// // to accounts and storage items, then looks them up in the new, merged files, and replaces them with
// // the updated references
//
//	func (d *DomainCommitted) commitmentValTransform(files *SelectedStaticFiles, merged *MergedFiles, val commitment.BranchData) ([]byte, error) {
//		if !d.shortenKeys || len(val) == 0 {
//			return val, nil
//		}
//		var transValBuf []byte
//		defer func(t time.Time) {
//			d.logger.Info("commitmentValTransform", "took", time.Since(t), "in_size", len(val), "out_size", len(transValBuf), "ratio", float64(len(transValBuf))/float64(len(val)))
//		}(time.Now())
//
//		accountPlainKeys, storagePlainKeys, err := val.ExtractPlainKeys()
//		if err != nil {
//			return nil, err
//		}
//
//		transAccountPks := make([][]byte, 0, len(accountPlainKeys))
//		var apkBuf, spkBuf []byte
//		var found bool
//		for _, accountPlainKey := range accountPlainKeys {
//			if len(accountPlainKey) == length.Addr {
//				// Non-optimised key originating from a database record
//				apkBuf = append(apkBuf[:0], accountPlainKey...)
//			} else {
//				var found bool
//				apkBuf, found = d.lookupByShortenedKey(accountPlainKey, files.accounts)
//				if !found {
//					d.logger.Crit("lost account full key", "shortened", fmt.Sprintf("%x", accountPlainKey))
//				}
//			}
//			accountPlainKey, found = d.findShortenKey(apkBuf, merged.accounts)
//			if !found {
//				d.logger.Crit("replacement for full account key was not found", "shortened", fmt.Sprintf("%x", apkBuf))
//			}
//			transAccountPks = append(transAccountPks, accountPlainKey)
//		}
//
//		transStoragePks := make([][]byte, 0, len(storagePlainKeys))
//		for _, storagePlainKey := range storagePlainKeys {
//			if len(storagePlainKey) == length.Addr+length.Hash {
//				// Non-optimised key originating from a database record
//				spkBuf = append(spkBuf[:0], storagePlainKey...)
//			} else {
//				// Optimised key referencing a state file record (file number and offset within the file)
//				var found bool
//				spkBuf, found = d.lookupByShortenedKey(storagePlainKey, files.storage)
//				if !found {
//					d.logger.Crit("lost storage full key", "shortened", fmt.Sprintf("%x", storagePlainKey))
//				}
//			}
//
//			storagePlainKey, found = d.findShortenKey(spkBuf, merged.storage)
//			if !found {
//				d.logger.Crit("replacement for full storage key was not found", "shortened", fmt.Sprintf("%x", apkBuf))
//			}
//			transStoragePks = append(transStoragePks, storagePlainKey)
//		}
//
//		transValBuf, err = val.ReplacePlainKeys(transAccountPks, transStoragePks, nil)
//		if err != nil {
//			return nil, err
//		}
//		return transValBuf, nil
//	}
//
//func (d *DomainCommitted) mergeFiles(ctx context.Context, oldFiles SelectedStaticFiles, mergedFiles MergedFiles, r DomainRanges, ps *background.ProgressSet) (valuesIn, indexIn, historyIn *filesItem, err error) {
//	if !r.any() {
//		return
//	}
//
//	domainFiles := oldFiles.commitment
//	indexFiles := oldFiles.commitmentIdx
//	historyFiles := oldFiles.commitmentHist
//
//	var comp ArchiveWriter
//	closeItem := true
//	defer func() {
//		if closeItem {
//			if comp != nil {
//				comp.Close()
//			}
//			if indexIn != nil {
//				indexIn.closeFilesAndRemove()
//			}
//			if historyIn != nil {
//				historyIn.closeFilesAndRemove()
//			}
//			if valuesIn != nil {
//				valuesIn.closeFilesAndRemove()
//			}
//		}
//	}()
//	if indexIn, historyIn, err = d.History.mergeFiles(ctx, indexFiles, historyFiles, HistoryRanges{
//		historyStartTxNum: r.historyStartTxNum,
//		historyEndTxNum:   r.historyEndTxNum,
//		history:           r.history,
//		indexStartTxNum:   r.indexStartTxNum,
//		indexEndTxNum:     r.indexEndTxNum,
//		index:             r.index}, ps); err != nil {
//		return nil, nil, nil, err
//	}
//
//	if !r.values {
//		closeItem = false
//		return
//	}
//
//	for _, f := range domainFiles {
//		f := f
//		defer f.decompressor.EnableReadAhead().DisableReadAhead()
//	}
//
//	fromStep, toStep := r.valuesStartTxNum/d.aggregationStep, r.valuesEndTxNum/d.aggregationStep
//	kvFilePath := d.kvFilePath(fromStep, toStep)
//	compr, err := compress.NewCompressor(ctx, "merge", kvFilePath, d.dirs.Tmp, compress.MinPatternScore, d.compressWorkers, log.LvlTrace, d.logger)
//	if err != nil {
//		return nil, nil, nil, fmt.Errorf("merge %s compressor: %w", d.filenameBase, err)
//	}
//
//	comp = NewArchiveWriter(compr, d.compression)
//	if d.noFsync {
//		comp.DisableFsync()
//	}
//	p := ps.AddNew("merge "+path.Base(kvFilePath), 1)
//	defer ps.Delete(p)
//
//	var cp CursorHeap
//	heap.Init(&cp)
//	for _, item := range domainFiles {
//		g := NewArchiveGetter(item.decompressor.MakeGetter(), d.compression)
//		g.Reset(0)
//		if g.HasNext() {
//			key, _ := g.Next(nil)
//			val, _ := g.Next(nil)
//			heap.Push(&cp, &CursorItem{
//				t:        FILE_CURSOR,
//				dg:       g,
//				key:      key,
//				val:      val,
//				endTxNum: item.endTxNum,
//				reverse:  true,
//			})
//		}
//	}
//	// In the loop below, the pair `keyBuf=>valBuf` is always 1 item behind `lastKey=>lastVal`.
//	// `lastKey` and `lastVal` are taken from the top of the multi-way merge (assisted by the CursorHeap cp), but not processed right away
//	// instead, the pair from the previous iteration is processed first - `keyBuf=>valBuf`. After that, `keyBuf` and `valBuf` are assigned
//	// to `lastKey` and `lastVal` correspondingly, and the next step of multi-way merge happens. Therefore, after the multi-way merge loop
//	// (when CursorHeap cp is empty), there is a need to process the last pair `keyBuf=>valBuf`, because it was one step behind
//	var keyBuf, valBuf []byte
//	for cp.Len() > 0 {
//		lastKey := common.Copy(cp[0].key)
//		lastVal := common.Copy(cp[0].val)
//		// Advance all the items that have this key (including the top)
//		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
//			ci1 := heap.Pop(&cp).(*CursorItem)
//			if ci1.dg.HasNext() {
//				ci1.key, _ = ci1.dg.Next(nil)
//				ci1.val, _ = ci1.dg.Next(nil)
//				heap.Push(&cp, ci1)
//			}
//		}
//
//		// For the rest of types, empty value means deletion
//		deleted := r.valuesStartTxNum == 0 && len(lastVal) == 0
//		if !deleted {
//			if keyBuf != nil {
//				if err = comp.AddWord(keyBuf); err != nil {
//					return nil, nil, nil, err
//				}
//				if err = comp.AddWord(valBuf); err != nil {
//					return nil, nil, nil, err
//				}
//			}
//			keyBuf = append(keyBuf[:0], lastKey...)
//			valBuf = append(valBuf[:0], lastVal...)
//		}
//	}
//	if keyBuf != nil {
//		if err = comp.AddWord(keyBuf); err != nil {
//			return nil, nil, nil, err
//		}
//		//fmt.Printf("last heap key %x\n", keyBuf)
//		if !bytes.Equal(keyBuf, keyCommitmentState) { // no replacement for state key
//			valBuf, err = d.commitmentValTransform(&oldFiles, &mergedFiles, valBuf)
//			if err != nil {
//				return nil, nil, nil, fmt.Errorf("merge: 2valTransform [%x] %w", valBuf, err)
//			}
//		}
//		if err = comp.AddWord(valBuf); err != nil {
//			return nil, nil, nil, err
//		}
//	}
//	if err = comp.Compress(); err != nil {
//		return nil, nil, nil, err
//	}
//	comp.Close()
//	comp = nil
//	ps.Delete(p)
//
//	valuesIn = newFilesItem(r.valuesStartTxNum, r.valuesEndTxNum, d.aggregationStep)
//	valuesIn.frozen = false
//	if valuesIn.decompressor, err = compress.NewDecompressor(kvFilePath); err != nil {
//		return nil, nil, nil, fmt.Errorf("merge %s decompressor [%d-%d]: %w", d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
//	}
//
//	if !UseBpsTree {
//		idxPath := d.kvAccessorFilePath(fromStep, toStep)
//		if valuesIn.index, err = buildIndexThenOpen(ctx, valuesIn.decompressor, d.compression, idxPath, d.dirs.Tmp, false, d.salt, ps, d.logger, d.noFsync); err != nil {
//			return nil, nil, nil, fmt.Errorf("merge %s buildIndex [%d-%d]: %w", d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
//		}
//	}
//
//	if UseBpsTree {
//		btPath := d.kvBtFilePath(fromStep, toStep)
//		valuesIn.bindex, err = CreateBtreeIndexWithDecompressor(btPath, DefaultBtreeM, valuesIn.decompressor, d.compression, *d.salt, ps, d.dirs.Tmp, d.logger, d.noFsync)
//		if err != nil {
//			return nil, nil, nil, fmt.Errorf("merge %s btindex [%d-%d]: %w", d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
//		}
//	}
//
//	{
//		bloomIndexPath := d.kvExistenceIdxFilePath(fromStep, toStep)
//		if dir.FileExist(bloomIndexPath) {
//			valuesIn.existence, err = OpenExistenceFilter(bloomIndexPath)
//			if err != nil {
//				return nil, nil, nil, fmt.Errorf("merge %s existence [%d-%d]: %w", d.filenameBase, r.valuesStartTxNum, r.valuesEndTxNum, err)
//			}
//		}
//	}
//
//	closeItem = false
//	d.stats.MergesCount++
//	return
//}
