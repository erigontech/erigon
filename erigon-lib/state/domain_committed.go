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
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"sync/atomic"
	"time"

	"github.com/google/btree"
	"golang.org/x/crypto/sha3"
	"golang.org/x/exp/slices"

	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cryptozerocopy"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
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

type DomainCommitted struct {
	*Domain
	trace        bool
	shortenKeys  bool
	updates      *UpdateTree
	mode         CommitmentMode
	patriciaTrie commitment.Trie
	branchMerger *commitment.BranchMerger
	justRestored atomic.Bool
	discard      bool
}

func NewCommittedDomain(d *Domain, mode CommitmentMode, trieVariant commitment.TrieVariant) *DomainCommitted {
	return &DomainCommitted{
		Domain:       d,
		mode:         mode,
		shortenKeys:  true,
		updates:      NewUpdateTree(mode),
		discard:      dbg.DiscardCommitment(),
		patriciaTrie: commitment.InitializeTrie(trieVariant),
		branchMerger: commitment.NewHexBranchMerger(8192),
	}
}

func (d *DomainCommitted) PatriciaState() ([]byte, error) {
	var state []byte
	var err error

	switch trie := (d.patriciaTrie).(type) {
	case *commitment.HexPatriciaHashed:
		state, err = trie.EncodeCurrentState(nil)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported state storing for patricia trie type: %T", d.patriciaTrie)
	}
	return state, nil
}

func (d *DomainCommitted) Reset() {
	d.patriciaTrie.Reset()
}

func (d *DomainCommitted) ResetFns(ctx commitment.PatriciaContext) {
	d.patriciaTrie.ResetContext(ctx)
}

func (d *DomainCommitted) Hasher() hash.Hash {
	return d.updates.keccak
}

func (d *DomainCommitted) SetCommitmentMode(m CommitmentMode) { d.mode = m }

// TouchPlainKey marks plainKey as updated and applies different fn for different key types
// (different behaviour for Code, Account and Storage key modifications).
func (d *DomainCommitted) TouchPlainKey(key string, val []byte, fn func(c *commitmentItem, val []byte)) {
	if d.discard {
		return
	}
	d.updates.TouchPlainKey(key, val, fn)
}

func (d *DomainCommitted) Size() uint64 {
	return d.updates.Size()
}

func (d *DomainCommitted) TouchAccount(c *commitmentItem, val []byte) {
	d.updates.TouchAccount(c, val)
}

func (d *DomainCommitted) TouchStorage(c *commitmentItem, val []byte) {
	d.updates.TouchStorage(c, val)
}

func (d *DomainCommitted) TouchCode(c *commitmentItem, val []byte) {
	d.updates.TouchCode(c, val)
}

type commitmentItem struct {
	plainKey []byte
	update   commitment.Update
}

func commitmentItemLessPlain(i, j *commitmentItem) bool {
	return bytes.Compare(i.plainKey, j.plainKey) < 0
}

func (d *DomainCommitted) storeCommitmentState(dc *DomainContext, blockNum uint64, rh, prevState []byte) error {
	state, err := d.PatriciaState()
	if err != nil {
		return err
	}
	cs := &commitmentState{txNum: dc.hc.ic.txNum, trieState: state, blockNum: blockNum}
	encoded, err := cs.Encode()
	if err != nil {
		return err
	}

	if d.trace {
		fmt.Printf("[commitment] put txn %d block %d rh %x\n", dc.hc.ic.txNum, blockNum, rh)
	}
	if err := dc.PutWithPrev(keyCommitmentState, nil, encoded, prevState); err != nil {
		return err
	}
	return nil
}

func (d *DomainCommitted) Restore(value []byte) (uint64, uint64, error) {
	cs := new(commitmentState)
	if err := cs.Decode(value); err != nil {
		if len(value) > 0 {
			return 0, 0, fmt.Errorf("failed to decode previous stored commitment state: %w", err)
		}
		// nil value is acceptable for SetState and will reset trie
	}
	if hext, ok := d.patriciaTrie.(*commitment.HexPatriciaHashed); ok {
		if err := hext.SetState(cs.trieState); err != nil {
			return 0, 0, fmt.Errorf("failed restore state : %w", err)
		}
		d.justRestored.Store(true)
		if d.trace {
			rh, err := hext.RootHash()
			if err != nil {
				return 0, 0, fmt.Errorf("failed to get root hash after state restore: %w", err)
			}
			fmt.Printf("[commitment] restored state: block=%d txn=%d rh=%x\n", cs.blockNum, cs.txNum, rh)
		}
	} else {
		return 0, 0, fmt.Errorf("state storing is only supported hex patricia trie")
	}
	return cs.blockNum, cs.txNum, nil
}

// nolint
func (d *DomainCommitted) findShortenKey(fullKey []byte, list ...*filesItem) (shortened []byte, found bool) {
	shortened = make([]byte, 2, 10)

	//dc := d.MakeContext()
	//defer dc.Close()

	for _, item := range list {
		g := NewArchiveGetter(item.decompressor.MakeGetter(), d.compression)
		//index := recsplit.NewIndexReader(item.index) // TODO is support recsplt is needed?
		// TODO: existence filter existence should be checked for domain which filesItem list is provided, not in commitmnet
		//if d.withExistenceIndex && item.existence != nil {
		//	hi, _ := dc.hc.ic.hashKey(fullKey)
		//	if !item.existence.ContainsHash(hi) {
		//		continue
		//		//return nil, false, nil
		//	}
		//}

		cur, err := item.bindex.Seek(g, fullKey)
		if err != nil {
			d.logger.Warn("commitment branch key replacement seek failed", "key", fmt.Sprintf("%x", fullKey), "err", err, "file", item.decompressor.FileName())
			continue
		}
		if cur == nil {
			continue
		}
		step := uint16(item.endTxNum / d.aggregationStep)
		shortened = encodeShortenedKey(shortened[:], step, cur.Di())
		if d.trace {
			fmt.Printf("replacing [%x] => {%x} step=%d, di=%d file=%s\n", fullKey, shortened, step, cur.Di(), item.decompressor.FileName())
		}
		found = true
		break
	}
	//if !found {
	//	d.logger.Warn("failed to find key reference", "key", fmt.Sprintf("%x", fullKey))
	//}
	return shortened, found
}

// nolint
func (d *DomainCommitted) lookupByShortenedKey(shortKey []byte, list []*filesItem) (fullKey []byte, found bool) {
	fileStep, offset := shortenedKey(shortKey)
	expected := uint64(fileStep) * d.aggregationStep

	for _, item := range list {
		if item.startTxNum > expected || item.endTxNum < expected {
			continue
		}

		g := NewArchiveGetter(item.decompressor.MakeGetter(), d.compression)
		fullKey, _, err := item.bindex.dataLookup(offset, g)
		if err != nil {
			return nil, false
		}
		if d.trace {
			fmt.Printf("shortenedKey [%x]=>{%x} step=%d offset=%d, file=%s\n", shortKey, fullKey, fileStep, offset, item.decompressor.FileName())
		}
		found = true
		break
	}
	return fullKey, found
}

// commitmentValTransform parses the value of the commitment record to extract references
// to accounts and storage items, then looks them up in the new, merged files, and replaces them with
// the updated references
func (d *DomainCommitted) commitmentValTransform(files *SelectedStaticFiles, merged *MergedFiles, val commitment.BranchData) ([]byte, error) {
	if !d.shortenKeys || len(val) == 0 {
		return val, nil
	}
	var transValBuf []byte
	defer func(t time.Time) {
		d.logger.Info("commitmentValTransform", "took", time.Since(t), "in_size", len(val), "out_size", len(transValBuf), "ratio", float64(len(transValBuf))/float64(len(val)))
	}(time.Now())

	accountPlainKeys, storagePlainKeys, err := val.ExtractPlainKeys()
	if err != nil {
		return nil, err
	}

	transAccountPks := make([][]byte, 0, len(accountPlainKeys))
	var apkBuf, spkBuf []byte
	var found bool
	for _, accountPlainKey := range accountPlainKeys {
		if len(accountPlainKey) == length.Addr {
			// Non-optimised key originating from a database record
			apkBuf = append(apkBuf[:0], accountPlainKey...)
		} else {
			var found bool
			apkBuf, found = d.lookupByShortenedKey(accountPlainKey, files.accounts)
			if !found {
				d.logger.Crit("lost account full key", "shortened", fmt.Sprintf("%x", accountPlainKey))
			}
		}
		accountPlainKey, found = d.findShortenKey(apkBuf, merged.accounts)
		if !found {
			d.logger.Crit("replacement for full account key was not found", "shortened", fmt.Sprintf("%x", apkBuf))
		}
		transAccountPks = append(transAccountPks, accountPlainKey)
	}

	transStoragePks := make([][]byte, 0, len(storagePlainKeys))
	for _, storagePlainKey := range storagePlainKeys {
		if len(storagePlainKey) == length.Addr+length.Hash {
			// Non-optimised key originating from a database record
			spkBuf = append(spkBuf[:0], storagePlainKey...)
		} else {
			// Optimised key referencing a state file record (file number and offset within the file)
			var found bool
			spkBuf, found = d.lookupByShortenedKey(storagePlainKey, files.storage)
			if !found {
				d.logger.Crit("lost storage full key", "shortened", fmt.Sprintf("%x", storagePlainKey))
			}
		}

		storagePlainKey, found = d.findShortenKey(spkBuf, merged.storage)
		if !found {
			d.logger.Crit("replacement for full storage key was not found", "shortened", fmt.Sprintf("%x", apkBuf))
		}
		transStoragePks = append(transStoragePks, storagePlainKey)
	}

	transValBuf, err = val.ReplacePlainKeys(transAccountPks, transStoragePks, nil)
	if err != nil {
		return nil, err
	}
	return transValBuf, nil
}

func (d *DomainCommitted) Close() {
	d.Domain.Close()
	d.updates.keys = nil
	d.updates.tree.Clear(true)
}

// Evaluates commitment for processed state.
func (d *DomainCommitted) ComputeCommitment(ctx context.Context, trace bool) (rootHash []byte, err error) {
	if dbg.DiscardCommitment() {
		d.updates.List(true)
		return nil, nil
	}
	defer func(s time.Time) { mxCommitmentTook.UpdateDuration(s) }(time.Now())

	touchedKeys, updates := d.updates.List(true)
	//fmt.Printf("[commitment] ComputeCommitment %d keys\n", len(touchedKeys))
	if len(touchedKeys) == 0 {
		rootHash, err = d.patriciaTrie.RootHash()
		return rootHash, err
	}

	if !d.justRestored.Load() {
		d.patriciaTrie.Reset()
	}

	// data accessing functions should be set when domain is opened/shared context updated
	d.patriciaTrie.SetTrace(trace)

	switch d.mode {
	case CommitmentModeDirect:
		rootHash, err = d.patriciaTrie.ProcessKeys(ctx, touchedKeys)
		if err != nil {
			return nil, err
		}
	case CommitmentModeUpdate:
		rootHash, err = d.patriciaTrie.ProcessUpdates(ctx, touchedKeys, updates)
		if err != nil {
			return nil, err
		}
	case CommitmentModeDisabled:
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid commitment mode: %d", d.mode)
	}
	d.justRestored.Store(false)

	return rootHash, err
}

// by that key stored latest root hash and tree state
var keyCommitmentState = []byte("state")

// SeekCommitment searches for last encoded state from DomainCommitted
// and if state found, sets it up to current domain
func (d *DomainCommitted) SeekCommitment(tx kv.Tx, sinceTx, untilTx uint64, cd *DomainContext) (blockNum, txNum uint64, ok bool, err error) {
	if dbg.DiscardCommitment() {
		return 0, 0, false, nil
	}
	if d.patriciaTrie.Variant() != commitment.VariantHexPatriciaTrie {
		return 0, 0, false, fmt.Errorf("state storing is only supported hex patricia trie")
	}

	if d.trace {
		fmt.Printf("[commitment] SeekCommitment [%d, %d]\n", sinceTx, untilTx)
	}

	var latestState []byte
	err = cd.IteratePrefix(tx, keyCommitmentState, func(key, value []byte) error {
		if len(value) < 16 {
			return fmt.Errorf("invalid state value size %d [%x]", len(value), value)
		}
		txn, bn := binary.BigEndian.Uint64(value), binary.BigEndian.Uint64(value[8:16])
		if d.trace {
			fmt.Printf("[commitment] Seek found committed txn %d block %d\n", txn, bn)
		}

		fmt.Printf("[dbg] SeekCommitment: %d\n", txn)
		if txn >= sinceTx && txn <= untilTx {
			latestState = value
			ok = true
		}
		return nil
	})
	if err != nil {
		return 0, 0, false, fmt.Errorf("failed to seek commitment state: %w", err)
	}
	if !ok {
		//idx, err := cd.hc.IdxRange(keyCommitmentState, int(untilTx), int(untilTx+d.aggregationStep), order.Asc, -1, tx)
		//if err != nil {
		//      return 0, 0, false, fmt.Errorf("failed to seek commitment state: %w", err)
		//}
		//topTxNum := uint64(0)
		//for idx.HasNext() {
		//      tn, err := idx.Next()
		//      if err != nil {
		//              return 0, 0, false, fmt.Errorf("failed to seek commitment state: %w", err)
		//      }
		//      if tn < sinceTx {
		//              continue
		//      }
		//      if tn <= untilTx {
		//              if d.trace {
		//                      fmt.Printf("[commitment] Seek found committed txn %d\n", tn)
		//              }
		//              topTxNum = tn
		//              continue
		//      }
		//      if tn > untilTx {
		//              topTxNum = tn
		//              break
		//      }
		//}
		//latestState, ok, err = cd.hc.GetNoStateWithRecent(keyCommitmentState, topTxNum, tx)
		//if err != nil {
		//      return 0, 0, false, fmt.Errorf("failed to seek commitment state: %w", err)
		//}
		//if !ok {
		//      return 0, 0, false, nil
		//}
		return 0, 0, false, nil
	}
	blockNum, txNum, err = d.Restore(latestState)
	return blockNum, txNum, true, err
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

// Optimised key referencing a state file record (file number and offset within the file)
func shortenedKey(apk []byte) (step uint16, offset uint64) {
	step = binary.BigEndian.Uint16(apk[:2])
	return step, decodeU64(apk[1:])
}

func encodeShortenedKey(buf []byte, step uint16, offset uint64) []byte {
	binary.BigEndian.PutUint16(buf[:2], step)
	encodeU64(offset, buf[2:])
	return buf
}
