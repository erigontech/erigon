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
	"hash"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/google/btree"
	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/etl"
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
	keccak hash.Hash
	keys   etl.Buffer
	mode   CommitmentMode
}

func NewUpdateTree(m CommitmentMode) *UpdateTree {
	return &UpdateTree{
		tree:   btree.NewG[*commitmentItem](64, commitmentItemLessPlain),
		keccak: sha3.NewLegacyKeccak256(),
		keys:   etl.NewOldestEntryBuffer(datasize.MB * 32),
		mode:   m,
	}
}

func (t *UpdateTree) get(key []byte) (*commitmentItem, bool) {
	c := &commitmentItem{plainKey: key, update: commitment.Update{}}
	copy(c.update.CodeHashOrStorage[:], commitment.EmptyCodeHash)
	if t.tree.Has(c) {
		return t.tree.Get(c)
	}
	c.plainKey = common.Copy(c.plainKey)
	return c, false
}

// TouchPlainKey marks plainKey as updated and applies different fn for different key types
// (different behaviour for Code, Account and Storage key modifications).
func (t *UpdateTree) TouchPlainKey(key, val []byte, fn func(c *commitmentItem, val []byte)) {
	switch t.mode {
	case CommitmentModeUpdate:
		item, _ := t.get(key)
		fn(item, val)
		t.tree.ReplaceOrInsert(item)
	case CommitmentModeDirect:
		t.keys.Put(key, nil)
	default:
	}
}

func (t *UpdateTree) TouchAccount(c *commitmentItem, val []byte) {
	if len(val) == 0 {
		c.update.Flags = commitment.DeleteUpdate
		return
	}
	if c.update.Flags&commitment.DeleteUpdate != 0 {
		c.update.Flags ^= commitment.DeleteUpdate
	}
	nonce, balance, chash := DecodeAccountBytes(val)
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
	copy(c.update.CodeHashOrStorage[:], t.keccak.Sum(nil))
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
func (t *UpdateTree) List(clear bool) ([][]byte, []commitment.Update) {
	switch t.mode {
	case CommitmentModeDirect:
		plainKeys := make([][]byte, t.keys.Len())
		t.keys.Sort()

		keyBuf := make([]byte, 0)
		for i := 0; i < len(plainKeys); i++ {
			key, _ := t.keys.Get(i, keyBuf, nil)
			plainKeys[i] = common.Copy(key)
		}
		if clear {
			t.keys.Reset()
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
	updates      *UpdateTree
	mode         CommitmentMode
	patriciaTrie commitment.Trie
	branchMerger *commitment.BranchMerger
	prevState    []byte

	comTook time.Duration
	discard bool
}

func NewCommittedDomain(d *Domain, mode CommitmentMode, trieVariant commitment.TrieVariant) *DomainCommitted {
	return &DomainCommitted{
		Domain:       d,
		mode:         mode,
		trace:        false,
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

func (d *DomainCommitted) ResetFns(
	branchFn func(prefix []byte) ([]byte, error),
	accountFn func(plainKey []byte, cell *commitment.Cell) error,
	storageFn func(plainKey []byte, cell *commitment.Cell) error,
) {
	d.patriciaTrie.ResetFns(branchFn, accountFn, storageFn)
}

func (d *DomainCommitted) Hasher() hash.Hash {
	return d.updates.keccak
}

func (d *DomainCommitted) SetCommitmentMode(m CommitmentMode) { d.mode = m }

// TouchPlainKey marks plainKey as updated and applies different fn for different key types
// (different behaviour for Code, Account and Storage key modifications).
func (d *DomainCommitted) TouchPlainKey(key, val []byte, fn func(c *commitmentItem, val []byte)) {
	if d.discard {
		return
	}
	d.updates.TouchPlainKey(key, val, fn)
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
	plainKey  []byte
	hashedKey []byte
	update    commitment.Update
}

func commitmentItemLessPlain(i, j *commitmentItem) bool {
	return bytes.Compare(i.plainKey, j.plainKey) < 0
}

func (d *DomainCommitted) storeCommitmentState(blockNum uint64, rh []byte) error {
	state, err := d.PatriciaState()
	if err != nil {
		return err
	}
	cs := &commitmentState{txNum: d.txNum, trieState: state, blockNum: blockNum}
	encoded, err := cs.Encode()
	if err != nil {
		return err
	}

	if d.trace {
		fmt.Printf("[commitment] put tx %d rh %x\n", d.txNum, rh)
	}
	if err := d.Domain.PutWithPrev(keyCommitmentState, nil, encoded, d.prevState); err != nil {
		return err
	}
	d.prevState = common.Copy(encoded)
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
func (d *DomainCommitted) replaceKeyWithReference(fullKey, shortKey []byte, typeAS string, list ...*filesItem) bool {
	numBuf := [2]byte{}
	var found bool
	for _, item := range list {
		//g := item.decompressor.MakeGetter()
		//index := recsplit.NewIndexReader(item.index)

		cur, err := item.bindex.Seek(fullKey)
		if err != nil {
			continue
		}
		if cur == nil {
			continue
		}
		step := uint16(item.endTxNum / d.aggregationStep)
		binary.BigEndian.PutUint16(numBuf[:], step)

		shortKey = encodeU64(cur.Ordinal(), numBuf[:])

		if d.trace {
			fmt.Printf("replacing %s [%x] => {%x} [step=%d, offset=%d, file=%s.%d-%d]\n", typeAS, fullKey, shortKey, step, cur.Ordinal(), typeAS, item.startTxNum, item.endTxNum)
		}
		found = true
		break
	}
	//if !found {
	//	log.Warn("bt index key replacement seek failed", "key", fmt.Sprintf("%x", fullKey))
	//}
	return found
}

// nolint
func (d *DomainCommitted) lookupShortenedKey(shortKey, fullKey []byte, typAS string, list []*filesItem) bool {
	fileStep, offset := shortenedKey(shortKey)
	expected := uint64(fileStep) * d.aggregationStep

	var found bool
	for _, item := range list {
		if item.startTxNum > expected || item.endTxNum < expected {
			continue
		}

		cur := item.bindex.OrdinalLookup(offset)
		//nolint
		fullKey = cur.Key()
		if d.trace {
			fmt.Printf("offsetToKey %s [%x]=>{%x} step=%d offset=%d, file=%s.%d-%d.kv\n", typAS, fullKey, shortKey, fileStep, offset, typAS, item.startTxNum, item.endTxNum)
		}
		found = true
		break
	}
	return found
}

// commitmentValTransform parses the value of the commitment record to extract references
// to accounts and storage items, then looks them up in the new, merged files, and replaces them with
// the updated references
func (d *DomainCommitted) commitmentValTransform(files *SelectedStaticFiles, merged *MergedFiles, val commitment.BranchData) ([]byte, error) {
	if len(val) == 0 {
		return nil, nil
	}
	accountPlainKeys, storagePlainKeys, err := val.ExtractPlainKeys()
	if err != nil {
		return nil, err
	}

	transAccountPks := make([][]byte, 0, len(accountPlainKeys))
	var apkBuf, spkBuf []byte
	for _, accountPlainKey := range accountPlainKeys {
		if len(accountPlainKey) == length.Addr {
			// Non-optimised key originating from a database record
			apkBuf = append(apkBuf[:0], accountPlainKey...)
		} else {
			f := d.lookupShortenedKey(accountPlainKey, apkBuf, "account", files.accounts)
			if !f {
				fmt.Printf("lost key %x\n", accountPlainKeys)
			}
		}
		d.replaceKeyWithReference(apkBuf, accountPlainKey, "account", merged.accounts)
		transAccountPks = append(transAccountPks, accountPlainKey)
	}

	transStoragePks := make([][]byte, 0, len(storagePlainKeys))
	for _, storagePlainKey := range storagePlainKeys {
		if len(storagePlainKey) == length.Addr+length.Hash {
			// Non-optimised key originating from a database record
			spkBuf = append(spkBuf[:0], storagePlainKey...)
		} else {
			// Optimised key referencing a state file record (file number and offset within the file)
			f := d.lookupShortenedKey(storagePlainKey, spkBuf, "storage", files.storage)
			if !f {
				fmt.Printf("lost skey %x\n", storagePlainKey)
			}
		}

		d.replaceKeyWithReference(spkBuf, storagePlainKey, "storage", merged.storage)
		transStoragePks = append(transStoragePks, storagePlainKey)
	}

	transValBuf, err := val.ReplacePlainKeys(transAccountPks, transStoragePks, nil)
	if err != nil {
		return nil, err
	}
	return transValBuf, nil
}

type ArchiveWriter interface {
	AddWord(word []byte) error
	Compress() error
	DisableFsync()
	Close()
}

type compWriter struct {
	*compress.Compressor
	c bool
}

func NewArchiveWriter(kv *compress.Compressor, compress bool) ArchiveWriter {
	return &compWriter{kv, compress}
}

func (c *compWriter) AddWord(word []byte) error {
	if c.c {
		return c.Compressor.AddWord(word)
	}
	return c.Compressor.AddUncompressedWord(word)
}

func (c *compWriter) Close() {
	if c.Compressor != nil {
		c.Compressor.Close()
	}
}

func (d *DomainCommitted) Close() {
	d.Domain.Close()
	d.updates.keys.Reset()
	d.updates.tree.Clear(true)
}

// Evaluates commitment for processed state.
func (d *DomainCommitted) ComputeCommitment(trace bool) (rootHash []byte, branchNodeUpdates map[string]commitment.BranchData, err error) {
	if dbg.DiscardCommitment() {
		d.updates.List(true)
		return nil, nil, nil
	}
	defer func(s time.Time) { mxCommitmentTook.UpdateDuration(s) }(time.Now())

	touchedKeys, updates := d.updates.List(true)
	mxCommitmentKeys.Add(len(touchedKeys))

	if len(touchedKeys) == 0 {
		rootHash, err = d.patriciaTrie.RootHash()
		return rootHash, nil, err
	}

	if len(touchedKeys) > 1 {
		d.patriciaTrie.Reset()
	}
	// data accessing functions should be set once before
	d.patriciaTrie.SetTrace(trace)

	switch d.mode {
	case CommitmentModeDirect:
		rootHash, branchNodeUpdates, err = d.patriciaTrie.ProcessKeys(touchedKeys)
		if err != nil {
			return nil, nil, err
		}
	case CommitmentModeUpdate:
		rootHash, branchNodeUpdates, err = d.patriciaTrie.ProcessUpdates(touchedKeys, updates)
		if err != nil {
			return nil, nil, err
		}
	case CommitmentModeDisabled:
		return nil, nil, nil
	default:
		return nil, nil, fmt.Errorf("invalid commitment mode: %d", d.mode)
	}
	return rootHash, branchNodeUpdates, err
}

var keyCommitmentState = []byte("state")

// SeekCommitment searches for last encoded state from DomainCommitted
// and if state found, sets it up to current domain
func (d *DomainCommitted) SeekCommitment(sinceTx, untilTx uint64, cd *DomainContext) (blockNum, txNum uint64, err error) {
	if d.patriciaTrie.Variant() != commitment.VariantHexPatriciaTrie {
		return 0, 0, fmt.Errorf("state storing is only supported hex patricia trie")
	}

	fmt.Printf("[commitment] SeekCommitment [%d, %d]\n", sinceTx, untilTx)
	var latestState []byte
	err = cd.IteratePrefix(d.tx, keyCommitmentState, func(key, value []byte) {
		txn := binary.BigEndian.Uint64(value)
		fmt.Printf("[commitment] Seek txn=%d %x\n", txn, value[:16])
		if txn >= sinceTx && txn <= untilTx {
			latestState = value
		}
	})
	if err != nil {
		return 0, 0, err
	}
	return d.Restore(latestState)
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
