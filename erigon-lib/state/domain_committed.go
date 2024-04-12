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

	"github.com/google/btree"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
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

type DomainCommitted struct {
	*Domain
	mode         CommitmentMode
	trace        bool
	commTree     *btree.BTreeG[*CommitmentItem]
	keccak       hash.Hash
	patriciaTrie commitment.Trie
	branchMerger *commitment.BranchMerger

	comKeys uint64
	comTook time.Duration
	logger  log.Logger
}

func NewCommittedDomain(d *Domain, mode CommitmentMode, trieVariant commitment.TrieVariant, logger log.Logger) *DomainCommitted {
	return &DomainCommitted{
		Domain:       d,
		patriciaTrie: commitment.InitializeTrie(trieVariant),
		commTree:     btree.NewG[*CommitmentItem](32, commitmentItemLess),
		keccak:       sha3.NewLegacyKeccak256(),
		mode:         mode,
		branchMerger: commitment.NewHexBranchMerger(8192),
		logger:       logger,
	}
}

func (d *DomainCommitted) SetCommitmentMode(m CommitmentMode) { d.mode = m }

// TouchPlainKey marks plainKey as updated and applies different fn for different key types
// (different behaviour for Code, Account and Storage key modifications).
func (d *DomainCommitted) TouchPlainKey(key, val []byte, fn func(c *CommitmentItem, val []byte)) {
	if d.mode == CommitmentModeDisabled {
		return
	}
	c := &CommitmentItem{plainKey: common.Copy(key), hashedKey: d.hashAndNibblizeKey(key)}
	if d.mode > CommitmentModeDirect {
		fn(c, val)
	}
	d.commTree.ReplaceOrInsert(c)
}

func (d *DomainCommitted) TouchPlainKeyAccount(c *CommitmentItem, val []byte) {
	if len(val) == 0 {
		c.update.Flags = commitment.DeleteUpdate
		return
	}
	c.update.DecodeForStorage(val)
	c.update.Flags = commitment.BalanceUpdate | commitment.NonceUpdate
	item, found := d.commTree.Get(&CommitmentItem{hashedKey: c.hashedKey})
	if !found {
		return
	}
	if item.update.Flags&commitment.CodeUpdate != 0 {
		c.update.Flags |= commitment.CodeUpdate
		copy(c.update.CodeHashOrStorage[:], item.update.CodeHashOrStorage[:])
	}
}

func (d *DomainCommitted) TouchPlainKeyStorage(c *CommitmentItem, val []byte) {
	c.update.ValLength = len(val)
	if len(val) == 0 {
		c.update.Flags = commitment.DeleteUpdate
	} else {
		c.update.Flags = commitment.StorageUpdate
		copy(c.update.CodeHashOrStorage[:], val)
	}
}

func (d *DomainCommitted) TouchPlainKeyCode(c *CommitmentItem, val []byte) {
	c.update.Flags = commitment.CodeUpdate
	item, found := d.commTree.Get(c)
	if !found {
		d.keccak.Reset()
		d.keccak.Write(val)
		copy(c.update.CodeHashOrStorage[:], d.keccak.Sum(nil))
		return
	}
	if item.update.Flags&commitment.BalanceUpdate != 0 {
		c.update.Flags |= commitment.BalanceUpdate
		c.update.Balance.Set(&item.update.Balance)
	}
	if item.update.Flags&commitment.NonceUpdate != 0 {
		c.update.Flags |= commitment.NonceUpdate
		c.update.Nonce = item.update.Nonce
	}
	if item.update.Flags == commitment.DeleteUpdate && len(val) == 0 {
		c.update.Flags = commitment.DeleteUpdate
	} else {
		d.keccak.Reset()
		d.keccak.Write(val)
		copy(c.update.CodeHashOrStorage[:], d.keccak.Sum(nil))
	}
}

type CommitmentItem struct {
	plainKey  []byte
	hashedKey []byte
	update    commitment.Update
}

func commitmentItemLess(i, j *CommitmentItem) bool {
	return bytes.Compare(i.hashedKey, j.hashedKey) < 0
}

// Returns list of both plain and hashed keys. If .mode is CommitmentModeUpdate, updates also returned.
func (d *DomainCommitted) TouchedKeyList() ([][]byte, [][]byte, []commitment.Update) {
	plainKeys := make([][]byte, d.commTree.Len())
	hashedKeys := make([][]byte, d.commTree.Len())
	updates := make([]commitment.Update, d.commTree.Len())

	j := 0
	d.commTree.Ascend(func(item *CommitmentItem) bool {
		plainKeys[j] = item.plainKey
		hashedKeys[j] = item.hashedKey
		updates[j] = item.update
		j++
		return true
	})

	d.commTree.Clear(true)
	return plainKeys, hashedKeys, updates
}

// TODO(awskii): let trie define hashing function
func (d *DomainCommitted) hashAndNibblizeKey(key []byte) []byte {
	hashedKey := make([]byte, length.Hash)

	d.keccak.Reset()
	d.keccak.Write(key[:length.Addr])
	copy(hashedKey[:length.Hash], d.keccak.Sum(nil))

	if len(key[length.Addr:]) > 0 {
		hashedKey = append(hashedKey, make([]byte, length.Hash)...)
		d.keccak.Reset()
		d.keccak.Write(key[length.Addr:])
		copy(hashedKey[length.Hash:], d.keccak.Sum(nil))
	}

	nibblized := make([]byte, len(hashedKey)*2)
	for i, b := range hashedKey {
		nibblized[i*2] = (b >> 4) & 0xf
		nibblized[i*2+1] = b & 0xf
	}
	return nibblized
}

func (d *DomainCommitted) storeCommitmentState(blockNum, txNum uint64) error {
	var state []byte
	var err error

	switch trie := (d.patriciaTrie).(type) {
	case *commitment.HexPatriciaHashed:
		state, err = trie.EncodeCurrentState(nil)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported state storing for patricia trie type: %T", d.patriciaTrie)
	}
	cs := &commitmentState{txNum: txNum, trieState: state, blockNum: blockNum}
	encoded, err := cs.Encode()
	if err != nil {
		return err
	}

	var stepbuf [2]byte
	step := uint16(txNum / d.aggregationStep)
	binary.BigEndian.PutUint16(stepbuf[:], step)
	if err = d.Domain.Put(keyCommitmentState, stepbuf[:], encoded); err != nil {
		return err
	}
	return nil
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

// Evaluates commitment for processed state. Commit=true - store trie state after evaluation
func (d *DomainCommitted) ComputeCommitment(trace bool) (rootHash []byte, branchNodeUpdates map[string]commitment.BranchData, err error) {
	defer func(s time.Time) { d.comTook = time.Since(s) }(time.Now())

	touchedKeys, hashedKeys, updates := d.TouchedKeyList()
	d.comKeys = uint64(len(touchedKeys))

	if len(touchedKeys) == 0 {
		rootHash, err = d.patriciaTrie.RootHash()
		return rootHash, nil, err
	}

	// data accessing functions should be set once before
	d.patriciaTrie.Reset()
	d.patriciaTrie.SetTrace(trace)

	switch d.mode {
	case CommitmentModeDirect:
		rootHash, branchNodeUpdates, err = d.patriciaTrie.ReviewKeys(touchedKeys, hashedKeys)
		if err != nil {
			return nil, nil, err
		}
	case CommitmentModeUpdate:
		rootHash, branchNodeUpdates, err = d.patriciaTrie.ProcessUpdates(touchedKeys, hashedKeys, updates)
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
func (d *DomainCommitted) SeekCommitment(aggStep, sinceTx uint64) (blockNum, txNum uint64, err error) {
	if d.patriciaTrie.Variant() != commitment.VariantHexPatriciaTrie {
		return 0, 0, fmt.Errorf("state storing is only supported hex patricia trie")
	}
	// todo add support of bin state dumping

	var (
		latestState []byte
		stepbuf     [2]byte
		step               = uint16(sinceTx/aggStep) - 1
		latestTxNum uint64 = sinceTx - 1
	)

	d.SetTxNum(latestTxNum)
	ctx := d.BeginFilesRo()
	defer ctx.Close()

	for {
		binary.BigEndian.PutUint16(stepbuf[:], step)

		s, err := ctx.Get(keyCommitmentState, stepbuf[:], d.tx)
		if err != nil {
			return 0, 0, err
		}
		if len(s) < 8 {
			break
		}
		v := binary.BigEndian.Uint64(s)
		if v == latestTxNum && len(latestState) != 0 {
			break
		}
		latestTxNum, latestState = v, s
		lookupTxN := latestTxNum + aggStep
		step = uint16(latestTxNum/aggStep) + 1
		d.SetTxNum(lookupTxN)
	}

	var latest commitmentState
	if err := latest.Decode(latestState); err != nil {
		return 0, 0, nil
	}

	if hext, ok := d.patriciaTrie.(*commitment.HexPatriciaHashed); ok {
		if err := hext.SetState(latest.trieState); err != nil {
			return 0, 0, err
		}
	} else {
		return 0, 0, fmt.Errorf("state storing is only supported hex patricia trie")
	}

	return latest.blockNum, latest.txNum, nil
}

type commitmentState struct {
	txNum     uint64
	blockNum  uint64
	trieState []byte
}

func (cs *commitmentState) Decode(buf []byte) error {
	if len(buf) < 10 {
		return fmt.Errorf("ivalid commitment state buffer size")
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

func decodeU64(from []byte) uint64 { //nolint
	var i uint64
	for _, b := range from {
		i = (i << 8) | uint64(b)
	}
	return i
}

func encodeU64(i uint64, to []byte) []byte { //nolintmergeFil
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
