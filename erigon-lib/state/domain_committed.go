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
