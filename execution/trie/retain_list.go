// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"sort"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type RetainDecider interface {
	Retain([]byte) bool
	IsCodeTouched(common.Hash) bool
}

type RetainDeciderWithMarker interface {
	RetainDecider
	// AddKeyWithMarker adds a key in KEY encoding with marker and returns the
	// nibble encoded key.
	AddKeyWithMarker(key []byte, marker bool) []byte
	RetainWithMarker(prefix []byte) (retain bool, nextMarkedKey []byte)
}

// ProofRetainer is an interface that is used to retain proof elements during trie computation
type ProofRetainer interface {
	// ProofElement requests a new proof element for a given prefix. Returns nil if
	// the prefix is not needed for the proof.
	ProofElement(prefix []byte) *proofElement
}

type MultiAccountProofRetainer struct {
	AccHexKeys [][]byte
	Rl         *RetainList
}

func NewMultiAccountProofRetainer(rl *RetainList) *MultiAccountProofRetainer {
	return &MultiAccountProofRetainer{
		AccHexKeys: make([][]byte, 0),
		Rl:         rl,
	}
}

func (pr *MultiAccountProofRetainer) ProofElement(prefix []byte) *proofElement {
	if !pr.Rl.Retain(prefix) && ((len(prefix) > 1) && !pr.Rl.Retain(prefix[:len(prefix)-2])) {
		return nil
	}

	found := false

	for _, accHexKey := range pr.AccHexKeys {
		if bytes.HasPrefix(accHexKey, prefix) {
			found = true
			break
		}

		if bytes.HasPrefix(prefix, accHexKey) {
			found = true
			break
		}
	}

	if !found {
		return nil
	}

	return &proofElement{
		hexKey: prefix,
	}
}

// DefaultProofRetainer is a wrapper around the RetainList passed to the trie builder.
// It is responsible for aggregating proof values from the trie computation and
// will return a valid accounts.AccProofresult after the trie root hash
// calculation has completed.
type DefaultProofRetainer struct {
	rl             *RetainList
	addr           common.Address
	acc            *accounts.Account
	accHexKey      []byte
	storageKeys    []common.Hash
	storageHexKeys [][]byte
	proofs         []*proofElement
}

// NewProofRetainer creates a new ProofRetainer instance for a given account and
// set of storage keys.  The trie keys corresponding to the account key, and its
// storage keys are added to the given RetainList.  The ProofRetainer should be
// set onto the FlatDBTrieLoader via SetProofRetainer before performing its Load
// operation in order to appropriately collect the proof elements.
func NewProofRetainer(addr common.Address, a *accounts.Account, storageKeys []common.Hash, rl *RetainList) (*DefaultProofRetainer, error) {
	addrHash, err := common.HashData(addr[:])
	if err != nil {
		return nil, err
	}
	accHexKey := rl.AddKey(addrHash[:])

	storageHexKeys := make([][]byte, len(storageKeys))
	for i, sk := range storageKeys {
		storageHash, err := common.HashData(sk[:])
		if err != nil {
			return nil, err
		}

		var compactEncoded [72]byte
		copy(compactEncoded[:32], addrHash[:])
		binary.BigEndian.PutUint64(compactEncoded[32:40], a.Incarnation)
		copy(compactEncoded[40:], storageHash[:])
		storageHexKeys[i] = rl.AddKey(compactEncoded[:])
	}

	return &DefaultProofRetainer{
		rl:             rl,
		addr:           addr,
		acc:            a,
		accHexKey:      accHexKey,
		storageKeys:    storageKeys,
		storageHexKeys: storageHexKeys,
	}, nil
}

// ProofElement requests a new proof element for a given prefix.  This proof
// element is retained by the ProofRetainer, and will be utilized to compute the
// proof after the trie computation has completed.  The prefix is the standard
// nibble encoded prefix used in the rest of the trie computations.
func (pr *DefaultProofRetainer) ProofElement(prefix []byte) *proofElement {
	if !pr.rl.Retain(prefix) {
		return nil
	}

	switch {
	case bytes.HasPrefix(pr.accHexKey, prefix):
		// This prefix is a node between the account and the root
	case bytes.HasPrefix(prefix, pr.accHexKey):
		// This prefix is the account or one of its storage nodes
	default:
		// we do not need a proof element for this prefix
		return nil
	}

	pe := &proofElement{
		hexKey: append([]byte{}, prefix...),
	}
	// Since we do a depth-first traversal, reverse the proof elements so that
	// they are ordered correctly root -> node -> ... -> leaf as dictated by
	// EIP-1186
	pr.proofs = append([]*proofElement{pe}, pr.proofs...)
	return pe
}

// ProofResult may be invoked only after the Load function of the
// FlatDBTrieLoader has successfully executed.  It will populate the Address,
// Balance, Nonce, and CodeHash from the account data supplied in the
// constructor, the StorageHash, storageKey values, and proof elements are
// supplied by the Load operation of the trie construction.
func (pr *DefaultProofRetainer) ProofResult() (*accounts.AccProofResult, error) {
	result := &accounts.AccProofResult{
		Address:  pr.addr,
		Balance:  (*hexutil.Big)(pr.acc.Balance.ToBig()),
		Nonce:    hexutil.Uint64(pr.acc.Nonce),
		CodeHash: pr.acc.CodeHash,
	}

	for _, pe := range pr.proofs {
		if !bytes.HasPrefix(pr.accHexKey, pe.hexKey) {
			continue
		}
		result.AccountProof = append(result.AccountProof, pe.proof.Bytes())
		if bytes.Equal(pr.accHexKey, pe.storageRootKey) {
			result.StorageHash = pe.storageRoot
		}
	}

	if pr.acc.Initialised && result.StorageHash == (common.Hash{}) {
		return nil, errors.New("did not find storage root in proof elements")
	}

	result.StorageProof = make([]accounts.StorProofResult, len(pr.storageKeys))
	for i, sk := range pr.storageKeys {
		result.StorageProof[i].Key = uint256.NewInt(0).SetBytes(sk[:]).Hex()
		hexKey := pr.storageHexKeys[i]
		if !pr.acc.Initialised || result.StorageHash == EmptyRoot {
			// The yellow paper makes it clear that the EmptyRoot is a special case
			// when the trie has no nodes, but EIP-1186 states that the proof is
			// "starting with the storageHash-Node".  Since the trie has no nodes,
			// it's unclear whether the correct proof should contain the EmptyRoot
			// pre-image of RLP([]byte(nil)), or be empty.  This implementation
			// chooses 'empty' as it seems more consistent and it is expected that
			// provers will treat the EmptyRoot as a special case and ignore the proof
			// bytes.
			result.StorageProof[i].Value = (*hexutil.Big)(new(big.Int))
			result.StorageProof[i].Proof = make([]hexutil.Bytes, 0)
			continue
		}

		for _, pe := range pr.proofs {
			if len(pe.hexKey) <= 2*32 {
				// Ignore the proof elements above the storage tree (64 bytes, as nibble
				// encoded)
				continue
			}
			if !bytes.HasPrefix(hexKey, pe.hexKey) {
				continue
			}

			if pe.storageValue != nil && bytes.Equal(pe.storageKey, hexKey[2*(length.Hash+length.Incarnation):]) {
				result.StorageProof[i].Value = (*hexutil.Big)(pe.storageValue.ToBig())
			}

			result.StorageProof[i].Proof = append(result.StorageProof[i].Proof, pe.proof.Bytes())
		}

		if result.StorageProof[i].Value == nil {
			result.StorageProof[i].Value = (*hexutil.Big)(new(big.Int))
		}
	}

	return result, nil
}

// proofElement represent a node or leaf in the trie and its
// corresponding RLP encoding.  We store the elements individually when
// aggregating as multiple keys (in particular storage keys) may need to
// reference the same proof elements in their Merkle proof.
type proofElement struct {
	// key is the hex encoded key indicating the path of the
	// element in the proof.
	hexKey []byte

	// buf is used to store the proof bytes
	proof bytes.Buffer

	// storageRoot stores the storage root if this is writing
	// an account leaf
	storageRoot common.Hash

	// storageRootKey stores the actual hexKey from which the storageRoot came.
	// This is needed because other proof nodes can be included in the negative
	// proof case.
	storageRootKey []byte

	// storageValue stores the value of the particular storage key if this writer
	// is for a storage key
	storageValue *uint256.Int

	// storageKey stores the actual hexKey from which the storageValue came. This
	// is needed because the same proofElement may be used to both prove and
	// disprove two different storage elements.
	storageKey []byte
}

// RetainList encapsulates the list of keys that are required to be fully available, or loaded
// (by using `BRANCH` opcode instead of `HASHER`) after processing of the sequence of key-value
// pairs
// DESCRIBED: docs/programmers_guide/guide.md#converting-sequence-of-keys-and-value-into-a-multiproof
type RetainList struct {
	inited      bool // Whether keys are sorted and "LTE" and "GT" indices set
	minLength   int  // Mininum length of prefixes for which `HashOnly` function can return `true`
	lteIndex    int  // Index of the "LTE" key in the keys slice. Next one is "GT"
	hexes       [][]byte
	markers     []bool
	codeTouches map[common.Hash]struct{}
}

// NewRetainList creates new RetainList
func NewRetainList(minLength int) *RetainList {
	return &RetainList{minLength: minLength, codeTouches: make(map[common.Hash]struct{})}
}

func (rl *RetainList) Len() int {
	return len(rl.hexes)
}
func (rl *RetainList) Less(i, j int) bool {
	return bytes.Compare(rl.hexes[i], rl.hexes[j]) < 0
}
func (rl *RetainList) Swap(i, j int) {
	rl.hexes[i], rl.hexes[j] = rl.hexes[j], rl.hexes[i]
	rl.markers[i], rl.markers[j] = rl.markers[j], rl.markers[i]
}

// AddKey adds a new key (in KEY encoding) to the list
func (rl *RetainList) AddKey(key []byte) []byte {
	return rl.AddKeyWithMarker(key, false)
}

func (rl *RetainList) AddKeyWithMarker(key []byte, marker bool) []byte {
	var nibbles = make([]byte, 2*len(key))
	for i, b := range key {
		nibbles[i*2] = b / 16
		nibbles[i*2+1] = b % 16
	}
	rl.AddHex(nibbles)
	rl.markers = append(rl.markers, marker)
	return nibbles
}

// AddHex adds a new key (in HEX encoding) to the list
func (rl *RetainList) AddHex(hex []byte) {
	rl.hexes = append(rl.hexes, hex)
}

func (rl *RetainList) AddMarker(marker bool) {
	rl.markers = append(rl.markers, marker)
}

// AddCodeTouch adds a new code touch into the resolve set
func (rl *RetainList) AddCodeTouch(codeHash common.Hash) {
	rl.codeTouches[codeHash] = struct{}{}
}

func (rl *RetainList) IsCodeTouched(codeHash common.Hash) bool {
	_, ok := rl.codeTouches[codeHash]
	return ok
}

func (rl *RetainList) ensureInited() {
	if rl.inited {
		return
	}
	if len(rl.markers) == 0 {
		rl.markers = make([]bool, len(rl.hexes))
	}
	if !sort.IsSorted(rl) {
		sort.Sort(rl)
	}
	rl.lteIndex = 0
	rl.inited = true
}

// Retain decides whether to emit `HASHER` or `BRANCH` for a given prefix, by
// checking if this is prefix of any of the keys added to the set
// Since keys in the set are sorted, and we expect that the prefixes will
// come in monotonically ascending order, we optimise for this, though
// the function would still work if the order is different
func (rl *RetainList) Retain(prefix []byte) bool {
	// if bytes.HasPrefix(prefix, common.FromHex("0x0d05")) {
	// 	fmt.Println("here!!!!!")
	// }

	rl.ensureInited()
	if len(prefix) < rl.minLength {
		return true
	}
	// Adjust "GT" if necessary
	var gtAdjusted bool
	for rl.lteIndex < len(rl.hexes)-1 && bytes.Compare(rl.hexes[rl.lteIndex+1], prefix) <= 0 {
		rl.lteIndex++
		gtAdjusted = true
	}
	// Adjust "LTE" if necessary (normally will not be necessary)
	for !gtAdjusted && rl.lteIndex > 0 && bytes.Compare(rl.hexes[rl.lteIndex], prefix) > 0 {
		rl.lteIndex--
	}
	if rl.lteIndex < len(rl.hexes) {
		if bytes.HasPrefix(rl.hexes[rl.lteIndex], prefix) {
			return true
		}
	}
	if rl.lteIndex < len(rl.hexes)-1 {
		if bytes.HasPrefix(rl.hexes[rl.lteIndex+1], prefix) {
			return true
		}
	}
	return false
}

func (rl *RetainList) RetainWithMarker(prefix []byte) (bool, []byte) {
	rl.ensureInited()
	if len(prefix) < rl.minLength {
		return true, nil
	}
	// Adjust "GT" if necessary
	var gtAdjusted bool
	for rl.lteIndex < len(rl.hexes)-1 && bytes.Compare(rl.hexes[rl.lteIndex+1], prefix) <= 0 {
		rl.lteIndex++
		gtAdjusted = true
	}
	// Adjust "LTE" if necessary (normally will not be necessary)
	for !gtAdjusted && rl.lteIndex > 0 && bytes.Compare(rl.hexes[rl.lteIndex], prefix) > 0 {
		rl.lteIndex--
	}
	if rl.lteIndex < len(rl.hexes) {
		if bytes.HasPrefix(rl.hexes[rl.lteIndex], prefix) {
			return true, rl.nextMarkedItem(rl.lteIndex)
		}
	}
	if rl.lteIndex < len(rl.hexes)-1 {
		if bytes.HasPrefix(rl.hexes[rl.lteIndex+1], prefix) {
			return true, rl.nextMarkedItem(rl.lteIndex + 1)
		}
	}

	if rl.lteIndex < len(rl.hexes) {
		if bytes.Compare(prefix, rl.hexes[rl.lteIndex]) <= 0 {
			return false, rl.nextMarkedItem(rl.lteIndex)
		}
	}
	if rl.lteIndex < len(rl.hexes)-1 {
		if bytes.Compare(prefix, rl.hexes[rl.lteIndex+1]) <= 0 {
			return false, rl.nextMarkedItem(rl.lteIndex + 1)
		}
	}

	return false, nil
}

func (rl *RetainList) nextMarkedItem(index int) []byte {
	for i := index; i < len(rl.markers); i++ {
		if rl.markers[i] {
			return rl.hexes[i]
		}
	}
	return nil
}

// Rewind lets us reuse this list from the beginning
func (rl *RetainList) Rewind() {
	rl.lteIndex = 0
}

func (rl *RetainList) String() string {
	return fmt.Sprintf("%x", rl.hexes)
}
