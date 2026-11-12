package trie

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestBlockWitness(t *testing.T) {
	tr := newEmpty()
	tr.Update([]byte("ABCD0001"), []byte("val1"))
	tr.Update([]byte("ABCE0002"), []byte("val2"))

	rl := NewRetainList(2)
	rl.AddKey([]byte("ABCD0001"))

	bwb := NewWitnessBuilder(tr.RootNode, false)

	hr := newHasher(false)
	defer returnHasherToPool(hr)

	var w *Witness
	var err error
	if w, err = bwb.Build(&MerklePathLimiter{rl, hr.hash}); err != nil {
		t.Errorf("Could not make block witness: %v", err)
	}

	tr1, err := BuildTrieFromWitness(w, false)
	if err != nil {
		t.Errorf("Could not restore trie from the block witness: %v", err)
	}
	if tr.Hash() != tr1.Hash() {
		t.Errorf("Reconstructed block witness has different root hash than source trie")
	}

	expected := []byte("val1")
	got, _ := tr1.Get([]byte("ABCD0001"))
	if !bytes.Equal(got, expected) {
		t.Errorf("unexpected value: %x (expected %x)", got, expected)
	}
}

func TestBlockWitnessAccount(t *testing.T) {
	tr := newEmpty()

	account := accounts.NewAccount()
	account.Balance.SetUint64(1 * 1000 * 1000)

	tr.UpdateAccount([]byte("ABCD0001"), &account)

	rl := NewRetainList(2)
	rl.AddKey([]byte("ABCD0001"))

	bwb := NewWitnessBuilder(tr.RootNode, false)

	hr := newHasher(false)
	defer returnHasherToPool(hr)

	var w *Witness
	var err error
	if w, err = bwb.Build(&MerklePathLimiter{rl, hr.hash}); err != nil {
		t.Errorf("Could not make block witness: %v", err)
	}

	trBin1, err := BuildTrieFromWitness(w, false)
	if err != nil {
		t.Errorf("Could not restore trie from the block witness: %v", err)
	}
	if tr.Hash() != trBin1.Hash() {
		t.Errorf("Reconstructed block witness has different root hash than source trie")
	}

	got, _ := trBin1.GetAccount([]byte("ABCD0001"))
	if !account.Equals(got) {
		t.Errorf("received account is not equal to the initial one")
	}
}

// RetainList encapsulates the list of keys that are required to be fully available, or loaded
// (by using `BRANCH` opcode instead of `HASHER`) after processing of the sequence of key-value
// pairs
// DESCRIBED: docs/programmers_guide/guide.md#converting-sequence-of-keys-and-value-into-a-multiproof
type RetainList struct {
	inited    bool // Whether keys are sorted and "LTE" and "GT" indices set
	minLength int  // Mininum length of prefixes for which `HashOnly` function can return `true`
	lteIndex  int  // Index of the "LTE" key in the keys slice. Next one is "GT"
	hexes     [][]byte
}

// NewRetainList creates new RetainList
func NewRetainList(minLength int) *RetainList {
	return &RetainList{minLength: minLength}
}

func (rl *RetainList) Len() int {
	return len(rl.hexes)
}

func (rl *RetainList) Less(i, j int) bool {
	return bytes.Compare(rl.hexes[i], rl.hexes[j]) < 0
}

func (rl *RetainList) Swap(i, j int) {
	rl.hexes[i], rl.hexes[j] = rl.hexes[j], rl.hexes[i]
}

// AddKey adds a new key (in KEY encoding) to the list
func (rl *RetainList) AddKey(key []byte) []byte {
	nibbles := make([]byte, 2*len(key))
	for i, b := range key {
		nibbles[i*2] = b / 16
		nibbles[i*2+1] = b % 16
	}
	rl.hexes = append(rl.hexes, nibbles)
	return nibbles
}

func (rl *RetainList) IsCodeTouched(accounts.CodeHash) bool {
	return false
}

func (rl *RetainList) ensureInited() {
	if rl.inited {
		return
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

func (rl *RetainList) String() string {
	return fmt.Sprintf("%x", rl.hexes)
}
