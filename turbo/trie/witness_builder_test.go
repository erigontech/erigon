package trie

import (
	"bytes"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/types/accounts"
)

func TestBlockWitness(t *testing.T) {
	tr := New(libcommon.Hash{})
	tr.Update([]byte("ABCD0001"), []byte("val1"))
	tr.Update([]byte("ABCE0002"), []byte("val2"))

	rl := NewRetainList(2)
	rl.AddKey([]byte("ABCD0001"))

	bwb := NewWitnessBuilder(tr.root, false)

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
	tr := New(libcommon.Hash{})

	account := accounts.NewAccount()
	account.Balance.SetUint64(1 * 1000 * 1000)

	tr.UpdateAccount([]byte("ABCD0001"), &account)

	rl := NewRetainList(2)
	rl.AddKey([]byte("ABCD0001"))

	bwb := NewWitnessBuilder(tr.root, false)

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
