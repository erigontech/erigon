package trie

import (
	"bytes"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
)

func TestBlockWitnessBinary(t *testing.T) {
	tr := New(common.Hash{})
	tr.Update([]byte("ABCD0001"), []byte("val1"), 0)
	tr.Update([]byte("ABCE0002"), []byte("val2"), 0)

	trBin := HexToBin(tr)

	rs := NewBinaryResolveSet(2)
	rs.AddKey([]byte("ABCD0001"))

	bwb := NewWitnessBuilder(trBin.Trie(), 1, false, rs, nil)

	hr := newHasher(false)
	defer returnHasherToPool(hr)

	var w *Witness
	var err error
	if w, err = bwb.Build(hr.hash); err != nil {
		t.Errorf("Could not make block witness: %v", err)
	}

	trBin1, _, err := BuildTrieFromWitness(w, true /*is-binary*/, false /*trace*/)
	if err != nil {
		t.Errorf("Could not restore trie from the block witness: %v", err)
	}
	if trBin.Trie().Hash() != trBin1.Hash() {
		t.Errorf("Reconstructed block witness has different root hash than source trie")
	}

	expected := []byte("val1")
	got, _ := trBin1.Get([]byte("ABCD0001"))
	if !bytes.Equal(got, expected) {
		t.Errorf("unexpected value: %x (expected %x)", got, expected)
	}
}

func TestBlockWitnessBinaryAccount(t *testing.T) {
	tr := New(common.Hash{})

	account := accounts.NewAccount()
	account.Balance.SetInt64(1 * 1000 * 1000)

	tr.UpdateAccount([]byte("ABCD0001"), &account)

	trBin := HexToBin(tr)

	rs := NewBinaryResolveSet(2)
	rs.AddKey([]byte("ABCD0001"))

	bwb := NewWitnessBuilder(trBin.Trie(), 1, false, rs, nil)

	hr := newHasher(false)
	defer returnHasherToPool(hr)

	var w *Witness
	var err error
	if w, err = bwb.Build(hr.hash); err != nil {
		t.Errorf("Could not make block witness: %v", err)
	}

	trBin1, _, err := BuildTrieFromWitness(w, true /*is-binary*/, false /*trace*/)
	if err != nil {
		t.Errorf("Could not restore trie from the block witness: %v", err)
	}
	if trBin.Trie().Hash() != trBin1.Hash() {
		t.Errorf("Reconstructed block witness has different root hash than source trie")
	}

	got, _ := trBin1.GetAccount([]byte("ABCD0001"))
	if !account.Equals(got) {
		t.Errorf("received account is not equal to the initial one")
	}
}
