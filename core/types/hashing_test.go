package types

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

func genTransactions(n uint64) Transactions {
	txs := Transactions{}

	for i := uint64(0); i < n; i++ {
		tx := NewTransaction(i, libcommon.Address{}, uint256.NewInt(1000+i), 10+i, uint256.NewInt(1000+i), []byte(fmt.Sprintf("hello%d", i)))
		txs = append(txs, tx)
	}

	return txs
}

func TestEncodeUint(t *testing.T) {
	for i := 0; i < 64000; i++ {
		bbOld := bytes.NewBuffer(make([]byte, 10))
		bbNew := bytes.NewBuffer(make([]byte, 10))
		bbOld.Reset()
		bbNew.Reset()
		_ = rlp.Encode(bbOld, uint(i))

		bbNew.Reset()
		encodeUint(uint(i), bbNew)

		if !bytes.Equal(bbOld.Bytes(), bbNew.Bytes()) {
			t.Errorf("unexpected byte sequence. got: %x (expected %x)", bbNew.Bytes(), bbOld.Bytes())
		}
	}
}

func TestDeriveSha(t *testing.T) {
	tests := []DerivableList{
		Transactions{},
		genTransactions(1),
		genTransactions(2),
		genTransactions(4),
		genTransactions(10),
		genTransactions(100),
		genTransactions(1000),
		genTransactions(10000),
		genTransactions(100000),
	}

	for _, test := range tests {
		checkDeriveSha(t, test)
	}
}

func checkDeriveSha(t *testing.T, list DerivableList) {
	legacySha := legacyDeriveSha(list)
	deriveSha := DeriveSha(list)
	if !hashesEqual(legacySha, deriveSha) {
		t.Errorf("unexpected hash: %v (expected: %v)\n", deriveSha.Hex(), legacySha.Hex())

	}
}

func hashesEqual(h1, h2 libcommon.Hash) bool {
	if len(h1) != len(h2) {
		return false
	}
	return h1.Hex() == h2.Hex()
}

func legacyDeriveSha(list DerivableList) libcommon.Hash {
	keybuf := new(bytes.Buffer)
	valbuf := new(bytes.Buffer)
	trie := trie.NewTestRLPTrie(libcommon.Hash{})
	for i := 0; i < list.Len(); i++ {
		keybuf.Reset()
		valbuf.Reset()
		_ = rlp.Encode(keybuf, uint(i))
		list.EncodeIndex(i, valbuf)
		trie.Update(keybuf.Bytes(), libcommon.CopyBytes(valbuf.Bytes()))
	}
	return trie.Hash()
}

var (
	smallTxList = genTransactions(100)
	largeTxList = genTransactions(100000)
)

func BenchmarkLegacySmallList(b *testing.B) {
	for i := 0; i < b.N; i++ {
		legacyDeriveSha(smallTxList)
	}
}

func BenchmarkCurrentSmallList(b *testing.B) {
	for i := 0; i < b.N; i++ {
		DeriveSha(smallTxList)
	}
}

func BenchmarkLegacyLargeList(b *testing.B) {
	for i := 0; i < b.N; i++ {
		legacyDeriveSha(largeTxList)
	}
}

func BenchmarkCurrentLargeList(b *testing.B) {
	for i := 0; i < b.N; i++ {
		DeriveSha(largeTxList)
	}
}
