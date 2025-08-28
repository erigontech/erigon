// Copyright 2024 The Erigon Authors
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

package types

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/trie"
)

func genTransactions(n uint64) Transactions {
	txs := Transactions{}

	for i := uint64(0); i < n; i++ {
		tx := NewTransaction(i, common.Address{}, uint256.NewInt(1000+i), 10+i, uint256.NewInt(1000+i), []byte(fmt.Sprintf("hello%d", i)))
		txs = append(txs, tx)
	}

	return txs
}

func TestEncodeUint(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Helper()

	legacySha := legacyDeriveSha(list)
	deriveSha := DeriveSha(list)
	if !hashesEqual(legacySha, deriveSha) {
		t.Errorf("unexpected hash: %v (expected: %v)\n", deriveSha.Hex(), legacySha.Hex())

	}
}

func hashesEqual(h1, h2 common.Hash) bool {
	if len(h1) != len(h2) {
		return false
	}
	return h1.Hex() == h2.Hex()
}

func legacyDeriveSha(list DerivableList) common.Hash {
	keybuf := new(bytes.Buffer)
	valbuf := new(bytes.Buffer)
	trie := trie.NewTestRLPTrie(common.Hash{})
	for i := 0; i < list.Len(); i++ {
		keybuf.Reset()
		valbuf.Reset()
		_ = rlp.Encode(keybuf, uint(i))
		list.EncodeIndex(i, valbuf)
		trie.Update(keybuf.Bytes(), common.CopyBytes(valbuf.Bytes()))
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
