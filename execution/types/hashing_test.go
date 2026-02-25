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
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/rlp"
)

func genTransactions(n uint64) Transactions {
	txs := Transactions{}

	for i := uint64(0); i < n; i++ {
		tx := NewTransaction(i, common.Address{}, uint256.NewInt(1000+i), 10+i, uint256.NewInt(1000+i), fmt.Appendf(nil, "hello%d", i))
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
		trie.Update(keybuf.Bytes(), common.Copy(valbuf.Bytes()))
	}
	return trie.Hash()
}

var (
	smallTxList = genTransactions(100)
	largeTxList = genTransactions(100000)
)

func BenchmarkLegacySmallList(b *testing.B) {
	for b.Loop() {
		legacyDeriveSha(smallTxList)
	}
}

func BenchmarkCurrentSmallList(b *testing.B) {
	for b.Loop() {
		DeriveSha(smallTxList)
	}
}

func BenchmarkLegacyLargeList(b *testing.B) {
	for b.Loop() {
		legacyDeriveSha(largeTxList)
	}
}

func BenchmarkCurrentLargeList(b *testing.B) {
	for b.Loop() {
		DeriveSha(largeTxList)
	}
}

func TestArbTransactionListHash(t *testing.T) {
	rawStartBlock := common.FromHex("0x6bf6a42d00000000000000000000000000000000000000000000000000000009a3bd877b0000000000000000000000000000000000000000000000000000000001072bc600000000000000000000000000000000000000000000000000000000055eaf170000000000000000000000000000000000000000000000000000000000000000")
	var tx1 ArbitrumInternalTx

	//bb := bytes.NewBuffer(rawStartBlock[1:])
	//stream := rlp.NewStream(bb, 0)
	//err := tx1.DecodeRLP(stream)
	err := rlp.DecodeBytes(rawStartBlock[:], &tx1)
	require.NoError(t, err)

	rawRetryable := common.FromHex("0xc9f95d3200000000000000000000000000000000000000000000000000000000000ce99300000000000000000000000000000000000000000000000000000009a3bd877b000000000000000000000000000000000000000000000000000060ffa32345b0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000060ffa32345b0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003749c4f034022c39ecaffaba182555d4508caccc000000000000000000000000000000000000000000000000000000000000016000000000000000000000000000000000000000000000000000000000000000c4cc29a3060000000000000000000000007a3d05c70581bd345fe117c06e45f9669205384f00000000000000000000000000000000000000000000000001a942840b9d400000000000000000000000000000000000000000000000000001a71922b86b2d5a000000000000000000000000000000000000000000000000000001881255a9470000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
	var tx2 ArbitrumSubmitRetryableTx
	err = rlp.DecodeBytes(rawRetryable[1:], &tx2)
	require.NoError(t, err)

	require.Equal(t, tx1.Hash(), common.HexToHash("0xd420a8799a87c13e6d6e9dbeb66b40d928f76eff1acfea38845a8960bc122fda"))
	require.Equal(t, tx2.Hash(), common.HexToHash("0xb96bee31487d0826e41f58618deed9dbcfd6ae20957a7d5cbbe07067db4c0746"))

	txns := make(Transactions, 2)
	txns[0] = &tx1
	txns[1] = &tx2

	txnRoot := DeriveSha(txns)
	require.Equal(t, txnRoot, common.HexToHash("0x9f586abcb16e6530ab4675ce632a2ee55af15f015472c3fd537312deb8288b25"))
}
