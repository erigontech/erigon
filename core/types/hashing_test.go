package types

import (
	"bytes"
	"fmt"
	"math/big"
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

func Test_ZkTransactionsDeriveSha(t *testing.T) {
	expectedRoot := libcommon.HexToHash("0xae3fd283f464b369dd815c359219da5e5dc77e0e090ed8326fea1bfa2a7ca7d1")
	expectedHash := libcommon.HexToHash("0x80799457daaf5814e1a940b97f5327bf3dd01660328dfd687d1fc89b0115bc79")
	price, err := uint256.FromHex("0x23fc27d85")
	if err != nil {
		t.Fatal(err)
	}
	to := libcommon.HexToAddress("0x4d5cf5032b2a844602278b01199ed191a86c93ff")
	//chainId := uint256.NewInt(1440)
	v, err := uint256.FromHex("0xb64")
	if err != nil {
		t.Fatal(err)
	}
	r, err := uint256.FromHex("0x3d03ac22287fd20cc464cd6050d234807e204926c0a771441bbe88a19bab43cb")
	if err != nil {
		t.Fatal(err)
	}
	s, err := uint256.FromHex("0x54efa762774a9d3d7bf2e22a07b9e0aae44368465bd77b78dd8da352d8ee650c")
	if err != nil {
		t.Fatal(err)
	}
	tx := &LegacyTx{
		CommonTx: CommonTx{
			TransactionMisc: TransactionMisc{},
			ChainID:         nil,
			Nonce:           4834,
			Gas:             21000,
			To:              &to,
			Value:           uint256.NewInt(1),
			Data:            nil,
			V:               *v,
			R:               *r,
			S:               *s,
		},
		GasPrice: price,
	}

	hash := tx.Hash()
	if hash != expectedHash {
		t.Errorf("unexpected tx hash have: %v want: %v\n", hash, expectedHash)
	}

	txs := []Transaction{tx}

	result := DeriveSha(Transactions(txs))
	if result != expectedRoot {
		t.Errorf("unexpected root hash have: %v want: %v\n", result.Hex(), expectedRoot)
	}
}

func Test_ZkReceiptsRootHash_InludingOmittedBloom(t *testing.T) {
	expected := libcommon.HexToHash("0xfc07704711a335a4bee26c8a5f813485ddf7a470953d070c4208a6a22668eda3")

	receipt := &Receipt{
		Type:              0,
		PostState:         libcommon.FromHex("0xe7e8b651b6c94f823a6b96aa3879f0700220f49fd074cc0fad39fa30f122899b"),
		Status:            1,
		CumulativeGasUsed: 107647,
		Bloom:             Bloom{},
		Logs: []*Log{
			{
				Address: libcommon.HexToAddress("0xcfa773cc48fbde3ca4d24eecb19d224d697026b2"),
				Topics: []libcommon.Hash{
					libcommon.HexToHash("0x25308c93ceeed162da955b3f7ce3e3f93606579e40fb92029faa9efe27545983"),
				},
				Data:        libcommon.FromHex("0x000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000f0558ff4cf60365244d099bcdb6cbb2e024bc12e000000000000000000000000000000000000000000000000002386f26fc10000"),
				BlockNumber: 1,
				TxHash:      libcommon.HexToHash("0x5a5648320cec13e74b0a8852e400156ca9c29b4809a1ba2006196513d595ca62"),
				TxIndex:     0,
				BlockHash:   libcommon.HexToHash("0xe8bf27967f928c2599767a4d5d2c1e7023097f42d17171e11f82308294114d60"),
				Index:       0,
				Removed:     false,
			},
		},
		TxHash:           libcommon.HexToHash("0x5a5648320cec13e74b0a8852e400156ca9c29b4809a1ba2006196513d595ca62"),
		ContractAddress:  libcommon.Address{},
		GasUsed:          107647,
		BlockHash:        libcommon.HexToHash("0xe8bf27967f928c2599767a4d5d2c1e7023097f42d17171e11f82308294114d60"),
		BlockNumber:      new(big.Int).SetUint64(1),
		TransactionIndex: 0,
	}

	result := DeriveSha(Receipts{receipt})
	if result != expected {
		t.Errorf("unexpected root hash have: %v want: %v\n", result.Hex(), expected.Hex())
	}
}
