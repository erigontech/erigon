// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package types

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"reflect"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
)

// from bcValidBlockTest.json, "SimpleTx"
func TestBlockEncoding(t *testing.T) {
	blockEnc := common.FromHex("f90260f901f9a083cafc574e1f51ba9dc0568fc617a08ea2429fb384059c972f13b19fa1c8dd55a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347948888f1f195afa192cfee860698584c030f4c9db1a0ef1552a40b7165c3cd773806b9e0c165b75356e0314bf0706f279c729f51e017a05fe50b260da6308036625b850b5d6ced6d0a9f814c0688bc91ffb7b7a3a54b67a0bc37d79753ad738a6dac4921e57392f145d8887476de3f783dfa7edae9283e52b90100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008302000001832fefd8825208845506eb0780a0bd4472abb6659ebe3ee06ee4d7b72a00a9f4d001caca51342001075469aff49888a13a5a8c8f2bb1c4f861f85f800a82c35094095e7baea6a6c7c4c2dfeb977efac326af552d870a801ba09bea4c4daac7c7c52e093e6a4c35dbbcf8856f1af7b059ba20253e70848d094fa08a8fae537ce25ed8cb5af9adac3f141af69bd515bd2ba031522df09b97dd72b1c0")
	var block Block
	if err := rlp.DecodeBytes(blockEnc, &block); err != nil {
		t.Fatal("decode error: ", err)
	}

	check := func(f string, got, want interface{}) {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s mismatch: got %v, want %v", f, got, want)
		}
	}
	check("Difficulty", block.Difficulty(), big.NewInt(131072))
	check("GasLimit", block.GasLimit(), uint64(3141592))
	check("GasUsed", block.GasUsed(), uint64(21000))
	check("Coinbase", block.Coinbase(), libcommon.HexToAddress("8888f1f195afa192cfee860698584c030f4c9db1"))
	check("MixDigest", block.MixDigest(), libcommon.HexToHash("bd4472abb6659ebe3ee06ee4d7b72a00a9f4d001caca51342001075469aff498"))
	check("Root", block.Root(), libcommon.HexToHash("ef1552a40b7165c3cd773806b9e0c165b75356e0314bf0706f279c729f51e017"))
	check("Hash", block.Hash(), libcommon.HexToHash("0a5843ac1cb04865017cb35a57b50b07084e5fcee39b5acadade33149f4fff9e"))
	check("Nonce", block.NonceU64(), uint64(0xa13a5a8c8f2bb1c4))
	check("Time", block.Time(), uint64(1426516743))
	check("Size", block.Size(), common.StorageSize(len(blockEnc)))

	var tx1 Transaction = NewTransaction(0, libcommon.HexToAddress("095e7baea6a6c7c4c2dfeb977efac326af552d87"), uint256.NewInt(10), 50000, uint256.NewInt(10), nil)
	tx1, _ = tx1.WithSignature(*LatestSignerForChainID(nil), common.Hex2Bytes("9bea4c4daac7c7c52e093e6a4c35dbbcf8856f1af7b059ba20253e70848d094f8a8fae537ce25ed8cb5af9adac3f141af69bd515bd2ba031522df09b97dd72b100"))
	check("len(Transactions)", len(block.Transactions()), 1)
	check("Transactions[0].Hash", block.Transactions()[0].Hash(), tx1.Hash())
	ourBlockEnc, err := rlp.EncodeToBytes(&block)
	if err != nil {
		t.Fatal("encode error: ", err)
	}
	if !bytes.Equal(ourBlockEnc, blockEnc) {
		t.Errorf("encoded block mismatch:\ngot:  %x\nwant: %x", ourBlockEnc, blockEnc)
	}
}

func TestEIP1559BlockEncoding(t *testing.T) {
	blockEnc := common.FromHex("f9030bf901fea083cafc574e1f51ba9dc0568fc617a08ea2429fb384059c972f13b19fa1c8dd55a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347948888f1f195afa192cfee860698584c030f4c9db1a0ef1552a40b7165c3cd773806b9e0c165b75356e0314bf0706f279c729f51e017a05fe50b260da6308036625b850b5d6ced6d0a9f814c0688bc91ffb7b7a3a54b67a0bc37d79753ad738a6dac4921e57392f145d8887476de3f783dfa7edae9283e52b90100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008302000001832fefd8825208845506eb0780a0bd4472abb6659ebe3ee06ee4d7b72a00a9f4d001caca51342001075469aff49888a13a5a8c8f2bb1c4843b9aca00f90106f85f800a82c35094095e7baea6a6c7c4c2dfeb977efac326af552d870a801ba09bea4c4daac7c7c52e093e6a4c35dbbcf8856f1af7b059ba20253e70848d094fa08a8fae537ce25ed8cb5af9adac3f141af69bd515bd2ba031522df09b97dd72b1b8a302f8a0018080843b9aca008301e24194095e7baea6a6c7c4c2dfeb977efac326af552d878080f838f7940000000000000000000000000000000000000001e1a0000000000000000000000000000000000000000000000000000000000000000080a0fe38ca4e44a30002ac54af7cf922a6ac2ba11b7d22f548e8ecb3f51f41cb31b0a06de6a5cbae13c0c856e33acf021b51819636cfc009d39eafb9f606d546e305a8c0")
	var block Block
	if err := rlp.DecodeBytes(blockEnc, &block); err != nil {
		t.Fatal("decode error: ", err)
	}

	check := func(f string, got, want interface{}) {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s mismatch: got %v, want %v", f, got, want)
		}
	}

	check("Difficulty", block.Difficulty(), big.NewInt(131072))
	check("GasLimit", block.GasLimit(), uint64(3141592))
	check("GasUsed", block.GasUsed(), uint64(21000))
	check("Coinbase", block.Coinbase(), libcommon.HexToAddress("8888f1f195afa192cfee860698584c030f4c9db1"))
	check("MixDigest", block.MixDigest(), libcommon.HexToHash("bd4472abb6659ebe3ee06ee4d7b72a00a9f4d001caca51342001075469aff498"))
	check("Root", block.Root(), libcommon.HexToHash("ef1552a40b7165c3cd773806b9e0c165b75356e0314bf0706f279c729f51e017"))
	check("Hash", block.Hash(), libcommon.HexToHash("c7252048cd273fe0dac09650027d07f0e3da4ee0675ebbb26627cea92729c372"))
	check("Nonce", block.NonceU64(), uint64(0xa13a5a8c8f2bb1c4))
	check("Time", block.Time(), uint64(1426516743))
	check("Size", block.Size(), common.StorageSize(len(blockEnc)))
	check("BaseFee", block.BaseFee(), new(big.Int).SetUint64(params.InitialBaseFee))

	var tx1 Transaction = NewTransaction(0, libcommon.HexToAddress("095e7baea6a6c7c4c2dfeb977efac326af552d87"), new(uint256.Int).SetUint64(10), 50000, new(uint256.Int).SetUint64(10), nil)
	tx1, _ = tx1.WithSignature(*LatestSignerForChainID(nil), common.Hex2Bytes("9bea4c4daac7c7c52e093e6a4c35dbbcf8856f1af7b059ba20253e70848d094f8a8fae537ce25ed8cb5af9adac3f141af69bd515bd2ba031522df09b97dd72b100"))

	addr := libcommon.HexToAddress("0x0000000000000000000000000000000000000001")
	accesses := AccessList{AccessTuple{
		Address: addr,
		StorageKeys: []libcommon.Hash{
			{0},
		},
	}}
	to := libcommon.HexToAddress("095e7baea6a6c7c4c2dfeb977efac326af552d87")
	feeCap, _ := uint256.FromBig(block.BaseFee())
	var tx2 Transaction = &DynamicFeeTransaction{
		CommonTx: CommonTx{
			ChainID: u256.Num1,
			Nonce:   0,
			To:      &to,
			Gas:     123457,
			Data:    []byte{},
		},
		FeeCap:     feeCap,
		Tip:        u256.Num0,
		AccessList: accesses,
	}
	tx2, err := tx2.WithSignature(*LatestSignerForChainID(big.NewInt(1)), common.Hex2Bytes("fe38ca4e44a30002ac54af7cf922a6ac2ba11b7d22f548e8ecb3f51f41cb31b06de6a5cbae13c0c856e33acf021b51819636cfc009d39eafb9f606d546e305a800"))
	if err != nil {
		t.Fatal("invalid signature error: ", err)
	}

	check("len(Transactions)", len(block.Transactions()), 2)
	check("Transactions[0].Hash", block.Transactions()[0].Hash(), tx1.Hash())
	check("Transactions[1].Hash", block.Transactions()[1].Hash(), tx2.Hash())
	check("Transactions[1].Type", block.Transactions()[1].Type(), tx2.Type())
	ourBlockEnc, err := rlp.EncodeToBytes(&block)
	if err != nil {
		t.Fatal("encode error: ", err)
	}
	if !bytes.Equal(ourBlockEnc, blockEnc) {
		t.Errorf("encoded block mismatch:\ngot:  %x\nwant: %x", ourBlockEnc, blockEnc)
	}
}

func TestEIP2718BlockEncoding(t *testing.T) {
	blockEnc := common.FromHex("f90319f90211a00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347948888f1f195afa192cfee860698584c030f4c9db1a0ef1552a40b7165c3cd773806b9e0c165b75356e0314bf0706f279c729f51e017a0e6e49996c7ec59f7a23d22b83239a60151512c65613bf84a0d7da336399ebc4aa0cafe75574d59780665a97fbfd11365c7545aa8f1abf4e5e12e8243334ef7286bb901000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000083020000820200832fefd882a410845506eb0796636f6f6c65737420626c6f636b206f6e20636861696ea0bd4472abb6659ebe3ee06ee4d7b72a00a9f4d001caca51342001075469aff49888a13a5a8c8f2bb1c4f90101f85f800a82c35094095e7baea6a6c7c4c2dfeb977efac326af552d870a801ba09bea4c4daac7c7c52e093e6a4c35dbbcf8856f1af7b059ba20253e70848d094fa08a8fae537ce25ed8cb5af9adac3f141af69bd515bd2ba031522df09b97dd72b1b89e01f89b01800a8301e24194095e7baea6a6c7c4c2dfeb977efac326af552d878080f838f7940000000000000000000000000000000000000001e1a0000000000000000000000000000000000000000000000000000000000000000001a03dbacc8d0259f2508625e97fdfc57cd85fdd16e5821bc2c10bdd1a52649e8335a0476e10695b183a87b0aa292a7f4b78ef0c3fbe62aa2c42c84e1d9c3da159ef14c0")
	var block Block
	if err := rlp.DecodeBytes(blockEnc, &block); err != nil {
		t.Fatal("decode error: ", err)
	}

	check := func(f string, got, want interface{}) {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s mismatch: got %v, want %v", f, got, want)
		}
	}
	check("Difficulty", block.Difficulty(), big.NewInt(131072))
	check("GasLimit", block.GasLimit(), uint64(3141592))
	check("GasUsed", block.GasUsed(), uint64(42000))
	check("Coinbase", block.Coinbase(), libcommon.HexToAddress("8888f1f195afa192cfee860698584c030f4c9db1"))
	check("MixDigest", block.MixDigest(), libcommon.HexToHash("bd4472abb6659ebe3ee06ee4d7b72a00a9f4d001caca51342001075469aff498"))
	check("Root", block.Root(), libcommon.HexToHash("ef1552a40b7165c3cd773806b9e0c165b75356e0314bf0706f279c729f51e017"))
	check("Nonce", block.NonceU64(), uint64(0xa13a5a8c8f2bb1c4))
	check("Time", block.Time(), uint64(1426516743))
	check("Size", block.Size(), common.StorageSize(len(blockEnc)))

	// Create legacy tx.
	to := libcommon.HexToAddress("095e7baea6a6c7c4c2dfeb977efac326af552d87")
	ten := new(uint256.Int).SetUint64(10)
	var tx1 Transaction = &LegacyTx{
		CommonTx: CommonTx{
			Nonce: 0,
			To:    &to,
			Value: ten,
			Gas:   50000,
		},
		GasPrice: ten,
	}
	sig := common.Hex2Bytes("9bea4c4daac7c7c52e093e6a4c35dbbcf8856f1af7b059ba20253e70848d094f8a8fae537ce25ed8cb5af9adac3f141af69bd515bd2ba031522df09b97dd72b100")
	tx1, _ = tx1.WithSignature(*LatestSignerForChainID(nil), sig)

	chainID, _ := uint256.FromBig(big.NewInt(1))
	// Create ACL tx.
	addr := libcommon.HexToAddress("0x0000000000000000000000000000000000000001")
	var tx2 Transaction = &AccessListTx{
		ChainID: chainID,
		LegacyTx: LegacyTx{
			CommonTx: CommonTx{
				Nonce: 0,
				To:    &to,
				Gas:   123457,
			},
			GasPrice: ten,
		},
		AccessList: AccessList{{Address: addr, StorageKeys: []libcommon.Hash{{0}}}},
	}
	sig2 := common.Hex2Bytes("3dbacc8d0259f2508625e97fdfc57cd85fdd16e5821bc2c10bdd1a52649e8335476e10695b183a87b0aa292a7f4b78ef0c3fbe62aa2c42c84e1d9c3da159ef1401")
	tx2, _ = tx2.WithSignature(*LatestSignerForChainID(big.NewInt(1)), sig2)

	check("len(Transactions)", len(block.Transactions()), 2)
	check("Transactions[0].Hash", block.Transactions()[0].Hash(), tx1.Hash())
	check("Transactions[1].Hash", block.Transactions()[1].Hash(), tx2.Hash())
	check("Transactions[1].Type()", block.Transactions()[1].Type(), uint8(AccessListTxType))

	ourBlockEnc, err := rlp.EncodeToBytes(&block)
	if err != nil {
		t.Fatal("encode error: ", err)
	}
	if !bytes.Equal(ourBlockEnc, blockEnc) {
		t.Errorf("encoded block mismatch:\ngot:  %x\nwant: %x", ourBlockEnc, blockEnc)
	}
}

func TestUncleHash(t *testing.T) {
	uncles := make([]*Header, 0)
	h := CalcUncleHash(uncles)
	exp := libcommon.HexToHash("1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347")
	if h != exp {
		t.Fatalf("empty uncle hash is wrong, got %x != %x", h, exp)
	}
}

var benchBuffer = bytes.NewBuffer(make([]byte, 0, 32000))

func BenchmarkEncodeBlock(b *testing.B) {
	block := makeBenchBlock()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		benchBuffer.Reset()
		if err := rlp.Encode(benchBuffer, block); err != nil {
			b.Fatal(err)
		}
	}
}

func makeBenchBlock() *Block {
	var (
		key, _   = crypto.GenerateKey()
		txs      = make([]Transaction, 70)
		receipts = make([]*Receipt, len(txs))
		signer   = LatestSigner(params.TestChainConfig)
		uncles   = make([]*Header, 3)
	)
	header := &Header{
		Difficulty: math.BigPow(11, 11),
		Number:     math.BigPow(2, 9),
		GasLimit:   12345678,
		GasUsed:    1476322,
		Time:       9876543,
		Extra:      []byte("coolest block on chain"),
	}
	for i := range txs {
		amount, _ := uint256.FromBig(math.BigPow(2, int64(i)))
		price := uint256.NewInt(300000)
		data := make([]byte, 100)
		tx := NewTransaction(uint64(i), libcommon.Address{}, amount, 123457, price, data)
		signedTx, err := SignTx(tx, *signer, key)
		if err != nil {
			panic(err)
		}
		txs[i] = signedTx
		receipts[i] = NewReceipt(false, tx.GetGas())
	}
	for i := range uncles {
		uncles[i] = &Header{
			Difficulty: math.BigPow(11, 11),
			Number:     math.BigPow(2, 9),
			GasLimit:   12345678,
			GasUsed:    1476322,
			Time:       9876543,
			Extra:      []byte("benchmark uncle"),
		}
	}
	return NewBlock(header, txs, uncles, receipts, nil /* withdrawals */)
}

func TestCanEncodeAndDecodeRawBody(t *testing.T) {
	body := &RawBody{
		Uncles: []*Header{
			{
				ParentHash:  libcommon.Hash{},
				UncleHash:   libcommon.Hash{},
				Coinbase:    libcommon.Address{},
				Root:        libcommon.Hash{},
				TxHash:      libcommon.Hash{},
				ReceiptHash: libcommon.Hash{},
				Bloom:       Bloom{},
				Difficulty:  big.NewInt(100),
				Number:      big.NewInt(1000),
				GasLimit:    50,
				GasUsed:     60,
				Time:        90,
				Extra:       []byte("testing"),
			},
			{
				GasUsed:    108,
				GasLimit:   100,
				Difficulty: big.NewInt(99),
				Number:     big.NewInt(1000),
			},
		},
		Transactions: [][]byte{
			{
				0xc0 + 3, 10, 20, 30,
			},
			{
				0xc0 + 3, 40, 50, 60,
			},
		},
	}
	expectedJson, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	writer := bytes.NewBuffer(nil)
	err = body.EncodeRLP(writer)
	if err != nil {
		t.Fatal(err)
	}
	rlpBytes := common.CopyBytes(writer.Bytes())
	writer.Reset()
	writer.WriteString(hexutility.Encode(rlpBytes))

	var rawBody RawBody
	fromHex := common.CopyBytes(common.FromHex(writer.String()))
	bodyReader := bytes.NewReader(fromHex)
	stream := rlp.NewStream(bodyReader, 0)

	err = rawBody.DecodeRLP(stream)
	if err != nil {
		t.Fatal(err)
	}

	resultJson, err := json.Marshal(rawBody)
	if err != nil {
		t.Fatal(err)
	}

	if len(rawBody.Transactions) != 2 {
		t.Fatalf("expected there to be 1 transaction once decoded")
	}
	if rawBody.Transactions[0][1] != 10 {
		t.Fatal("expected first element in transactions to be 10")
	}
	if rawBody.Transactions[1][3] != 60 {
		t.Fatal("expected 2nd element in transactions to end in 60")
	}
	if rawBody.Uncles[0].GasLimit != 50 {
		t.Fatal("expected gas limit of first uncle to be 50")
	}
	if rawBody.Uncles[1].GasLimit != 100 {
		t.Fatal("expected gas limit of 2nd uncle to be 100")
	}
	if string(resultJson) != string(expectedJson) {
		t.Fatalf("encoded and decoded json do not match, got\n%s\nwant\n%s", resultJson, expectedJson)
	}
}

func TestAuRaHeaderEncoding(t *testing.T) {
	difficulty, ok := new(big.Int).SetString("8398142613866510000000000000000000000000000000", 10)
	require.True(t, ok)

	header := Header{
		ParentHash:  libcommon.HexToHash("0x8b00fcf1e541d371a3a1b79cc999a85cc3db5ee5637b5159646e1acd3613fd15"),
		UncleHash:   libcommon.HexToHash("1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"),
		Coinbase:    libcommon.HexToAddress("0x571846e42308df2dad8ed792f44a8bfddf0acb4d"),
		Root:        libcommon.HexToHash("0x351780124dae86b84998c6d4fe9a88acfb41b4856b4f2c56767b51a4e2f94dd4"),
		TxHash:      libcommon.HexToHash("0x6a35133fbff7ea2cb5ee7635c9fb623f96d31d689d806a2bfe40a2b1d90ee99c"),
		ReceiptHash: libcommon.HexToHash("0x324f54860e214ea896ea7a05bda30f85541be3157de77a9059a04fdb1e86badd"),
		Difficulty:  difficulty,
		Number:      big.NewInt(24679923),
		GasLimit:    30_000_000,
		GasUsed:     3_074_345,
		Time:        1666343339,
		Extra:       common.FromHex("0x1234"),
		BaseFee:     big.NewInt(7_000_000_000),
		AuRaStep:    13078,
		AuRaSeal:    common.FromHex("0x75bda30f85541be059646e1acd3613fd100846e42308df2dad8ed79b9a9e91c9db994386599a683820a1394684d41fc139c4805684142e6b15a722a2e9cc51f7ee"),
	}

	encoded, err := rlp.EncodeToBytes(&header)
	require.NoError(t, err)

	var decoded Header
	require.NoError(t, rlp.DecodeBytes(encoded, &decoded))

	assert.Equal(t, header, decoded)
}

func TestWithdrawalsEncoding(t *testing.T) {
	header := Header{
		ParentHash: libcommon.HexToHash("0x8b00fcf1e541d371a3a1b79cc999a85cc3db5ee5637b5159646e1acd3613fd15"),
		Coinbase:   libcommon.HexToAddress("0x571846e42308df2dad8ed792f44a8bfddf0acb4d"),
		Root:       libcommon.HexToHash("0x351780124dae86b84998c6d4fe9a88acfb41b4856b4f2c56767b51a4e2f94dd4"),
		Difficulty: common.Big0,
		Number:     big.NewInt(20_000_000),
		GasLimit:   30_000_000,
		GasUsed:    3_074_345,
		Time:       1666343339,
		Extra:      make([]byte, 0),
		MixDigest:  libcommon.HexToHash("0x7f04e338b206ef863a1fad30e082bbb61571c74e135df8d1677e3f8b8171a09b"),
		BaseFee:    big.NewInt(7_000_000_000),
	}

	withdrawals := make([]*Withdrawal, 2)
	withdrawals[0] = &Withdrawal{
		Index:     44555666,
		Validator: 89,
		Address:   libcommon.HexToAddress("0x690b9a9e9aa1c9db991c7721a92d351db4fac990"),
		Amount:    *uint256.NewInt(2 * params.Ether),
	}
	withdrawals[1] = &Withdrawal{
		Index:     44555667,
		Validator: 37,
		Address:   libcommon.HexToAddress("0x95222290dd7278aa3ddd389cc1e1d165cc4bafe5"),
		Amount:    *uint256.NewInt(5 * params.Ether),
	}

	block := NewBlock(&header, nil, nil, nil, withdrawals)
	_ = block.Size()

	encoded, err := rlp.EncodeToBytes(block)
	require.NoError(t, err)

	var decoded Block
	require.NoError(t, rlp.DecodeBytes(encoded, &decoded))

	assert.Equal(t, block, &decoded)

	// Now test with empty withdrawals
	block2 := NewBlock(&header, nil, nil, nil, []*Withdrawal{})
	_ = block2.Size()

	encoded2, err := rlp.EncodeToBytes(block2)
	require.NoError(t, err)

	var decoded2 Block
	require.NoError(t, rlp.DecodeBytes(encoded2, &decoded2))

	assert.Equal(t, block2, &decoded2)
}

func TestBlockRawBodyPreShanghai(t *testing.T) {
	require := require.New(t)

	const rawBodyForStorageRlp = "f901f4c0f901f0f901eda00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000940000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000808080808080a00000000000000000000000000000000000000000000000000000000000000000880000000000000000"
	bstring, _ := hex.DecodeString(rawBodyForStorageRlp)

	body := new(RawBody)
	rlp.DecodeBytes(bstring, body)

	require.Nil(body.Withdrawals)
	require.Equal(1, len(body.Uncles))
	require.Equal(0, len(body.Transactions))
}

func TestBlockRawBodyPostShanghaiNoWithdrawals(t *testing.T) {
	require := require.New(t)

	const rawBodyForStorageRlp = "f901f5c0f901f0f901eda00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000940000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000808080808080a00000000000000000000000000000000000000000000000000000000000000000880000000000000000c0"
	bstring, _ := hex.DecodeString(rawBodyForStorageRlp)

	body := new(RawBody)
	rlp.DecodeBytes(bstring, body)

	require.NotNil(body.Withdrawals)
	require.Equal(0, len(body.Withdrawals))
	require.Equal(1, len(body.Uncles))
	require.Equal(0, len(body.Transactions))
}

func TestBlockRawBodyPostShanghaiWithdrawals(t *testing.T) {
	require := require.New(t)

	const rawBodyForStorageRlp = "f90230c0f901f0f901eda00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000940000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000808080808080a00000000000000000000000000000000000000000000000000000000000000000880000000000000000f83adc0f82157c94ff000000000000000000000000000000000000008203e8dc1082157d94ff000000000000000000000000000000000000008203e9"

	bstring, _ := hex.DecodeString(rawBodyForStorageRlp)

	body := new(RawBody)
	rlp.DecodeBytes(bstring, body)

	require.NotNil(body.Withdrawals)
	require.Equal(1, len(body.Uncles))
	require.Equal(0, len(body.Transactions))
	require.Equal(2, len(body.Withdrawals))
}
