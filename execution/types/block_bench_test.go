// Copyright 2014 The go-ethereum Authors
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

package types

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
)

func BenchmarkEncodeBlock(b *testing.B) {
	block := makeBenchBlock()

	for b.Loop() {
		benchBuffer.Reset()
		if err := rlp.Encode(benchBuffer, block); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBodyOnlyTxnDecodeRLPBytes(b *testing.B) {
	enc := encodedBenchBody(b)

	var out BodyOnlyTxn
	b.ReportAllocs()
	for b.Loop() {
		if err := out.DecodeRLPBytes(enc); err != nil {
			b.Fatal(err)
		}
	}
	if out.BaseTxnID != BaseTxnID(1234567) || out.TxCount != 250 {
		b.Fatalf("unexpected decode result: %+v", out)
	}
}

func BenchmarkBodyForStorageDecodeBytes(b *testing.B) {
	enc := encodedBenchBody(b)

	var out BodyForStorage
	b.ReportAllocs()
	for b.Loop() {
		if err := rlp.DecodeBytes(enc, &out); err != nil {
			b.Fatal(err)
		}
	}
	if out.BaseTxnID != BaseTxnID(1234567) || out.TxCount != 250 {
		b.Fatalf("unexpected decode result: %+v", out)
	}
}

func BenchmarkDecodeBlock(b *testing.B) {
	block := makeBenchBlock()
	encoded, err := rlp.EncodeToBytes(block)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()

	for b.Loop() {
		var decoded Block
		if err := rlp.DecodeBytes(encoded, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func encodedBenchBody(b *testing.B) []byte {
	b.Helper()
	var buf bytes.Buffer
	if err := rlp.Encode(&buf, &BodyForStorage{BaseTxnID: BaseTxnID(1234567), TxCount: 250}); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}

var benchBuffer = bytes.NewBuffer(make([]byte, 0, 32000))

func makeBenchBlock() *Block {
	var (
		key, _   = crypto.GenerateKey()
		txs      = make([]Transaction, 70)
		receipts = make([]*Receipt, len(txs))
		signer   = LatestSigner(chain.AllProtocolChanges)
		uncles   = make([]*Header, 3)
	)
	header := &Header{
		Difficulty: *uint256.NewInt(285311670611), // 11^11
		Number:     *uint256.NewInt(0x200),        // 2^9
		GasLimit:   12345678,
		GasUsed:    1476322,
		Time:       9876543,
		Extra:      []byte("coolest block on chain"),
	}
	for i := range txs {
		amount, _ := uint256.FromBig(math.BigPow(2, int64(i)))
		price := uint256.NewInt(300000)
		data := make([]byte, 100)
		tx := NewTransaction(uint64(i), common.Address{}, amount, 123457, price, data)
		signedTx, err := SignTx(tx, *signer, key)
		if err != nil {
			panic(err)
		}
		txs[i] = signedTx
		receipts[i] = NewReceipt(false, tx.GetGasLimit())
	}
	for i := range uncles {
		uncles[i] = &Header{
			Difficulty: *uint256.NewInt(285311670611), // 11^11
			Number:     *uint256.NewInt(0x200),        // 2^9
			GasLimit:   12345678,
			GasUsed:    1476322,
			Time:       9876543,
			Extra:      []byte("benchmark uncle"),
		}
	}
	return NewBlock(header, txs, uncles, receipts, nil /* withdrawals */, nil)
}
