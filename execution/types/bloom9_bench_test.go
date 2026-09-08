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
	"math/big"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

func BenchmarkBloom9(b *testing.B) {
	test := []byte("testestestest")
	for b.Loop() {
		Bloom9(test)
	}
}

func BenchmarkBloom9Lookup(b *testing.B) {
	toTest := []byte("testtest")
	bloom := new(Bloom)
	for b.Loop() {
		bloom.Test(toTest)
	}
}

func BenchmarkCreateBloom(b *testing.B) {

	one, _ := uint256.FromBig(big.NewInt(1))
	two, _ := uint256.FromBig(big.NewInt(2))

	var txs = Transactions{
		NewContractCreation(1, one, 1, one, nil),
		NewTransaction(2, common.HexToAddress("0x2"), two, 2, two, nil),
	}
	postState := common.Hash{2}
	var rSmall = Receipts{
		&Receipt{
			Status:            ReceiptStatusFailed,
			CumulativeGasUsed: 1,
			Logs: []*Log{
				{Address: common.BytesToAddress([]byte{0x11})},
				{Address: common.BytesToAddress([]byte{0x01, 0x11})},
			},
			TxHash:          txs[0].Hash(),
			ContractAddress: common.BytesToAddress([]byte{0x01, 0x11, 0x11}),
			GasUsed:         1,
		},
		&Receipt{
			PostState:         postState[:],
			CumulativeGasUsed: 3,
			Logs: []*Log{
				{Address: common.BytesToAddress([]byte{0x22})},
				{Address: common.BytesToAddress([]byte{0x02, 0x22})},
			},
			TxHash:          txs[1].Hash(),
			ContractAddress: common.BytesToAddress([]byte{0x02, 0x22, 0x22}),
			GasUsed:         2,
		},
	}

	var rLarge = make(Receipts, 200)
	// Fill it with 200 receipts x 2 logs
	for i := 0; i < 200; i += 2 {
		copy(rLarge[i:], rSmall)
	}
	var rLargeWithBloom = make(Receipts, len(rLarge))
	for i, receipt := range rLarge {
		cpy := *receipt
		cpy.Bloom = CreateBloom(Receipts{&cpy})
		rLargeWithBloom[i] = &cpy
	}
	b.Run("small", func(b *testing.B) {
		b.ReportAllocs()
		var bl Bloom
		for b.Loop() {
			bl = CreateBloom(rSmall)
		}
		b.StopTimer()
		var exp = common.HexToHash("c384c56ece49458a427c67b90fefe979ebf7104795be65dc398b280f24104949")
		got := crypto.Keccak256Hash(bl.Bytes())
		if got != exp {
			b.Errorf("Got %x, exp %x", got, exp)
		}
	})
	b.Run("large", func(b *testing.B) {
		b.ReportAllocs()
		var bl Bloom
		for b.Loop() {
			bl = CreateBloom(rLarge)
		}
		b.StopTimer()
		var exp = common.HexToHash("c384c56ece49458a427c67b90fefe979ebf7104795be65dc398b280f24104949")
		got := crypto.Keccak256Hash(bl.Bytes())
		if got != exp {
			b.Errorf("Got %x, exp %x", got, exp)
		}
	})
	b.Run("large/or-receipt-blooms", func(b *testing.B) {
		b.ReportAllocs()
		var bl Bloom
		for b.Loop() {
			bl = Bloom{}
			for _, receipt := range rLargeWithBloom {
				bl.Or(&receipt.Bloom)
			}
		}
		b.StopTimer()
		var exp = common.HexToHash("c384c56ece49458a427c67b90fefe979ebf7104795be65dc398b280f24104949")
		got := crypto.Keccak256Hash(bl.Bytes())
		if got != exp {
			b.Errorf("Got %x, exp %x", got, exp)
		}
	})
}
