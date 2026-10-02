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

package hexutil

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"
)

func BenchmarkEncodeBig(b *testing.B) {
	inputs := make([]*big.Int, len(encodeBigTests))
	for i, test := range encodeBigTests {
		inputs[i] = test.input.(*big.Int)
	}
	b.ReportAllocs()
	for b.Loop() {
		for _, bigint := range inputs {
			EncodeBig(bigint)
		}
	}
}

func BenchmarkAppendQuoted(b *testing.B) {
	for _, n := range []int{20, 32, 64, 256, 1024} {
		src := make([]byte, n)
		for i := range src {
			src[i] = byte(i * 7)
		}
		dst := make([]byte, 0, QuotedLen(n))
		b.Run(fmt.Sprintf("stdlib/%d", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for b.Loop() {
				dst = append(hex.AppendEncode(append(dst[:0], `"0x`...), src), '"')
			}
		})
		b.Run(fmt.Sprintf("hexutil/%d", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for b.Loop() {
				dst = AppendQuoted(dst[:0], src)
			}
		})
	}
}
