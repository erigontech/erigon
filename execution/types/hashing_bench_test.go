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

	"github.com/erigontech/erigon/execution/rlp"
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

func BenchmarkDeriveShaRawValues(b *testing.B) {
	for _, count := range []int{128, 4096, 10_000_000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			encoded := bytes.Repeat([]byte{rlp.EmptyListCode}, count)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := DeriveShaRawValues(encoded); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRlpHashHeader(b *testing.B) {
	for b.Loop() {
		RlpHash(benchHeader)
	}
}
