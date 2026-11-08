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

package eth

import (
	"io"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
)

// BenchmarkHashOrNumberEncodeRLP pins why the hash branch encodes through a pointer:
// a common.Hash boxed by value is not addressable, so the reflection encoder copies
// it with reflect.New before it can take a byte slice of it.
func BenchmarkHashOrNumberEncodeRLP(b *testing.B) {
	hn := &HashOrNumber{Hash: common.Hash{1, 2, 3}}

	b.Run("byValue", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := rlp.Encode(io.Discard, hn.Hash); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("byPointer", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := rlp.Encode(io.Discard, &hn.Hash); err != nil {
				b.Fatal(err)
			}
		}
	})
}
