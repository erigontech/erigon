// Copyright 2026 The Erigon Authors
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

package state

import (
	"math/rand/v2"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func readOnlyBuilder(addrs []accounts.Address) *BlockAccessListBuilder {
	b := NewBlockAccessListBuilder()
	for _, a := range addrs {
		ensureAccountState(b.ac, a).updateReadBalance(*uint256.NewInt(1))
	}
	return b
}

func BenchmarkBlockAccessListValidate(b *testing.B) {
	r := rand.New(rand.NewPCG(1, 2))
	addrs := make([]accounts.Address, 48951)
	for i := range addrs {
		var a common.Address
		for j := range a {
			a[j] = byte(r.Uint32())
		}
		addrs[i] = accounts.InternAddress(a)
	}
	want := readOnlyBuilder(addrs).Build()
	wantHash := want.Hash()
	b.Run("build-hash", func(b *testing.B) {
		for b.Loop() {
			b.StopTimer()
			bb := readOnlyBuilder(addrs)
			b.StartTimer()
			if bb.Build().Hash() != wantHash {
				b.Fatal("mismatch")
			}
		}
	})
	b.Run("matches", func(b *testing.B) {
		for b.Loop() {
			b.StopTimer()
			bb := readOnlyBuilder(addrs)
			b.StartTimer()
			if !bb.Matches(want) {
				b.Fatal("mismatch")
			}
		}
	})
}
