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
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func benchVMAddrs(n int) []accounts.Address {
	addrs := make([]accounts.Address, n)
	for i := range addrs {
		addrs[i] = accounts.InternAddress(common.BytesToAddress([]byte{byte(i), byte(i >> 8), 0x7c}))
	}
	return addrs
}

func fillVersionMap(vm *VersionMap, addrs []accounts.Address, txs int) {
	for tx := range txs {
		v := Version{TxIndex: tx, Incarnation: 0}
		for _, a := range addrs {
			vm.WriteBalance(a, v, *uint256.NewInt(uint64(tx)), true)
			vm.WriteNonce(a, v, uint64(tx), true)
		}
	}
}

// BenchmarkVersionMapBlockCycle models the per-block life of a version map:
// build it, write a block's worth of cells, then let the block end.
//
// "drop" is the behaviour before Release existed, where the map was discarded
// and every cell it held became garbage; "release" returns them to their pools.
func BenchmarkVersionMapBlockCycle(b *testing.B) {
	for _, n := range []int{16, 128} {
		for _, txs := range []int{8, 32} {
			addrs := benchVMAddrs(n)
			b.Run(fmt.Sprintf("drop/accounts=%d/txs=%d", n, txs), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					vm := NewVersionMap(nil)
					fillVersionMap(vm, addrs, txs)
				}
			})
			b.Run(fmt.Sprintf("release/accounts=%d/txs=%d", n, txs), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					vm := NewVersionMap(nil)
					fillVersionMap(vm, addrs, txs)
					vm.Release()
				}
			})
		}
	}
}
