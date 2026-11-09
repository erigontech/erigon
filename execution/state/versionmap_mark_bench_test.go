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
	"sync"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

func BenchmarkMarkWritesValidated(b *testing.B) {
	const n = 4096
	vm := NewVersionMap(nil)
	writes := make([]*WriteSet, n)
	addrs := make([]accounts.Address, 0, 2*n)
	for i := range n {
		sender, recipient := getAddress(2*i+1), getAddress(2*i+2)
		v := Version{TxIndex: i, Incarnation: 1}
		ws := &WriteSet{}
		ws.SetBalance(sender, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: sender, Path: BalancePath, Key: accounts.NilKey, Version: v}, Val: *uint256.NewInt(uint64(i))})
		ws.SetNonce(sender, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: sender, Path: NoncePath, Key: accounts.NilKey, Version: v}, Val: uint64(i)})
		ws.SetBalance(recipient, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: recipient, Path: BalancePath, Key: accounts.NilKey, Version: v}, Val: *uint256.NewInt(uint64(i))})
		vm.FlushVersionedWrites(ws, false)
		writes[i] = ws
		addrs = append(addrs, sender, recipient)
	}
	for _, readers := range []int{0, 8} {
		b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
			stop := make(chan struct{})
			var wg sync.WaitGroup
			for r := range readers {
				wg.Go(func() {
					for j := r; ; j++ {
						select {
						case <-stop:
							return
						default:
						}
						vm.ReadBalance(addrs[j%len(addrs)], n+1)
					}
				})
			}
			i := 0
			for b.Loop() {
				vm.MarkWritesValidated(writes[i], nil)
				if i++; i == n {
					i = 0
				}
			}
			close(stop)
			wg.Wait()
		})
	}
}
