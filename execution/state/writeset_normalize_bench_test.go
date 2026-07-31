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

	"github.com/erigontech/erigon/execution/types/accounts"
)

func benchAddr(i int) accounts.Address {
	return accounts.InternAddress([20]byte{byte(i >> 8), byte(i)})
}

func benchKey(i int) accounts.StorageKey {
	return accounts.InternKey([32]byte{byte(i >> 8), byte(i)})
}

// buildNormalizeInput models a typical tx write set: addrs accounts with
// balance+nonce writes, each with slots storage writes, plus a few
// storage-only addresses (they exercise the account-field fill loop).
func buildNormalizeInput(addrs, slots int) *WriteSet {
	ws := &WriteSet{}
	ver := Version{TxIndex: 0, Incarnation: 0}
	for i := range addrs {
		a := benchAddr(i)
		ws.SetBalance(a, &VersionedWrite[uint256.Int]{
			WriteHeader: WriteHeader{Address: a, Path: BalancePath, Version: ver},
			Val:         *uint256.NewInt(uint64(i + 1)),
		})
		ws.SetNonce(a, &VersionedWrite[uint64]{
			WriteHeader: WriteHeader{Address: a, Path: NoncePath, Version: ver},
			Val:         uint64(i + 1),
		})
		for s := range slots {
			k := benchKey(s)
			ws.SetStorage(a, k, &VersionedWrite[uint256.Int]{
				WriteHeader: WriteHeader{Address: a, Path: StoragePath, Key: k, Version: ver},
				Val:         *uint256.NewInt(uint64(s + 1)),
			})
		}
	}
	for i := range 2 {
		a := benchAddr(addrs + i)
		k := benchKey(i)
		ws.SetStorage(a, k, &VersionedWrite[uint256.Int]{
			WriteHeader: WriteHeader{Address: a, Path: StoragePath, Key: k, Version: ver},
			Val:         *uint256.NewInt(7),
		})
	}
	return ws
}

func BenchmarkWriteSetNormalize(b *testing.B) {
	for _, size := range []struct{ addrs, slots int }{{4, 2}, {16, 8}} {
		b.Run(fmt.Sprintf("addrs=%d/slots=%d", size.addrs, size.slots), func(b *testing.B) {
			ws := buildNormalizeInput(size.addrs, size.slots)
			vm := NewVersionMap(nil)
			vm.FlushVersionedWrites(ws, true, "")
			reader := &minimalStateReader{}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out, err := ws.Normalize(vm, 0, 0, reader, nil, true, false, false)
				if err != nil {
					b.Fatal(err)
				}
				sinkNormalized = out
			}
		})
	}
}

var sinkNormalized *WriteSet
