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

func countStorage(ws *WriteSet) (n int) {
	for _, inner := range ws.storage {
		n += len(inner)
	}
	return n
}

func benchStorageVal(slot int) uint256.Int { return *uint256.NewInt(uint64(slot + 1)) }

func buildNormalizeInput(addrs, slots, txIndex int) *WriteSet {
	ws := &WriteSet{}
	ver := Version{TxIndex: txIndex, Incarnation: 0}
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
		if i%3 == 0 {
			code := accounts.NewCode([]byte{0x60, byte(i), 0x60, 0x00, 0x55})
			ws.SetCode(a, &VersionedWrite[accounts.Code]{
				WriteHeader: WriteHeader{Address: a, Path: CodePath, Version: ver},
				Val:         code,
			})
			ws.SetCodeHash(a, &VersionedWrite[accounts.CodeHash]{
				WriteHeader: WriteHeader{Address: a, Path: CodeHashPath, Version: ver},
				Val:         code.Hash,
			})
		}
		for s := range slots {
			k := benchKey(s)
			val := benchStorageVal(s)
			if s%2 == 1 {
				val = *uint256.NewInt(uint64(s + 1000))
			}
			ws.SetStorage(a, k, &VersionedWrite[uint256.Int]{
				WriteHeader: WriteHeader{Address: a, Path: StoragePath, Key: k, Version: ver},
				Val:         val,
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

// Flushes a prior tx's storage into the versionMap so write-backs hit the no-op filter instead of falling through to the state reader.
func seedPriorTx(vm *VersionMap, addrs, slots int) {
	prior := &WriteSet{}
	ver := Version{TxIndex: 0, Incarnation: 0}
	for i := range addrs {
		a := benchAddr(i)
		for s := range slots {
			k := benchKey(s)
			prior.SetStorage(a, k, &VersionedWrite[uint256.Int]{
				WriteHeader: WriteHeader{Address: a, Path: StoragePath, Key: k, Version: ver},
				Val:         benchStorageVal(s),
			})
		}
	}
	vm.FlushVersionedWrites(prior, true, "")
}

func BenchmarkWriteSetNormalize(b *testing.B) {
	const txIndex = 1
	for _, size := range []struct{ addrs, slots int }{{4, 2}, {16, 8}} {
		b.Run(fmt.Sprintf("addrs=%d/slots=%d", size.addrs, size.slots), func(b *testing.B) {
			ws := buildNormalizeInput(size.addrs, size.slots, txIndex)
			vm := NewVersionMap(nil)
			seedPriorTx(vm, size.addrs, size.slots)
			vm.FlushVersionedWrites(ws, true, "")
			reader := &minimalStateReader{}
			// Guards the shape being measured: if the no-op filter stops dropping writes, these numbers would silently mean something else.
			probe, err := ws.Normalize(vm, txIndex, 0, reader, nil, true, false, false)
			if err != nil {
				b.Fatal(err)
			}
			if kept, in := countStorage(probe), countStorage(ws); kept >= in {
				b.Fatalf("no-op filter dropped nothing: %d storage writes in, %d out", in, kept)
			}
			b.ReportAllocs()
			for b.Loop() {
				out, err := ws.Normalize(vm, txIndex, 0, reader, nil, true, false, false)
				if err != nil {
					b.Fatal(err)
				}
				sinkNormalized = out
			}
		})
	}
}

var sinkNormalized *WriteSet
