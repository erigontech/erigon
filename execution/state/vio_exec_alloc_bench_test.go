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

package state

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Sinks defeat dead-code elimination so the benchmarked read isn't optimized away.
var (
	sinkU256 uint256.Int
	sinkU64  uint64
	sinkHash accounts.CodeHash
	sinkBool bool
)

// BenchmarkVersionMapRead_Typed measures the typed ReadXxx primitives, which
// return T directly with no interface box. The boxed Read().Value() arm was
// removed when the generic any-boxed VersionMap.Read API was deleted. Run:
//
//	go test ./execution/state/ -run=^$ -bench=BenchmarkVersionMapRead_Typed -benchmem
func BenchmarkVersionMapRead_Typed(b *testing.B) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x01})
	key := accounts.InternKey([32]byte{0x02})
	const txIdx = 100 // read above the writers' TxIndex so the map hit is a Done read

	mvhm.WriteBalance(addr, Version{TxIndex: 0}, *uint256.NewInt(12345), true)
	mvhm.WriteNonce(addr, Version{TxIndex: 0}, 1_000_003, true)
	mvhm.WriteCodeHash(addr, Version{TxIndex: 0}, accounts.InternCodeHash(common.HexToHash("0xaabb")), true)
	mvhm.WriteStorage(addr, key, Version{TxIndex: 0}, *uint256.NewInt(99), true)

	b.Run("Balance", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sinkU256, _, _ = mvhm.ReadBalance(addr, txIdx)
		}
	})

	b.Run("Nonce", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sinkU64, _, _ = mvhm.ReadNonce(addr, txIdx)
		}
	})

	b.Run("CodeHash", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sinkHash, _, _ = mvhm.ReadCodeHash(addr, txIdx)
		}
	})

	b.Run("Storage", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sinkU256, _, _ = mvhm.ReadStorage(addr, key, txIdx)
		}
	})
}

// BenchmarkVersionedExecReads drives the real IBS typed read path
// (GetBalance/GetNonce/GetCodeHash/GetState) over a versionMap pre-populated by
// prior "transactions", resetting per iteration to model the per-tx read
// boundary the parallel executor enforces. allocs/op here is the per-tx
// read-side garbage the typed-vio model aims to drive to zero; the non-storage
// paths currently box via ReadResult.Value() any. Run:
//
//	go test ./execution/state/ -run=^$ -bench=BenchmarkVersionedExecReads -benchmem
func BenchmarkVersionedExecReads(b *testing.B) {
	_, tx, domains := NewTestRwTx(b)
	mvhm := NewVersionMap(nil)

	const nAddrs = 64
	addrs := make([]accounts.Address, nAddrs)
	key := accounts.InternKey([32]byte{0x02})
	for i := range addrs {
		var a [20]byte
		a[0], a[1] = byte(i), byte(i>>8)
		addrs[i] = accounts.InternAddress(a)
		mvhm.WriteBalance(addrs[i], Version{TxIndex: 0}, *uint256.NewInt(uint64(1000 + i)), true)
		mvhm.WriteNonce(addrs[i], Version{TxIndex: 0}, uint64(i), true)
		mvhm.WriteCodeHash(addrs[i], Version{TxIndex: 0}, accounts.InternCodeHash(common.BytesToHash([]byte{0xaa, byte(i)})), true)
		mvhm.WriteStorage(addrs[i], key, Version{TxIndex: 0}, *uint256.NewInt(uint64(i)), true)
	}

	reader := NewReaderV3(domains.AsGetter(tx))
	ibs := NewWithVersionMap(reader, mvhm)
	defer ibs.Release(false)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ibs.Reset()
		ibs.SetTxContext(1, 100)
		a := addrs[i%nAddrs]
		bal, _ := ibs.GetBalance(a)
		sinkU256 = bal
		n, _ := ibs.GetNonce(a)
		sinkU64 = n
		h, _ := ibs.GetCodeHash(a)
		sinkHash = h
		s, _ := ibs.GetState(a, key)
		sinkU256 = s
	}
}
