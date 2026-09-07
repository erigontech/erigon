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

package stagedsync

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func benchFeeDropSets(n int, coinbase accounts.Address) (prev, next *state.WriteSet) {
	base := &state.WriteSet{}
	for i := range n {
		var a common.Address
		binary.BigEndian.PutUint64(a[12:], uint64(i+1))
		addr := accounts.InternAddress(a)
		base.SetBalance(addr, &state.VersionedWrite[uint256.Int]{
			WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath}})
		base.SetNonce(addr, &state.VersionedWrite[uint64]{
			WriteHeader: state.WriteHeader{Address: addr, Path: state.NoncePath}})
		var h common.Hash
		binary.BigEndian.PutUint64(h[24:], uint64(i+1))
		key := accounts.InternKey(h)
		base.SetStorage(addr, key, &state.VersionedWrite[uint256.Int]{
			WriteHeader: state.WriteHeader{Address: addr, Path: state.StoragePath, Key: key}})
	}
	for _, tip := range []**state.WriteSet{&prev, &next} {
		ws := &state.WriteSet{}
		ws.SetBalance(coinbase, &state.VersionedWrite[uint256.Int]{
			WriteHeader: state.WriteHeader{Address: coinbase, Path: state.BalancePath}})
		ws.SetAddress(coinbase, &state.VersionedWrite[*accounts.Account]{
			WriteHeader: state.WriteHeader{Address: coinbase, Path: state.AddressPath}, Val: &accounts.Account{}})
		*tip = base.MergeInto(ws)
	}
	// The half of the credit this round stopped emitting.
	prev.SetSelfDestruct(coinbase, &state.VersionedWrite[bool]{
		WriteHeader: state.WriteHeader{Address: coinbase, Path: state.SelfDestructPath}, Val: true})
	return prev, next
}

// BenchmarkDropStaleVersionedWrites sizes the retraction scan against the tx's
// own write set: the credit it retracts is the same two addresses either way.
func BenchmarkDropStaleVersionedWrites(b *testing.B) {
	coinbase := feeMergeTestAddr("0x7777777777777777777777777777777777777777")
	burnt := feeMergeTestAddr("0x8888888888888888888888888888888888888888")
	version := state.Version{TxIndex: 0}

	for _, n := range []int{4, 32, 256} {
		b.Run(fmt.Sprintf("writes=%d", n*3), func(b *testing.B) {
			be := feeMergeTestExecutor(b)
			prev, next := benchFeeDropSets(n, coinbase)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				be.dropStaleVersionedWrites(version, prev, next, [2]accounts.Address{coinbase, burnt})
			}
		})
	}
}

func feeMergeBenchWorkerWrites(b *testing.B, addrs, slots int) *state.WriteSet {
	b.Helper()
	ws := &state.WriteSet{}
	for a := range addrs {
		addr := feeMergeTestAddr(fmt.Sprintf("0x%040x", a+1))
		ws.SetBalance(addr, &state.VersionedWrite[uint256.Int]{
			WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath},
			Val:         *uint256.NewInt(uint64(a)),
		})
		ws.SetNonce(addr, &state.VersionedWrite[uint64]{
			WriteHeader: state.WriteHeader{Address: addr, Path: state.NoncePath},
			Val:         uint64(a),
		})
		for k := range slots {
			key := accounts.InternKey(common.HexToHash(fmt.Sprintf("0x%x", k+1)))
			ws.SetStorage(addr, key, &state.VersionedWrite[uint256.Int]{
				WriteHeader: state.WriteHeader{Address: addr, Path: state.StoragePath, Key: key},
				Val:         *uint256.NewInt(uint64(k)),
			})
		}
	}
	return ws
}

func feeMergeBenchTip(version state.Version, coinbase, burnt accounts.Address, amount uint64) *state.WriteSet {
	tip := &state.WriteSet{}
	for _, addr := range [...]accounts.Address{coinbase, burnt} {
		acc := accounts.Account{Balance: *uint256.NewInt(amount), CodeHash: accounts.EmptyCodeHash}
		tip.SetBalance(addr, &state.VersionedWrite[uint256.Int]{
			WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath, Version: version},
			Val:         acc.Balance,
		})
		tip.SetAddress(addr, &state.VersionedWrite[*accounts.Account]{
			WriteHeader: state.WriteHeader{Address: addr, Path: state.AddressPath, Version: version},
			Val:         &acc,
		})
	}
	return tip
}

// BenchmarkRecordFeeMerge measures one re-credit round against a worker write
// set the size mainnet actually produces. The percentiles come from replaying
// blocks 25881700-15, where a tx that gets credited sees 12-24 rounds.
func BenchmarkRecordFeeMerge(b *testing.B) {
	coinbase := feeMergeTestAddr("0x00000000000000000000000000000000000000c0")
	burnt := feeMergeTestAddr("0x00000000000000000000000000000000000000b1")
	version := state.Version{TxIndex: 0, TxNum: 1}
	feeAddrs := [2]accounts.Address{coinbase, burnt}

	for _, size := range []struct {
		name         string
		addrs, slots int
	}{
		{"p50=4", 2, 0},
		{"p90=10", 2, 3},
		{"p99=24", 4, 4},
		{"max=444", 37, 10},
	} {
		b.Run(size.name, func(b *testing.B) {
			be := feeMergeTestExecutor(b)
			base := feeMergeBenchWorkerWrites(b, size.addrs, size.slots)
			be.recordWorkerWrites(version, base)
			be.recordFeeMerge(version, base, feeMergeBenchTip(version, coinbase, burnt, 1), feeCreditNew, feeAddrs)

			b.ReportAllocs()
			b.ResetTimer()
			rounds := 0
			for ; b.Loop(); rounds++ {
				tip := feeMergeBenchTip(version, coinbase, burnt, uint64(rounds+2))
				be.recordFeeMerge(version, be.blockIO.WriteSet(version.TxIndex), tip, feeCreditNew, feeAddrs)
			}
			b.StopTimer()
			// Only a rebuild supersedes a set, so this counts the rounds that
			// rebuilt rather than rewrote.
			b.ReportMetric(float64(len(be.superseded))/float64(rounds), "rebuilds/op")
			be.superseded.release()
		})
	}
}
