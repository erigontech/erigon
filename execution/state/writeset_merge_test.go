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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

func mergeAddr(b byte) accounts.Address {
	return accounts.InternAddress([20]byte{b})
}

func mergeKey(b byte) accounts.StorageKey {
	return accounts.InternKey([32]byte{b})
}

func balanceWrite(addr accounts.Address, val uint64, incarnation int) *VersionedWrite[uint256.Int] {
	return &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 0, Incarnation: incarnation}},
		Val:         *uint256.NewInt(val),
	}
}

func nonceWrite(addr accounts.Address, val uint64) *VersionedWrite[uint64] {
	return &VersionedWrite[uint64]{
		WriteHeader: WriteHeader{Address: addr, Path: NoncePath},
		Val:         val,
	}
}

func storageWrite(addr accounts.Address, key accounts.StorageKey, val uint64) *VersionedWrite[uint256.Int] {
	return &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: key, Version: Version{TxIndex: 0}},
		Val:         *uint256.NewInt(val),
	}
}

func mergeIntoFixture() (prev, next *WriteSet) {
	a, b, c := mergeAddr(0xa1), mergeAddr(0xb2), mergeAddr(0xc3)
	k1, k2 := mergeKey(0x01), mergeKey(0x02)
	prev = newWriteSet(
		balanceWrite(a, 1, 0),
		nonceWrite(a, 5),
		storageWrite(a, k1, 10),
		storageWrite(a, k2, 20),
		balanceWrite(b, 2, 0),
	)
	next = newWriteSet(
		balanceWrite(a, 100, 1),
		storageWrite(a, k1, 111),
		balanceWrite(c, 3, 1),
	)
	return prev, next
}

func assertSameWrites(t *testing.T, want, got *WriteSet) {
	t.Helper()
	require.Equal(t, want.Count(), got.Count())
	for h := range want.AllHeaders() {
		assert.True(t, got.Has(h), "missing header %v", h)
	}
	for addr, vw := range want.balance {
		gotVW, ok := got.balance[addr]
		require.True(t, ok)
		assert.Equal(t, vw.Val, gotVW.Val, "balance mismatch at %v", addr)
		assert.Equal(t, vw.Version, gotVW.Version, "balance version mismatch at %v", addr)
	}
	for addr, inner := range want.storage {
		for key, vw := range inner {
			gotVW, ok := got.storage[addr][key]
			require.True(t, ok)
			assert.Equal(t, vw.Val, gotVW.Val, "storage mismatch at %v/%v", addr, key)
		}
	}
}

// MergeInto must produce the same union as the cloning Merge: next wins on
// (addr, path, key), prev fills the gaps.
func TestWriteSetMergeInto_MatchesMergeOracle(t *testing.T) {
	prevOracle, nextOracle := mergeIntoFixture()
	oracle := prevOracle.Merge(nextOracle)

	prev, next := mergeIntoFixture()
	merged := prev.MergeInto(next)

	assert.Same(t, next, merged, "MergeInto must return next itself, not a copy")
	assertSameWrites(t, oracle, merged)
}

// prev is aliased by execResult.TxOut, which finalize later reads and mutates
// (StripBalanceWrite deletes map entries). MergeInto must never touch prev's
// maps, and deleting from prev afterwards must not affect the merged set.
func TestWriteSetMergeInto_PrevStaysIndependent(t *testing.T) {
	a := mergeAddr(0xa1)
	b := mergeAddr(0xb2)

	prev, next := mergeIntoFixture()
	prevCount := prev.Count()
	merged := prev.MergeInto(next)

	assert.Equal(t, prevCount, prev.Count(), "MergeInto must not grow or shrink prev")
	bw, ok := prev.balance[a]
	require.True(t, ok)
	assert.Equal(t, *uint256.NewInt(1), bw.Val, "prev's own value must survive")

	// Simulate StripBalanceWrite on TxOut after the merge.
	delete(prev.balance, b)
	gotVW, ok := merged.balance[b]
	require.True(t, ok, "merged set must keep entries deleted from prev")
	assert.Equal(t, *uint256.NewInt(2), gotVW.Val)
}

func TestWriteSetMergeInto_EmptyEdges(t *testing.T) {
	prev, next := mergeIntoFixture()

	empty := &WriteSet{}
	assert.Same(t, next, empty.MergeInto(next), "empty prev returns next")
	assert.Same(t, prev, prev.MergeInto(&WriteSet{}), "empty next returns prev")
	assert.Same(t, prev, prev.MergeInto(nil), "nil next returns prev")
	assert.Same(t, next, (*WriteSet)(nil).MergeInto(next), "nil prev returns next")
}

// Fee-merge shape from the parallel apply loop: prev is a full tx write set,
// next is the small calcFees output.
func buildMergeBenchSets(addrs, slots int) (*WriteSet, *WriteSet) {
	prev := &WriteSet{}
	for i := range addrs {
		addr := mergeAddr(byte(i + 1))
		prev.SetBalance(addr, balanceWrite(addr, uint64(i+1), 0))
		prev.SetNonce(addr, nonceWrite(addr, uint64(i)))
		for s := range slots {
			key := mergeKey(byte(s + 1))
			prev.SetStorage(addr, key, storageWrite(addr, key, uint64(s)))
		}
	}
	coinbase := mergeAddr(0xfe)
	next := newWriteSet(balanceWrite(coinbase, 1000, 1))
	return prev, next
}

func BenchmarkWriteSetMerge(b *testing.B) {
	for _, size := range []struct{ addrs, slots int }{{4, 2}, {16, 8}} {
		b.Run(fmt.Sprintf("addrs=%d/slots=%d", size.addrs, size.slots), func(b *testing.B) {
			prev, next := buildMergeBenchSets(size.addrs, size.slots)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sinkWS = prev.Merge(next)
			}
		})
	}
}

func BenchmarkWriteSetMergeInto(b *testing.B) {
	for _, size := range []struct{ addrs, slots int }{{4, 2}, {16, 8}} {
		b.Run(fmt.Sprintf("addrs=%d/slots=%d", size.addrs, size.slots), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				prev, next := buildMergeBenchSets(size.addrs, size.slots)
				b.StartTimer()
				sinkWS = prev.MergeInto(next)
			}
		})
	}
}

var sinkWS *WriteSet
