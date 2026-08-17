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
	"maps"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
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

func nonceWrite(addr accounts.Address, val uint64, incarnation int) *VersionedWrite[uint64] {
	return &VersionedWrite[uint64]{
		WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: Version{TxIndex: 0, Incarnation: incarnation}},
		Val:         val,
	}
}

func storageWrite(addr accounts.Address, key accounts.StorageKey, val uint64) *VersionedWrite[uint256.Int] {
	return &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: key, Version: Version{TxIndex: 0}},
		Val:         *uint256.NewInt(val),
	}
}

func addressWrite(addr accounts.Address, nonce uint64) *VersionedWrite[*accounts.Account] {
	return &VersionedWrite[*accounts.Account]{
		WriteHeader: WriteHeader{Address: addr, Path: AddressPath, Version: Version{TxIndex: 0}},
		Val:         &accounts.Account{Nonce: nonce},
	}
}

func incarnationWrite(addr accounts.Address, val uint64) *VersionedWrite[uint64] {
	return &VersionedWrite[uint64]{
		WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath, Version: Version{TxIndex: 0}},
		Val:         val,
	}
}

func selfDestructWrite(addr accounts.Address, val bool) *VersionedWrite[bool] {
	return &VersionedWrite[bool]{
		WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: Version{TxIndex: 0}},
		Val:         val,
	}
}

func createContractWrite(addr accounts.Address, val bool) *VersionedWrite[bool] {
	return &VersionedWrite[bool]{
		WriteHeader: WriteHeader{Address: addr, Path: CreateContractPath, Version: Version{TxIndex: 0}},
		Val:         val,
	}
}

func codeWrite(addr accounts.Address, code []byte) *VersionedWrite[accounts.Code] {
	return &VersionedWrite[accounts.Code]{
		WriteHeader: WriteHeader{Address: addr, Path: CodePath, Version: Version{TxIndex: 0}},
		Val:         accounts.NewCode(code),
	}
}

func codeHashWrite(addr accounts.Address, b byte) *VersionedWrite[accounts.CodeHash] {
	return &VersionedWrite[accounts.CodeHash]{
		WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath, Version: Version{TxIndex: 0}},
		Val:         accounts.InternCodeHash(common.Hash{b}),
	}
}

func codeSizeWrite(addr accounts.Address, val int) *VersionedWrite[int] {
	return &VersionedWrite[int]{
		WriteHeader: WriteHeader{Address: addr, Path: CodeSizePath, Version: Version{TxIndex: 0}},
		Val:         val,
	}
}

// a conflicts on every path, b is prev-only, c is next-only, same shape across storage keys k1/k2/k3.
func mergeIntoFixture() (prev, next *WriteSet) {
	a, b, c := mergeAddr(0xa1), mergeAddr(0xb2), mergeAddr(0xc3)
	k1, k2, k3 := mergeKey(0x01), mergeKey(0x02), mergeKey(0x03)
	prev = newWriteSet(
		addressWrite(a, 5),
		balanceWrite(a, 1, 0),
		nonceWrite(a, 5, 0),
		incarnationWrite(a, 0),
		selfDestructWrite(a, false),
		createContractWrite(a, false),
		codeWrite(a, []byte{0x60, 0x00}),
		codeHashWrite(a, 0x11),
		codeSizeWrite(a, 2),
		storageWrite(a, k1, 10),
		storageWrite(a, k2, 20),
		addressWrite(b, 6),
		balanceWrite(b, 2, 0),
		nonceWrite(b, 7, 0),
		codeWrite(b, []byte{0x60, 0xaa}),
		codeSizeWrite(b, 9),
	)
	next = newWriteSet(
		addressWrite(a, 50),
		balanceWrite(a, 100, 1),
		nonceWrite(a, 50, 1),
		incarnationWrite(a, 1),
		selfDestructWrite(a, true),
		createContractWrite(a, true),
		codeWrite(a, []byte{0x60, 0x01, 0x02}),
		codeHashWrite(a, 0x22),
		codeSizeWrite(a, 3),
		storageWrite(a, k1, 111),
		storageWrite(a, k3, 33),
		balanceWrite(c, 3, 1),
		selfDestructWrite(c, true),
	)
	return prev, next
}

// writeKey excludes Version so two writes to the same (addr, path, key) collide, matching what a merge dedups on.
type writeKey struct {
	addr accounts.Address
	key  accounts.StorageKey
	path AccountPath
}

func writeKeysOf(s *WriteSet) map[writeKey]bool {
	keys := map[writeKey]bool{}
	for h := range s.AllHeaders() {
		keys[writeKey{addr: h.Address, key: h.Key, path: h.Path}] = true
	}
	return keys
}

func assertSameWrites(t *testing.T, want, got *WriteSet) {
	t.Helper()
	require.Equal(t, want.Count(), got.Count())
	for h := range want.AllHeaders() {
		require.True(t, got.Has(h), "missing header %v", h)
		assert.Equal(t, writeSetVal(want, h), writeSetVal(got, h), "value mismatch at %v", h)
	}
}

func TestWriteSetMergeInto_MatchesMergeOracle(t *testing.T) {
	prevOracle, nextOracle := mergeIntoFixture()
	oracle := prevOracle.Merge(nextOracle)

	prev, next := mergeIntoFixture()
	merged := prev.MergeInto(next)

	assert.Same(t, next, merged, "MergeInto must return next itself, not a copy")
	assertSameWrites(t, oracle, merged)
}

// A matched key keeps next's write, not prev's, but the merged set still holds every key either side wrote.
func TestWriteSetMergeInto_MatchedKeyKeepsNext(t *testing.T) {
	prev, next := mergeIntoFixture()
	prevKeys, nextKeys := writeKeysOf(prev), writeKeysOf(next)
	nextHeaders := map[writeKey]WriteHeader{}
	nextVals := map[writeKey]any{}
	for h := range next.AllHeaders() {
		k := writeKey{addr: h.Address, key: h.Key, path: h.Path}
		nextHeaders[k] = h
		nextVals[k] = writeSetVal(next, h)
	}
	require.NotZero(t, matchedKeys(prevKeys, nextKeys), "fixture must have matched keys")

	merged := prev.MergeInto(next)

	for k, h := range nextHeaders {
		require.True(t, merged.Has(h), "next's write dropped at %v", h)
		assert.Equal(t, nextVals[k], writeSetVal(merged, h), "next must win at %v", h)
	}
	union := prevKeys
	maps.Copy(union, nextKeys)
	assert.Equal(t, union, writeKeysOf(merged), "merged set must hold every key either side wrote")
}

func matchedKeys(a, b map[writeKey]bool) int {
	n := 0
	for k := range a {
		if b[k] {
			n++
		}
	}
	return n
}

// prev may be aliased and mutated by the caller after merging, so MergeInto must never touch prev's own maps.
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

// Mirrors the parallel apply loop's fee merge: prev a full tx write set, next the small fee output.
func buildMergeBenchSets(addrs, slots int) (*WriteSet, *WriteSet) {
	prev := &WriteSet{}
	for i := range addrs {
		addr := mergeAddr(byte(i + 1))
		prev.SetBalance(addr, balanceWrite(addr, uint64(i+1), 0))
		prev.SetNonce(addr, nonceWrite(addr, uint64(i), 0))
		for s := range slots {
			key := mergeKey(byte(s + 1))
			prev.SetStorage(addr, key, storageWrite(addr, key, uint64(s)))
		}
	}
	coinbase := mergeAddr(0xfe)
	next := newWriteSet(balanceWrite(coinbase, 1000, 1))
	return prev, next
}

// Inputs build inside the timed loop (MergeInto consumes next, so a pair can't be reused) — build cost is equal on both sides.
func benchWriteSetMerge(b *testing.B, addrs, slots int, merge func(prev, next *WriteSet) *WriteSet) {
	b.ReportAllocs()
	for b.Loop() {
		prev, next := buildMergeBenchSets(addrs, slots)
		sinkWS = merge(prev, next)
	}
}

func BenchmarkWriteSetMerge(b *testing.B) {
	for _, size := range []struct{ addrs, slots int }{{4, 2}, {16, 8}} {
		b.Run(fmt.Sprintf("addrs=%d/slots=%d", size.addrs, size.slots), func(b *testing.B) {
			benchWriteSetMerge(b, size.addrs, size.slots, (*WriteSet).Merge)
		})
	}
}

func BenchmarkWriteSetMergeInto(b *testing.B) {
	for _, size := range []struct{ addrs, slots int }{{4, 2}, {16, 8}} {
		b.Run(fmt.Sprintf("addrs=%d/slots=%d", size.addrs, size.slots), func(b *testing.B) {
			benchWriteSetMerge(b, size.addrs, size.slots, (*WriteSet).MergeInto)
		})
	}
}

// ReleaseMaps returns containers to their pools but leaves VersionedWrite values untouched — they may still be shared after MergeInto.
func TestWriteSetReleaseMaps(t *testing.T) {
	a, b := mergeAddr(0xa1), mergeAddr(0xb2)
	k2 := mergeKey(0x02)

	prev, next := mergeIntoFixture()
	merged := prev.MergeInto(next)

	prev.ReleaseMaps()
	assert.True(t, prev.IsEmpty(), "released set must be empty")

	aw, ok := merged.address[b]
	require.True(t, ok)
	require.NotNil(t, aw.Val, "shared address write must not be released")
	assert.Equal(t, uint64(6), aw.Val.Nonce)
	cw, ok := merged.code[b]
	require.True(t, ok)
	assert.Equal(t, []byte{0x60, 0xaa}, cw.Val.Bytes, "shared code write must not be released")
	bw, ok := merged.balance[b]
	require.True(t, ok)
	assert.Equal(t, *uint256.NewInt(2), bw.Val)
	nw, ok := merged.nonce[b]
	require.True(t, ok)
	assert.Equal(t, uint64(7), nw.Val)
	csw, ok := merged.codeSize[b]
	require.True(t, ok)
	assert.Equal(t, 9, csw.Val)
	sw, ok := merged.storage[a][k2]
	require.True(t, ok)
	assert.Equal(t, *uint256.NewInt(20), sw.Val)

	prev.SetBalance(a, balanceWrite(a, 7, 2))
	bw, ok = prev.balance[a]
	require.True(t, ok)
	assert.Equal(t, *uint256.NewInt(7), bw.Val)

	prev.ReleaseMaps()
	prev.ReleaseMaps()
}

func TestVersionedIOReleaseOutputMaps(t *testing.T) {
	io := NewVersionedIO(2)
	ws, _ := mergeIntoFixture()
	io.RecordWrites(Version{TxIndex: 0}, ws)

	require.NotZero(t, io.WriteCount())
	io.ReleaseOutputMaps()

	// Direct field access: the normal accessors panic once outputs are released.
	for _, output := range io.outputs {
		assert.True(t, output.IsEmpty(), "slots must be empty after release")
	}

	io.ReleaseOutputMaps()
	NewVersionedIO(1).ReleaseOutputMaps()
}

func benchVersionedFeeMerge(b *testing.B, addrs, slots int, merge func(prev, next *WriteSet) *WriteSet) {
	io := NewVersionedIO(1)
	vm := NewVersionMap(nil)
	version := Version{TxIndex: 0, Incarnation: 1}
	b.ReportAllocs()
	for b.Loop() {
		txOut, tip := buildMergeBenchSets(addrs, slots)
		io.RecordWrites(version, txOut)
		merged := merge(txOut, tip)
		io.RecordWrites(version, merged)
		vm.FlushVersionedWrites(merged, true, "")
		sinkWS = merged
	}
}

func BenchmarkVersionedFeeMergeClone(b *testing.B) {
	for _, size := range []struct{ addrs, slots int }{{4, 2}, {16, 8}} {
		b.Run(fmt.Sprintf("addrs=%d/slots=%d", size.addrs, size.slots), func(b *testing.B) {
			benchVersionedFeeMerge(b, size.addrs, size.slots, (*WriteSet).Merge)
		})
	}
}

func BenchmarkVersionedFeeMergeInto(b *testing.B) {
	for _, size := range []struct{ addrs, slots int }{{4, 2}, {16, 8}} {
		b.Run(fmt.Sprintf("addrs=%d/slots=%d", size.addrs, size.slots), func(b *testing.B) {
			benchVersionedFeeMerge(b, size.addrs, size.slots, (*WriteSet).MergeInto)
		})
	}
}

var sinkWS *WriteSet
