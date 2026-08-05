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

package cache

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
)

func applyAllTestCache(t *testing.T) *StateCache {
	t.Helper()
	c := NewStateCache(1<<20, 1<<20, 1<<20, 1<<20)
	t.Cleanup(c.Close)
	return c
}

// ApplyAll must be observationally identical to per-key Apply: same entries,
// same deletions and cascades, same frontier advance (so the same fills are
// rejected afterwards). Only the locking is batched.
func TestApplierApplyAllMatchesPerKeyApply(t *testing.T) {
	t.Parallel()

	addr := make([]byte, 20)
	addr[0] = 1
	deleted := make([]byte, 20)
	deleted[0] = 2
	slot := make([]byte, 52)
	slot[0] = 3
	code := []byte{0x60, 0x00, 0x60, 0x00}

	updates := []Update{
		{Domain: kv.AccountsDomain, Key: append([]byte(nil), addr...), Val: []byte{1}, TxNum: 30},
		{Domain: kv.AccountsDomain, Key: append([]byte(nil), deleted...), Val: nil, TxNum: 31},
		{Domain: kv.StorageDomain, Key: append([]byte(nil), slot...), Val: []byte{7}, TxNum: 32},
		{Domain: kv.CodeDomain, Key: append([]byte(nil), addr...), Val: append([]byte(nil), code...), TxNum: 33},
	}

	perKey := applyAllTestCache(t)
	for _, u := range updates {
		perKey.Applier().Apply(u.Domain, u.Key, u.Val, u.TxNum)
	}
	batched := applyAllTestCache(t)
	batched.Applier().ApplyAll(append([]Update(nil), updates...))

	for name, c := range map[string]*StateCache{"per-key": perKey, "batched": batched} {
		v, ok := c.View(nil).Get(kv.AccountsDomain, addr)
		require.True(t, ok, name)
		require.Equal(t, []byte{1}, v, name)
		_, ok = c.View(nil).Get(kv.AccountsDomain, deleted)
		require.False(t, ok, name)
		v, ok = c.View(nil).Get(kv.StorageDomain, slot)
		require.True(t, ok, name)
		require.Equal(t, []byte{7}, v, name)
		gotCode, ok := c.View(nil).GetCodeByHash(crypto.Keccak256(code))
		require.True(t, ok, name)
		require.Equal(t, code, gotCode, name)

		staleView := c.View(FrontierFunc(func(kv.Domain) (uint64, bool) { return 20, true }))
		staleKey := make([]byte, 20)
		staleKey[0] = 9
		staleView.Fill(kv.AccountsDomain, staleKey, []byte{9}, 5)
		_, ok = c.View(nil).Get(kv.AccountsDomain, staleKey)
		require.False(t, ok, "%s: the batch apply must advance the frontier and reject stale fills", name)
	}
}

// One batch may span several chunks; entries on both sides of the chunk
// boundary must land.
func TestApplierApplyAllCrossesChunkBoundary(t *testing.T) {
	t.Parallel()

	c := applyAllTestCache(t)
	n := applyChunkSize + 3
	updates := make([]Update, 0, n)
	for i := range n {
		key := make([]byte, 20)
		binary.BigEndian.PutUint32(key, uint32(i))
		updates = append(updates, Update{Domain: kv.AccountsDomain, Key: key, Val: []byte{1}, TxNum: uint64(i)})
	}
	c.Applier().ApplyAll(updates)

	for _, i := range []int{0, applyChunkSize - 1, applyChunkSize, n - 1} {
		key := make([]byte, 20)
		binary.BigEndian.PutUint32(key, uint32(i))
		_, ok := c.View(nil).Get(kv.AccountsDomain, key)
		require.True(t, ok, "index %d", i)
	}
}

// The admission counters distinguish surviving reader warming from rejected
// stale fills.
func TestFillAdmissionCounters(t *testing.T) {
	t.Parallel()

	c := applyAllTestCache(t)
	fresh := c.View(FrontierFunc(func(kv.Domain) (uint64, bool) { return 100, true }))
	key := make([]byte, 20)
	key[0] = 1
	fresh.Fill(kv.AccountsDomain, key, []byte{1}, 50)
	require.EqualValues(t, 1, c.fillsAdmitted.Load())
	require.EqualValues(t, 0, c.fillsRejected.Load())

	c.Applier().Apply(kv.AccountsDomain, key, []byte{2}, 200)
	stale := c.View(FrontierFunc(func(kv.Domain) (uint64, bool) { return 100, true }))
	stale.Fill(kv.AccountsDomain, key, []byte{1}, 50)
	require.EqualValues(t, 1, c.fillsAdmitted.Load())
	require.EqualValues(t, 1, c.fillsRejected.Load())
}

func BenchmarkApplierApply(b *testing.B) {
	for _, batched := range []bool{false, true} {
		b.Run(fmt.Sprintf("batched=%t", batched), func(b *testing.B) {
			c := NewStateCache(64<<20, 64<<20, 64<<20, 64<<20)
			defer c.Close()
			const n = 100_000
			updates := make([]Update, 0, n)
			for i := range n {
				key := make([]byte, 20)
				binary.BigEndian.PutUint32(key, uint32(i))
				updates = append(updates, Update{Domain: kv.AccountsDomain, Key: key, Val: key[:8], TxNum: uint64(i)})
			}
			applier := c.Applier()
			b.ResetTimer()
			for b.Loop() {
				if batched {
					applier.ApplyAll(updates)
				} else {
					for _, u := range updates {
						applier.Apply(u.Domain, u.Key, u.Val, u.TxNum)
					}
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/n, "ns/update")
		})
	}
}
