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

package commitment

import (
	"bytes"
	"encoding/binary"
	"sort"
	"testing"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func hexNibbles(b []byte) []byte {
	out := make([]byte, len(b)*2)
	for i, x := range b {
		out[2*i] = x >> 4
		out[2*i+1] = x & 0x0f
	}
	return out
}

// keyForBranch builds the CommitmentDomain key for a branch of contract H's
// storage subtree reached by the given slot-path nibbles.
func keyForBranch(contractNibbles, slotPath []byte) []byte {
	full := make([]byte, 0, len(contractNibbles)+len(slotPath))
	full = append(full, contractNibbles...)
	full = append(full, slotPath...)
	return nibbles.HexToCompact(full)
}

func inRange(k, from, to []byte) bool {
	return bytes.Compare(k, from) >= 0 && (to == nil || bytes.Compare(k, to) < 0)
}

func TestContractTrunkKeyRanges(t *testing.T) {
	hashA := make([]byte, 32)
	for i := range hashA {
		hashA[i] = byte(7*i + 3) // arbitrary
	}
	hashB := make([]byte, 32)
	for i := range hashB {
		hashB[i] = byte(251 - 3*i)
	}
	nibA := hexNibbles(hashA)
	nibB := hexNibbles(hashB)

	evenFrom, evenTo, oddFrom, oddTo := contractTrunkKeyRanges(nibA)

	// Every branch of A at assorted slot-paths must land in exactly one range,
	// per the parity of its total nibble length.
	slotPaths := [][]byte{
		{},                          // depth 64 — subtree root
		{0x0},                       // 65
		{0xf},                       // 65
		{0x1, 0x2},                  // 66
		{0xf, 0xf},                  // 66
		{0x3, 0x4, 0x5},             // 67
		{0x6, 0x7, 0x8, 0x9},        // 68
		{0xa, 0xb, 0xc, 0xd, 0xe},   // 69
		make([]byte, 64),            // 128 — deepest
	}
	for _, sp := range slotPaths {
		k := keyForBranch(nibA, sp)
		total := 64 + len(sp)
		if total%2 == 0 {
			if !inRange(k, evenFrom, evenTo) {
				t.Fatalf("depth %d branch %x not in even range [%x,%x)", total, k, evenFrom, evenTo)
			}
			if inRange(k, oddFrom, oddTo) {
				t.Fatalf("depth %d (even) branch %x unexpectedly in odd range", total, k)
			}
		} else {
			if !inRange(k, oddFrom, oddTo) {
				t.Fatalf("depth %d branch %x not in odd range [%x,%x)", total, k, oddFrom, oddTo)
			}
			if inRange(k, evenFrom, evenTo) {
				t.Fatalf("depth %d (odd) branch %x unexpectedly in even range", total, k)
			}
		}
		// round-trip: CompactToHex(key) must reproduce the full path
		got := nibbles.CompactToHex(k)
		want := append(append([]byte{}, nibA...), sp...)
		if !bytes.Equal(got, want) {
			t.Fatalf("CompactToHex round-trip mismatch: got %x want %x", got, want)
		}
	}

	// A different contract's branches must be in neither of A's ranges.
	for _, sp := range slotPaths[:6] {
		k := keyForBranch(nibB, sp)
		if inRange(k, evenFrom, evenTo) || inRange(k, oddFrom, oddTo) {
			t.Fatalf("foreign-contract branch %x leaked into A's ranges", k)
		}
	}
}

func TestNextSubtree(t *testing.T) {
	cases := []struct{ in, want []byte }{
		{[]byte{0x01, 0x02}, []byte{0x01, 0x03}},
		{[]byte{0x01, 0xff}, []byte{0x02}},
		{[]byte{0x00}, []byte{0x01}},
	}
	for _, c := range cases {
		if got := nextSubtree(c.in); !bytes.Equal(got, c.want) {
			t.Fatalf("nextSubtree(%x) = %x, want %x", c.in, got, c.want)
		}
	}
	if nextSubtree([]byte{0xff, 0xff}) != nil {
		t.Fatalf("nextSubtree(0xffff) should be nil")
	}
}

// branchVal returns a deterministic branch-node value of size sz with a valid
// 4-byte header (touchMap||afterMap) — afterMap=0 so the BFS-children logic
// (in Run, not LoadBulk) would queue nothing; LoadBulk ignores the bitmap.
func branchVal(seed, sz int) []byte {
	if sz < 4 {
		sz = 4
	}
	v := make([]byte, sz)
	binary.BigEndian.PutUint16(v[0:2], uint16(seed))
	for i := 4; i < sz; i++ {
		v[i] = byte(seed + i)
	}
	return v
}

func TestLoadBulk_ShallowestFirstWithinBudget(t *testing.T) {
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = byte(0x40 + i)
	}
	nib := hexNibbles(hash)

	// Synthetic branch set at depths 64..69, mixed parity, fixed value size.
	const valSz = 100
	type ent struct {
		key   []byte
		val   []byte
		depth int
	}
	var ents []ent
	add := func(sp []byte) {
		ents = append(ents, ent{key: keyForBranch(nib, sp), val: branchVal(len(ents)+1, valSz), depth: 64 + len(sp)})
	}
	add([]byte{})                    // 64
	add([]byte{0x1})                 // 65
	add([]byte{0x2})                 // 65
	add([]byte{0x3, 0x4})            // 66
	add([]byte{0x5, 0x6})            // 66
	add([]byte{0x7, 0x8, 0x9})       // 67
	add([]byte{0xa, 0xb, 0xc, 0xd})  // 68
	add([]byte{0xe, 0xf, 0x0, 0x1, 0x2}) // 69

	// Fake range reader: yields the entries whose key falls in [from,to), in
	// ascending key order.
	rr := CommitmentRangeReader(func(from, to []byte, yield func(k, v []byte, step uint64) bool) error {
		var in []ent
		for _, e := range ents {
			if bytes.Compare(e.key, from) >= 0 && (to == nil || bytes.Compare(e.key, to) < 0) {
				in = append(in, e)
			}
		}
		sort.Slice(in, func(i, j int) bool { return bytes.Compare(in[i].key, in[j].key) < 0 })
		for _, e := range in {
			if !yield(e.key, e.val, 1) {
				return nil
			}
		}
		return nil
	})

	entryCost := estimatedEntryOverheadBytes + 33 /*≈compact key len*/ + valSz // approx; key lens are 33-36
	// budget for ~the 4 shallowest (depths 64,65,65,66) — give a bit of slack
	budget := 4*entryCost + 80

	c := NewBranchCache(100)
	p, err := NewContractTrunkPreload(hash)
	if err != nil {
		t.Fatal(err)
	}
	n, err := p.LoadBulk(budget, rr, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 || n != p.PinnedTotal() {
		t.Fatalf("LoadBulk pinned %d (PinnedTotal=%d), expected >0 and consistent", n, p.PinnedTotal())
	}
	if c.PinnedCount() != n {
		t.Fatalf("cache PinnedCount=%d != pinned %d", c.PinnedCount(), n)
	}
	// Sort the synthetic set the way LoadBulk does (shallowest first, key tie-break).
	sortedExpected := append([]ent(nil), ents...)
	sort.Slice(sortedExpected, func(i, j int) bool {
		if sortedExpected[i].depth != sortedExpected[j].depth {
			return sortedExpected[i].depth < sortedExpected[j].depth
		}
		return bytes.Compare(sortedExpected[i].key, sortedExpected[j].key) < 0
	})
	// The first n pinned must be exactly the n shallowest.
	for i := 0; i < n; i++ {
		v, _, ok := c.Get(sortedExpected[i].key)
		if !ok {
			t.Fatalf("expected branch #%d (depth %d, key %x) to be pinned, but it's not in cache", i, sortedExpected[i].depth, sortedExpected[i].key)
		}
		if !bytes.Equal(v, sortedExpected[i].val) {
			t.Fatalf("pinned branch #%d value mismatch", i)
		}
	}
	// The (n+1)-th shallowest must NOT be pinned (budget cut it off).
	if n < len(ents) {
		if _, _, ok := c.Get(sortedExpected[n].key); ok {
			t.Fatalf("branch #%d should have been excluded by the budget but is pinned", n)
		}
	}
	if p.MaxDepthReached() != sortedExpected[n-1].depth {
		t.Fatalf("MaxDepthReached=%d, expected %d", p.MaxDepthReached(), sortedExpected[n-1].depth)
	}

	// Phased extend: a larger total budget pins the rest.
	n2, err := p.LoadBulk(1<<20, rr, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n2 == 0 || p.PinnedTotal() != len(ents) {
		t.Fatalf("extend: pinned %d more, total %d, expected total %d", n2, p.PinnedTotal(), len(ents))
	}
	if c.PinnedCount() != len(ents) {
		t.Fatalf("after extend, cache PinnedCount=%d != %d", c.PinnedCount(), len(ents))
	}
}
