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

package v4

import (
	"bytes"
	"math/bits"
	"sort"
	"testing"
)

func collectPaths(t *testing.T, ctx *mockContext, addr [32]byte, n *node, out *[][]byte) {
	t.Helper()
	for nib := range 16 {
		bit := uint16(1) << nib
		if n.childMask&bit == 0 {
			continue
		}
		if n.leafMask&bit != 0 {
			full := append(append([]byte(nil), n.path...), byte(nib))
			suffix, _ := n.leafAt(nib)
			full = append(full, unpackPath(suffix, 64-len(n.path)-1, nil)...)
			*out = append(*out, full)
			continue
		}
		child := n.child(nib)
		if child == nil {
			cp := append(append([]byte(nil), n.path...), byte(nib))
			cp = append(cp, n.childExtAt(nib)...)
			if len(n.path) != 0 && bits.OnesCount16(n.childMask) == 1 && n.leafMask == 0 {
				cp = append([]byte(nil), n.path...)
			}
			loaded, err := unfold(ctx, cp, planeStorage, addr[:])
			if err != nil || loaded == nil {
				t.Fatalf("unfold %x: %v", cp, err)
			}
			loaded.path = cp
			child = loaded
		}
		collectPaths(t, ctx, addr, child, out)
	}
}

func TestZZStorageTrieHoldsExactlyTheInsertedPaths(t *testing.T) {
	for i, seed := range [][][]byte{
		{slotPath(5, 5, 1), slotPath(5, 5, 2), slotPath(5, 6)},
		{slotPath(1, 2, 3), slotPath(1, 2, 4), slotPath(9)},
		{slotPath(5, 5, 1), slotPath(5, 6)},
	} {
		var addr [32]byte
		addr[0] = byte(0x40 + i)
		ctx := newMockContext()
		seedStorage(t, ctx, addr, seed)

		root, err := unfold(ctx, nil, planeStorage, addr[:])
		if err != nil {
			t.Fatal(err)
		}
		var got [][]byte
		collectPaths(t, ctx, addr, root, &got)

		want := append([][]byte(nil), seed...)
		sort.Slice(want, func(a, b int) bool { return bytes.Compare(want[a], want[b]) < 0 })
		sort.Slice(got, func(a, b int) bool { return bytes.Compare(got[a], got[b]) < 0 })
		if len(got) != len(want) {
			t.Errorf("seed %d: got %d paths, want %d", i, len(got), len(want))
		}
		for j := range want {
			if j < len(got) && !bytes.Equal(got[j], want[j]) {
				t.Errorf("seed %d path %d: got %x want %x", i, j, got[j], want[j])
			}
		}
	}
}
