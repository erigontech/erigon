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

package v2

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

type branchReadRecorder struct {
	*MockState
	read map[string]bool
}

func (r *branchReadRecorder) Branch(prefix []byte) ([]byte, kv.Step, error) {
	r.read[string(prefix)] = true
	return r.MockState.Branch(prefix)
}

func TestWarmupKeyReadsEveryBranchOnThePath(t *testing.T) {
	t.Parallel()

	ub := NewUpdateBuilder()
	var newSlots [][]byte
	for i := range 64 {
		addr := fmt.Sprintf("%040x", i+1)
		ub.Balance(addr, uint64(i+1))
		slots := [...]int{300, 2, 2, 2, 2, 1, 1, 0}[i%8]
		if slots == 0 {
			newSlots = append(newSlots, decodeHex(addr+fmt.Sprintf("%064x", 1)))
		}
		for s := range slots {
			ub.Storage(addr, fmt.Sprintf("%064x", i*1000+s+1), fmt.Sprintf("%02x", s%250+1))
		}
	}
	plainKeys, updates := ub.Build()
	_, ms := sequentialRoot(t, plainKeys, updates)

	w := &commitment.Warmuper{MaxDepth: commitment.WarmupMaxDepth}
	extensionHops, storageRootExtensions := 0, 0
	for _, pk := range append(plainKeys, newSlots...) {
		hk := commitment.KeyToHexNibbleHash(pk)
		want := map[string]bool{}
		var depths []int
		for prefix := range ms.cm {
			if nib := nibbles.CompactToHex([]byte(prefix)); len(nib) < len(hk) && bytes.HasPrefix(hk, nib) {
				want[prefix] = true
				depths = append(depths, len(nib))
			}
		}
		slices.Sort(depths)
		for i := 1; i < len(depths); i++ {
			if depths[i] > depths[i-1]+1 && (depths[i-1] >= 64 || depths[i] < 64) {
				extensionHops++
			}
			if depths[i-1] < 64 && depths[i] > 64 {
				storageRootExtensions++
			}
		}
		rec := &branchReadRecorder{MockState: ms, read: map[string]bool{}}
		w.WarmupKey(rec, hk, 0)
		require.Equal(t, want, rec.read, "plain key %x", pk)
	}
	require.NotZero(t, extensionHops, "fixture must carry extension nodes between branches")
	require.NotZero(t, storageRootExtensions, "fixture must carry storage roots that are extension nodes")
}
