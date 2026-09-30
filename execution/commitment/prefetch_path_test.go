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

package commitment

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func TestPrefetchBranchPathReadsEveryBranchOnThePath(t *testing.T) {
	t.Parallel()

	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	ub := NewUpdateBuilder()
	for i := range 64 {
		addr := fmt.Sprintf("%040x", i+1)
		ub.Balance(addr, uint64(i+1))
		slots := 2
		if i%8 == 0 {
			slots = 300
		}
		for s := range slots {
			ub.Storage(addr, fmt.Sprintf("%064x", i*1000+s+1), fmt.Sprintf("%02x", s%250+1))
		}
	}
	plainKeys, updates := ub.Build()
	upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer upds.Close()
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))
	_, err := hph.Process(t.Context(), upds, "", nil, WarmupConfig{})
	require.NoError(t, err)

	storageBranches, extensionHops, storageRootExtensions := 0, 0, 0
	for _, pk := range plainKeys {
		hk := KeyToHexNibbleHash(pk)
		want := map[string]bool{}
		var depths []int
		for prefix := range ms.cm {
			if nib := nibbles.CompactToHex([]byte(prefix)); len(nib) < len(hk) && bytes.HasPrefix(hk, nib) {
				want[prefix] = true
				depths = append(depths, len(nib))
				if len(nib) >= 64 {
					storageBranches++
				}
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
		got := map[string]bool{}
		PrefetchBranchPath(hk, func(prefix []byte) []byte {
			data, _, err := ms.Branch(prefix)
			require.NoError(t, err)
			if len(data) > 0 {
				got[string(prefix)] = true
			}
			return data
		})
		require.Equal(t, want, got, "plain key %x", pk)
	}
	require.NotZero(t, storageBranches, "fixture must carry storage-plane branches")
	require.NotZero(t, extensionHops, "fixture must carry extension nodes between branches")
	require.NotZero(t, storageRootExtensions, "fixture must carry storage roots that are extension nodes")
}
