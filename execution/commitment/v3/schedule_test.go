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

package v3

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/internal/commitmenttest"
)

func TestStorageWorkerCountHonoursTheCap(t *testing.T) {
	require.Equal(t, 16*storageOversubscribe, storageWorkerCount(0, 16, 0), "no cap: procs oversubscribed")
	require.Equal(t, 15, storageWorkerCount(0, 16, 15), "the cap bounds the oversubscribed count")
	require.Equal(t, 4, storageWorkerCount(4, 16, 15), "explicit workers below the cap")
	require.Equal(t, 2, storageWorkerCount(4, 16, 2), "the cap bounds explicit workers")
}

func TestSmallRoundFoldsSerially(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	var base []commitmenttest.Op
	for i := range 2000 {
		base = append(base, parityAccount(parityAddress(i), i))
	}
	round := func(keys, bump int) []commitmenttest.Op {
		ops := make([]commitmenttest.Op, 0, keys)
		for _, i := range rng.Perm(4000)[:keys] {
			address := parityAddress(i / 2)
			if i%2 == 0 {
				ops = append(ops, parityAccount(address, i+bump))
				continue
			}
			ops = append(ops, commitmenttest.StorageAt(commitmenttest.SlotKey(address, paritySlot(i)), i+bump))
		}
		return ops
	}
	for _, keys := range []int{1, 2, serialFoldMaxKeys - 1, serialFoldMaxKeys, serialFoldMaxKeys + 1, 4 * serialFoldMaxKeys} {
		for _, feed := range []bool{false, true} {
			t.Run(fmt.Sprintf("keys=%d/feed=%t", keys, feed), func(t *testing.T) {
				rounds := [][]commitmenttest.Op{round(keys, 1), round(keys, 2)}
				requireSameRuns(t, base, v3Config{feed: feed}, v3Config{workers: 4, feed: feed}, rounds...)
				seeded := newShardedContext()
				runV3(t, seeded, v3Config{workers: 1}, base)
				_, _, calls := runV3(t, seeded, v3Config{feed: feed}, rounds...)
				if keys < serialFoldMaxKeys {
					require.Zero(t, calls)
				} else {
					require.Positive(t, calls)
				}
			})
		}
	}
}
