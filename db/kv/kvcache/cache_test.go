// Copyright 2021 The Erigon Authors
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

package kvcache

import (
	"testing"

	"github.com/stretchr/testify/require"
	btree2 "github.com/tidwall/btree"
)

func StateVersionID(v *CoherentView) uint64 {
	return v.stateVersionID
}

func StateEvict(c *Coherent) *ThreadSafeEvictionList {
	return c.stateEvict
}

func Roots(c *Coherent) map[uint64]*CoherentRoot {
	return c.roots
}

func LatestStateVersionID(c *Coherent) uint64 {
	return c.latestStateVersionID
}

func RootCache(r *CoherentRoot) *btree2.BTreeG[*Element] {
	return r.cache
}

func TestEvictionInUnexpectedOrder(t *testing.T) {
	// Order: View - 2, OnNewBlock - 2, View - 5, View - 6, OnNewBlock - 3, OnNewBlock - 4, View - 5, OnNewBlock - 5, OnNewBlock - 100
	require := require.New(t)
	cfg := DefaultCoherentConfig
	cfg.CacheSize = 3
	cfg.NewBlockWait = 0
	c := New(cfg)
	c.selectOrCreateRoot(2)
	require.Len(c.roots, 1)
	require.Zero(int(c.latestStateVersionID))
	require.False(c.roots[2].isCanonical)

	c.add([]byte{1}, nil, c.roots[2], 2)
	require.Zero(c.stateEvict.Len())

	c.advanceRoot(2)
	require.Len(c.roots, 1)
	require.Equal(2, int(c.latestStateVersionID))
	require.True(c.roots[2].isCanonical)

	c.add([]byte{1}, nil, c.roots[2], 2)
	require.Equal(1, c.stateEvict.Len())

	c.selectOrCreateRoot(5)
	require.Len(c.roots, 2)
	require.Equal(2, int(c.latestStateVersionID))
	require.False(c.roots[5].isCanonical)

	c.add([]byte{2}, nil, c.roots[5], 5) // not added to evict list
	require.Equal(1, c.stateEvict.Len())
	c.add([]byte{2}, nil, c.roots[2], 2) // added to evict list, because it's latest view
	require.Equal(2, c.stateEvict.Len())

	c.selectOrCreateRoot(6)
	require.Len(c.roots, 3)
	require.Equal(2, int(c.latestStateVersionID))
	require.False(c.roots[6].isCanonical) // parrent exists, but parent has isCanonical=false

	c.advanceRoot(3)
	require.Len(c.roots, 4)
	require.Equal(3, int(c.latestStateVersionID))
	require.True(c.roots[3].isCanonical)

	c.advanceRoot(4)
	require.Len(c.roots, 5)
	require.Equal(4, int(c.latestStateVersionID))
	require.True(c.roots[4].isCanonical)

	c.selectOrCreateRoot(5)
	require.Len(c.roots, 5)
	require.Equal(4, int(c.latestStateVersionID))
	require.False(c.roots[5].isCanonical)

	c.advanceRoot(5)
	require.Len(c.roots, 5)
	require.Equal(5, int(c.latestStateVersionID))
	require.True(c.roots[5].isCanonical)

	c.advanceRoot(100)
	require.Len(c.roots, 6)
	require.Equal(100, int(c.latestStateVersionID))
	require.True(c.roots[100].isCanonical)

	//c.add([]byte{1}, nil, c.roots[2], 2)
	require.Equal(0, c.latestStateView.cache.Len())
	require.Equal(0, c.stateEvict.Len())
}
