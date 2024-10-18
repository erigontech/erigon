// Copyright 2024 The Erigon Authors
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

//go:build !nofuzz

package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Fuzz_BtreeIndex_Allocation(f *testing.F) {
	f.Add(uint64(1_000_000), uint64(1024))
	f.Fuzz(func(t *testing.T, keyCount, M uint64) {
		if keyCount < M*4 || M < 4 {
			t.Skip()
		}
		bt := newBtAlloc(keyCount, M, false, nil, nil)
		bt.traverseDfs()
		require.GreaterOrEqual(t, bt.N, keyCount)

		require.LessOrEqual(t, float64(bt.N-keyCount)/float64(bt.N), 0.05)

	})
}
