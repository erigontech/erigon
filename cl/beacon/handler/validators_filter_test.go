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

package handler

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSortedIndicesBelow(t *testing.T) {
	in := []uint64{7, 3, math.MaxUint64, 3, 0, 10, 9}
	require.Equal(t, []uint64{0, 3, 7, 9}, sortedIndicesBelow(in, 10))
	require.Equal(t, []uint64{7, 3, math.MaxUint64, 3, 0, 10, 9}, in, "input must not be reordered")
	require.Empty(t, sortedIndicesBelow([]uint64{10, math.MaxUint64}, 10))
}
