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

package bal

import (
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types"
)

func TestFinalChange(t *testing.T) {
	t.Parallel()

	if _, ok := finalChangeUpTo([]*types.BalanceChange(nil), math.MaxUint32); ok {
		t.Fatal("finalChangeUpTo on an empty list returned ok=true")
	}

	single, ok := finalChangeUpTo([]*types.BalanceChange{
		{Index: 7, Value: *uint256.NewInt(70)},
	}, math.MaxUint32)
	require.True(t, ok)
	assert.Equal(t, uint64(70), single.Value.Uint64())

	ascending, ok := finalChangeUpTo([]*types.BalanceChange{
		{Index: 0, Value: *uint256.NewInt(10)},
		{Index: 2, Value: *uint256.NewInt(20)},
		{Index: 9, Value: *uint256.NewInt(90)},
	}, math.MaxUint32)
	require.True(t, ok)
	assert.Equal(t, uint64(90), ascending.Value.Uint64())
}

func TestFinalChangeUpTo(t *testing.T) {
	t.Parallel()

	changes := []*types.BalanceChange{
		{Index: 0, Value: *uint256.NewInt(10)},
		{Index: 2, Value: *uint256.NewInt(20)},
		{Index: 9, Value: *uint256.NewInt(90)},
	}

	if _, ok := finalChangeUpTo([]*types.BalanceChange{{Index: 5, Value: *uint256.NewInt(1)}}, 4); ok {
		t.Fatal("ceiling below the only change must return ok=false")
	}

	at2, ok := finalChangeUpTo(changes, 2)
	require.True(t, ok)
	assert.Equal(t, uint64(20), at2.Value.Uint64(), "ceiling==Index returns that change")

	between, ok := finalChangeUpTo(changes, 5)
	require.True(t, ok)
	assert.Equal(t, uint64(20), between.Value.Uint64(), "ceiling between changes returns the earlier one")

	whole, ok := finalChangeUpTo(changes, 9)
	require.True(t, ok)
	assert.Equal(t, uint64(90), whole.Value.Uint64())
	all, ok := finalChangeUpTo(changes, ^uint32(0))
	require.True(t, ok)
	assert.Equal(t, uint64(90), all.Value.Uint64(), "MaxUint32 ceiling == finalChange")

	if _, ok := finalChangeUpTo([]*types.BalanceChange(nil), 100); ok {
		t.Fatal("finalChangeUpTo on an empty list returned ok=true")
	}
}
