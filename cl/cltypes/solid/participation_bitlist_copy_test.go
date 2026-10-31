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

package solid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParticipationBitListCopyToCopiesOnlyLogicalLength(t *testing.T) {
	source := NewParticipationBitList(3, 16)
	source.Set(0, 1)
	source.u[3] = 0xff
	target := NewParticipationBitList(8, 16)
	for index := range target.l {
		target.Set(index, 0xaa)
	}
	targetBackingCapacity := cap(target.u)
	sourceRoot, err := source.HashSSZ()
	require.NoError(t, err)
	want := NewParticipationBitList(3, 16)
	want.Set(0, 1)
	wantRoot, err := want.HashSSZ()
	require.NoError(t, err)

	source.CopyTo(target)

	require.Equal(t, source.l, len(target.u))
	require.Equal(t, source.l, target.l)
	require.Equal(t, 16, target.c)
	require.Equal(t, targetBackingCapacity, cap(target.u))
	require.Equal(t, []byte{1, 0, 0}, target.u)
	targetRoot, err := target.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, wantRoot, targetRoot)
	target.Set(0, 2)
	require.Equal(t, byte(1), source.Get(0))
	unchangedSourceRoot, err := source.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, sourceRoot, unchangedSourceRoot)
}
