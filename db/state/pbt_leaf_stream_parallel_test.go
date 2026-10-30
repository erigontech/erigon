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

package state

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPBinLeafAddressRangesPartition(t *testing.T) {
	ranges := pbinLeafAddressRanges(16)
	require.Len(t, ranges, 64)
	require.Nil(t, ranges[0].from)
	require.Nil(t, ranges[len(ranges)-1].to)
	for i := 1; i < len(ranges); i++ {
		require.Equal(t, ranges[i-1].to, ranges[i].from)
	}
	for _, r := range ranges {
		if r.from != nil && r.to != nil {
			require.Less(t, bytes.Compare(r.from, r.to), 0)
		}
	}
}
