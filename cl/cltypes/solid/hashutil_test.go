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

package solid_test

import (
	"testing"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestGetDepth(t *testing.T) {
	// Test cases with expected depths
	testCases := map[uint64]uint8{
		0:  0,
		1:  0,
		2:  1,
		3:  2,
		4:  2,
		5:  3,
		6:  3,
		7:  3,
		8:  3,
		9:  4,
		10: 4,
		16: 4,
		17: 5,
		32: 5,
		33: 6,
	}

	for v, expectedDepth := range testCases {
		actualDepth := solid.GetDepth(v)
		require.Equal(t, expectedDepth, actualDepth)
	}
}
