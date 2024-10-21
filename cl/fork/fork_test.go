// Copyright 2022 The Erigon Authors
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

package fork

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMainnetComputeDomain(t *testing.T) {
	domainType := [4]uint8{0x1, 0x0, 0x0, 0x0}
	currentVersion := [4]uint8{0x3, 0x0, 0x0, 0x0}
	genesesis := [32]uint8{0x4b, 0x36, 0x3d, 0xb9, 0x4e, 0x28, 0x61, 0x20, 0xd7, 0x6e, 0xb9, 0x5, 0x34, 0xf, 0xdd, 0x4e, 0x54, 0xbf, 0xe9, 0xf0, 0x6b, 0xf3, 0x3f, 0xf6,
		0xcf, 0x5a, 0xd2, 0x7f, 0x51, 0x1b, 0xfe, 0x95}

	expectedResult := []byte{0x1, 0x0, 0x0, 0x0, 0xbb, 0xa4, 0xda, 0x96, 0x35, 0x4c, 0x9f, 0x25, 0x47, 0x6c, 0xf1, 0xbc, 0x69, 0xbf, 0x58, 0x3a, 0x7f, 0x9e, 0xa, 0xf0, 0x49, 0x30, 0x5b, 0x62, 0xde, 0x67, 0x66, 0x40}

	result, err := ComputeDomain(domainType[:], currentVersion, genesesis)
	require.NoError(t, err)
	require.Equal(t, expectedResult, result)
}
