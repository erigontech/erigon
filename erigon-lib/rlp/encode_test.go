// Copyright 2023 The Erigon Authors
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

package rlp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common/hexutility"
)

// Strings of length 56 are a boundary case.
// See https://ethereum.org/en/developers/docs/data-structures-and-encoding/rlp/#definition
func TestStringLen56(t *testing.T) {
	str := hexutility.MustDecodeHex("7907ca011864321def1e92a3021868f397516ce37c959f25f8dddd3161d7b8301152b35f135c814fae9f487206471b6b0d713cd51a2d3598")
	require.Equal(t, 56, len(str))

	strLen := StringLen(str)
	assert.Equal(t, 56+2, strLen)

	encoded := make([]byte, strLen)
	EncodeString(str, encoded)

	dataPos, dataLen, err := String(encoded, 0)
	require.NoError(t, err)
	assert.Equal(t, dataPos, 2)
	assert.Equal(t, dataLen, 56)
}
