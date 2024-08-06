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

func TestExtraData(t *testing.T) {
	// Create a new ExtraData instance
	extraData := solid.NewExtraData()

	// Set the bytes
	bytes := []byte{0x01, 0x02, 0x03}
	extraData.SetBytes(bytes)

	// Encode and decode the ExtraData
	encoded, err := extraData.EncodeSSZ(nil)
	require.NoError(t, err)

	decoded := solid.NewExtraData()
	err = decoded.DecodeSSZ(encoded, 0)
	require.NoError(t, err)

	// Verify the bytes
	require.Equal(t, bytes, decoded.Bytes())

	// Verify the encoding size
	require.Equal(t, len(bytes), extraData.EncodingSizeSSZ())

	// Calculate the expected hash
	expectedHash, err := extraData.HashSSZ()
	require.NoError(t, err)

	// Verify the hash
	actualHash, err := decoded.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expectedHash, actualHash)
}
