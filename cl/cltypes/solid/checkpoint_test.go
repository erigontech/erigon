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

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes/solid"
)

var testCheckpoint = solid.NewCheckpointFromParameters(libcommon.HexToHash("0x3"), 69)

var expectedTestCheckpointMarshalled = libcommon.Hex2Bytes("45000000000000000000000000000000000000000000000000000000000000000000000000000003")
var expectedTestCheckpointRoot = libcommon.Hex2Bytes("be8567f9fdae831b10720823dbcf0e3680e61d6a2a27d85ca00f6c15a7bbb1ea")

func TestCheckpointMarshalUnmarmashal(t *testing.T) {
	marshalled, err := testCheckpoint.EncodeSSZ(nil)
	require.NoError(t, err)
	assert.Equal(t, marshalled, expectedTestCheckpointMarshalled)
	checkpoint := solid.NewCheckpoint()
	require.NoError(t, checkpoint.DecodeSSZ(marshalled, 0))
	require.Equal(t, checkpoint, testCheckpoint)
}

func TestCheckpointHashTreeRoot(t *testing.T) {
	root, err := testCheckpoint.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, root[:], expectedTestCheckpointRoot)
}
