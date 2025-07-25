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

package cltypes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes"
)

var testEth1Data = &cltypes.Eth1Data{
	Root:         common.HexToHash("0x2"),
	BlockHash:    common.HexToHash("0x3"),
	DepositCount: 69,
}

var expectedTestEth1DataMarshalled = common.Hex2Bytes("000000000000000000000000000000000000000000000000000000000000000245000000000000000000000000000000000000000000000000000000000000000000000000000003")
var expectedTestEth1DataRoot = common.Hex2Bytes("adbafa10f1d6046b59cb720371c5e70ce2c6c3067b0e87985f5cd0899a515886")

func TestEth1DataMarshalUnmarmashal(t *testing.T) {
	marshalled, _ := testEth1Data.EncodeSSZ(nil)
	assert.Equal(t, expectedTestEth1DataMarshalled, marshalled)
	testData2 := &cltypes.Eth1Data{}
	require.NoError(t, testData2.DecodeSSZ(marshalled, 0))
	require.Equal(t, testData2, testEth1Data)
}

func TestEth1DataHashTreeRoot(t *testing.T) {
	root, err := testEth1Data.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, expectedTestEth1DataRoot, root[:])
}
