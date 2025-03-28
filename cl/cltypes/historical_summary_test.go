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

	"github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/stretchr/testify/require"
)

var (
	serializedHistoricalSummarySnappy = common.Hex2Bytes("40f03f6c6eee828430632dd18c6b608ea98806380fe7711b75ed235551bc95dacfc04c158258ebdb1c95ff566d6e458fc220d2f345bfc063fe717dd26e0c161f70d7ce")
	historicalSummaryRoot             = common.Hex2Bytes("7be1818e82411b397340c97f61d2c785397f5c41747e6c94cef3cba91262db22")
)

func TestHistoricalSummary(t *testing.T) {
	decompressed, _ := utils.DecompressSnappy(serializedHistoricalSummarySnappy, false)
	obj := &cltypes.HistoricalSummary{}
	require.NoError(t, obj.DecodeSSZ(decompressed, 0))
	root, err := obj.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, root[:], historicalSummaryRoot)
	reencoded, err := obj.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, reencoded, decompressed)
}
