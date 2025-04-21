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

package jsonrpc

import (
	"testing"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/stretchr/testify/require"
)

func TestGetContractCreator(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewOtterscanAPI(newBaseApiForTest(m), m.DB, 25)

	addr := libcommon.HexToAddress("0x537e697c7ab75a26f9ecf0ce810e3154dfcaaf44")
	expectCreator := libcommon.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	expectCredByTx := libcommon.HexToHash("0x6e25f89e24254ba3eb460291393a4715fd3c33d805334cbd05c1b2efe1080f18")
	t.Run("valid inputs", func(t *testing.T) {
		require := require.New(t)
		results, err := api.GetContractCreator(m.Ctx, addr)
		require.NoError(err)
		require.Equal(expectCreator, results.Creator)
		require.Equal(expectCredByTx, results.Tx)
	})
	t.Run("not existing addr", func(t *testing.T) {
		require := require.New(t)
		results, err := api.GetContractCreator(m.Ctx, libcommon.HexToAddress("0x1234"))
		require.NoError(err)
		require.Nil(results)
	})
	t.Run("pass creator as addr", func(t *testing.T) {
		require := require.New(t)
		results, err := api.GetContractCreator(m.Ctx, expectCreator)
		require.NoError(err)
		require.Nil(results)
	})
}
