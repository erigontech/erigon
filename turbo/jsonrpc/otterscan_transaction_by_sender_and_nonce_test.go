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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

func TestGetTransactionBySenderAndNonce(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewOtterscanAPI(NewBaseApi(nil, nil, m.BlockReader, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs, nil), m.DB, 25)

	addr := common.HexToAddress("0x537e697c7ab75a26f9ecf0ce810e3154dfcaaf44")
	expectCreator := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	expectCredByTx := common.HexToHash("0x6e25f89e24254ba3eb460291393a4715fd3c33d805334cbd05c1b2efe1080f18")
	_, _, _ = addr, expectCreator, expectCredByTx
	t.Run("valid input", func(t *testing.T) {
		require := require.New(t)
		reply, err := api.GetTransactionBySenderAndNonce(m.Ctx, expectCreator, 0)
		require.NoError(err)
		expectTxHash := common.HexToHash("0x3f3cb8a0e13ed2481f97f53f7095b9cbc78b6ffb779f2d3e565146371a8830ea")
		require.Equal(&expectTxHash, reply)

		reply, err = api.GetTransactionBySenderAndNonce(m.Ctx, expectCreator, 1)
		require.NoError(err)
		expectTxHash = common.HexToHash("0xcdc63ba35b09f6667f179e271ece766a6ec00a07673c0cf1e7d4e8feb1697566")
		require.Equal(&expectTxHash, reply)

		// skip others...

		reply, err = api.GetTransactionBySenderAndNonce(m.Ctx, expectCreator, 38)
		require.NoError(err)
		expectTxHash = common.HexToHash("0xb6449d8e167a8826d050afe4c9f07095236ff769a985f02649b1023c2ded2059")
		require.Equal(&expectTxHash, reply)

		//reply, err = api.GetTransactionBySenderAndNonce(m.Ctx, expectCreator, 39)
		//require.NoError(err)
		//require.Nil(reply)
	})
	t.Run("not existing addr", func(t *testing.T) {
		require := require.New(t)
		results, err := api.GetContractCreator(m.Ctx, common.HexToAddress("0x1234"))
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
