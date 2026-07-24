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

package jsonrpc

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func TestOverlayGetBeginEnd(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := &OverlayAPIImpl{BaseAPI: newBaseApiForTest(m)}
	tx, err := m.OverlayDB().BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	latestExecuted, _, _, err := rpchelper.GetBlockNumber(m.Ctx, rpc.BlockNumberOrHashWithNumber(rpc.LatestExecutedBlockNumber), tx, api._blockReader, nil)
	require.NoError(t, err)

	begin, end, err := getBeginEnd(m.Ctx, tx, api, filters.FilterCriteria{FromBlock: big.NewInt(2), ToBlock: big.NewInt(5)})
	require.NoError(t, err)
	require.Equal(t, uint64(2), begin)
	require.Equal(t, uint64(5), end)

	begin, end, err = getBeginEnd(m.Ctx, tx, api, filters.FilterCriteria{})
	require.NoError(t, err)
	require.Equal(t, latestExecuted, begin)
	require.Equal(t, latestExecuted, end)

	_, _, err = getBeginEnd(m.Ctx, tx, api, filters.FilterCriteria{FromBlock: big.NewInt(5), ToBlock: big.NewInt(2)})
	require.EqualError(t, err, "end (2) < begin (5)")

	block, err := api._blockReader.BlockByNumber(m.Ctx, tx, 1)
	require.NoError(t, err)
	blockHash := block.Hash()
	begin, end, err = getBeginEnd(m.Ctx, tx, api, filters.FilterCriteria{BlockHash: &blockHash})
	require.NoError(t, err)
	require.Equal(t, uint64(1), begin)
	require.Equal(t, uint64(1), end)
}
