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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/rpc"
)

// TestNotFoundMustReturnNil - next methods - when record not found in db - must return nil instead of error
// see https://github.com/erigontech/erigon/issues/1645
func TestNotFoundMustReturnNil(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	assertions := require.New(t)
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	ctx := context.Background()

	a, err := api.GetTransactionByBlockNumberAndIndex(ctx, 10_000, 1)
	assertions.Nil(a)
	assertions.NoError(err)

	b, err := api.GetTransactionByBlockHashAndIndex(ctx, common.Hash{}, 1)
	assertions.Nil(b)
	assertions.NoError(err)

	c, err := api.GetTransactionByBlockNumberAndIndex(ctx, 10_000, 1)
	assertions.Nil(c)
	assertions.NoError(err)

	d, err := api.GetTransactionReceipt(ctx, common.Hash{})
	assertions.Nil(d)
	assertions.NoError(err)

	e, err := api.GetBlockByHash(ctx, rpc.BlockNumberOrHashWithHash(common.Hash{}, true), false)
	assertions.Nil(e)
	assertions.NoError(err)

	f, err := api.GetBlockByNumber(ctx, 10_000, false)
	assertions.Nil(f)
	assertions.NoError(err)

	g, err := api.GetUncleByBlockHashAndIndex(ctx, common.Hash{}, 1)
	assertions.Nil(g)
	assertions.NoError(err)

	h, err := api.GetUncleByBlockNumberAndIndex(ctx, 10_000, 1)
	assertions.Nil(h)
	assertions.NoError(err)

	j, err := api.GetBlockTransactionCountByNumber(ctx, 10_000)
	assertions.Nil(j)
	assertions.NoError(err)
}

// TestNotFoundMustReturnError - next methods - when record not found in db - must return error
// see https://github.com/erigontech/erigon/issues/18225
func TestNotFoundMustReturnError(t *testing.T) {
	assertions := require.New(t)
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	ctx := context.Background()

	a, err := api.GetBalance(ctx, common.Address{}, rpc.BlockNumberOrHashWithNumber(10_000))
	assertions.Nil(a)
	assertions.EqualError(err, "block not found: 10000")
}
