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

package rawdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/execution/chain"
)

type testL2Config struct {
	Stack string `json:"stack"`
}

func (c *testL2Config) Name() string { return c.Stack }

func (c *testL2Config) ResolveRules(l2Version, blockNum, blockTime uint64, r *chain.Rules) {}

func TestChainConfigL2JSONRoundTrip(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	hash := common.Hash{1}

	cfg := &chain.Config{L2: &testL2Config{Stack: "testl2"}}
	require.NoError(t, WriteChainConfig(tx, hash, cfg))

	got, err := ReadChainConfig(tx, hash)
	require.NoError(t, err)
	require.JSONEq(t, `{"stack":"testl2"}`, string(got.L2JSON))
	require.True(t, got.IsL2())
}
