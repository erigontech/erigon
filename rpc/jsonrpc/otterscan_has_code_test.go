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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/rpc"
)

// ots_hasCode answers about the end of the block it is given, so a contract is
// visible from its deployment block onwards. eth_getCode reads the same
// boundary; the two must never disagree.
func TestHasCodeAnswersAboutEndOfBlock(t *testing.T) {
	const deployedIn = 1

	m, _, contractAddr, _ := chainWithDeployedContract(t)
	base := newBaseApiForTest(m)
	ots := NewOtterscanAPI(base, m.DB, 25)
	eth := newEthApiForTest(base, m.DB, nil, nil)

	var deployHash common.Hash
	require.NoError(t, m.DB.View(m.Ctx, func(tx kv.Tx) error {
		hash, ok, err := m.BlockReader.CanonicalHash(m.Ctx, tx, deployedIn)
		require.NoError(t, err)
		require.True(t, ok)
		deployHash = hash
		return nil
	}))

	for _, tc := range []struct {
		name     string
		selector rpc.BlockNumberOrHash
		want     bool
	}{
		{"block before deployment", rpc.BlockNumberOrHashWithNumber(deployedIn - 1), false},
		{"deployment block", rpc.BlockNumberOrHashWithNumber(deployedIn), true},
		{"block after deployment", rpc.BlockNumberOrHashWithNumber(deployedIn + 1), true},
		{"deployment block hash", rpc.BlockNumberOrHashWithHash(deployHash, true), true},
		{"latest", rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selector := tc.selector

			hasCode, err := ots.HasCode(m.Ctx, contractAddr, selector)
			require.NoError(t, err)
			require.Equal(t, tc.want, hasCode)

			code, err := eth.GetCode(m.Ctx, contractAddr, &selector)
			require.NoError(t, err)
			require.Equal(t, len(code) > 0, hasCode, "must agree with eth_getCode")
		})
	}
}
