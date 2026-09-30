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

package commands

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	chainpkg "github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/internal/commitmenttest/temporal"
)

func TestValidatePBTImportPointRefusesNonCanonicalBlock(t *testing.T) {
	db, _ := temporal.Open(t, 8)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	genesis := common.Hash{1}
	require.NoError(t, rawdb.WriteCanonicalHash(tx, genesis, 0))
	require.NoError(t, rawdb.WriteChainConfig(tx, genesis, &chainpkg.Config{BinaryTrieTime: new(uint64)}))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 1, 1))
	header := &types.Header{Number: *uint256.NewInt(1), Time: 0}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, common.Hash{2}, 1))
	require.NoError(t, tx.Commit())
	_, _, err = validatePBTImportPoint(t.Context(), db, header.Hash())
	require.ErrorContains(t, err, "not canonical")
}
