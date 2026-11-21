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

package execmodule

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/types"
)

// Block prune walks kv.Headers, so a bad block whose header is purged must not
// leave its body, senders or transactions behind.
func TestPurgeBadChainDeletesTheWholeBlock(t *testing.T) {
	ctx := t.Context()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	e := &ExecModule{db: db, blockReader: freezeblocks.NewBlockReader(nil), logger: log.New()}

	validHash := common.Hash{0x01}
	bad := types.NewBlock(&types.Header{Number: *uint256.NewInt(2), ParentHash: validHash}, []types.Transaction{
		types.NewTransaction(0, common.Address{0x02}, uint256.NewInt(1), 21_000, uint256.NewInt(1), nil),
		types.NewTransaction(1, common.Address{0x02}, uint256.NewInt(1), 21_000, uint256.NewInt(1), nil),
	}, nil, nil, nil, nil)
	key := dbutils.BlockBodyKey(bad.NumberU64(), bad.Hash())

	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, rawdb.WriteBlock(tx, bad))
	require.NoError(t, rawdb.WriteSenders(tx, bad.Hash(), bad.NumberU64(), []common.Address{{0x03}, {0x03}}))
	require.NotZero(t, countRows(t, tx, kv.EthTx))

	require.NoError(t, e.purgeBadChain(ctx, tx, validHash, bad.Hash()))

	require.Nil(t, rawdb.ReadHeader(tx, bad.Hash(), bad.NumberU64()))
	_, ok, err := rawdb.ReadBodyOnlyTxnByKey(tx, key)
	require.NoError(t, err)
	require.False(t, ok, "body of a purged block must be deleted")
	senders, err := tx.GetOne(kv.Senders, key)
	require.NoError(t, err)
	require.Nil(t, senders, "senders of a purged block must be deleted")
	require.Zero(t, countRows(t, tx, kv.EthTx), "transactions of a purged block must be deleted")
}

func countRows(t *testing.T, tx kv.Tx, table string) (n int) {
	t.Helper()
	require.NoError(t, tx.ForEach(table, nil, func(_, _ []byte) error { n++; return nil }))
	return n
}
