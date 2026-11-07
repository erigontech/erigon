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

package migrations

import (
	"context"
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/types"
)

func TestRemoveOrphanedBlockBodies(t *testing.T) {
	ctx := t.Context()
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	migrationsDB := newTestMigrationsDB(t)
	fixtures := []struct {
		name                             string
		number                           uint64
		header, canonical, indexed, keep bool
		txs                              int
		hash                             common.Hash
		base                             types.BaseTxnID
	}{
		{name: "genesis", number: 0, keep: true},
		{name: "old orphan", number: 1, txs: 2},
		{name: "canonical", number: 10, header: true, canonical: true, keep: true, txs: 2},
		{name: "side fork", number: 10, header: true, keep: true, txs: 2},
		{name: "rejected sibling", number: 10, txs: 2},
		{name: "canonical without header", number: 8, canonical: true, keep: true, txs: 2},
		{name: "indexed without header", number: 9, indexed: true, keep: true, txs: 2},
		{name: "empty orphan", number: 11},
		{name: "future orphan", number: 1000000, txs: 2},
	}
	var sequence uint64
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		for i := range fixtures {
			f := &fixtures[i]
			header := &types.Header{Number: *uint256.NewInt(f.number), Extra: []byte(f.name)}
			f.hash = header.Hash()
			require.NoError(t, rawdb.WriteHeader(tx, header))
			rawTxs := make([][]byte, f.txs)
			for j := range rawTxs {
				rawTxs[j] = []byte{byte(i), byte(j)}
			}
			_, err := rawdb.WriteRawBody(tx, f.hash, f.number, &types.RawBody{Transactions: rawTxs})
			require.NoError(t, err)
			body, ok, err := rawdb.ReadBodyOnlyTxnByKey(tx, dbutils.BlockBodyKey(f.number, f.hash))
			require.NoError(t, err)
			require.True(t, ok)
			f.base = body.BaseTxnID
			require.NoError(t, rawdb.WriteTd(tx, f.hash, f.number, *uint256.NewInt(1)))
			require.NoError(t, rawdb.WriteSenders(tx, f.hash, f.number, []common.Address{{byte(i)}}))
			require.NoError(t, rawdb.WriteBlockAccessListBytes(tx, f.hash, f.number, []byte{0xc0}))
			if f.canonical {
				require.NoError(t, rawdb.WriteCanonicalHash(tx, f.hash, f.number))
			}
			if !f.header {
				rawdb.DeleteHeader(tx, f.hash, f.number)
			}
			if f.indexed {
				require.NoError(t, rawdb.WriteHeaderNumber(tx, f.hash, f.number))
			}
		}
		var err error
		sequence, err = tx.ReadSequence(kv.EthTx)
		return err
	}))
	migrator := NewMigrator(dbcfg.ChainDB)
	dataDir := t.TempDir()
	for range 2 {
		require.NoError(t, migrator.Apply(db, migrationsDB, dataDir, "", log.New()))
		require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
			for i, f := range fixtures {
				for _, table := range []string{kv.BlockBody, kv.HeaderTD, kv.Senders, kv.BlockAccessList} {
					exists, err := tx.Has(table, dbutils.BlockBodyKey(f.number, f.hash))
					require.NoError(t, err)
					require.Equal(t, f.keep, exists, "%s in %s", f.name, table)
				}
				for j := 0; j < f.txs; j++ {
					value, err := tx.GetOne(kv.EthTx, hexutil.EncodeTs(f.base.At(j)))
					require.NoError(t, err)
					if f.keep {
						require.Equal(t, []byte{byte(i), byte(j)}, value, f.name)
					} else {
						require.Empty(t, value, f.name)
					}
				}
			}
			after, err := tx.ReadSequence(kv.EthTx)
			require.NoError(t, err)
			require.Equal(t, sequence, after)
			return nil
		}))
		require.NoError(t, migrationsDB.Update(ctx, func(tx kv.RwTx) error {
			return tx.Delete(kv.Migrations, []byte("remove_orphaned_block_bodies"))
		}))
	}
}

func TestRemoveOrphanedBlockBodiesRestartsAfterFailedCommit(t *testing.T) {
	ctx := t.Context()
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	const blocks = 2500
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		for number := uint64(1); number <= blocks; number++ {
			hash := common.Hash{1}
			_, err := rawdb.WriteRawBody(tx, hash, number, &types.RawBody{Transactions: [][]byte{{1}}})
			require.NoError(t, err)
			require.NoError(t, rawdb.WriteTd(tx, hash, number, *uint256.NewInt(1)))
		}
		return nil
	}))
	stopped := errors.New("commit interrupted")
	err := removeOrphanedBlockBodies.Up(db, datadir.Dirs{}, nil, func(kv.RwTx, []byte, bool) error {
		return stopped
	}, log.New())
	require.ErrorIs(t, err, stopped)
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		bodies, err := tx.Count(kv.BlockBody)
		require.NoError(t, err)
		require.Greater(t, bodies, uint64(0))
		require.Less(t, bodies, uint64(blocks))
		for _, table := range []string{kv.EthTx, kv.HeaderTD} {
			count, err := tx.Count(table)
			require.NoError(t, err)
			require.Equal(t, bodies, count)
		}
		return nil
	}))
	done := false
	require.NoError(t, removeOrphanedBlockBodies.Up(db, datadir.Dirs{}, nil, func(_ kv.RwTx, _ []byte, isDone bool) error {
		done = isDone
		return nil
	}, log.New()))
	require.True(t, done)
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		for _, table := range []string{kv.BlockBody, kv.EthTx, kv.HeaderTD} {
			count, err := tx.Count(table)
			require.NoError(t, err)
			require.Zero(t, count)
		}
		sequence, err := tx.ReadSequence(kv.EthTx)
		require.NoError(t, err)
		require.Equal(t, uint64(blocks*3), sequence)
		return nil
	}))
}

type bodyCursorErrorDB struct {
	kv.RwDB
	err error
}

func (db bodyCursorErrorDB) Update(ctx context.Context, f func(kv.RwTx) error) error {
	return db.RwDB.Update(ctx, func(tx kv.RwTx) error {
		return f(bodyCursorErrorTx{RwTx: tx, err: db.err})
	})
}

type bodyCursorErrorTx struct {
	kv.RwTx
	err error
}

func (tx bodyCursorErrorTx) Cursor(table string) (kv.Cursor, error) {
	cursor, err := tx.RwTx.Cursor(table) //nolint:gocritic // The caller closes the returned cursor wrapper.
	if err != nil || table != kv.BlockBody {
		return cursor, err
	}
	return bodyCursorError{Cursor: cursor, err: tx.err}, nil
}

type bodyCursorError struct {
	kv.Cursor
	err error
}

func (c bodyCursorError) Next() ([]byte, []byte, error) { return nil, nil, c.err }

func TestRemoveOrphanedBlockBodiesPropagatesCursorError(t *testing.T) {
	ctx := t.Context()
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	hash := common.Hash{1}
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		_, err := rawdb.WriteRawBody(tx, hash, 1, &types.RawBody{Transactions: [][]byte{{1}}})
		return err
	}))
	cursorErr := errors.New("body cursor failed")
	done := false
	err := removeOrphanedBlockBodies.Up(bodyCursorErrorDB{RwDB: db, err: cursorErr}, datadir.Dirs{}, nil, func(_ kv.RwTx, _ []byte, isDone bool) error {
		done = isDone
		return nil
	}, log.New())
	require.ErrorIs(t, err, cursorErr)
	require.False(t, done)
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		exists, err := tx.Has(kv.BlockBody, dbutils.BlockBodyKey(1, hash))
		require.NoError(t, err)
		require.True(t, exists)
		return nil
	}))
}
