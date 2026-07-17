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

package blockio

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/memdb"
)

// TruncateBodies runs inside the caller's open write tx (see ResetBlocks). It
// must clear EthTx/MaxTxNum on that same tx: opening its own write tx would
// deadlock on mdbx's per-env writer serialization.
func TestTruncateBodiesInsideOpenWriteTx(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)

	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		for i := byte(1); i <= 5; i++ {
			if err := tx.Put(kv.EthTx, []byte{0, 0, 0, 0, 0, 0, 0, i}, []byte{i}); err != nil {
				return err
			}
			if err := tx.Put(kv.BlockBody, dbutils.BlockBodyKey(uint64(i), common.Hash{i}), []byte{i}); err != nil {
				return err
			}
		}
		return tx.Put(kv.MaxTxNum, []byte{0, 0, 0, 0, 0, 0, 0, 1}, []byte{1})
	}))

	done := make(chan error, 1)
	go func() {
		done <- db.Update(ctx, func(tx kv.RwTx) error {
			return NewBlockWriter().TruncateBodies(db, tx, 2)
		})
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("TruncateBodies deadlocked while called inside an open write tx")
	}

	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		var bodies []uint64
		require.NoError(t, tx.ForEach(kv.BlockBody, nil, func(k, _ []byte) error {
			bodies = append(bodies, binary.BigEndian.Uint64(k))
			return nil
		}))
		require.Equal(t, []uint64{1}, bodies) // from==2: bodies below it survive the cut

		ethTxCount, err := tx.Count(kv.EthTx)
		require.NoError(t, err)
		require.Zero(t, ethTxCount)
		maxTxNumCount, err := tx.Count(kv.MaxTxNum)
		require.NoError(t, err)
		require.Zero(t, maxTxNumCount)
		return nil
	}))
}
