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
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/state/changeset"
)

func TestPruneDiffSets(t *testing.T) {
	ctx := context.Background()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	err := db.Update(ctx, func(tx kv.RwTx) error {
		for bn := uint64(1); bn <= 10; bn++ {
			if err := changeset.WriteDiffSet(tx, bn, common.Hash{byte(bn)}, &changeset.StateChangeSet{}); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	// each block writes 2 rows (chunk-count key + 1 chunk key); batch of 3 forces multiple commits
	deleted, err := pruneDiffSets(ctx, db, 5, 3, log.New())
	require.NoError(t, err)
	require.Equal(t, uint64(8), deleted)
	err = db.View(ctx, func(tx kv.Tx) error {
		lowest, err := changeset.ReadLowestUnwindableBlock(tx)
		require.NoError(t, err)
		require.Equal(t, uint64(5), lowest)
		_, ok, err := changeset.ReadDiffSet(tx, 4, common.Hash{4})
		require.NoError(t, err)
		require.False(t, ok)
		_, ok, err = changeset.ReadDiffSet(tx, 5, common.Hash{5})
		require.NoError(t, err)
		require.True(t, ok)
		c, err := tx.Cursor(kv.ChangeSets3)
		require.NoError(t, err)
		defer c.Close()
		firstK, _, err := c.First()
		require.NoError(t, err)
		require.NotNil(t, firstK)
		require.Equal(t, uint64(5), binary.BigEndian.Uint64(firstK[:8]))
		return nil
	})
	require.NoError(t, err)
}

func TestResolvePruneDiffSetsTarget(t *testing.T) {
	target, err := resolvePruneDiffSetsTarget(200, 0, 96)
	require.NoError(t, err)
	require.Equal(t, uint64(104), target)
	target, err = resolvePruneDiffSetsTarget(200, 50, 96)
	require.NoError(t, err)
	require.Equal(t, uint64(50), target)
	target, err = resolvePruneDiffSetsTarget(200, 104, 96)
	require.NoError(t, err)
	require.Equal(t, uint64(104), target)
	_, err = resolvePruneDiffSetsTarget(200, 105, 96)
	require.ErrorContains(t, err, "reorg window")
	_, err = resolvePruneDiffSetsTarget(200, 300, 96)
	require.ErrorContains(t, err, "reorg window")
	target, err = resolvePruneDiffSetsTarget(50, 0, 96)
	require.NoError(t, err)
	require.Equal(t, uint64(0), target)
	_, err = resolvePruneDiffSetsTarget(50, 10, 96)
	require.ErrorContains(t, err, "reorg window")
}

func TestPruneDiffSetsNothingBelowTarget(t *testing.T) {
	ctx := context.Background()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	err := db.Update(ctx, func(tx kv.RwTx) error {
		return changeset.WriteDiffSet(tx, 100, common.Hash{1}, &changeset.StateChangeSet{})
	})
	require.NoError(t, err)
	deleted, err := pruneDiffSets(ctx, db, 50, 3, log.New())
	require.NoError(t, err)
	require.Equal(t, uint64(0), deleted)
	err = db.View(ctx, func(tx kv.Tx) error {
		_, ok, err := changeset.ReadDiffSet(tx, 100, common.Hash{1})
		require.NoError(t, err)
		require.True(t, ok)
		return nil
	})
	require.NoError(t, err)
}
