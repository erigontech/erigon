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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/prune"
)

// TestClampPruneProgressTo pins that an unwind rewinds recorded prune
// progress along with the files.
//
// Trimming snapshot files back without touching progress leaves the DB
// claiming to have pruned a range that now exists in neither files nor DB.
// CheckFilesDBGap reads that as a corrupt datadir and exits the process —
// which kills the node mid-SetHead, so the caller sees no response at all.
func TestClampPruneProgressTo(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	const (
		aheadTable  = "TblAccountVals"
		behindTable = "TblStorageVals"
		newTip      = 124_981_468
	)

	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := SavePruneValProgress(tx, aheadTable, &prune.Stat{
			TxFrom: 0, TxTo: 129_687_500, // pruned past where the files now end
			KeyProgress: prune.Done, ValueProgress: prune.Done,
		}); err != nil {
			return err
		}
		return SavePruneValProgress(tx, behindTable, &prune.Stat{
			TxFrom: 0, TxTo: 100_000_000, // already below the new tip
			KeyProgress: prune.Done, ValueProgress: prune.Done,
		})
	}))

	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return ClampPruneProgressTo(tx, newTip)
	}))

	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		ahead, err := GetPruneValProgress(tx, []byte(aheadTable))
		require.NoError(t, err)
		require.LessOrEqual(t, ahead.TxTo, uint64(newTip),
			"progress past the new file tip must be rewound — otherwise CheckFilesDBGap exits the process")

		behind, err := GetPruneValProgress(tx, []byte(behindTable))
		require.NoError(t, err)
		require.Equal(t, uint64(100_000_000), behind.TxTo,
			"progress already below the tip describes real pruning and must be kept")
		return nil
	}))
}
