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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// A datadir is retired when the DB holds less than one step past the files: a
// partial step cannot go into a file yet.
func TestAssertRetired(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	for name, tc := range map[string]struct {
		lastTxNumInDB uint64 // 0: the DB holds no accounts history
		noCommitment  bool
		retired       bool
	}{
		"empty DB":                     {0, false, true},
		"partial step in DB":           {29, false, true},
		"one full step in DB":          {30, false, false},
		"no commitment files, retired": {29, true, true},
	} {
		t.Run(name, func(t *testing.T) {
			db, agg := testDbAndAggregatorv3(t, 10)
			ranges := []testFileRange{{0, 1}, {1, 2}}
			generateStateFiles(t, agg.Dirs(), ranges)
			if !tc.noCommitment {
				generateCommitmentFile(t, agg.Dirs(), ranges)
			}
			require.NoError(t, agg.OpenFolder(db))
			tx, err := db.BeginRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			if tc.lastTxNumInDB > 0 {
				key := binary.BigEndian.AppendUint64(nil, tc.lastTxNumInDB)
				require.NoError(t, tx.Put(agg.d[kv.AccountsDomain].History.InvertedIndex.KeysTable, key, []byte{1}))
			}
			at := agg.BeginFilesRo()
			defer at.Close()
			err = at.AssertRetired(tx)
			if tc.retired {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "seg retire")
			}
		})
	}
}
