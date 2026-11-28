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

package antiquary

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestCaplinBlockRetireRangeSafetyMarginBoundary(t *testing.T) {
	for _, test := range []struct {
		name      string
		finalized uint64
		wantTo    uint64
	}{
		{name: "margin", finalized: 20_000, wantTo: 0},
		{name: "below margin", finalized: 19_999, wantTo: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				if err := beacon_indicies.MarkRootCanonical(tx, 40_000, common.Hash{1}); err != nil {
					return err
				}
				return beacon_indicies.WriteHighestFinalized(tx, test.finalized)
			}))
			snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, nil, dirs, log.New())
			t.Cleanup(snapshots.Close)
			require.NoError(t, snapshots.OpenFolder())
			var from, to uint64
			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
				var err error
				from, to, err = CaplinBlockRetireRange(tx, snapshots)
				return err
			}))
			require.Equal(t, uint64(0), from)
			require.Equal(t, test.wantTo, to)
		})
	}
}
