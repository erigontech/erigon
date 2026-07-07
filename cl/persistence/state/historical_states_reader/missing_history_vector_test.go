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

package historical_states_reader

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

// A dense block_roots/state_roots history slot with a missing snapshot entry (a
// frozen data gap, never a valid state) must surface as ErrMissingHistoryVectorData
// so the antiquary re-antiquates past it instead of stalling permanently.
func TestReadHistoryHashVector_MissingEntryIsTyped(t *testing.T) {
	const size = uint64(64)
	const slot = 2 * size // needFromGenesis=0; window [slot-size, slot-1]

	genesisState, err := initial_state.GetGenesisState(chainspec.MainnetChainID)
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		hole uint64
	}{
		{"first-slot hole", slot - size},
		{"mid-window hole", slot - size/2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := memdb.NewTestDB(t, dbcfg.ChainDB)
			tx, err := db.BeginRw(context.Background())
			require.NoError(t, err)
			defer tx.Rollback()

			var h [32]byte
			for s := slot - size; s < slot; s++ {
				if s == tc.hole {
					continue
				}
				h[0], h[1] = byte(s), byte(s>>8)
				require.NoError(t, tx.Put(kv.BlockRoot, base_encoding.Encode64ToBytes4(s), h[:]))
			}

			r := NewHistoricalStatesReader(&clparams.MainnetBeaconConfig, nil, nil, genesisState, nil, nil)
			getter := state_accessors.GetValFnTxAndSnapshot(tx, nil)
			out := solid.NewHashVector(int(size))

			err = r.readHistoryHashVector(tx, getter, genesisState.BlockRoots(), slot, size, kv.BlockRoot, out)
			require.ErrorIs(t, err, ErrMissingHistoryVectorData)
			require.ErrorContains(t, err, kv.BlockRoot)
		})
	}
}
