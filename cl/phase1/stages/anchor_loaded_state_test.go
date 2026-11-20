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

package stages

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

type slotRecordingBlockReader struct {
	freezeblocks.BeaconSnapshotReader
	slots []uint64
}

func (r *slotRecordingBlockReader) ReadBlockBySlot(_ context.Context, _ kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	r.slots = append(r.slots, slot)
	return nil, nil
}

func TestAnchorOnLoadedStateReplaysPersistedHeadAboveGenesis(t *testing.T) {
	for _, tc := range []struct {
		name         string
		soleProducer bool
		wantSlots    []uint64
	}{
		{name: "sole producer", soleProducer: true, wantSlots: []uint64{1, 2, 3}},
		{name: "networked node"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newLockOrderFixture(t, 0)
			cfg := fixture.cfg
			cfg.state = fixture.headState
			cfg.indiciesDB = mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			reader := &slotRecordingBlockReader{}
			cfg.blockReader = reader
			require.NoError(t, cfg.indiciesDB.Update(t.Context(), func(tx kv.RwTx) error {
				return beacon_indicies.MarkRootCanonical(tx, 3, common.Hash{0x03})
			}))

			require.NoError(t, anchorOnLoadedState(t.Context(), log.New(), cfg, fixture.headRoot, tc.soleProducer))

			require.Equal(t, tc.wantSlots, reader.slots)
		})
	}
}
