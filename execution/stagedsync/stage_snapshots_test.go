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

package stagedsync

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
)

// TestBuildOrDeferE2Indices_LifecycleGateNoOps verifies that when
// SnapshotsCfg.lifecycleDrivenByStorage is set, buildOrDeferE2Indices
// returns nil without calling cfg.blockRetire — proving the storage
// component's lifecycle driver is taking over without the stage
// having to coordinate.
func TestBuildOrDeferE2Indices_LifecycleGateNoOps(t *testing.T) {
	cfg := SnapshotsCfg{
		chainConfig: &chain.Config{},
		blockRetire: nil,
	}
	cfg.SetLifecycleDrivenByStorage(true)

	s := &StageState{}
	require.NoError(t, buildOrDeferE2Indices(context.Background(), s, cfg, 0),
		"flag-on must short-circuit before touching blockRetire")
}

func TestBuildOrDeferE3Accessors_LifecycleGateNoOps(t *testing.T) {
	cfg := SnapshotsCfg{}
	cfg.SetLifecycleDrivenByStorage(true)

	s := &StageState{}
	require.NoError(t, buildOrDeferE3Accessors(context.Background(), s, cfg, nil, 0),
		"flag-on must short-circuit before touching agg")
}

func TestSetLifecycleDrivenByStorage_DefaultsFalse(t *testing.T) {
	cfg := SnapshotsCfg{}
	require.False(t, cfg.lifecycleDrivenByStorage,
		"default must be false — stage drives until production wires the flag on")
	cfg.SetLifecycleDrivenByStorage(true)
	require.True(t, cfg.lifecycleDrivenByStorage)
	cfg.SetLifecycleDrivenByStorage(false)
	require.False(t, cfg.lifecycleDrivenByStorage)
}

// stubPruneReader supplies the frozen tip pruneCanonicalMarkers derives
// its threshold from.
type stubPruneReader struct {
	dbservices.FullBlockReader
	frozen uint64
}

func (s stubPruneReader) FrozenBlocks() uint64                    { return s.frozen }
func (s stubPruneReader) FrozenBorBlocks(bool) uint64             { return s.frozen }
func (s stubPruneReader) BorSnapshots() dbservices.BlockSnapshots { return nil }

// TestPruneCanonicalMarkers_KeepsTotalDifficulty pins that pruning
// canonical markers does not take TD with them.
//
// Mode-B/D unwind to an arbitrary historical target, and the insert of
// the first block after that target reads its parent's TD. TD lives
// only in MDBX — snapshots do not carry it — so deleting it for a block
// in the snapshot range makes any unwind targeting there unrecoverable:
// "parent's total difficulty not found", which no FCU nudge clears.
//
// The seed deliberately writes TD for every frozen header for this
// reason. Pruning it back out of the same range put the two in direct
// contradiction and produced a hole that survived four soak cycles and
// three wrong fixes on the seeding side.
func TestPruneCanonicalMarkers_KeepsTotalDifficulty(t *testing.T) {
	t.Parallel()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	// Markers + TD well below the prune threshold, so the prune walks them.
	const frozen = 5_000_000
	blocks := []uint64{1, 100, 1000}
	for _, bn := range blocks {
		var h common.Hash
		binary.BigEndian.PutUint64(h[:8], bn)
		require.NoError(t, rawdb.WriteCanonicalHash(tx, h, bn))
		require.NoError(t, rawdb.WriteTd(tx, h, bn, *uint256.NewInt(bn)))
	}

	require.NoError(t, pruneCanonicalMarkers(ctx, tx, stubPruneReader{frozen: frozen}))

	for _, bn := range blocks {
		var h common.Hash
		binary.BigEndian.PutUint64(h[:8], bn)
		td, err := rawdb.ReadTd(tx, h, bn)
		require.NoError(t, err)
		require.NotNil(t, td,
			"TD at %d must survive marker pruning — an unwind targeting there needs it as parent TD", bn)
	}
}
