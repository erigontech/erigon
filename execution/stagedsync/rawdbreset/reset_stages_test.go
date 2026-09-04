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

package rawdbreset_test

import (
	"context"
	"testing"

	"github.com/holiman/uint256"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

// TestResetCanonicalAndRefillFromSnapshots_ClearsStaleSidechainPointers
// verifies the fix for a stale-canonical-pointer leak observed on hoodi
// snapshotters running release/3.4: a sidechain block was once canonical from
// CL's POV and was committed into kv.HeaderCanonical by a successful
// forkchoice update; subsequent reorg-to-real-canonical FCUs failed on
// execution (pre-#21157 unwind bug), the tx rolled back, and the sidechain
// hash stayed in kv.HeaderCanonical. integration reset_state cleared MDBX
// domain state but did NOT touch the canonical-hash mapping, so forward
// catchup after restart re-applied the sidechain block as canonical and
// re-introduced the phantom.
//
// Pre-fix, ResetCanonicalAndRefillFromSnapshots did not exist (compile
// error) and ResetState left kv.HeaderCanonical untouched. With the fix,
// ResetCanonicalAndRefillFromSnapshots wipes the entire kv.HeaderCanonical
// table, clears Headers/BlockHashes/Bodies/Senders/Snapshots stage progress
// and (when frozen blocks are present) hands re-population off to
// FillDBFromSnapshots. The next forkchoice update from CL then drives
// canonical assignments for the post-tip range fresh, with no chance for
// stale sidechain pointers to survive.
func TestResetCanonicalAndRefillFromSnapshots_ClearsStaleSidechainPointers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	logger := log.New()
	br := freezeblocks.NewBlockReader(db.(freezeblocks.HasBlockFiles).DebugBlockFiles(), nil)

	const sideTipHeight = uint64(110)
	staleHashAt105 := common.Hash{0x99}

	err := db.Update(ctx, func(tx kv.RwTx) error {
		for h := uint64(0); h <= sideTipHeight; h++ {
			if err := rawdb.WriteCanonicalHash(tx, common.Hash{byte(h)}, h); err != nil {
				return err
			}
		}
		if err := rawdb.WriteCanonicalHash(tx, staleHashAt105, 105); err != nil {
			return err
		}
		if err := rawdb.WriteHeadHeaderHash(tx, common.Hash{byte(sideTipHeight)}); err != nil {
			return err
		}
		for _, st := range []stages.SyncStage{stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders, stages.Snapshots} {
			if err := stages.SaveStageProgress(tx, st, sideTipHeight); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	err = db.View(ctx, func(tx kv.Tx) error {
		h, errRead := rawdb.ReadCanonicalHash(tx, 105)
		require.NoError(t, errRead)
		require.Equal(t, staleHashAt105, h, "stale entry must be present before reset")
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, rawdbreset.ResetCanonicalAndRefillFromSnapshots(ctx, db, dirs, br, logger))

	err = db.View(ctx, func(tx kv.Tx) error {
		for h := uint64(0); h <= sideTipHeight; h++ {
			hash, errRead := rawdb.ReadCanonicalHash(tx, h)
			require.NoError(t, errRead)
			require.Equal(t, common.Hash{}, hash, "canonical hash at %d must be cleared", h)
		}
		for _, st := range []stages.SyncStage{stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders, stages.Snapshots} {
			progress, errRead := stages.GetStageProgress(tx, st)
			require.NoError(t, errRead)
			require.Zero(t, progress, "%s stage progress must be reset to 0 so FillDBFromSnapshots can re-advance it on the next start", st)
		}
		return nil
	})
	require.NoError(t, err)
}

// TestResetCanonicalAndRefillFromSnapshots_NoOpOnEmptyDB exercises the
// idempotency guarantee: calling on a fresh db with no canonical entries
// and no frozen blocks must succeed and leave everything empty.
func TestResetCanonicalAndRefillFromSnapshots_NoOpOnEmptyDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	logger := log.New()
	br := freezeblocks.NewBlockReader(db.(freezeblocks.HasBlockFiles).DebugBlockFiles(), nil)

	require.NoError(t, rawdbreset.ResetCanonicalAndRefillFromSnapshots(ctx, db, dirs, br, logger))

	err := db.View(ctx, func(tx kv.Tx) error {
		for _, st := range []stages.SyncStage{stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders, stages.Snapshots} {
			progress, errRead := stages.GetStageProgress(tx, st)
			require.NoError(t, errRead)
			require.Zero(t, progress, "%s stage progress must be zero on empty db", st)
		}
		return nil
	})
	require.NoError(t, err)
}

// stubFrozenHeaders serves a synthetic frozen header range for the
// incremental TD seed. Only the two methods the seed uses are real.
type stubFrozenHeaders struct {
	dbservices.FullBlockReader
	frozen     uint64
	visibleTo  uint64
	difficulty uint64
}

func (s stubFrozenHeaders) FrozenBlocks() uint64 { return s.frozen }

// GetPruneMarkerSafeThreshold consults these; the embedded interface is
// nil so they must be real.
func (s stubFrozenHeaders) FrozenBorBlocks(bool) uint64             { return s.frozen }
func (s stubFrozenHeaders) BorSnapshots() dbservices.BlockSnapshots { return nil }

// hashAt returns the hash the seed will actually key TD under: the
// header's own hash, so CanonicalHash and HeaderByNumber agree the way
// they do in production.
func (s stubFrozenHeaders) hashAt(n uint64) common.Hash {
	h, _ := s.HeaderByNumber(context.Background(), nil, n)
	if h == nil {
		return common.Hash{}
	}
	return h.Hash()
}

func (s stubFrozenHeaders) CanonicalHash(_ context.Context, _ kv.Getter, n uint64) (common.Hash, bool, error) {
	if n > s.frozen {
		return common.Hash{}, false, nil
	}
	return s.hashAt(n), true, nil
}

func (s stubFrozenHeaders) HeaderByNumber(_ context.Context, _ kv.Getter, n uint64) (*types.Header, error) {
	if n > s.frozen {
		return nil, nil
	}
	return &types.Header{Number: *uint256.NewInt(n), Difficulty: *uint256.NewInt(s.difficulty)}, nil
}

// TestExtendTDFromSnapshots_SeedsBlocksArrivingAfterFirstFill pins the
// hole cycles 25 and 26 both produced. The one-shot postIndexed seed
// covers FrozenBlocks() as of the instant it fires; snapshot files that
// land afterwards extend frozen coverage and were never seeded, leaving
// a permanent TD gap that wedges any unwind targeting into it.
//
// Both cycles' gap started at exactly 3351998 — the frozen tip when
// phase-1 indexing completed — which is what makes this deterministic
// rather than a race.
func TestExtendTDFromSnapshots_SeedsBlocksArrivingAfterFirstFill(t *testing.T) {
	t.Parallel()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	br := stubFrozenHeaders{frozen: 100, difficulty: 2}
	// First seed covered [0, 100] and recorded its watermark.
	require.NoError(t, rawdb.WriteTd(tx, br.hashAt(100), 100, *uint256.NewInt(200)))
	require.NoError(t, rawdbreset.SaveSnapshotSeedProgress(tx, 100))

	// More files arrive: frozen coverage now reaches 150.
	br.frozen = 150
	require.NoError(t, rawdbreset.ExtendTDFromSnapshots("test", ctx, tx, br, log.New()))

	for _, b := range []uint64{101, 125, 150} {
		td, err := rawdb.ReadTd(tx, br.hashAt(b), b)
		require.NoError(t, err)
		require.NotNil(t, td, "block %d arrived after the first seed and must still get a TD row", b)
		require.Equal(t, uint256.NewInt(200+2*(b-100)).String(), td.String(), "TD must continue the chain sum at block %d", b)
	}

	got, err := rawdbreset.SnapshotSeedProgress(tx)
	require.NoError(t, err)
	require.Equal(t, uint64(150), got, "watermark must advance to the new frozen tip")
}

// TestExtendTDFromSnapshots_NoOpWhenCoverageUnchanged pins that a
// re-invocation with no new frozen blocks does nothing — the seed is
// driven off coverage growth and must be cheap to call repeatedly.
func TestExtendTDFromSnapshots_NoOpWhenCoverageUnchanged(t *testing.T) {
	t.Parallel()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	br := stubFrozenHeaders{frozen: 100, difficulty: 1}
	require.NoError(t, rawdbreset.SaveSnapshotSeedProgress(tx, 100))
	require.NoError(t, rawdbreset.ExtendTDFromSnapshots("test", ctx, tx, br, log.New()))

	td, err := rawdb.ReadTd(tx, br.hashAt(100), 100)
	require.NoError(t, err)
	require.Nil(t, td, "coverage unchanged — the seed must not walk or write anything")
}

// TestExtendTDFromSnapshots_SkipsBlocksAlreadySeeded pins idempotency:
// a block that already has a TD row keeps its existing value rather
// than being rewritten from a recomputed sum.
func TestExtendTDFromSnapshots_SkipsBlocksAlreadySeeded(t *testing.T) {
	t.Parallel()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	br := stubFrozenHeaders{frozen: 10, difficulty: 5}
	require.NoError(t, rawdb.WriteTd(tx, br.hashAt(5), 5, *uint256.NewInt(1)))
	require.NoError(t, rawdb.WriteTd(tx, br.hashAt(7), 7, *uint256.NewInt(4242)))
	require.NoError(t, rawdbreset.SaveSnapshotSeedProgress(tx, 5))

	require.NoError(t, rawdbreset.ExtendTDFromSnapshots("test", ctx, tx, br, log.New()))

	td, err := rawdb.ReadTd(tx, br.hashAt(7), 7)
	require.NoError(t, err)
	require.Equal(t, uint256.NewInt(4242).String(), td.String(), "an existing TD row must not be rewritten")
}

// headersRange makes stubFrozenHeaders yield only the headers whose
// files are "visible", which can stop short of FrozenBlocks(). Real
// ForEachHeader walks the visible segment set, and that set ends at the
// first coverage gap, while FrozenBlocks() reports segmentsMax and
// ignores gaps.
func (s stubFrozenHeaders) HeadersRange(ctx context.Context, walker func(*types.Header) error) error {
	for n := uint64(0); n <= s.visibleTo; n++ {
		h, _ := s.HeaderByNumber(ctx, nil, n)
		if h == nil {
			return nil
		}
		if err := walker(h); err != nil {
			return err
		}
	}
	return nil
}

// TestFillDBFromSnapshots_WatermarkRecordsWhatWasWritten pins that the
// seed watermark reports the highest block actually seeded, not the
// frozen tip the fill aimed at.
//
// The two disagree whenever the visible header set ends before
// segmentsMax. Recording the intent strands everything between: the
// incremental pass resumes above the unwritten range and never looks
// down, so the gap becomes permanent. Cycle 27 showed exactly that — a
// watermark of 3555999 sitting above an unfilled TD gap of
// (3351998, 3405999).
func TestFillDBFromSnapshots_WatermarkRecordsWhatWasWritten(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	// Frozen coverage claims 200; only 100 headers are actually visible.
	br := stubFrozenHeaders{frozen: 200, visibleTo: 100, difficulty: 1}

	// Let only the Headers stage run; the others short-circuit on their
	// own progress guard.
	for _, st := range []stages.SyncStage{stages.Bodies, stages.BlockHashes, stages.Senders} {
		require.NoError(t, stages.SaveStageProgress(tx, st, 200))
	}

	require.NoError(t, rawdbreset.FillDBFromSnapshots("test", ctx, tx, dirs, br, log.New()))

	got, err := rawdbreset.SnapshotSeedProgress(tx)
	require.NoError(t, err)
	require.Equal(t, uint64(100), got,
		"watermark must be the highest block seeded, not FrozenBlocks() — otherwise the unwritten remainder is stranded")
}
