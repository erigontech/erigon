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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

// Pins that the unwind early-return (u.UnwindPoint >= s.BlockNumber) prunes
// uncommitted overlay writes above committed progress (s.BlockNumber) — the
// whole (s.BlockNumber, u.UnwindPoint] range re-executes since u.Done is skipped,
// so those writes must go too, not just ones above u.UnwindPoint — while a write
// at/below committed progress survives (no over-pruning).
func TestUnwindExecutionStage_PrunesUncommittedOverlayWrite(t *testing.T) {
	t.Parallel()

	logger := log.New()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDBWithStepSize(t, dirs, 10_000)

	snaps := db.(freezeblocks.HasBlockFiles).DebugBlockFiles()
	br := freezeblocks.NewBlockReader(snaps, nil)

	ctx := context.Background()
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	// txNum layout: block b owns txNums [perBlock*b, perBlock*b+perBlock-1], so
	// TxnumReader().Min(b) == perBlock*b (the block's first/system txNum).
	const perBlock = uint64(10)
	for b := uint64(0); b <= 8; b++ {
		require.NoError(t, rawdbv3.TxNums.Append(tx, b, b*perBlock+perBlock-1))
	}

	doms, err := execctx.NewSharedDomains(ctx, tx, logger)
	require.NoError(t, err)
	defer doms.Close()
	doms.SetChangesetAccumulator(&changeset.StateChangeSet{})

	const (
		committedBlock = uint64(5) // execution-stage progress (last flushed block)
		unwindPoint    = uint64(7) // = failedBlock-1; >= committedBlock => disk no-op
		failedBlock    = uint64(8) // block whose post-exec gas check failed mid-batch
	)
	// The early-return prunes from Min(s.BlockNumber+1) = Min(committedBlock+1),
	// the first re-executed txNum — verify that boundary (not unwindPoint+1).
	pruneFloorTxNum, err := br.TxnumReader().Min(ctx, tx, committedBlock+1)
	require.NoError(t, err)
	require.Equal(t, (committedBlock+1)*perBlock, pruneFloorTxNum, "sanity: prune floor == first txNum of committedBlock+1")

	// staleKey mirrors ca5daf64 slot0: first-written by failedBlock's tx19 at a
	// txNum strictly above the unwind boundary — must be pruned on unwind.
	staleAddr := common.HexToAddress("0xca5daf6473971693b760cc65d726f72c6849d615")
	staleSlot := common.Hash{} // slot 0
	staleKey := append(append([]byte{}, staleAddr[:]...), staleSlot[:]...)
	staleValHash := common.HexToHash("0xd7549f2a387fa81a1d5a77adc7bd3f782ac0780c460689d88e22aee6916a3a34")
	staleVal := staleValHash[:]
	const staleTxNum = failedBlock*perBlock + 5 // inside failedBlock, above the boundary

	// reexecKey is written by a block in (committedBlock, unwindPoint] — i.e.
	// above committed progress. The early return skips u.Done, so progress stays
	// at committedBlock and this block re-executes; its overlay write must be
	// pruned, even though it's at/below the unwind point.
	reexecAddr := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	reexecSlot := common.Hash{31: 0x01}
	reexecKey := append(append([]byte{}, reexecAddr[:]...), reexecSlot[:]...)
	reexecVal := []byte{0xbe, 0xef}
	const reexecTxNum = unwindPoint * perBlock // inside the unwind-target block (block 7 > committed 5)

	// keepKey is written at/below committed progress — committed state, must survive.
	keepAddr := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	keepSlot := common.Hash{31: 0x02}
	keepKey := append(append([]byte{}, keepAddr[:]...), keepSlot[:]...)
	keepVal := []byte{0xca, 0xfe}
	const keepTxNum = committedBlock*perBlock + 3 // inside the committed block

	doms.SetTxNum(keepTxNum)
	require.NoError(t, doms.DomainPut(kv.StorageDomain, tx, keepKey, keepVal, keepTxNum, nil))
	doms.SetTxNum(reexecTxNum)
	require.NoError(t, doms.DomainPut(kv.StorageDomain, tx, reexecKey, reexecVal, reexecTxNum, nil))
	doms.SetTxNum(staleTxNum)
	require.NoError(t, doms.DomainPut(kv.StorageDomain, tx, staleKey, staleVal, staleTxNum, nil))

	// Sanity: all visible through the overlay before the unwind.
	got, _, err := doms.GetLatest(kv.StorageDomain, tx, staleKey)
	require.NoError(t, err)
	require.Equal(t, staleVal, got, "precondition: stale write must be visible pre-unwind")
	got, _, err = doms.GetLatest(kv.StorageDomain, tx, reexecKey)
	require.NoError(t, err)
	require.Equal(t, reexecVal, got, "precondition: re-exec-range write must be visible pre-unwind")

	cfg := ExecuteBlockCfg{blockReader: br}
	s := &StageState{ID: stages.Execution, BlockNumber: committedBlock}
	u := &UnwindState{ID: stages.Execution, UnwindPoint: unwindPoint, CurrentBlockNumber: failedBlock}
	require.GreaterOrEqual(t, u.UnwindPoint, s.BlockNumber, "test must exercise the no-op-disk-unwind branch")

	require.NoError(t, UnwindExecutionStage(u, s, doms, tx, ctx, cfg, logger))

	// The failed block's first-time write must be gone, so a re-execution reads
	// the committed value (absent here -> empty) and charges SSTORE_SET again.
	got, _, err = doms.GetLatest(kv.StorageDomain, tx, staleKey)
	require.NoError(t, err)
	require.Empty(t, got,
		"after Unwind, the overlay must not return failedBlock(%d) tx write %x (got %x)",
		failedBlock, staleVal, got)

	// A write in (committedBlock, unwindPoint] re-executes too, so it must also
	// be pruned — the prune floor is s.BlockNumber+1, not u.UnwindPoint+1.
	got, _, err = doms.GetLatest(kv.StorageDomain, tx, reexecKey)
	require.NoError(t, err)
	require.Empty(t, got,
		"after Unwind, the overlay must not return the re-exec-range write at block %d (got %x): "+
			"prune floor must be s.BlockNumber+1, not u.UnwindPoint+1",
		unwindPoint, got)

	// A write at/below committed progress is committed state — must survive.
	got, _, err = doms.GetLatest(kv.StorageDomain, tx, keepKey)
	require.NoError(t, err)
	require.Equal(t, keepVal, got, "write at/below committed progress must survive the prune")
}

func makeHeader(number uint64, root common.Hash) *types.Header {
	return &types.Header{
		Number:     *uint256.NewInt(number),
		Root:       root,
		Difficulty: *uint256.NewInt(1),
	}
}

// TestFindExecutedDiffsetAtHeight_FallsBackAfterCanonicalReorg pins the unwind diffset
// lookup. After a reorg clears the canonical hash for the unwound range, a lookup keyed
// on the canonical hash misses; without the fallback to the stored header the diffset is
// never found, the unwind silently no-ops, and the unwound block's state survives as
// phantom data.
func TestFindExecutedDiffsetAtHeight_FallsBackAfterCanonicalReorg(t *testing.T) {
	t.Parallel()

	logger := log.New()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDBWithStepSize(t, dirs, 16)

	// Block reader backed only by MDBX — the unwind range is at the tip, above any
	// frozen snapshot boundary, so no snapshots are needed.
	snaps := db.(freezeblocks.HasBlockFiles).DebugBlockFiles()
	br := freezeblocks.NewBlockReader(snaps, nil)

	ctx := context.Background()
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	doms, err := execctx.NewSharedDomains(ctx, tx, logger)
	require.NoError(t, err)
	defer doms.Close()

	const height = uint64(10)
	hOld := makeHeader(height, common.Hash{0x01})
	require.NoError(t, rawdb.WriteHeader(tx, hOld))

	// Diffset is stored under hOld — the block actually executed at this height.
	addr := common.Address{0xde, 0xad}
	cs := &changeset.StateChangeSet{}
	cs.Diffs[kv.AccountsDomain].DomainUpdate(addr[:], kv.Step(0), nil)
	require.NoError(t, changeset.WriteDiffSet(tx, height, hOld.Hash(), cs))

	// Phase 1: hOld is canonical → direct hit under the canonical hash.
	require.NoError(t, rawdb.WriteCanonicalHash(tx, hOld.Hash(), height))
	diffs, executed, found, err := findExecutedDiffsetAtHeight(ctx, tx, br, doms, height)
	require.NoError(t, err)
	require.True(t, found, "diffset must be found when the canonical hash matches the stored hash")
	require.Equal(t, hOld.Hash(), executed, "executedHash must be hOld when canonical points at hOld")
	require.NotEmpty(t, diffs[kv.AccountsDomain], "AccountsDomain diff list must be non-empty")

	// Phase 2: a reorg clears the canonical hash for the unwound range. A canonical-hash
	// lookup now misses (the pre-fix failure mode); the diffset must still be located by
	// falling back to the stored header.
	require.NoError(t, rawdb.TruncateCanonicalHash(tx, height, false))
	_, canonOk, err := br.CanonicalHash(ctx, tx, height)
	require.NoError(t, err)
	require.False(t, canonOk, "sanity: canonical hash must be cleared at the unwound height — the pre-fix lookup keys on this and misses")

	diffs, executed, found, err = findExecutedDiffsetAtHeight(ctx, tx, br, doms, height)
	require.NoError(t, err)
	require.True(t, found, "diffset must be located via the stored-header fallback after the canonical hash was cleared")
	require.Equal(t, hOld.Hash(), executed, "executedHash must remain hOld (the actually-executed block) after the canonical flip")
	require.NotEmpty(t, diffs[kv.AccountsDomain], "AccountsDomain diff list must survive the fallback")

	// Phase 3: a height whose canonical header has no stored diffset — found must be
	// false (no spurious match, no error).
	const heightEmpty = uint64(11)
	hEmpty := makeHeader(heightEmpty, common.Hash{0x03})
	require.NoError(t, rawdb.WriteHeader(tx, hEmpty))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, hEmpty.Hash(), heightEmpty))
	_, _, found, err = findExecutedDiffsetAtHeight(ctx, tx, br, doms, heightEmpty)
	require.NoError(t, err)
	require.False(t, found, "must report not-found when no diffset is stored at this height")
}
