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
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

// TestFindExecutedDiffsetAtHeight_FallsBackAfterCanonicalReorg is a
// regression test for the CREATE2-collision-after-reorg bug that surfaced on
// hoodi at block 2 789 993 (release/3.4).
//
// Repro of the original chain of events:
//
//  1. Erigon executes a sidechain block H_old at height N. Its diffset is
//     stored under (N, H_old) in kv.ChangeSets3. H_old is briefly canonical.
//  2. Headers stage receives the canonical chain, re-canonicalises height N
//     to H_new. The canonical pointer flips before execution stage unwinds.
//  3. unwindExec3 asks for the diffset under the *current* canonical hash
//     (H_new) — but the diffset was stored under H_old.
//
// Before the fix, step 3 returned !ok and was silently treated as "nothing
// to unwind" (changeSet stays nil → sd.unwindChangesetRaw stays nil → no
// tombstones written to AccountsDomain/CodeDomain → phantom CREATE2 state
// remains in latest-state tables). Re-executing the canonical chain over
// the phantom triggered an EIP-684/EIP-1014 collision on the next CREATE2
// to the same counterfactual address, consuming the entire gas limit and
// surfacing as `gas used mismatch` at the boundary block.
//
// The helper findExecutedDiffsetAtHeight must now fall back from the
// current canonical hash to the previously-applied sidechain hash.
func TestFindExecutedDiffsetAtHeight_FallsBackAfterCanonicalReorg(t *testing.T) {
	t.Parallel()

	logger := log.New()
	dirs := datadir.New(t.TempDir())
	rawDb := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).MustOpen()
	t.Cleanup(rawDb.Close)

	agg, err := dbstate.NewTest(dirs).StepSize(16).Logger(logger).Open(context.Background(), rawDb)
	require.NoError(t, err)
	t.Cleanup(agg.Close)

	db, err := temporal.New(rawDb, agg)
	require.NoError(t, err)

	// Block reader backed only by MDBX — no snapshots needed because the unwind
	// range is at the tip, well above any frozen snapshot boundary.
	snaps := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dirs.Snap, logger)
	t.Cleanup(snaps.Close)
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
	hNew := makeHeader(height, common.Hash{0x02})
	require.NoError(t, rawdb.WriteHeader(tx, hOld))
	require.NoError(t, rawdb.WriteHeader(tx, hNew))

	// Diffset is stored under hOld — this is the block we actually executed.
	addr := common.Address{0xde, 0xad}
	cs := &changeset.StateChangeSet{}
	cs.Diffs[kv.AccountsDomain].DomainUpdate(addr.Bytes(), kv.Step(0), nil /* prev=nil → []byte{} tombstone on unwind */)
	require.NoError(t, changeset.WriteDiffSet(tx, height, hOld.Hash(), cs))

	// Phase 1: hOld is canonical → direct hit under the current canonical hash.
	require.NoError(t, rawdb.WriteCanonicalHash(tx, hOld.Hash(), height))
	got, executed, found, err := findExecutedDiffsetAtHeight(ctx, tx, br, doms, height)
	require.NoError(t, err)
	require.True(t, found, "diffset must be found when canonical hash matches stored hash")
	require.Equal(t, hOld.Hash(), executed, "executedHash must be hOld when canonical points at hOld")
	require.NotEmpty(t, got[kv.AccountsDomain], "AccountsDomain diff list must be non-empty")

	// Phase 2: headers stage re-canonicalises to hNew (which has no diffset).
	// Before the fix the lookup returns !ok, the unwind silently no-ops and
	// the phantom state survives. The fix must locate the diffset by walking
	// the other header(s) at this height.
	require.NoError(t, rawdb.WriteCanonicalHash(tx, hNew.Hash(), height))

	// Document the pre-fix failure mode: a direct lookup against the now-
	// canonical hash returns !ok, which is precisely the unwind regression.
	_, ok, err := doms.GetDiffset(tx, hNew.Hash(), height)
	require.NoError(t, err)
	require.False(t, ok, "sanity: pre-fix code path (direct GetDiffset under new canonical) must miss — this is the bug")

	got, executed, found, err = findExecutedDiffsetAtHeight(ctx, tx, br, doms, height)
	require.NoError(t, err)
	require.True(t, found, "diffset must be located via fallback after canonical flip")
	require.Equal(t, hOld.Hash(), executed, "executedHash must remain hOld (the actually-executed block) after canonical flip")
	require.NotEmpty(t, got[kv.AccountsDomain], "AccountsDomain diff list must survive the fallback")

	// Phase 3: there is genuinely no stored diffset at this height under any
	// known header — found must be false (no spurious matches).
	const heightEmpty = uint64(11)
	hEmpty := makeHeader(heightEmpty, common.Hash{0x03})
	require.NoError(t, rawdb.WriteHeader(tx, hEmpty))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, hEmpty.Hash(), heightEmpty))
	_, _, found, err = findExecutedDiffsetAtHeight(ctx, tx, br, doms, heightEmpty)
	require.NoError(t, err)
	require.False(t, found, "must report not-found when no header at this height has a stored diffset")
}

func makeHeader(num uint64, parentMarker common.Hash) *types.Header {
	return &types.Header{
		Number:     *uint256.NewInt(num),
		ParentHash: parentMarker,
		Difficulty: *uint256.NewInt(1),
		Extra:      parentMarker.Bytes(), // make the hash distinct per `parentMarker`
	}
}

// TestUnwindExecutionStage_PrunesUncommittedOverlayWrite is a regression test
// for the Hoodi block-3004265 gas-used mismatch (release/3.4).
//
// Repro of the original chain of events:
//
//  1. In serial batch execution, block 3004265 tx19 does a first-time SSTORE to
//     ca5daf64 slot0. That write lands in the in-RAM SharedDomains /
//     TemporalMemBatch overlay (stamped with tx19's txNum) but the block then
//     fails its post-execution gas check, so its step is never committed.
//  2. The executor schedules UnwindTo(3004264). Because 3004265 was never
//     committed, the committed execution-stage progress (s.BlockNumber) sits at
//     or below the unwind point, so UnwindExecutionStage hit the
//     `u.UnwindPoint >= s.BlockNumber` early return and skipped unwindExec3 —
//     i.e. it never called sd.Unwind, so the overlay prune added by #20625
//     (which only runs from unwindExec3) never executed.
//  3. The same overlay is reused across the unwind→retry loop inside one
//     sync.Run, so on retry the storage read returned tx19's own stale
//     first-write (…a3a34) instead of the committed 0. The contract took the
//     "already initialised" branch, skipped an SSTORE_SET (20000 gas), the block
//     came up exactly 21045 gas short, and the node spun in an unwind/retry loop.
//
// Before the fix, step 2's early return leaves the overlay untouched, so the
// failed block's write is still visible after the unwind — this test asserts it
// is gone, while a write at/below the unwind point survives (no over-pruning).
func TestUnwindExecutionStage_PrunesUncommittedOverlayWrite(t *testing.T) {
	t.Parallel()

	logger := log.New()
	dirs := datadir.New(t.TempDir())
	rawDb := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).MustOpen()
	t.Cleanup(rawDb.Close)

	// stepSize far above every txNum used here keeps everything in step 0 — the
	// overlay prune compares raw txNums, so the commitment/step machinery is moot.
	agg, err := dbstate.NewTest(dirs).StepSize(10_000).Logger(logger).Open(context.Background(), rawDb)
	require.NoError(t, err)
	t.Cleanup(agg.Close)

	db, err := temporal.New(rawDb, agg)
	require.NoError(t, err)

	// Block reader backed only by MDBX — the unwind range is at the tip, above
	// any frozen snapshot boundary, so no snapshots are needed.
	snaps := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dirs.Snap, logger)
	t.Cleanup(snaps.Close)
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
	boundaryTxNum, err := br.TxnumReader().Min(ctx, tx, unwindPoint+1)
	require.NoError(t, err)
	require.Equal(t, failedBlock*perBlock, boundaryTxNum, "sanity: Min(failedBlock) == first txNum of failedBlock")

	// staleKey mirrors ca5daf64 slot0: first-written by failedBlock's tx19 at a
	// txNum strictly above the unwind boundary — must be pruned on unwind.
	staleAddr := common.HexToAddress("0xca5daf6473971693b760cc65d726f72c6849d615")
	staleKey := append(append([]byte{}, staleAddr.Bytes()...), common.Hash{}.Bytes()...) // slot 0
	staleVal := common.HexToHash("0xd7549f2a387fa81a1d5a77adc7bd3f782ac0780c460689d88e22aee6916a3a34").Bytes()
	const staleTxNum = failedBlock*perBlock + 5 // inside failedBlock, above the boundary

	// keepKey is written by a block at/below the unwind point — must survive.
	keepAddr := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	keepKey := append(append([]byte{}, keepAddr.Bytes()...), common.Hash{31: 0x01}.Bytes()...)
	keepVal := []byte{0xbe, 0xef}
	const keepTxNum = unwindPoint * perBlock // inside the unwind-target block

	doms.SetTxNum(keepTxNum)
	require.NoError(t, doms.DomainPut(kv.StorageDomain, tx, keepKey, keepVal, keepTxNum, nil))
	doms.SetTxNum(staleTxNum)
	require.NoError(t, doms.DomainPut(kv.StorageDomain, tx, staleKey, staleVal, staleTxNum, nil))

	// Sanity: both visible through the overlay before the unwind.
	got, _, err := doms.GetLatest(kv.StorageDomain, tx, staleKey)
	require.NoError(t, err)
	require.Equal(t, staleVal, got, "precondition: stale write must be visible pre-unwind")
	got, _, err = doms.GetLatest(kv.StorageDomain, tx, keepKey)
	require.NoError(t, err)
	require.Equal(t, keepVal, got, "precondition: keep write must be visible pre-unwind")

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
		"after Unwind to block %d, the overlay must not return failedBlock(%d) tx write %x (got %x): "+
			"UnwindExecutionStage skipped the overlay prune on the no-op-disk-unwind path",
		unwindPoint, failedBlock, staleVal, got)

	// A write at/below the unwind point must be untouched (no over-pruning).
	got, _, err = doms.GetLatest(kv.StorageDomain, tx, keepKey)
	require.NoError(t, err)
	require.Equal(t, keepVal, got, "write at/below the unwind point must survive the prune")
}
