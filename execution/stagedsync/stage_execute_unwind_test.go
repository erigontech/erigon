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
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain/networkname"
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
