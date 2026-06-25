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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/ethconfig"
)

// Pins that the unwind early-return (u.UnwindPoint >= s.BlockNumber) prunes
// uncommitted overlay writes above the unwind point — which the overlay-reusing
// retry loop would otherwise read as stale — while a write at/below the unwind
// point survives (no over-pruning).
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
	staleSlot := common.Hash{} // slot 0
	staleKey := append(append([]byte{}, staleAddr[:]...), staleSlot[:]...)
	staleValHash := common.HexToHash("0xd7549f2a387fa81a1d5a77adc7bd3f782ac0780c460689d88e22aee6916a3a34")
	staleVal := staleValHash[:]
	const staleTxNum = failedBlock*perBlock + 5 // inside failedBlock, above the boundary

	// keepKey is written by a block at/below the unwind point — must survive.
	keepAddr := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	keepSlot := common.Hash{31: 0x01}
	keepKey := append(append([]byte{}, keepAddr[:]...), keepSlot[:]...)
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
