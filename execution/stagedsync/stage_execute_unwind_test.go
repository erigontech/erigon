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

// TestUnwindExecutionStage_PrunesUncommittedOverlayWrite is a regression test
// for the Hoodi block-3004265 gas-used mismatch (originally found on
// release/3.4, same gap on main).
//
// When a block fails its post-execution gas check mid-batch, its writes sit in
// the in-RAM SharedDomains/TemporalMemBatch overlay but were never committed, so
// the committed execution-stage progress (s.BlockNumber) is <= u.UnwindPoint and
// UnwindExecutionStage takes the no-disk-rollback branch. That branch does not
// call u.Done, so re-execution resumes from the committed block (SeekCommitment
// returns s.BlockNumber; re-execution resumes at s.BlockNumber+1) — NOT from
// u.UnwindPoint+1. The overlay must therefore be pruned to the last committed
// txNum, Max(s.BlockNumber): every write of a re-executed block is dropped —
// the failed block's, a block in (s.BlockNumber, u.UnwindPoint], AND the first
// re-executed block's system tx at Min(s.BlockNumber+1) (one past the boundary)
// — while a write at/below the last committed txNum survives (no over-pruning).
// Otherwise the retry re-reads a stale value (Hoodi: ca5daf64 slot0 kept tx19's
// first-write, the contract took the "already initialised" branch, skipped an
// SSTORE_SET, came up 21045 gas short, and the node spun in an unwind/retry loop).
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
	// TxnumReader().Max(b) == perBlock*b+perBlock-1 (the block's last txNum) and
	// Min(b) == perBlock*b (its first/system txNum).
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
	// Re-execution resumes from the committed progress, so the overlay is pruned
	// to the last committed txNum, Max(committedBlock) — NOT Min(committedBlock+1)
	// (which would leak the first re-executed block's system tx) and NOT
	// Min(unwindPoint+1).
	lastCommittedTxNum, err := br.TxnumReader().Max(ctx, tx, committedBlock)
	require.NoError(t, err)
	require.Equal(t, committedBlock*perBlock+perBlock-1, lastCommittedTxNum, "sanity: prune boundary == last committed txNum")
	firstReExecTxNum, err := br.TxnumReader().Min(ctx, tx, committedBlock+1)
	require.NoError(t, err)
	require.Equal(t, lastCommittedTxNum+1, firstReExecTxNum, "sanity: first re-executed txNum is one past the boundary")

	put := func(hexAddr string, slot byte, val []byte, txNum uint64) []byte {
		addr := common.HexToAddress(hexAddr)
		slotHash := common.Hash{31: slot}
		key := append(append([]byte{}, addr[:]...), slotHash[:]...)
		doms.SetTxNum(txNum)
		require.NoError(t, doms.DomainPut(kv.StorageDomain, tx, key, val, txNum, nil))
		return key
	}

	// survivorKey: committed block, below the prune boundary → must survive.
	survivorVal := []byte{0xbe, 0xef}
	survivorKey := put("0x00000000000000000000000000000000000000aa", 0x01, survivorVal, committedBlock*perBlock)
	// boundaryKey: exactly at the last committed txNum → must survive (no over-prune).
	boundaryVal := []byte{0xb0, 0x0d}
	boundaryKey := put("0x00000000000000000000000000000000000000cc", 0x03, boundaryVal, lastCommittedTxNum)
	// firstReExecKey: exactly Min(committedBlock+1), the first re-executed block's
	// system tx (one past the boundary) — the extra txNum the old Min(resumeBlock)
	// boundary leaked → must be pruned.
	firstReExecVal := []byte{0x33, 0x44}
	firstReExecKey := put("0x00000000000000000000000000000000000000dd", 0x04, firstReExecVal, firstReExecTxNum)
	// midKey: a block in (committedBlock, unwindPoint]. Kept by the old
	// Min(unwindPoint+1) boundary, but must be dropped — that block is re-executed
	// from the committed progress and would otherwise re-read its own stale write.
	midVal := []byte{0x11, 0x22}
	midKey := put("0x00000000000000000000000000000000000000bb", 0x02, midVal, unwindPoint*perBlock)
	// staleKey: mirrors ca5daf64 slot0, first-written by the failed block's tx19.
	staleValHash := common.HexToHash("0xd7549f2a387fa81a1d5a77adc7bd3f782ac0780c460689d88e22aee6916a3a34")
	staleVal := staleValHash[:]
	staleKey := put("0xca5daf6473971693b760cc65d726f72c6849d615", 0x00, staleVal, failedBlock*perBlock+5)

	// Sanity: all visible through the overlay before the unwind.
	for _, c := range []struct {
		key, val []byte
		name     string
	}{
		{survivorKey, survivorVal, "survivor"},
		{boundaryKey, boundaryVal, "boundary"},
		{firstReExecKey, firstReExecVal, "first-re-exec"},
		{midKey, midVal, "mid"},
		{staleKey, staleVal, "stale"},
	} {
		got, _, err := doms.GetLatest(kv.StorageDomain, tx, c.key)
		require.NoError(t, err)
		require.Equal(t, c.val, got, "precondition: %s write must be visible pre-unwind", c.name)
	}

	cfg := ExecuteBlockCfg{blockReader: br}
	s := &StageState{ID: stages.Execution, BlockNumber: committedBlock}
	u := &UnwindState{ID: stages.Execution, UnwindPoint: unwindPoint, CurrentBlockNumber: failedBlock}
	require.GreaterOrEqual(t, u.UnwindPoint, s.BlockNumber, "test must exercise the no-op-disk-unwind branch")

	require.NoError(t, UnwindExecutionStage(u, s, doms, tx, ctx, cfg, logger))

	// Every write of a re-executed block must be gone: the failed block's, a block
	// in (committedBlock, unwindPoint], AND the first re-executed block's system tx
	// at exactly Min(committedBlock+1) — re-execution resumes from the committed
	// boundary and would re-read any of them as a stale value.
	for _, c := range []struct {
		key  []byte
		name string
	}{
		{staleKey, "failed-block (above unwindPoint)"},
		{midKey, "(committedBlock, unwindPoint]"},
		{firstReExecKey, "first re-executed block system tx (Min(committedBlock+1))"},
	} {
		got, _, err := doms.GetLatest(kv.StorageDomain, tx, c.key)
		require.NoError(t, err)
		require.Empty(t, got,
			"after Unwind (committed=%d, unwindPoint=%d), overlay must not return the %s write (got %x)",
			committedBlock, unwindPoint, c.name, got)
	}

	// Writes at or below the last committed txNum must be untouched (no over-pruning).
	for _, c := range []struct {
		key, val []byte
		name     string
	}{
		{survivorKey, survivorVal, "below committed"},
		{boundaryKey, boundaryVal, "at last committed txNum"},
	} {
		got, _, err := doms.GetLatest(kv.StorageDomain, tx, c.key)
		require.NoError(t, err)
		require.Equal(t, c.val, got, "%s write must survive the prune", c.name)
	}
}
