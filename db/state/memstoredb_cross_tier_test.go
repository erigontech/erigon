// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0-or-later

package state_test

import (
	"context"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/memstoredb"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// openBackend builds a temporal RwDB on top of either MDBX or memstoredb,
// using the same Aggregator config so the cross-tier scenario (frozen .kv
// files produced by BuildFiles + live writes in chaindata) reads through the
// same code path on both sides.
func openBackend(tb testing.TB, useMemstore bool, stepSize uint64, dataDir string) (kv.TemporalRwDB, *state.Aggregator) {
	tb.Helper()
	logger := log.New()
	dirs := datadir.New(dataDir)

	var rawDB kv.RwDB
	if useMemstore {
		rawDB = memstoredb.New(dbcfg.ChainDB, nil)
	} else {
		rawDB = mdbx.New(dbcfg.ChainDB, logger).
			InMem(tb, dirs.Chaindata).
			GrowthStep(32 * datasize.MB).
			MapSize(16 * datasize.GB).
			MustOpen()
	}
	tb.Cleanup(rawDB.Close)

	agg := state.NewTest(dirs).StepSize(stepSize).Logger(logger).MustOpen(tb.Context(), rawDB)
	tb.Cleanup(agg.Close)
	require.NoError(tb, agg.OpenFolder())

	tdb, err := temporal.New(rawDB, agg)
	require.NoError(tb, err)
	tb.Cleanup(tdb.Close)
	return tdb, agg
}

// computeCrossTierRoot does the cross-tier scenario:
//  1. Build state for steps 0..frozenSteps into chaindata, then BuildFiles to
//     freeze them into .kv snapshots.
//  2. Continue writing one more step's worth of state — this stays in
//     chaindata and is the surface that mainnet sync exercises just past the
//     last frozen step.
//  3. ComputeCommitment and return the root.
//
// The same sequence of writes is used regardless of the underlying backend
// so any divergence between MDBX and memstoredb falls out as a different root.
func computeCrossTierRoot(t *testing.T, useMemstore bool, stepSize uint64, frozenSteps uint64) []byte {
	t.Helper()
	dataDir := t.TempDir()
	db, agg := openBackend(t, useMemstore, stepSize, dataDir)
	ctx := context.Background()

	totalFrozenTxs := frozenSteps * stepSize
	tailTxs := stepSize * 3 // three steps of live writes past the last frozen step

	// Phase 1: write totalFrozenTxs worth of state, computing commitment at
	// each step boundary so Flush sees an up-to-date commitment state. Then
	// BuildFiles to freeze them into .kv snapshots.
	func() {
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()
		domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(t, err)
		defer domains.Close()
		writeStateWithStepCommits(t, ctx, rwTx, domains, 0, totalFrozenTxs, stepSize)
		require.NoError(t, domains.Flush(ctx, rwTx))
		require.NoError(t, rwTx.Commit())
	}()
	require.NoError(t, agg.BuildFiles(totalFrozenTxs))
	// Verify the frozen .kv files actually got produced — otherwise we're
	// not testing the cross-tier scenario at all.
	{
		entries, err := dir.ListFiles(datadir.New(dataDir).SnapDomain, ".kv")
		require.NoError(t, err)
		require.NotEmpty(t, entries, "BuildFiles produced no .kv snapshot files — cross-tier scenario not exercised")
	}

	// Phase 2: write the tail in many small commits (one per step), each in
	// its own RwTx, mimicking the per-batch shape of the stage loop. Between
	// batches, open a fresh RoTx and read back values to ensure the previous
	// commit is visible — this is the cross-tx visibility path that an
	// in-memory KV must match MDBX on.
	var root []byte
	for batchStart := totalFrozenTxs; batchStart < totalFrozenTxs+tailTxs; batchStart += stepSize {
		batchEnd := batchStart + stepSize
		func() {
			rwTx, err := db.BeginTemporalRw(ctx)
			require.NoError(t, err)
			defer rwTx.Rollback()
			domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
			require.NoError(t, err)
			defer domains.Close()
			writeStateWithStepCommits(t, ctx, rwTx, domains, batchStart, batchEnd, stepSize)
			root, err = domains.ComputeCommitment(ctx, rwTx, true, 0, batchEnd-1, "", nil)
			require.NoError(t, err)
			require.NoError(t, domains.Flush(ctx, rwTx))
			require.NoError(t, rwTx.Commit())
		}()
		// Verify the just-committed state is visible from a fresh RoTx — this
		// is the path domain reads take after a per-batch commit.
		func() {
			roTx, err := db.BeginTemporalRo(ctx)
			require.NoError(t, err)
			defer roTx.Rollback()
			probe := make([]byte, length.Addr)
			probe[0] = 1
			_, _, err = roTx.GetLatest(kv.AccountsDomain, probe)
			require.NoError(t, err)
		}()
	}
	return root
}

// writeStateWithStepCommits writes a deterministic batch of account updates
// for the txNum range [from, to), calling ComputeCommitment at every step
// boundary so Flush sees an up-to-date commitment state. The same call
// sequence is used by both backends so any divergence in reads or writes
// shows up as a different commitment root.
func writeStateWithStepCommits(t *testing.T, ctx context.Context, rwTx kv.TemporalRwTx, domains *execctx.SharedDomains, from, to, stepSize uint64) {
	t.Helper()
	const (
		numAccounts = 200
		numSlots    = 8
	)
	addrs := make([][]byte, numAccounts)
	for i := range addrs {
		addrs[i] = make([]byte, length.Addr)
		addrs[i][0] = byte(i)
		addrs[i][1] = byte(i >> 8)
		addrs[i][2] = byte(i >> 16)
	}
	storKey := make([]byte, length.Addr+length.Hash)
	for txNum := from; txNum < to; txNum++ {
		for i, addr := range addrs {
			acc := accounts.Account{
				Nonce:    txNum + uint64(i),
				Balance:  *uint256.NewInt((txNum + 1) * (uint64(i) + 1)),
				CodeHash: accounts.EmptyCodeHash,
			}
			buf := accounts.SerialiseV3(&acc)
			prev, _, err := domains.GetLatest(kv.AccountsDomain, rwTx, addr)
			require.NoError(t, err)
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, addr, buf, txNum, prev))

			// Touch a rotating subset of storage slots per account so the
			// storage domain accumulates updates that straddle the snapshot
			// boundary (mainnet's wrong-trie-root hits show on blocks with
			// heavy storage activity).
			for s := uint64(0); s < numSlots; s++ {
				slotIdx := (txNum + s + uint64(i)) % 64 // 64 distinct slot keys per account
				copy(storKey, addr)
				for b := 0; b < 8; b++ {
					storKey[length.Addr+length.Hash-1-b] = byte(slotIdx >> (b * 8))
				}
				val := []byte{byte(txNum), byte(s), byte(i), 0xab}
				prevVal, _, err := domains.GetLatest(kv.StorageDomain, rwTx, storKey)
				require.NoError(t, err)
				require.NoError(t, domains.DomainPut(kv.StorageDomain, rwTx, storKey, val, txNum, prevVal))
			}
		}
		if (txNum+1)%stepSize == 0 {
			_, err := domains.ComputeCommitment(ctx, rwTx, true, 0, txNum, "", nil)
			require.NoError(t, err)
		}
	}
}

// TestMemstoreDB_CrossTier_CommitmentParity is the cross-tier reproducer for
// the wrong-state-root behaviour observed on the live mainnet rig with
// --experimental.inmem-kv: three "Wrong trie root" hits in the first ~2
// minutes after sync crossed the last frozen step boundary (steps 9080→9081,
// blocks 25199046 / 25199641 / 25201119). Existing memstoredb unit tests all
// keep state inside memstoredb only; this test reproduces the *combination*
// of frozen .kv snapshot files + live chaindata writes that mainnet hits.
//
// The MDBX run is treated as the source of truth; memstoredb must compute
// the same root for the same sequence of writes. A mismatch is the same
// bug class as the rig's wrong-trie-root.
func TestMemstoreDB_CrossTier_CommitmentParity(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	const (
		stepSize    = uint64(16)
		frozenSteps = uint64(4) // → frozen .kv covers txs 0..64
	)

	mdbxRoot := computeCrossTierRoot(t, false, stepSize, frozenSteps)
	require.NotEmpty(t, mdbxRoot, "MDBX baseline root must not be empty")

	memstoreRoot := computeCrossTierRoot(t, true, stepSize, frozenSteps)
	require.NotEmpty(t, memstoreRoot, "memstoredb root must not be empty")

	require.Equal(t, mdbxRoot, memstoreRoot,
		"memstoredb produced a different commitment root from MDBX for the same write sequence — this is the same bug class as the rig's wrong-trie-root past the snapshot boundary")
}
