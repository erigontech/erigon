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

package execctx_test

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// withCommitmentFlag pins the three experimental commitment flags to select the
// requested trie variant for the duration of the test, restoring the originals
// on cleanup. The non-selected flags are forced false so PickTrieVariant's
// streaming-over-parallel-over-concurrent precedence is exercised in isolation.
func withCommitmentFlag(t *testing.T, variant commitment.TrieVariant) {
	t.Helper()
	origStream := statecfg.ExperimentalStreamingCommitment
	origPar := statecfg.ExperimentalParallelCommitment
	origConc := statecfg.ExperimentalConcurrentCommitment
	t.Cleanup(func() {
		statecfg.ExperimentalStreamingCommitment = origStream
		statecfg.ExperimentalParallelCommitment = origPar
		statecfg.ExperimentalConcurrentCommitment = origConc
	})
	statecfg.ExperimentalStreamingCommitment = variant == commitment.VariantStreamingHexPatricia
	statecfg.ExperimentalParallelCommitment = variant == commitment.VariantParallelHexPatricia
	statecfg.ExperimentalConcurrentCommitment = false
}

// runWriteCommitBatch writes a small fixed set of account updates to the
// SharedDomains and returns the resulting commitment root. The update set is
// identical across calls so two roots from two SharedDomains can be compared.
//
// The workload is intentionally narrow (one account) so Prepare emits a single
// leafTask with zero split-points — ParallelPatriciaHashed.Process rejects
// multi-bucket workloads without a top-level split-point (see the "Task 10
// root barrier" rejection in parallel_patricia_hashed.go), which is a follow-up
// optimisation tracked outside this task. The single-leafTask path exercises
// the full wiring (flag → variant → mode → Process) and is sufficient for
// asserting root-hash equivalence with the sequential trie.
func runWriteCommitBatch(t *testing.T, sd *execctx.SharedDomains, rwTx kv.TemporalRwTx) []byte {
	t.Helper()

	ctx := t.Context()
	addr := make([]byte, length.Addr)
	addr[0] = 0x42
	addr[length.Addr-1] = 0x99

	acc := accounts.Account{
		Nonce:    7,
		Balance:  *uint256.NewInt(0xdeadbeef),
		CodeHash: accounts.EmptyCodeHash,
	}
	pv, _, err := sd.GetLatest(kv.AccountsDomain, rwTx, addr)
	require.NoError(t, err)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, addr, accounts.SerialiseV3(&acc), 1, pv))

	rh, err := sd.ComputeCommitment(ctx, rwTx, false, 0, 1, "", nil)
	require.NoError(t, err)
	require.NotEmpty(t, rh)
	return rh
}

// TestSharedDomains_ParallelFlagOff_UsesSequentialTrie verifies the default
// (flag off): NewSharedDomains constructs a *HexPatriciaHashed trie. ModeParallel
// codepath must not be exercised when the flag is off.
func TestSharedDomains_ParallelFlagOff_UsesSequentialTrie(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Do not call t.Parallel() — withCommitmentFlag mutates a process-global
	// flag; concurrent flag flips race against each other.

	withCommitmentFlag(t, commitment.VariantHexPatriciaTrie)

	stepSize := uint64(16)
	db := newTestDb(t, stepSize)

	ctx := t.Context()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()

	trie := sd.GetCommitmentCtx().Trie()
	require.Equal(t, commitment.VariantHexPatriciaTrie, trie.Variant(),
		"flag off must construct the sequential HexPatriciaHashed trie")
}

// TestSharedDomains_ParallelFlagOn_UsesParallelTrie verifies that turning the
// flag on routes NewSharedDomains to construct a *ParallelPatriciaHashed trie
// (and consequently a ModeParallel Updates buffer).
func TestSharedDomains_ParallelFlagOn_UsesParallelTrie(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Do not call t.Parallel() — withCommitmentFlag mutates a process-global
	// flag; concurrent flag flips race against each other.

	withCommitmentFlag(t, commitment.VariantParallelHexPatricia)

	stepSize := uint64(16)
	db := newTestDb(t, stepSize)

	ctx := t.Context()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()

	trie := sd.GetCommitmentCtx().Trie()
	require.Equal(t, commitment.VariantParallelHexPatricia, trie.Variant(),
		"flag on must construct the ParallelPatriciaHashed trie")
}

// TestSharedDomains_ParallelFlag_RootEquivalence is the cardinal correctness
// check from the parallel-hph plan: the same update set must produce the same
// root hash whether the trie is sequential (flag off) or parallel (flag on).
// Both runs use isolated in-memory DBs so each pass starts from a clean state.
func TestSharedDomains_ParallelFlag_RootEquivalence(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Do not call t.Parallel() — withCommitmentFlag mutates a process-global
	// flag; concurrent flag flips race against each other.

	stepSize := uint64(16)

	runOnce := func(t *testing.T, parallel bool) []byte {
		t.Helper()
		variant := commitment.VariantHexPatriciaTrie
		if parallel {
			variant = commitment.VariantParallelHexPatricia
		}
		withCommitmentFlag(t, variant)

		db := newTestDb(t, stepSize)

		ctx := t.Context()
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()

		sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(t, err)
		defer sd.Close()

		// ParallelPatriciaHashed needs a TrieContextFactory built from a
		// separate RO DB so each worker holds its own transaction. Production
		// wires this in stagedsync/exec3 and squeeze; for the test we use the
		// same DB as the underlying RwDB.
		sd.EnableParaTrieDB(db)

		// Sanity check that the flag-to-trie wiring matches the requested mode.
		got := sd.GetCommitmentCtx().Trie().Variant()
		require.Equalf(t, variant, got, "trie variant for parallel=%v", parallel)

		return runWriteCommitBatch(t, sd, rwTx)
	}

	seqRoot := runOnce(t, false)
	parRoot := runOnce(t, true)

	require.Equalf(t, seqRoot, parRoot,
		"sequential and parallel commitment roots must match: sequential=%x parallel=%x",
		seqRoot, parRoot)
}

// TestPickTrieVariant_StreamingFlag asserts the flag-to-variant mapping and its
// precedence: streaming wins over parallel and concurrent when set.
func TestPickTrieVariant_StreamingFlag(t *testing.T) {
	// Do not call t.Parallel() — mutates process-global statecfg flags.
	withCommitmentFlag(t, commitment.VariantStreamingHexPatricia)
	require.Equal(t, commitment.VariantStreamingHexPatricia, execctx.PickTrieVariant())

	// Streaming precedence over the other concurrent paths.
	statecfg.ExperimentalParallelCommitment = true
	statecfg.ExperimentalConcurrentCommitment = true
	require.Equal(t, commitment.VariantStreamingHexPatricia, execctx.PickTrieVariant())

	statecfg.ExperimentalStreamingCommitment = false
	require.Equal(t, commitment.VariantParallelHexPatricia, execctx.PickTrieVariant())
}

// TestSharedDomains_StreamingFlagOn_UsesStreamingTrie verifies that turning the
// streaming flag on routes NewSharedDomains to a streaming-backed trie (a
// *ParallelPatriciaHashed shell whose Variant reports streaming).
func TestSharedDomains_StreamingFlagOn_UsesStreamingTrie(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Do not call t.Parallel() — mutates process-global statecfg flags.

	withCommitmentFlag(t, commitment.VariantStreamingHexPatricia)

	stepSize := uint64(16)
	db := newTestDb(t, stepSize)

	ctx := t.Context()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()

	trie := sd.GetCommitmentCtx().Trie()
	require.Equal(t, commitment.VariantStreamingHexPatricia, trie.Variant(),
		"flag on must construct the streaming-backed trie")
}

// TestSharedDomains_StreamingFlag_RootEquivalence asserts the streaming trie
// produces the same commitment root as the sequential trie over the same update
// set. Both runs use isolated in-memory DBs so each starts from a clean state.
func TestSharedDomains_StreamingFlag_RootEquivalence(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Do not call t.Parallel() — mutates process-global statecfg flags.

	stepSize := uint64(16)

	runOnce := func(t *testing.T, streaming bool) []byte {
		t.Helper()
		variant := commitment.VariantHexPatriciaTrie
		if streaming {
			variant = commitment.VariantStreamingHexPatricia
		}
		withCommitmentFlag(t, variant)

		db := newTestDb(t, stepSize)

		ctx := t.Context()
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()

		sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(t, err)
		defer sd.Close()

		sd.EnableParaTrieDB(db)

		got := sd.GetCommitmentCtx().Trie().Variant()
		require.Equalf(t, variant, got, "trie variant for streaming=%v", streaming)

		return runWriteCommitBatch(t, sd, rwTx)
	}

	seqRoot := runOnce(t, false)
	strRoot := runOnce(t, true)

	require.Equalf(t, seqRoot, strRoot,
		"sequential and streaming commitment roots must match: sequential=%x streaming=%x",
		seqRoot, strRoot)
}
