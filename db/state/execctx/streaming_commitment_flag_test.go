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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
)

// withStreamingCommitmentFlag toggles statecfg.ExperimentalStreamingCommitment
// for the duration of the test and restores the original values on cleanup. It
// pins the older parallel/concurrent flags to false so PickTrieVariant's
// streaming-first precedence is exercised in isolation.
func withStreamingCommitmentFlag(t *testing.T, on bool) {
	t.Helper()
	origStream := statecfg.ExperimentalStreamingCommitment
	origPar := statecfg.ExperimentalParallelCommitment
	origConc := statecfg.ExperimentalConcurrentCommitment
	t.Cleanup(func() {
		statecfg.ExperimentalStreamingCommitment = origStream
		statecfg.ExperimentalParallelCommitment = origPar
		statecfg.ExperimentalConcurrentCommitment = origConc
	})
	statecfg.ExperimentalStreamingCommitment = on
	statecfg.ExperimentalParallelCommitment = false
	statecfg.ExperimentalConcurrentCommitment = false
}

// TestPickTrieVariant_StreamingFlag asserts the flag-to-variant mapping and its
// precedence: streaming wins over parallel and concurrent when set.
func TestPickTrieVariant_StreamingFlag(t *testing.T) {
	// Do not call t.Parallel() — mutates process-global statecfg flags.
	withStreamingCommitmentFlag(t, true)
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

	withStreamingCommitmentFlag(t, true)

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
		withStreamingCommitmentFlag(t, streaming)

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
		want := commitment.VariantHexPatriciaTrie
		if streaming {
			want = commitment.VariantStreamingHexPatricia
		}
		require.Equalf(t, want, got, "trie variant for streaming=%v", streaming)

		return runWriteCommitBatch(t, sd, rwTx)
	}

	seqRoot := runOnce(t, false)
	strRoot := runOnce(t, true)

	require.Equalf(t, seqRoot, strRoot,
		"sequential and streaming commitment roots must match: sequential=%x streaming=%x",
		seqRoot, strRoot)
}
