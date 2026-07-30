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

package commitmentdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/commitment"
)

func pbinRecoveredError(t *testing.T, fn func()) (err error) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		recovered, ok := r.(error)
		require.True(t, ok, "panic value must carry the error: %v", r)
		err = recovered
	}()
	fn()
	return nil
}

// TestPBinRefusesDeferredCommitmentUpdates pins the enabling side of the
// deferred-update path: hex and parallel take the request, bin refuses it by
// name. Silently accepting it would leave the flag set with no trie honouring
// it, so Process would apply inline while the caller waited for a flush.
func TestPBinRefusesDeferredCommitmentUpdates(t *testing.T) {
	t.Parallel()

	hexCtx := pbinStateTestCtx(t, commitment.VariantHexPatriciaTrie)
	hexCtx.SetDeferCommitmentUpdates(true)
	require.True(t, hexCtx.deferCommitmentUpdates)

	binCtx := pbinStateTestCtx(t, commitment.VariantBinPatriciaTrie)
	err := pbinRecoveredError(t, func() { binCtx.SetDeferCommitmentUpdates(true) })
	require.ErrorIs(t, err, commitment.ErrPBinUnsupported)
	require.False(t, binCtx.deferCommitmentUpdates, "the refused request must not leave the flag set")

	require.NoError(t, pbinRecoveredError(t, func() { binCtx.SetDeferCommitmentUpdates(false) }))
}

// TestPBinComputeCommitmentRefusesDeferredTake covers the taking side: were the
// flag ever set under bin, the post-Process type switch would find no trie
// carrying deferred updates and hand back an empty pendingUpdate.
func TestPBinComputeCommitmentRefusesDeferredTake(t *testing.T) {
	t.Parallel()

	binCtx := pbinStateTestCtx(t, commitment.VariantBinPatriciaTrie)
	binCtx.deferCommitmentUpdates = true

	_, err := binCtx.ComputeCommitment(t.Context(), nil, false, 1, 1, "test", nil)
	require.ErrorIs(t, err, commitment.ErrPBinUnsupported)
}

// TestPBinComputeCommitmentRefusesTrieTrace: the trace records branch records
// and replays them through the hex trie, so a bin trace would replay as a
// different tree. The trace is env-gated, so refusing costs a normal run nothing.
func TestPBinComputeCommitmentRefusesTrieTrace(t *testing.T) {
	prev := dbg.TrieTraceFile
	dbg.TrieTraceFile = t.TempDir() + "/trie-trace.toml"
	t.Cleanup(func() { dbg.TrieTraceFile = prev })

	binCtx := pbinStateTestCtx(t, commitment.VariantBinPatriciaTrie)
	_, err := binCtx.ComputeCommitment(t.Context(), nil, false, 1, 1, "test", nil)
	require.ErrorIs(t, err, commitment.ErrPBinUnsupported)

	hexCtx := pbinStateTestCtx(t, commitment.VariantHexPatriciaTrie)
	_, err = hexCtx.ComputeCommitment(t.Context(), nil, false, 1, 1, "test", nil)
	require.NoError(t, err)
}

// TestPBinRefusesCollapseTracer guards the witness path: the tracer only ever
// reaches a HexPatriciaHashed, so under bin it was installed nowhere and the
// caller collected no collapse paths.
func TestPBinRefusesCollapseTracer(t *testing.T) {
	t.Parallel()

	tracer := func(hashedKeyPath, branchPrefix []byte) {}

	hexCtx := pbinStateTestCtx(t, commitment.VariantHexPatriciaTrie)
	require.NoError(t, pbinRecoveredError(t, func() { hexCtx.SetCollapseTracer(tracer) }))

	binCtx := pbinStateTestCtx(t, commitment.VariantBinPatriciaTrie)
	err := pbinRecoveredError(t, func() { binCtx.SetCollapseTracer(tracer) })
	require.ErrorIs(t, err, commitment.ErrPBinUnsupported)

	require.NoError(t, pbinRecoveredError(t, func() { binCtx.SetCollapseTracer(nil) }), "clearing must stay allowed")
}

// TestPBinBranchChildCountRefusesBin: the prefix is a hex nibble path compacted
// into a commitment key, which addresses no bin record — the read used to miss
// and report a child count of zero.
func TestPBinBranchChildCountRefusesBin(t *testing.T) {
	t.Parallel()

	binCtx := pbinStateTestCtx(t, commitment.VariantBinPatriciaTrie)
	_, err := binCtx.BranchChildCount(nil, []byte{0x0a, 0x0b})
	require.ErrorIs(t, err, commitment.ErrPBinUnsupported)
}
