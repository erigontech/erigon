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

package commitment

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

// TestInitializeTrieAndUpdates_BinVariant pins the registration. M0 runs the
// binary engine in ModeDirect whatever mode the caller asks for: ModeParallel's
// prefix trie is a hex-nibble structure with no meaning at arity 2.
func TestInitializeTrieAndUpdates_BinVariant(t *testing.T) {
	t.Parallel()

	cfg := DefaultTrieConfig()
	cfg.Variant = VariantBinPatriciaTrie
	trie, upd := InitializeTrieAndUpdates(ModeParallel, t.TempDir(), cfg)
	defer upd.Close()
	defer trie.Release()

	require.IsType(t, (*PBinPatriciaHashed)(nil), trie)
	require.Equal(t, VariantBinPatriciaTrie, trie.Variant())
	require.Equal(t, ModeDirect, upd.Mode())
	require.Nil(t, upd.parallel)
	require.False(t, upd.IsConcurrentCommitment())
}

func TestParseTrieVariantBin(t *testing.T) {
	t.Parallel()

	require.Equal(t, VariantBinPatriciaTrie, ParseTrieVariant("bin"))
	require.Equal(t, VariantHexPatriciaTrie, ParseTrieVariant("hex"))
	require.Equal(t, VariantParallelHexPatricia, ParseTrieVariant("parallel"))
}

// TestPBinResetReuse checks that a run over a populated state depends only on
// what the context holds: an engine that dropped its in-memory root, and one
// that never had it, must both reproduce the root of the run that built it.
func TestPBinResetReuse(t *testing.T) {
	t.Parallel()

	corpus := pbinTestMixedCorpus()
	pph, ms := pbinTestEngine(t)
	require.NoError(t, ms.applyPlainUpdates(corpus.plainKeys, corpus.updates))

	want := corpus.oracleRoot(t)
	require.Equal(t, want, pbinTestProcess(t, pph, corpus.plainKeys, corpus.updates))

	pph.Reset()
	require.Equal(t, want, pbinTestProcess(t, pph, corpus.plainKeys, corpus.updates), "reset engine re-reads the tree from the context")

	fresh := NewPBinPatriciaHashed(ms)
	require.Equal(t, want, pbinTestProcess(t, fresh, corpus.plainKeys, corpus.updates), "fresh engine over the same state agrees")
}

// TestPBinResetClearsTrieState is the state-level half of the reuse contract:
// Reset leaves the engine indistinguishable from a new one but keeps the
// context, which the Trie interface hands over separately.
func TestPBinResetClearsTrieState(t *testing.T) {
	t.Parallel()

	corpus := new(pbinTestCorpus).
		storage(pbinOracleAddr(1), pbinOracleSlot(256), 0x01).
		storage(pbinOracleAddr(1), pbinOracleSlot(257), 0x02)

	pph, ms := pbinTestEngine(t)
	require.NoError(t, ms.applyPlainUpdates(corpus.plainKeys, corpus.updates))
	pbinTestProcess(t, pph, corpus.plainKeys, corpus.updates)
	require.Equal(t, pbinNodeBranch, pph.grid.root.kind)

	pph.Reset()
	require.Equal(t, pbinNodeEmpty, pph.grid.root.kind)
	require.Zero(t, pph.currentKey.bitLen)
	require.Zero(t, pph.grid.activeRows)
	require.False(t, pph.rootChecked)
	require.False(t, pph.rootTouched)
	require.False(t, pph.rootPresent)
	require.Same(t, ms, pph.ctx)

	root, err := pph.RootHash()
	require.NoError(t, err)
	require.Equal(t, make([]byte, 32), root)
}

// TestPBinResetContext swaps the state under a released-and-reused engine.
func TestPBinResetContext(t *testing.T) {
	t.Parallel()

	corpus := new(pbinTestCorpus).account(pbinOracleAddr(7), 1, 2, common.Hash{0x07})

	pph, _ := pbinTestEngine(t)
	other := NewMockState(t)
	require.NoError(t, other.applyPlainUpdates(corpus.plainKeys, corpus.updates))

	pph.ResetContext(other)
	require.Same(t, other, pph.ctx)
	require.Equal(t, corpus.oracleRoot(t), pbinTestProcess(t, pph, corpus.plainKeys, corpus.updates))
}

// TestPBinReleaseReuse guards the pool: a released engine carries no state into
// its next life, so the next run over a different context matches a fresh one.
func TestPBinReleaseReuse(t *testing.T) {
	t.Parallel()

	corpus := pbinTestMixedCorpus()
	pph, ms := pbinTestEngine(t)
	require.NoError(t, ms.applyPlainUpdates(corpus.plainKeys, corpus.updates))
	pbinTestProcess(t, pph, corpus.plainKeys, corpus.updates)
	pph.Release()

	next := NewMockState(t)
	require.NoError(t, next.applyPlainUpdates(corpus.plainKeys, corpus.updates))
	reused := NewPBinPatriciaHashed(next)
	require.Equal(t, corpus.oracleRoot(t), pbinTestProcess(t, reused, corpus.plainKeys, corpus.updates))
}

// TestPBinSetTraceWriter pins what the engine traces: the two counters the
// split-rehash decision is waiting on.
func TestPBinSetTraceWriter(t *testing.T) {
	t.Parallel()

	corpus := pbinTestDeepSharedPrefixCorpus()
	pph, ms := pbinTestEngine(t)
	require.NoError(t, ms.applyPlainUpdates(corpus.plainKeys, corpus.updates))

	var trace bytes.Buffer
	pph.SetTraceWriter(&trace)
	pbinTestProcess(t, pph, corpus.plainKeys, corpus.updates)
	require.Contains(t, trace.String(), "splitsInsidePrefix=")
	require.Contains(t, trace.String(), "materializeReads=")

	trace.Reset()
	pph.SetTraceWriter(nil)
	pbinTestProcess(t, pph, corpus.plainKeys, corpus.updates)
	require.Empty(t, trace.String())
}
