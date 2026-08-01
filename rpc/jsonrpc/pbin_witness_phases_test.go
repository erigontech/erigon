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

package jsonrpc

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// The collapse phase is driven with every dependency nil: if the bin skip ever goes
// away, the phase dereferences one of them instead of returning cleanly.
func TestPBinWitnessSkipsCollapseDetection(t *testing.T) {
	t.Parallel()

	var siblingPaths [][]byte
	var err error
	require.NotPanics(t, func() {
		siblingPaths, err = detectCollapseSiblings(t.Context(), nil, nil, nil, nil,
			0, 0, 0, 0, common.Hash{}, nil, witnessModeLegacy, true /* binTrie */)
	}, "the binary trie must not enter collapse detection: SetCollapseTracer panics under bin")
	require.NoError(t, err)
	require.Empty(t, siblingPaths, "the binary trie never collapses a branch")
}

// A collapse sibling reaching the bin trie phase would be touched as a hashed key with
// an empty plain key, which the bin update stream cannot resolve. The guard runs before
// any dependency is used, so nil deps are enough to reach it.
func TestPBinWitnessTrieRefusesCollapseSiblings(t *testing.T) {
	t.Parallel()

	nodes, err := buildWitnessTrie(t.Context(), nil, nil, nil, nil, 0, common.Hash{},
		[][]byte{{0x01, 0x02}}, nil, true /* produceExclusionProofs */, true /* binTrie */)
	require.Error(t, err)
	require.Nil(t, nodes)
	require.Contains(t, err.Error(), "collapse sibling")
}

func TestPBinWitnessModeRejectsExplicitCanonical(t *testing.T) {
	t.Parallel()

	str := func(s string) *string { return &s }

	for _, tc := range []struct {
		name  string
		param *string
	}{
		{"absent", nil},
		{"empty", str("")},
		{"legacy", str("legacy")},
	} {
		t.Run(tc.name+" mode resolves to legacy under bin", func(t *testing.T) {
			got, err := resolveWitnessMode(tc.param, true /* binTrie */)
			require.NoError(t, err, "rejecting the legacy default would reject every bin request")
			require.Equal(t, witnessModeLegacy, got)
		})
	}

	got, err := resolveWitnessMode(str("canonical"), true /* binTrie */)
	require.ErrorIs(t, err, errWitnessCanonicalHexOnly)
	require.Equal(t, witnessModeLegacy, got)

	got, err = resolveWitnessMode(str("canonical"), false /* binTrie */)
	require.NoError(t, err, "hex keeps both modes")
	require.Equal(t, witnessModeCanonical, got)
}

// TestPBinExecutionWitnessRejectsCanonicalRequest pins the wiring: the mode gate reads
// the datadir's variant, so an explicit canonical request under bin is refused, while a
// default-mode request gets past the gate and fails for its own reasons.
func TestPBinExecutionWitnessRejectsCanonicalRequest(t *testing.T) {
	// No t.Parallel: mutates process-global statecfg flags.
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, &rpccfg.DebugApiConfig{})

	orig := statecfg.ExperimentalBinCommitment
	t.Cleanup(func() { statecfg.ExperimentalBinCommitment = orig })
	statecfg.ExperimentalBinCommitment = true
	require.True(t, binCommitmentTrie())

	canonical := "canonical"
	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	_, err := api.ExecutionWitness(t.Context(), latest, &canonical)
	require.ErrorIs(t, err, errWitnessCanonicalHexOnly)

	_, err = api.ExecutionWitness(t.Context(), latest, nil)
	require.NotErrorIs(t, err, errWitnessCanonicalHexOnly, "the legacy default must get past the mode gate")
}

// The 0x80 empty storage-trie node is an MPT artifact with no binary-trie counterpart.
func TestPBinWitnessOmitsEmptyStorageNode(t *testing.T) {
	t.Parallel()

	accountLeaf := hexutil.Bytes(append([]byte{0xf8, 0x44}, trie.EmptyRoot[:]...))
	nodes := []hexutil.Bytes{accountLeaf}

	hexLegacy := appendLegacyEmptyStorageNode(nodes, witnessModeLegacy, false /* binTrie */)
	require.Len(t, hexLegacy, 2)
	require.Equal(t, hexutil.Bytes{0x80}, hexLegacy[1])

	require.Equal(t, nodes, appendLegacyEmptyStorageNode(nodes, witnessModeLegacy, true /* binTrie */),
		"the binary trie has no empty storage-trie node")
	require.Equal(t, nodes, appendLegacyEmptyStorageNode(nodes, witnessModeCanonical, false /* binTrie */))
}

// errWitnessCanonicalHexOnly and errWitnessCanonicalUnavailable both refuse a canonical
// request but for unrelated reasons; a caller distinguishing them must not be able to
// match one with the other.
func TestPBinWitnessCanonicalErrorsAreDistinct(t *testing.T) {
	t.Parallel()

	require.False(t, errors.Is(errWitnessCanonicalHexOnly, errWitnessCanonicalUnavailable))
	require.False(t, errors.Is(errWitnessCanonicalUnavailable, errWitnessCanonicalHexOnly))
}
