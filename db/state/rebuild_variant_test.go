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

package state_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestRebuildTargetResolve(t *testing.T) {
	for _, variant := range []commitment.TrieVariant{
		commitment.VariantHexPatriciaTrie,
		commitment.VariantParallelHexPatricia,
		commitment.VariantCommitmentV3,
	} {
		resolved, err := state.RebuildTarget{Variant: variant}.Resolve()
		require.NoError(t, err)
		require.Equal(t, variant, resolved.Variant)
	}

	_, err := state.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie}.Resolve()
	require.ErrorContains(t, err, "convert-pbt")

	_, err = state.RebuildTarget{Variant: "verkle"}.Resolve()
	require.Error(t, err)

	resolved, err := state.RebuildTarget{MaxShardSteps: 16}.Resolve()
	require.NoError(t, err)
	require.Equal(t, uint64(16), resolved.MaxShardSteps)
}

func TestRebuildCommitmentFilesV3Target(t *testing.T) {
	previousV3, previousParallel, previousBin, previousHexBin, previousSchema := statecfg.ExperimentalCommitmentV3, statecfg.ExperimentalParallelCommitment, statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment, statecfg.Schema
	t.Cleanup(func() {
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.ExperimentalParallelCommitment = previousParallel
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.Schema = previousSchema
	})
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.ExperimentalParallelCommitment = false
	statecfg.ExperimentalBinCommitment = false
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	target, err := state.DefaultRebuildTarget().Resolve()
	require.NoError(t, err)
	require.Equal(t, commitment.VariantCommitmentV3, target.Variant)
}

func TestRebuildCommitmentFilesDefaultTargetIsProcessVariant(t *testing.T) {
	previousParallel, previousV3 := statecfg.ExperimentalParallelCommitment, statecfg.ExperimentalCommitmentV3
	t.Cleanup(func() {
		statecfg.ExperimentalParallelCommitment = previousParallel
		statecfg.ExperimentalCommitmentV3 = previousV3
	})
	statecfg.ExperimentalParallelCommitment = false
	statecfg.ExperimentalCommitmentV3 = false
	target, err := state.DefaultRebuildTarget().Resolve()
	require.NoError(t, err)
	require.Equal(t, commitment.VariantHexPatriciaTrie, target.Variant)
}
