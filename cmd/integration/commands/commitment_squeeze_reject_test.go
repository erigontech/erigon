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

package commands

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
)

func resolveTarget(t *testing.T, variant commitment.TrieVariant) dbstate.RebuildTarget {
	t.Helper()
	target, err := dbstate.RebuildTarget{Variant: variant}.Resolve()
	require.NoError(t, err)
	return target
}

// withRebuildFlags restores the command's flag globals, which the rebuild reads
// directly, so one test's combination does not leak into the next.
func withRebuildFlags(t *testing.T, set func()) {
	t.Helper()
	prevSqueeze, prevClear, prevResume, prevNoHistory, prevReset, prevDatadir :=
		squeeze, clearCommitment, resume, noHistory, reset, datadirCli
	t.Cleanup(func() {
		squeeze, clearCommitment, resume, noHistory, reset, datadirCli =
			prevSqueeze, prevClear, prevResume, prevNoHistory, prevReset, prevDatadir
	})
	squeeze, clearCommitment, resume, noHistory, reset = false, false, false, false, false
	datadirCli = t.TempDir()
	set()
}

func TestRefuseSqueezeForBinTarget(t *testing.T) {
	for _, tc := range []struct {
		name    string
		variant commitment.TrieVariant
		squeeze bool
		wantErr bool
	}{
		{"bin with squeeze", commitment.VariantBinPatriciaTrie, true, true},
		{"bin without squeeze", commitment.VariantBinPatriciaTrie, false, false},
		{"hex with squeeze", commitment.VariantHexPatriciaTrie, true, false},
		{"hex without squeeze", commitment.VariantHexPatriciaTrie, false, false},
		{"parallel hex with squeeze", commitment.VariantParallelHexPatricia, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := refuseSqueezeForBinTarget(resolveTarget(t, tc.variant), tc.squeeze)
			if !tc.wantErr {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, "--squeeze")
			require.ErrorContains(t, err, "BranchData")
		})
	}
}

// TestCommitmentRebuildRefusesSqueezeBeforeAnyWork passes a nil db on purpose:
// the check has to run before the rebuild reads the database or the filesystem,
// so a version that squeezes first and errors last cannot pass.
func TestCommitmentRebuildRefusesSqueezeBeforeAnyWork(t *testing.T) {
	withRebuildFlags(t, func() { squeeze = true })

	err := commitmentRebuild(nil, context.Background(), log.New(), binTarget(t), nil)
	require.ErrorContains(t, err, "--squeeze")
}

// Every one of these writes to the source datadir or to files the staged output
// does not hold, so the refusal has to come from the flags alone — the run is
// rejected before the output datadir is created and the source is hardlinked in.
func TestCheckRebuildFlags(t *testing.T) {
	for _, tc := range []struct {
		name    string
		set     func()
		output  bool
		wantErr string
	}{
		{"output with no-history", func() { noHistory = true }, true, ""},
		{"output without no-history", func() {}, true, "--no-history"},
		{"output with clear-commitment", func() { noHistory, clearCommitment = true, true }, true, "--clear-commitment"},
		{"output with reset", func() { noHistory, reset = true, true }, true, "--reset"},
		{"clear-commitment with resume", func() { clearCommitment, resume = true, true }, false, "--resume"},
		{"clear-commitment with no-history", func() { clearCommitment, noHistory = true, true }, false, "--no-history"},
		{"in-place plain run", func() {}, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withRebuildFlags(t, tc.set)
			err := checkRebuildFlags(resolveTarget(t, commitment.VariantHexPatriciaTrie), tc.output)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// The datadir's own scheme does not decide this: a bin rebuild reading a
// hex-configured datadir is the migration case Task 1 exists for.
func TestRefuseSqueezeIgnoresDatadirScheme(t *testing.T) {
	src := sourceDatadirFixture(t)
	settings, err := dbstate.ReadErigonDBSettings(src)
	require.NoError(t, err)
	require.Nil(t, settings.TrieVariant)

	require.Error(t, refuseSqueezeForBinTarget(binTarget(t), true))
	require.NoError(t, refuseSqueezeForBinTarget(resolveTarget(t, commitment.VariantHexPatriciaTrie), true))
}
