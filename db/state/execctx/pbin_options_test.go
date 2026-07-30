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

func withBinCommitmentFlag(t *testing.T, on bool) {
	t.Helper()
	orig := statecfg.ExperimentalBinCommitment
	t.Cleanup(func() { statecfg.ExperimentalBinCommitment = orig })
	statecfg.ExperimentalBinCommitment = on
}

// The genesis-style option demotes only the experimental parallel/streaming tries;
// bin is a persisted datadir property, so demoting it would compute a hex block-0
// root over a datadir the executor then reads as bin.
func TestPBinWithoutParallelCommitmentKeepsBin(t *testing.T) {
	// No t.Parallel: mutates process-global statecfg flags.
	for _, tc := range []struct {
		name string
		flag commitment.TrieVariant
		want commitment.TrieVariant
	}{
		{"hex", commitment.VariantHexPatriciaTrie, commitment.VariantHexPatriciaTrie},
		{"streaming", commitment.VariantStreamingHexPatricia, commitment.VariantHexPatriciaTrie},
		{"parallel", commitment.VariantParallelHexPatricia, commitment.VariantHexPatriciaTrie},
		{"bin", commitment.VariantBinPatriciaTrie, commitment.VariantBinPatriciaTrie},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withBinCommitmentFlag(t, tc.flag == commitment.VariantBinPatriciaTrie)
			withCommitmentFlag(t, tc.flag)

			db := newTestDb(t, 16)
			tx, err := db.BeginTemporalRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()

			sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithoutParallelCommitment())
			require.NoError(t, err)
			defer sd.Close()

			require.Equal(t, tc.want, sd.GetCommitmentCtx().Trie().Variant())
		})
	}
}

// Paths that can only read hex branch records must fail loudly on a bin datadir
// instead of reinterpreting bit-path records as hex ones.
func TestPBinHexOnlyCommitmentRefusesBin(t *testing.T) {
	// No t.Parallel: mutates process-global statecfg flags.
	withBinCommitmentFlag(t, true)

	db := newTestDb(t, 16)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithHexCommitmentOnly())
	require.ErrorIs(t, err, execctx.ErrBinCommitmentUnsupported)
	require.Nil(t, sd)
}

func TestPBinHexOnlyCommitmentDemotesParallel(t *testing.T) {
	// No t.Parallel: mutates process-global statecfg flags.
	withBinCommitmentFlag(t, false)
	withCommitmentFlag(t, commitment.VariantParallelHexPatricia)

	db := newTestDb(t, 16)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithHexCommitmentOnly())
	require.NoError(t, err)
	defer sd.Close()

	require.Equal(t, commitment.VariantHexPatriciaTrie, sd.GetCommitmentCtx().Trie().Variant())
}
