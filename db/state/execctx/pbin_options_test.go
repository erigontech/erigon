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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
)

// Mutates a process-global flag, so no test using it may run in parallel.
func withBinCommitmentFlag(t *testing.T, on bool) {
	t.Helper()
	orig := statecfg.ExperimentalBinCommitment
	t.Cleanup(func() { statecfg.ExperimentalBinCommitment = orig })
	statecfg.ExperimentalBinCommitment = on
}

// Bin is a persisted datadir property, so WithSequentialCommitment demotes only the
// experimental parallel/streaming tries: demoting bin would give a hex block-0 root.
func TestPBinWithSequentialCommitmentKeepsBin(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag commitment.TrieVariant
		want commitment.TrieVariant
	}{
		{"hex", commitment.VariantHexPatriciaTrie, commitment.VariantHexPatriciaTrie},
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

			sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithSequentialCommitment())
			require.NoError(t, err)
			defer sd.Close()

			require.Equal(t, tc.want, sd.GetCommitmentCtx().Trie().Variant())
		})
	}
}

// WithHexCommitmentOnly callers can only read hex branch records, so a bin datadir
// must fail loudly instead of having its bit-path records read as hex ones.
func TestPBinHexOnlyCommitmentRefusesBin(t *testing.T) {
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

func TestSharedDomainsBuildsContextsForEachCommitmentMode(t *testing.T) {
	originalBin := statecfg.ExperimentalBinCommitment
	originalHexBin := statecfg.ExperimentalHexBinCommitment
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = originalBin
		statecfg.ExperimentalHexBinCommitment = originalHexBin
	})

	for _, tc := range []struct {
		name      string
		bin       bool
		hexBin    bool
		canonical kv.Domain
		variants  map[kv.Domain]commitment.TrieVariant
	}{
		{
			name:      "hex",
			canonical: kv.CommitmentDomain,
			variants:  map[kv.Domain]commitment.TrieVariant{kv.CommitmentDomain: commitment.VariantHexPatriciaTrie},
		},
		{
			name:      "bin",
			bin:       true,
			canonical: kv.CommitmentDomain,
			variants:  map[kv.Domain]commitment.TrieVariant{kv.CommitmentDomain: commitment.VariantBinPatriciaTrie},
		},
		{
			name:      "hex+bin",
			bin:       true,
			hexBin:    true,
			canonical: kv.CommitmentDomain,
			variants: map[kv.Domain]commitment.TrieVariant{
				kv.CommitmentDomain:    commitment.VariantHexPatriciaTrie,
				kv.CommitmentBinDomain: commitment.VariantBinPatriciaTrie,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statecfg.ExperimentalBinCommitment = tc.bin
			statecfg.ExperimentalHexBinCommitment = tc.hexBin

			db := newTestDb(t, 16)
			tx, err := db.BeginTemporalRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()

			sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
			require.NoError(t, err)
			defer sd.Close()

			require.Equal(t, tc.canonical, sd.GetCommitmentCtx().CommitmentDomain())
			for domain, variant := range tc.variants {
				ctx := sd.GetCommitmentCtxForDomain(domain)
				require.NotNil(t, ctx, "domain %s", domain)
				require.Equal(t, variant, ctx.Trie().Variant(), "domain %s", domain)
			}
			for _, domain := range []kv.Domain{kv.CommitmentDomain, kv.CommitmentBinDomain} {
				if _, expected := tc.variants[domain]; !expected {
					require.Nil(t, sd.GetCommitmentCtxForDomain(domain), "unexpected domain %s", domain)
				}
			}
		})
	}
}

func TestSharedDomainsHexOnlyOptionSelectsHexArmInDualMode(t *testing.T) {
	originalBin := statecfg.ExperimentalBinCommitment
	originalHexBin := statecfg.ExperimentalHexBinCommitment
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = originalBin
		statecfg.ExperimentalHexBinCommitment = originalHexBin
	})

	db := newTestDb(t, 16)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithHexCommitmentOnly())
	require.NoError(t, err)
	defer sd.Close()

	require.NotNil(t, sd.GetCommitmentCtxForDomain(kv.CommitmentDomain))
	require.Nil(t, sd.GetCommitmentCtxForDomain(kv.CommitmentBinDomain))
	require.Equal(t, commitment.VariantHexPatriciaTrie, sd.GetCommitmentCtx().Trie().Variant())
}
