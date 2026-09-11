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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/changeset"
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

func withDualCommitmentFlags(t *testing.T) {
	t.Helper()
	withBinCommitmentFlag(t, true)
	orig := statecfg.ExperimentalHexBinCommitment
	t.Cleanup(func() { statecfg.ExperimentalHexBinCommitment = orig })
	statecfg.ExperimentalHexBinCommitment = true
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
		name     string
		bin      bool
		hexBin   bool
		variants map[kv.Domain]commitment.TrieVariant
	}{
		{
			name:     "hex",
			variants: map[kv.Domain]commitment.TrieVariant{kv.CommitmentDomain: commitment.VariantHexPatriciaTrie},
		},
		{
			name:     "bin",
			bin:      true,
			variants: map[kv.Domain]commitment.TrieVariant{kv.CommitmentDomain: commitment.VariantBinPatriciaTrie},
		},
		{
			name:   "hex+bin",
			bin:    true,
			hexBin: true,
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

			require.Equal(t, kv.CommitmentDomain, sd.GetCommitmentCtx().CommitmentDomain())
			for domain, variant := range tc.variants {
				ctx := sd.GetCommitmentCtxForDomain(domain)
				require.NotNil(t, ctx, "domain %s", domain)
				require.Equal(t, variant, ctx.Trie().Variant(), "domain %s", domain)
			}
			require.Len(t, sd.CommitmentDomains(), len(tc.variants))
		})
	}
}

func TestSharedDomainsHexOnlyOptionSelectsHexArmInDualMode(t *testing.T) {
	withDualCommitmentFlags(t)

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

func TestSharedDomainsExplicitCommitmentDomainSelectsBinArm(t *testing.T) {
	withDualCommitmentFlags(t)

	db := newTestDb(t, 16)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithCommitmentDomain(kv.CommitmentBinDomain))
	require.NoError(t, err)
	defer sd.Close()

	require.Equal(t, kv.CommitmentBinDomain, sd.GetCommitmentCtx().CommitmentDomain())
	require.Equal(t, commitment.VariantBinPatriciaTrie, sd.GetCommitmentCtx().Trie().Variant())
}

func TestSharedDomainsDualDefaultKeepsHexAfterActivation(t *testing.T) {
	withDualCommitmentFlags(t)

	db := newTestDb(t, 16)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	tx.AggTx().(interface{ SetCanonicalCommitmentDomain(kv.Domain) }).SetCanonicalCommitmentDomain(kv.CommitmentBinDomain)

	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	require.Equal(t, kv.CommitmentDomain, sd.GetCommitmentCtx().CommitmentDomain())
	require.Equal(t, commitment.VariantHexPatriciaTrie, sd.GetCommitmentCtx().Trie().Variant())
	require.NotNil(t, sd.GetCommitmentCtxForDomain(kv.CommitmentBinDomain))
}

func TestSharedDomainsDropsStoppedCommitmentPendingWrites(t *testing.T) {
	for _, withoutChangeset := range []bool{false, true} {
		t.Run(map[bool]string{false: "with changeset", true: "without changeset"}[withoutChangeset], func(t *testing.T) {
			db := newTestDb(t, 16)
			tx, err := db.BeginTemporalRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
			require.NoError(t, err)
			defer sd.Close()
			cs := &changeset.StateChangeSet{}
			sd.SetChangesetAccumulator(cs)
			sd.SetDeferCommitmentUpdates(true)
			for _, b := range []byte{0x11, 0x22} {
				require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, bytes.Repeat([]byte{b}, 20), encAccount(1), 1, nil))
			}
			_, err = sd.ComputeCommitment(t.Context(), tx, false, 0, 1, "test", nil)
			require.NoError(t, err)
			require.True(t, sd.GetCommitmentCtx().HasPendingUpdate())
			require.Empty(t, cs.Diffs[kv.CommitmentDomain].GetDiffSet())
			tx.AggTx().(interface{ StopCommitmentDomain(kv.Domain) }).StopCommitmentDomain(kv.CommitmentDomain)

			if withoutChangeset {
				err = sd.FlushPendingUpdatesWithoutChangeset(tx)
			} else {
				err = sd.FlushPendingUpdates(t.Context(), tx)
			}
			require.NoError(t, err)
			require.False(t, sd.GetCommitmentCtx().HasPendingUpdate())
			branch, _, err := sd.GetLatest(kv.CommitmentDomain, tx, []byte{0})
			require.NoError(t, err)
			require.Empty(t, branch)
			require.Empty(t, cs.Diffs[kv.CommitmentDomain].GetDiffSet())
		})
	}
}

func TestSharedDomainsRestoresAfterShadowStopsAdvancing(t *testing.T) {
	originalBin, originalHexBin := statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment
	originalParallel := statecfg.ExperimentalParallelCommitment
	originalHash, originalSuite := statecfg.BinCommitmentHash, commitment.PBinHashSuiteName()
	statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment = true, true
	statecfg.ExperimentalParallelCommitment = false
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment = originalBin, originalHexBin
		statecfg.ExperimentalParallelCommitment = originalParallel
		statecfg.BinCommitmentHash = originalHash
		require.NoError(t, commitment.SetPBinHashSuite(originalSuite))
	})

	for _, mode := range []string{"frozen", "stopped"} {
		t.Run(mode, func(t *testing.T) {
			db := newTestDb(t, 16)
			tx, err := db.BeginTemporalRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
			require.NoError(t, err)
			defer sd.Close()
			key := bytes.Repeat([]byte{0x11}, 20)
			require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, key, encAccount(1), 1, nil))
			hexRoot, err := sd.ComputeCommitment(t.Context(), tx, true, 0, 1, "test", nil)
			require.NoError(t, err)
			hexRoot = bytes.Clone(hexRoot)
			binCtx := sd.GetCommitmentCtxForDomain(kv.CommitmentBinDomain)
			binCtx.GetUpdates().Close()
			binCtx.SetUpdates(binCtx.NewBinUpdates(map[string]struct{}{string(key): {}}))
			_, err = binCtx.ComputeCommitment(t.Context(), tx, true, 0, 1, "test", nil)
			require.NoError(t, err)
			agg := tx.AggTx().(interface{ Agg() *dbstate.Aggregator }).Agg()
			agg.SetCanonicalCommitmentDomain(kv.CommitmentBinDomain)
			if mode == "frozen" {
				variant, hash := dbstate.TrieVariantHexBin, commitment.PBinHashSuiteName()
				require.NoError(t, dbstate.WriteErigonDBSettings(tx.Debug().Dirs(), &dbstate.ErigonDBSettings{
					StepSize: 16, StepsInFrozenFile: 8, TrieVariant: &variant, TrieHash: &hash,
				}))
				require.NoError(t, agg.FreezeDomain(kv.CommitmentDomain, 1))
			} else {
				agg.StopCommitmentDomain(kv.CommitmentDomain)
			}
			require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, key, encAccount(2), 3, encAccount(1)))
			binCtx.GetUpdates().Close()
			binCtx.SetUpdates(binCtx.NewBinUpdates(map[string]struct{}{string(key): {}}))
			binRoot, err := binCtx.ComputeCommitment(t.Context(), tx, true, 1, 3, "test", nil)
			require.NoError(t, err)
			binRoot = bytes.Clone(binRoot)
			require.NoError(t, rawdbv3.TxNums.Append(tx, 0, 1))
			require.NoError(t, rawdbv3.TxNums.Append(tx, 1, 3))
			sd.SetTxNum(3)
			require.NoError(t, sd.Commit(t.Context(), tx))
			require.NoError(t, tx.Commit())
			if mode == "frozen" {
				require.NoError(t, agg.ReloadErigonDBSettings(true))
			}

			roTx, err := db.BeginTemporalRo(t.Context())
			require.NoError(t, err)
			defer roTx.Rollback()
			restored, err := execctx.NewSharedDomains(t.Context(), roTx, log.New())
			require.NoError(t, err)
			defer restored.Close()
			txNum, blockNum, err := restored.SeekCommitment(t.Context(), roTx)
			require.NoError(t, err)
			require.EqualValues(t, 3, txNum)
			require.EqualValues(t, 1, blockNum)
			root, err := restored.GetCommitmentCtxForDomain(kv.CommitmentBinDomain).Trie().RootHash()
			require.NoError(t, err)
			require.Equal(t, binRoot, root)
			if mode == "frozen" {
				root, err = restored.GetCommitmentCtxForDomain(kv.CommitmentDomain).Trie().RootHash()
				require.NoError(t, err)
				require.Equal(t, hexRoot, root)
			}
		})
	}
}
