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

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func withCommitmentFlag(t *testing.T, variant commitment.TrieVariant) {
	t.Helper()
	origPar := statecfg.ExperimentalParallelCommitment
	t.Cleanup(func() {
		statecfg.ExperimentalParallelCommitment = origPar
	})
	statecfg.ExperimentalParallelCommitment = variant == commitment.VariantParallelHexPatricia
}

// Update set is identical across calls so the returned roots can be compared.
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

func TestSharedDomains_ParallelFlag_RootEquivalence(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// No t.Parallel: mutates process-global statecfg flags.

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

		sd.EnableParaTrieDB(db)

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

// TestSharedDomains_WithParaTrieDB_SelectsParallelTrie pins the construction-time
// wiring: selecting the parallel trie and supplying the DB it needs happen in one
// expression, so there is no second call left to forget.
func TestSharedDomains_WithParaTrieDB_SelectsParallelTrie(t *testing.T) {
	// No t.Parallel: mutates process-global statecfg flags.
	withCommitmentFlag(t, commitment.VariantParallelHexPatricia)

	db := newTestDb(t, 16)
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	sd, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New(), execctx.WithParaTrieDB(db))
	require.NoError(t, err)
	defer sd.Close()

	require.Equal(t, commitment.VariantParallelHexPatricia, sd.GetCommitmentCtx().Trie().Variant())
}

// TestSharedDomains_ParallelTrieNotWired_IsLoudOnComputeCommitment pins the
// fallback as loud. Selecting the parallel trie without ever supplying a DB
// leaves the context on the sequential trie, which is the intended escape hatch
// for DB-less RPC and integrity contexts and a bug for anything computing a root.
func TestSharedDomains_ParallelTrieNotWired_IsLoudOnComputeCommitment(t *testing.T) {
	// No t.Parallel: mutates process-global statecfg, dbg and root-logger state.
	withCommitmentFlag(t, commitment.VariantParallelHexPatricia)
	defer func(v bool) { dbg.AssertEnabled = v }(dbg.AssertEnabled)

	compute := func(t *testing.T) ([]byte, error) {
		t.Helper()
		db := newTestDb(t, 16)
		rwTx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		t.Cleanup(rwTx.Rollback)

		sd, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
		require.NoError(t, err)
		t.Cleanup(sd.Close)

		return sd.ComputeCommitment(t.Context(), rwTx, false, 0, 1, "", nil)
	}

	t.Run("errors under assertions", func(t *testing.T) {
		dbg.AssertEnabled = true
		_, err := compute(t)
		require.ErrorContains(t, err, "no DB was wired",
			"an unwired parallel selection computed a root silently")
	})

	t.Run("warns and still computes with assertions off", func(t *testing.T) {
		dbg.AssertEnabled = false
		logs := captureRootLog(t)

		rh, err := compute(t)
		require.NoError(t, err, "the sequential fallback must stay usable in production")
		require.NotEmpty(t, rh, "the fallback must still produce a root")
		require.Contains(t, logs.String(), "no DB was wired",
			"the fallback is silent on a non-assert build")
	})
}

// captureRootLog redirects the root logger — the one the package-level log.Warn
// in commitmentdb writes to — into a buffer for the duration of the test.
func captureRootLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := log.Root().GetHandler()
	log.Root().SetHandler(log.StreamHandler(&buf, log.LogfmtFormat()))
	t.Cleanup(func() { log.Root().SetHandler(prev) })
	return &buf
}

// TestSharedDomains_WithParaTrieDB_BindsPinController pins the half of the wiring
// the trie variant does not show: WithParaTrieDB must also install the adaptive
// pin controller's cache-miss callback, as the EnableParaTrieDB wrapper does.
// Skipping it leaves roots correct and silently stops all pinning.
func TestSharedDomains_WithParaTrieDB_BindsPinController(t *testing.T) {
	// No t.Parallel: mutates process-global statecfg and dbg flags.
	withCommitmentFlag(t, commitment.VariantParallelHexPatricia)
	defer func(v bool) { dbg.DisableAdaptivePin = v }(dbg.DisableAdaptivePin)
	dbg.DisableAdaptivePin = false

	db := newTestDb(t, 16)
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	p, ok := rwTx.AggTx().(commitment.BranchCacheProvider)
	require.True(t, ok)
	cache := p.BranchCache()
	require.NotNil(t, cache, "test aggregator has no branch cache, nothing to bind")

	// A storage prefix, so a miss reaches the controller's own callback too.
	prefix := make([]byte, 33)
	prefix[0] = 0x20

	misses := 0
	cache.SetMissCallback(func([]byte) { misses++ })
	cache.Get(prefix)
	require.Equal(t, 1, misses, "an absent prefix must fire the miss callback")

	sd, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New(), execctx.WithParaTrieDB(db))
	require.NoError(t, err)
	defer sd.Close()

	cache.Get(prefix)
	require.Equal(t, 1, misses,
		"WithParaTrieDB left the pin controller unbound: the callback is still the test's, so onCacheMiss never fires and nothing is pinned")
}
