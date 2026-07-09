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

package integrity_test

import (
	"context"
	"math/rand"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestCheckStateVerify_VersionRegimes builds two single-regime datadirs — one referenced and one
// plain, each with a merged commitment file whose range exceeds the referencing threshold — and
// asserts the regime-aware integrity checks pass on both. The regime is decided by the flag on
// WRITE and recovered from file content on read: with the flag on, the merged file must carry
// shortened keys (referenced); with it off it must be plain (read/verified directly, not as a
// referenced file with stale offsets).
func TestCheckStateVerify_VersionRegimes(t *testing.T) {
	t.Run("referenced", func(t *testing.T) {
		t.Parallel()
		runVersionRegimeCheck(t, true)
	})
	t.Run("plain", func(t *testing.T) {
		t.Parallel()
		runVersionRegimeCheck(t, false)
	})
}

func runVersionRegimeCheck(t *testing.T, referencesInCommitmentBranches bool) {
	t.Helper()
	logger := log.New()
	ctx := t.Context()
	const stepSize = uint64(10)
	const txs = 80 // 8 steps -> merge produces a >= threshold commitment file

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDBWithStepSize(t, dirs, stepSize)
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, referencesInCommitmentBranches)

	writeAndBuild(t, ctx, db, agg, txs)
	require.NoError(t, agg.MergeLoop(ctx))

	// Reopen so the commitment file's referenced regime is re-derived from the on-disk version,
	// exercising the version-based path independently of the in-memory flag used during merge.
	db = reopenAgg(t, db, agg, dirs, stepSize, logger)

	gotReferenced, gotSpan := largestCommitmentFile(t, ctx, db, stepSize)
	require.GreaterOrEqual(t, gotSpan, 2*stepSize, "expected a merged commitment file spanning >= 2 steps")
	require.Equal(t, referencesInCommitmentBranches, gotReferenced, "merged commitment file's version regime must match the write flag")

	require.NoError(t, integrity.CheckStateVerify(ctx, db, true /* failFast */, 0 /* fromStep */, logger))

	// CheckCommitmentKvDeref dereferences short keys and structurally validates each branch;
	// that validator needs real chain data, so it is asserted only for the plain regime, where
	// it must recognise the v2.1 file as plain and skip it (the version-aware skip under test).
	if !referencesInCommitmentBranches {
		require.NoError(t, integrity.CheckCommitmentKvDeref(ctx, db, nil /* cache */, true /* failFast */, logger))
	}
}

func writeAndBuild(t *testing.T, ctx context.Context, db kv.TemporalRwDB, agg *state.Aggregator, txs uint64) {
	t.Helper()
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	rnd := rand.New(rand.NewSource(7))
	for txNum := uint64(1); txNum <= txs; txNum++ {
		addr := make([]byte, length.Addr)
		loc := make([]byte, length.Hash)
		rnd.Read(addr)
		rnd.Read(loc)

		acc := accounts.Account{Nonce: txNum, Balance: *uint256.NewInt(txNum * 1000), CodeHash: accounts.EmptyCodeHash}
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, addr, accounts.SerialiseV3(&acc), txNum, nil))

		storageKey := append(common.Copy(addr), loc...)
		require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, storageKey, []byte{addr[0], loc[0]}, txNum, nil))

		_, err = domains.ComputeCommitment(ctx, tx, true, txNum, txNum, "test", nil)
		require.NoError(t, err)
	}

	require.NoError(t, domains.Flush(ctx, tx))
	require.NoError(t, tx.Commit())
	require.NoError(t, agg.BuildFiles(txs))
}

func reopenAgg(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator, dirs datadir.Dirs, stepSize uint64, logger log.Logger) kv.TemporalRwDB {
	t.Helper()
	agg.Close()
	newAgg := state.NewTest(dirs).StepSize(stepSize).Logger(logger).MustOpen(t.Context(), db)
	t.Cleanup(newAgg.Close)
	require.NoError(t, newAgg.OpenFolder())
	require.NoError(t, newAgg.BuildMissedAccessors(t.Context(), 1))
	newDB, err := temporal.New(db, newAgg)
	require.NoError(t, err)
	return newDB
}

// largestCommitmentFile returns the version-derived referenced regime and txNum span of the
// widest-range commitment .kv file visible after reopen (the merged one).
func largestCommitmentFile(t *testing.T, ctx context.Context, db kv.TemporalRoDB, stepSize uint64) (bool, uint64) {
	t.Helper()
	tx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	aggTx := state.AggTx(tx)
	defer aggTx.Close()

	var referenced bool
	var bestSpan uint64
	found := false
	for _, f := range aggTx.Files(kv.CommitmentDomain) {
		if !strings.HasSuffix(f.Fullpath(), ".kv") {
			continue
		}
		if span := f.EndRootNum() - f.StartRootNum(); span >= bestSpan {
			referenced, bestSpan, found = state.CommitmentBranchReferenced(f.Version(), stepSize, f.StartRootNum(), f.EndRootNum()), span, true
		}
	}
	require.True(t, found, "expected at least one commitment .kv file")
	return referenced, bestSpan
}
