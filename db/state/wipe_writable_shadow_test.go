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

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestWipeWritableShadowPast_NotAtStepBoundary pins the precondition
// check: lastTxNum must land on a step boundary. Aligned-mode SetHead
// enforces this via the snapshot-trim invariant; the primitive's own
// guard makes the file-vs-DB layering contract explicit for any future
// caller (fork-from CLI, tooling) that constructs a wipe call directly.
func TestWipeWritableShadowPast_NotAtStepBoundary(t *testing.T) {
	t.Parallel()

	const stepSize uint64 = 8
	db, agg := newWipeTestDB(t, stepSize)

	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	// lastTxNum=5: (5+1)%8 = 6, not on step boundary.
	err = agg.WipeWritableShadowPast(t.Context(), rwTx, 5)
	require.Error(t, err)
	require.Contains(t, err.Error(), "step boundary")
}

// TestWipeWritableShadowPast_ClearsValuesPastBoundary pins the core
// behavior: domain entries whose step coordinate > stepBoundary get
// deleted; entries at steps <= stepBoundary survive.
//
// Setup: writes to three accounts at txNums spanning three steps
// (stepSize=8 → step 0 is tx 0..7, step 1 is tx 8..15, step 2 is
// tx 16..23). The flush moves all three writes into the DB shadow.
//
// Action: WipeWritableShadowPast with lastTxNum=7 (= end of step 0,
// step boundary). stepBoundary computes as (7+1)/8 = 1. Entries with
// step > 1 (= step 2 only) should be deleted.
//
// Hmm — wait: with stepBoundary=1, entries at step 0 (acc1) and
// step 1 (acc2) should survive; only step 2 (acc3) should go. That
// matches the wipe semantics (delete past, retain ≤).
//
// Assertion: read each account post-wipe via GetLatest; acc1 and
// acc2 still resolve to their written value, acc3 is gone.
func TestWipeWritableShadowPast_ClearsValuesPastBoundary(t *testing.T) {
	t.Parallel()

	const stepSize uint64 = 8
	db, agg := newWipeTestDB(t, stepSize)

	acc1Addr := [20]byte{1}
	acc2Addr := [20]byte{2}
	acc3Addr := [20]byte{3}

	// --- Phase 1: write three accounts at three different steps ---
	{
		rwTx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer rwTx.Rollback()

		domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
		require.NoError(t, err)
		defer domains.Close()

		writeAcc := func(addr [20]byte, txNum uint64, nonce uint64) {
			acc := accounts.Account{
				Nonce:    nonce,
				Balance:  *uint256.NewInt(nonce * 100),
				CodeHash: accounts.EmptyCodeHash,
			}
			buf := accounts.SerialiseV3(&acc)
			domains.SetTxNum(txNum)
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, addr[:], buf, txNum, nil))
		}

		writeAcc(acc1Addr, 0, 1)  // step 0
		writeAcc(acc2Addr, 8, 2)  // step 1
		writeAcc(acc3Addr, 16, 3) // step 2

		require.NoError(t, domains.Flush(t.Context(), rwTx))
		require.NoError(t, rwTx.Commit())
	}

	// --- Phase 2: verify all three accounts resolve before the wipe ---
	{
		roTx, err := db.BeginTemporalRo(t.Context())
		require.NoError(t, err)
		defer roTx.Rollback()
		require.NotEmpty(t, getLatestAccount(t, roTx, acc1Addr), "acc1 must exist before wipe")
		require.NotEmpty(t, getLatestAccount(t, roTx, acc2Addr), "acc2 must exist before wipe")
		require.NotEmpty(t, getLatestAccount(t, roTx, acc3Addr), "acc3 must exist before wipe")
	}

	// --- Phase 3: wipe past lastTxNum=7 (end of step 0; stepBoundary=1) ---
	{
		rwTx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer rwTx.Rollback()

		require.NoError(t, agg.WipeWritableShadowPast(t.Context(), rwTx, 7))
		require.NoError(t, rwTx.Commit())
	}

	// --- Phase 4: verify only acc1 (step 0) and acc2 (step 1) survive ---
	//
	// stepBoundary = (7+1)/8 = 1. Wipe deletes entries whose step > 1,
	// i.e. step 2 (acc3). Steps 0 and 1 (acc1, acc2) are retained.
	{
		roTx, err := db.BeginTemporalRo(t.Context())
		require.NoError(t, err)
		defer roTx.Rollback()
		require.NotEmpty(t, getLatestAccount(t, roTx, acc1Addr), "acc1 (step 0) must survive wipe")
		require.NotEmpty(t, getLatestAccount(t, roTx, acc2Addr), "acc2 (step 1) must survive wipe")
		require.Empty(t, getLatestAccount(t, roTx, acc3Addr), "acc3 (step 2) must be wiped")
	}
}

// newWipeTestDB constructs a temporal DB + aggregator sized for fast
// wipe tests. Mirrors testDbAndAggregatorv3 from squeeze_test.go but
// exported in this file so wipe tests have a self-contained fixture.
func newWipeTestDB(tb testing.TB, stepSize uint64) (kv.TemporalRwDB, *dbstate.Aggregator) {
	tb.Helper()
	return testDbAndAggregatorv3(tb, stepSize)
}

// getLatestAccount reads the latest account value for addr via the
// public TemporalGetter API. Returns nil when absent.
func getLatestAccount(t *testing.T, tx kv.TemporalTx, addr [20]byte) []byte {
	t.Helper()
	v, _, err := tx.GetLatest(kv.AccountsDomain, addr[:])
	require.NoError(t, err)
	return v
}
