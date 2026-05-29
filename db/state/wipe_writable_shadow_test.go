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

// TestWipeWritableShadowPast_NonAligned_BoundaryStepReplay pins the
// non-aligned wipe path: when lastTxNum sits mid-step, the boundary
// step's shadow entries that reflect writes at txnum > lastTxNum must
// be replayed from history back to their as-of-lastTxNum values
// (deleted if the key didn't exist as of lastTxNum, restored to the
// earlier value if it did).
//
// Setup (stepSize=8 → boundary step 0 covers txnums 0..7):
//   - acc1 written at txnum 3 (in boundary step, ≤ lastTxNum=5 → must
//     survive the wipe)
//   - acc2 written at txnum 6 (in boundary step, > lastTxNum=5 → must
//     be removed since it didn't exist as of lastTxNum)
//
// Action: WipeWritableShadowPast(lastTxNum=5). Non-aligned ((5+1)%8 ==
// 6 ≠ 0), so the boundary-step diff-replay path fires.
//
// Assertion: acc1 survives, acc2 is gone. The whole-step wipe alone
// wouldn't remove acc2 (it shares step 0 with acc1); the boundary-step
// diff-replay is what surfaces acc2 via HistoryKeyTxNumRange and
// removes it via the as-of-lastTxNum GetAsOf returning nil.
func TestWipeWritableShadowPast_NonAligned_BoundaryStepReplay(t *testing.T) {
	t.Parallel()

	const stepSize uint64 = 8
	db, agg := newWipeTestDB(t, stepSize)

	acc1Addr := [20]byte{1}
	acc2Addr := [20]byte{2}

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

		writeAcc(acc1Addr, 3, 1) // step 0, txnum 3 ≤ lastTxNum=5
		writeAcc(acc2Addr, 6, 2) // step 0, txnum 6 > lastTxNum=5

		require.NoError(t, domains.Flush(t.Context(), rwTx))
		require.NoError(t, rwTx.Commit())
	}

	{
		rwTx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer rwTx.Rollback()

		require.NoError(t, agg.WipeWritableShadowPast(t.Context(), rwTx, 5))
		require.NoError(t, rwTx.Commit())
	}

	{
		roTx, err := db.BeginTemporalRo(t.Context())
		require.NoError(t, err)
		defer roTx.Rollback()
		require.NotEmpty(t, getLatestAccount(t, roTx, acc1Addr), "acc1 (txnum 3 ≤ lastTxNum=5) must survive non-aligned wipe")
		require.Empty(t, getLatestAccount(t, roTx, acc2Addr), "acc2 (txnum 6 > lastTxNum=5) must be removed by boundary-step diff-replay")
	}
}

// TestWipeWritableShadowPast_ClearsValuesPastBoundary pins the per-tx
// contract: after wipe past lastTxNum, no writable-shadow entry covers
// any txnum > lastTxNum, regardless of which step that txnum sits in.
//
// Setup: three accounts written at txnums 0, 8, 16 (stepSize=8 → step
// 0 is tx 0..7, step 1 is tx 8..15, step 2 is tx 16..23). The flush
// moves all three writes into the DB shadow.
//
// Action: WipeWritableShadowPast with lastTxNum=7 — the last txnum of
// step 0, on a step boundary. The per-tx contract says any write at
// txnum > 7 must be gone post-wipe.
//
// Assertion: only acc1 (written at txnum 0) survives. acc2 (txnum 8)
// AND acc3 (txnum 16) are wiped — both are at txnums > 7 even though
// only acc3's step is "well past" the boundary. The earlier
// step-granular semantics retained acc2 and shadowed legitimate file
// data at sub-op #3's commitment-anchor check; the strict per-tx
// contract removes that wedge.
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

		writeAcc(acc1Addr, 0, 1)  // step 0 (txnum 0 — kept)
		writeAcc(acc2Addr, 8, 2)  // step 1 (txnum 8 — past lastTxNum=7, wiped)
		writeAcc(acc3Addr, 16, 3) // step 2 (txnum 16 — past lastTxNum=7, wiped)

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

	// --- Phase 3: wipe past lastTxNum=7 (last txnum of step 0) ---
	{
		rwTx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer rwTx.Rollback()

		require.NoError(t, agg.WipeWritableShadowPast(t.Context(), rwTx, 7))
		require.NoError(t, rwTx.Commit())
	}

	// --- Phase 4: verify only acc1 (txnum 0) survives ---
	//
	// stepBoundary = (7+1)/8 = 1. Wipe deletes entries whose step >=
	// 1, i.e. steps 1 (acc2) and 2 (acc3). Only step 0 (acc1) is
	// retained — its single txnum (0) is the only one <= lastTxNum.
	{
		roTx, err := db.BeginTemporalRo(t.Context())
		require.NoError(t, err)
		defer roTx.Rollback()
		require.NotEmpty(t, getLatestAccount(t, roTx, acc1Addr), "acc1 (txnum 0 ≤ lastTxNum) must survive wipe")
		require.Empty(t, getLatestAccount(t, roTx, acc2Addr), "acc2 (txnum 8 > lastTxNum) must be wiped")
		require.Empty(t, getLatestAccount(t, roTx, acc3Addr), "acc3 (txnum 16 > lastTxNum) must be wiped")
	}
}

// TestWipeWritableShadowPast_ClearsMultipleStepDupsOfSameKey pins the
// wipe's behavior for the common pattern of a single key written many
// times across different steps (KeyCommitmentState is the canonical
// example — every block's commitment overwrites the same key with a
// new step-prefixed dup value). The DupSort cursor needs to walk
// every dup and delete the ones past stepBoundary; an iterator that
// e.g. moves to next primary key after deletion would leave dups
// behind.
//
// Setup: write `same` at txnums 0, 8, 16, 24 (steps 0, 1, 2, 3). All
// four writes share the same primary key but land at different steps
// (encoded as ^step in the value prefix → four dup values).
//
// Action: WipeWritableShadowPast with lastTxNum=7 (last txnum of
// step 0; stepBoundary=1).
//
// Assertion: post-wipe, GetLatest(same) resolves to the step-0 value
// (nonce=1). If the wipe leaves a step-1+ dup behind, GetLatest would
// see it (higher step = lower encoded prefix = earlier in DupSort
// order = what SeekExact returns first) and we'd read the wrong
// nonce.
func TestWipeWritableShadowPast_ClearsMultipleStepDupsOfSameKey(t *testing.T) {
	t.Parallel()

	const stepSize uint64 = 8
	db, agg := newWipeTestDB(t, stepSize)

	sameAddr := [20]byte{0xAB}

	{
		rwTx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer rwTx.Rollback()

		domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
		require.NoError(t, err)
		defer domains.Close()

		writeAt := func(txNum, nonce uint64) {
			acc := accounts.Account{
				Nonce:    nonce,
				Balance:  *uint256.NewInt(nonce * 100),
				CodeHash: accounts.EmptyCodeHash,
			}
			buf := accounts.SerialiseV3(&acc)
			domains.SetTxNum(txNum)
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, sameAddr[:], buf, txNum, nil))
		}

		writeAt(0, 1)  // step 0 nonce=1 (the one that must survive)
		writeAt(8, 2)  // step 1 nonce=2 (must be wiped)
		writeAt(16, 3) // step 2 nonce=3 (must be wiped)
		writeAt(24, 4) // step 3 nonce=4 (must be wiped)

		require.NoError(t, domains.Flush(t.Context(), rwTx))
		require.NoError(t, rwTx.Commit())
	}

	{
		rwTx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer rwTx.Rollback()

		require.NoError(t, agg.WipeWritableShadowPast(t.Context(), rwTx, 7))
		require.NoError(t, rwTx.Commit())
	}

	{
		roTx, err := db.BeginTemporalRo(t.Context())
		require.NoError(t, err)
		defer roTx.Rollback()
		raw := getLatestAccount(t, roTx, sameAddr)
		require.NotEmpty(t, raw, "step-0 nonce=1 write must survive")
		var got accounts.Account
		require.NoError(t, accounts.DeserialiseV3(&got, raw))
		require.Equal(t, uint64(1), got.Nonce,
			"GetLatest must resolve to step-0 value (nonce=1); a non-1 nonce means a step>=1 dup wasn't wiped")
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
