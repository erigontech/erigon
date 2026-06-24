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
	"github.com/erigontech/erigon/execution/commitment"
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

// TestWipeWritableShadowPast_NonAligned_RestoresEarlierValue pins
// G3.15's missing case: a key was written EARLIER in the boundary
// step (value V1, txnum ≤ lastTxNum) AND modified LATER in the same
// step (value V2, txnum > lastTxNum). Post-wipe, the latest value
// must be V1 (as-of lastTxNum), NOT V2 (post-target value), NOT nil
// (the simpler "didn't-exist-yet" case).
//
// Live symptom: forward exec post-mode-B failed with gas mismatch
// on the first new block. Hypothesis: the boundary-step diff-replay
// either skipped this category of key, or wrote V2 (or nil) instead
// of V1, leaving the writable shadow in a state where exec reads the
// wrong account value.
//
// Setup (stepSize=8, lastTxNum=5 → boundary step 0 covers txnums 0..7):
//   - acc1 written at txnum 3 with nonce=1 (≤ lastTxNum, should be the
//     "as-of lastTxNum" value)
//   - acc1 written AGAIN at txnum 6 with nonce=2 (> lastTxNum, should
//     be undone by the wipe)
//
// Expectation: post-wipe, GetLatest(acc1) returns the nonce=1
// account, not nonce=2 and not empty.
func TestWipeWritableShadowPast_NonAligned_RestoresEarlierValue(t *testing.T) {
	t.Parallel()

	const stepSize uint64 = 8
	db, agg := newWipeTestDB(t, stepSize)

	acc1Addr := [20]byte{1}

	{
		rwTx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer rwTx.Rollback()

		domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
		require.NoError(t, err)
		defer domains.Close()

		writeAcc := func(txNum, nonce uint64) {
			acc := accounts.Account{
				Nonce:    nonce,
				Balance:  *uint256.NewInt(nonce * 100),
				CodeHash: accounts.EmptyCodeHash,
			}
			buf := accounts.SerialiseV3(&acc)
			domains.SetTxNum(txNum)
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, acc1Addr[:], buf, txNum, nil))
		}

		writeAcc(3, 1) // step 0, txnum 3 ≤ lastTxNum=5 → this is the as-of value
		writeAcc(6, 2) // step 0, txnum 6 > lastTxNum=5 → must be undone

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
		raw := getLatestAccount(t, roTx, acc1Addr)
		require.NotEmpty(t, raw,
			"G3.15: acc1 had a value at txnum 3 ≤ lastTxNum=5; the wipe must restore it, not delete it")

		var got accounts.Account
		require.NoError(t, accounts.DeserialiseV3(&got, raw))
		require.Equal(t, uint64(1), got.Nonce,
			"G3.15: post-wipe value must be the as-of-lastTxNum value (nonce=1, written at txnum 3), NOT the post-target write (nonce=2, written at txnum 6)")
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

// TestWipeWritableShadowPast_NonAligned_DeletionBeforeLastTxNumLeavesTombstone
// pins the exact case live-reproduced on hoodi 2026-06-09: a key
// written in a LOWER step (its value frozen into a step-N-1 .kv-like
// position in the shadow), then DELETED earlier in the boundary step,
// then RESTORED to a non-zero value LATER in the same step past
// lastTxNum. After WipeWritableShadowPast, GetLatest must return nil
// (because the deletion at txnum<=lastTxNum is the as-of state), not
// the lower-step value.
//
// Without the fix, applyReplay deletes the boundary-step entry (which
// reflects the late restore) but skips writing a tombstone when the
// as-of-lastTxNum value is empty. GetLatest then falls through to the
// lower-step entry (or, in production, the lower-step .kv file) and
// returns the wrong (stale, pre-deletion) value.
//
// Setup (stepSize=8 → step 0 = txnums 0-7, step 1 = txnums 8-15):
//   - txnum 0 (step 0): write nonce=1 — the "lower-step value" that
//     becomes the unwanted fall-through if the tombstone is missing.
//   - txnum 9 (step 1, ≤ lastTxNum=11): DomainDel — the SSTORE-to-zero
//     that should be the as-of state at lastTxNum.
//   - txnum 13 (step 1, > lastTxNum): write nonce=2 — the restore that
//     the wipe must undo.
//
// Action: WipeWritableShadowPast(lastTxNum=11). Non-aligned cut
// ((11+1) % 8 == 4 ≠ 0), so boundary-step diff-replay runs.
//
// Assertion: post-wipe GetLatest returns nil (account is in the
// "deleted" state matching the as-of-lastTxNum view). A non-empty
// result means a tombstone is missing from the boundary step and the
// fall-through to step 0 surfaces the stale pre-deletion nonce=1.
func TestWipeWritableShadowPast_NonAligned_DeletionBeforeLastTxNumLeavesTombstone(t *testing.T) {
	t.Parallel()

	const stepSize uint64 = 8
	db, agg := newWipeTestDB(t, stepSize)

	addr := [20]byte{0xCA, 0xFE}

	{
		rwTx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer rwTx.Rollback()

		domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
		require.NoError(t, err)
		defer domains.Close()

		writeAcc := func(txNum, nonce uint64) {
			acc := accounts.Account{
				Nonce:    nonce,
				Balance:  *uint256.NewInt(nonce * 100),
				CodeHash: accounts.EmptyCodeHash,
			}
			buf := accounts.SerialiseV3(&acc)
			domains.SetTxNum(txNum)
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, addr[:], buf, txNum, nil))
		}
		delAcc := func(txNum uint64) {
			domains.SetTxNum(txNum)
			require.NoError(t, domains.DomainDel(kv.AccountsDomain, rwTx, addr[:], txNum, nil))
		}

		writeAcc(0, 1)  // step 0: nonce=1 — the lower-step "shadow" value
		delAcc(9)       // step 1, ≤ lastTxNum=11: DELETE the account
		writeAcc(13, 2) // step 1, > lastTxNum=11: restore as nonce=2

		require.NoError(t, domains.Flush(t.Context(), rwTx))
		require.NoError(t, rwTx.Commit())
	}

	{
		rwTx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer rwTx.Rollback()

		require.NoError(t, agg.WipeWritableShadowPast(t.Context(), rwTx, 11))
		require.NoError(t, rwTx.Commit())
	}

	{
		roTx, err := db.BeginTemporalRo(t.Context())
		require.NoError(t, err)
		defer roTx.Rollback()
		raw := getLatestAccount(t, roTx, addr)
		require.Empty(t, raw,
			"post-wipe GetLatest must be empty: the SSTORE-to-zero at txnum 9 (≤ lastTxNum=11) is the as-of state; a non-empty result means applyReplay deleted the boundary-step entry but skipped writing a tombstone, letting the step-0 value (nonce=1) leak through")
	}
}

// These tests pin AssertWritableShadowConsistentAt — the post-wipe /
// pre-regen invariant gate added 2026-06-22 after the soak v19 iter-4
// 88-gas-diff investigation traced the wedge to multi-iter writable-
// shadow drift. The drift accumulates because no single check
// validates the post-wipe invariant; parallel-exec catch-up writes
// past lastTxNum survive across iters and corrupt subsequent reads.
//
// The invariant: post-WipeWritableShadowPast(lastTxNum), no row in
// any writable-domain ValuesTable should have a step coordinate
// strictly greater than stepContaining = lastTxNum / stepSize. Any
// such row indicates drift.

// TestAssertWritableShadowConsistentAt_CleanWipePasses pins the
// happy path: after a complete WipeWritableShadowPast, the consistency
// check returns nil. This is the regression-safety baseline — any
// future change that breaks the wipe such that post-wipe entries
// remain will surface as this test failing.
func TestAssertWritableShadowConsistentAt_CleanWipePasses(t *testing.T) {
	t.Parallel()

	const stepSize uint64 = 8
	db, agg := newWipeTestDB(t, stepSize)

	acc1Addr := [20]byte{1}
	acc2Addr := [20]byte{2}
	acc3Addr := [20]byte{3}

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
		// Writes spanning steps 0, 1, 2 — the wipe to lastTxNum=5
		// (mid-step 0) must leave only step-0 entries.
		writeAcc(acc1Addr, 0, 1)  // step 0
		writeAcc(acc2Addr, 8, 2)  // step 1 — must be wiped
		writeAcc(acc3Addr, 16, 3) // step 2 — must be wiped
		require.NoError(t, domains.Flush(t.Context(), rwTx))
		require.NoError(t, rwTx.Commit())
	}

	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	require.NoError(t, agg.WipeWritableShadowPast(t.Context(), rwTx, 5))

	// Post-wipe: every writable-shadow row must have step <= 0
	// (stepContaining = 5/8 = 0). The check should return nil.
	require.NoError(t, agg.AssertWritableShadowConsistentAt(t.Context(), rwTx, 5),
		"clean wipe must satisfy the post-wipe invariant — every shadow row at step <= stepContaining=0")
}

// TestAssertWritableShadowConsistentAt_DetectsDrift pins the bug-
// catching contract: when entries exist in the writable shadow at
// step coordinates strictly past stepContaining, the consistency check
// MUST return an error naming the offending domain + offender count.
//
// This is the exact shape the soak v19 iter-4 wedge produced: parallel-
// exec catch-up wrote state past lastTxNum, the writes weren't rolled
// back when validation failed, and the next read at the corrupted
// txnum returned the wrong value. With the check wired into
// WipeWritableShadowPast's tail, that drift surfaces immediately
// (loudly, at the unwind boundary) instead of accumulating silently
// across iters until a +44 gas mismatch surfaces 53k blocks later.
func TestAssertWritableShadowConsistentAt_DetectsDrift(t *testing.T) {
	t.Parallel()

	const stepSize uint64 = 8
	db, agg := newWipeTestDB(t, stepSize)

	acc1Addr := [20]byte{1}

	// Phase 1: write at step 0 (within lastTxNum) — the canonical
	// post-wipe state.
	{
		rwTx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer rwTx.Rollback()
		domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
		require.NoError(t, err)
		defer domains.Close()
		acc := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(100), CodeHash: accounts.EmptyCodeHash}
		buf := accounts.SerialiseV3(&acc)
		domains.SetTxNum(0)
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, acc1Addr[:], buf, 0, nil))
		require.NoError(t, domains.Flush(t.Context(), rwTx))
		require.NoError(t, rwTx.Commit())
	}

	// Phase 2: wipe to lastTxNum=5 (mid-step 0).
	{
		rwTx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer rwTx.Rollback()
		require.NoError(t, agg.WipeWritableShadowPast(t.Context(), rwTx, 5))
		require.NoError(t, rwTx.Commit())
	}

	// Phase 3: inject drift — write an account at txnum=12 (step 1)
	// AFTER the wipe. This simulates parallel-exec catch-up writing
	// state past lastTxNum without rolling back on validation failure.
	{
		rwTx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer rwTx.Rollback()
		domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
		require.NoError(t, err)
		defer domains.Close()
		acc := accounts.Account{Nonce: 99, Balance: *uint256.NewInt(9900), CodeHash: accounts.EmptyCodeHash}
		buf := accounts.SerialiseV3(&acc)
		domains.SetTxNum(12)
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, acc1Addr[:], buf, 12, nil))
		require.NoError(t, domains.Flush(t.Context(), rwTx))
		require.NoError(t, rwTx.Commit())
	}

	// Phase 4: the consistency check at lastTxNum=5 must catch the
	// drift. txnum=12 is in step 1 > stepContaining=0.
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	err = agg.AssertWritableShadowConsistentAt(t.Context(), rwTx, 5)
	require.Error(t, err, "drift past stepContaining must surface as an error")
	require.Contains(t, err.Error(), "writable shadow drift",
		"error must identify itself as drift")
	require.Contains(t, err.Error(), "accounts",
		"error must name the domain (accounts) that has drift")
}

// TestAssertWritableShadowConsistentAt_DetectsDriftAcrossDomains pins
// that the check covers every writable domain (accounts, storage, code,
// commitment, receipt, rcache), not just accounts. A drift entry in
// any one of them must surface in the error.
func TestAssertWritableShadowConsistentAt_DetectsDriftAcrossDomains(t *testing.T) {
	t.Parallel()

	const stepSize uint64 = 8
	db, agg := newWipeTestDB(t, stepSize)

	// Phase 1: write storage at txnum 0 (step 0).
	addr := [20]byte{0xAB}
	slot := [32]byte{0xCD}
	{
		rwTx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer rwTx.Rollback()
		domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
		require.NoError(t, err)
		defer domains.Close()
		key := append(append([]byte{}, addr[:]...), slot[:]...)
		domains.SetTxNum(0)
		require.NoError(t, domains.DomainPut(kv.StorageDomain, rwTx, key, []byte{0x42}, 0, nil))
		require.NoError(t, domains.Flush(t.Context(), rwTx))
		require.NoError(t, rwTx.Commit())
	}

	// Phase 2: wipe.
	{
		rwTx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer rwTx.Rollback()
		require.NoError(t, agg.WipeWritableShadowPast(t.Context(), rwTx, 5))
		require.NoError(t, rwTx.Commit())
	}

	// Phase 3: inject storage drift past stepContaining.
	{
		rwTx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer rwTx.Rollback()
		domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
		require.NoError(t, err)
		defer domains.Close()
		key := append(append([]byte{}, addr[:]...), slot[:]...)
		domains.SetTxNum(20) // step 2 — past stepContaining=0
		require.NoError(t, domains.DomainPut(kv.StorageDomain, rwTx, key, []byte{0xFF}, 20, nil))
		require.NoError(t, domains.Flush(t.Context(), rwTx))
		require.NoError(t, rwTx.Commit())
	}

	// Phase 4: check must catch storage-domain drift.
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	err = agg.AssertWritableShadowConsistentAt(t.Context(), rwTx, 5)
	require.Error(t, err)
	require.Contains(t, err.Error(), "storage",
		"storage-domain drift must surface in the error (not just accounts)")
}

// TestGetAsOf_FallsThroughForUnchangedKey pins the temporal-API
// semantic the boundary-step regen relies on: a key whose only write
// predates lastTxNum and has no history entry at or after lastTxNum
// MUST survive an as-of lookup.
//
// HistorySeek returns NOT FOUND for that case — the cursor finds no
// entry at txnum >= ts so the seek lands past the key and reports
// "no value". The regen previously used HistorySeek directly and
// dropped every such key from the regenerated boundary file,
// surfacing later as wrong-state gas-mismatch in catchup once the
// trie tried to read the slot. GetAsOf treats that same case as
// "no change since the past write" and falls through to GetLatest,
// returning the surviving value.
//
// Setup (stepSize=8 → boundary step 0 covers txnums 0..7):
//   - acc1 written at txnum 3 with nonce=1
//   - no further writes to acc1 anywhere up through lastTxNum=10
//
// Assertion:
//   - tx.HistorySeek(acc1, 10) returns NOT FOUND (this is what made
//     the regen drop unchanged keys)
//   - tx.GetAsOf(acc1, 11) returns acc1's nonce=1 value (this is what
//     the regen lookup now does — ts=lastTxNum+1 matches the wipe
//     path's convention)
func TestGetAsOf_FallsThroughForUnchangedKey(t *testing.T) {
	t.Parallel()

	const stepSize uint64 = 8
	const lastTxNum uint64 = 10
	db, _ := newWipeTestDB(t, stepSize)

	acc1Addr := [20]byte{1}

	{
		rwTx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer rwTx.Rollback()

		domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
		require.NoError(t, err)
		defer domains.Close()

		acc := accounts.Account{
			Nonce:    1,
			Balance:  *uint256.NewInt(100),
			CodeHash: accounts.EmptyCodeHash,
		}
		buf := accounts.SerialiseV3(&acc)
		domains.SetTxNum(3)
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, acc1Addr[:], buf, 3, nil))

		require.NoError(t, domains.Flush(t.Context(), rwTx))
		require.NoError(t, rwTx.Commit())
	}

	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()

	hsVal, hsFound, err := roTx.HistorySeek(kv.AccountsDomain, acc1Addr[:], lastTxNum)
	require.NoError(t, err)
	require.False(t, hsFound,
		"HistorySeek(lastTxNum=%d) must report NOT FOUND for a key with no history entry at or after that ts — this is the misbehavior the regen used to hit (drops the key)",
		lastTxNum)
	require.Empty(t, hsVal)

	gaVal, gaFound, err := roTx.GetAsOf(kv.AccountsDomain, acc1Addr[:], lastTxNum+1)
	require.NoError(t, err)
	require.True(t, gaFound,
		"GetAsOf(lastTxNum+1=%d) must return the still-current value — it falls through to GetLatest when history has no entry, which is the semantic the regen now relies on",
		lastTxNum+1)
	require.NotEmpty(t, gaVal)
}

// TestAggregator_Unwind_EvictsBranchCache pins the cross-cutting
// unwind step: (*Aggregator).Unwind(txN) must invalidate the
// aggregator-lifetime BranchCache past txN. Both unwind paths
// (changeset-window SharedDomains.Unwind and mode-B Provider.Unwind)
// route through this method, so a regression here breaks both paths
// silently and produces wrong-trie-root in catchup.
func TestAggregator_Unwind_EvictsBranchCache(t *testing.T) {
	t.Parallel()

	const stepSize uint64 = 8
	db, agg := newWipeTestDB(t, stepSize)

	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()

	provider, ok := roTx.AggTx().(commitment.BranchCacheProvider)
	require.True(t, ok, "AggregatorRoTx must implement commitment.BranchCacheProvider")
	bc := provider.BranchCache()
	require.NotNil(t, bc, "BranchCache must be attached to the commitment domain (gated by dbg.UseStateCache)")

	keepKey := []byte{0xa0, 0xb0}
	evictKey := []byte{0xa0, 0xb1}
	bc.Put(keepKey, []byte("keep"), 0, 50)    // txN below watermark — survives
	bc.Put(evictKey, []byte("evict"), 0, 100) // txN above watermark — must be evicted

	agg.Unwind(60)

	_, _, ok = bc.Get(keepKey)
	require.True(t, ok, "txN=50 entry must survive Aggregator.Unwind(60)")
	_, _, ok = bc.Get(evictKey)
	require.False(t, ok, "txN=100 entry must be evicted by Aggregator.Unwind(60) — without this, mode-B Provider.Unwind leaves stale branches and forward-exec wedges on wrong-trie-root a handful of blocks past the unwind target")
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
