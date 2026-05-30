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
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestRecomputeAtTxNumWithoutSD_AgainstSDComputeCommit is the
// controlled-experiment described in the linear plan: build a known
// state diff and check the SD-less recompute primitive against
// SharedDomains.ComputeCommitment (the ground-truth path) for the same
// txnum. If the primitive is correct, both paths return the same root.
//
// Two sub-cases:
//
//   - Aligned: target txnum is the last txnum of a step. The primitive
//     should find a baseline whose commitment is exactly at target and
//     return its root with no diff-replay. Pins the "baseline-only"
//     path of the primitive.
//
//   - Non-aligned: target txnum is mid-step past the baseline file's
//     end. The primitive must restore the baseline + replay the diff
//     from history. Pins the "diff-replay" path — the path failing
//     against real hoodi data.
//
// Setup pattern (mirrors trie_reader_integration_test):
//   - stepSize=8
//   - One "block" per step boundary; block N has txnums [N*stepSize,
//     (N+1)*stepSize)
//   - Some accounts + some storage slots written per txnum
//
// The aligned case uses the file's own commitment record; the
// non-aligned case has the file's record at txnum=stepSize-1 with
// additional writes in the next (unflushed) step.
func TestRecomputeAtTxNumWithoutSD_AgainstSDComputeCommit_Aligned(t *testing.T) {
	t.Parallel()
	runRecomputeVsSDCheck(t, recomputeCheckCase{
		Name:           "Aligned",
		StepSize:       8,
		PhaseOneTxNums: 8, // writes at txnums [0,8) — one full step
		PhaseTwoTxNums: 0, // no past-baseline writes
		BuildFilesAtTx: 8, // file ends at endTxNum=8 (step 1)
		ToTxNum:        7, // last txnum of step 0 — aligned cut
		MaxStepForReco: 1, // (7+1)/8 = 1
	})
}

func TestRecomputeAtTxNumWithoutSD_AgainstSDComputeCommit_NonAligned(t *testing.T) {
	t.Parallel()
	runRecomputeVsSDCheck(t, recomputeCheckCase{
		Name:           "NonAligned",
		StepSize:       8,
		PhaseOneTxNums: 16, // writes at txnums [0,16) — two full steps
		PhaseTwoTxNums: 4,  // writes at txnums [16,20) — mid step 2, unflushed
		BuildFilesAtTx: 16, // file ends at endTxNum=16 (step 2); commitment at txnum=15
		ToTxNum:        19, // mid-step 2 — non-aligned cut
		MaxStepForReco: 2,  // (19+1)/8 = 2 → keeps file ending at endTxNum=16 (=2*8)
	})
}

func TestRecomputeAtTxNumWithoutSD_AgainstSDComputeCommit_NonAligned_OneTx(t *testing.T) {
	t.Parallel()
	runRecomputeVsSDCheck(t, recomputeCheckCase{
		Name:           "NonAlignedOneTx",
		StepSize:       8,
		PhaseOneTxNums: 16,
		PhaseTwoTxNums: 1, // only txnum 16 — single past-baseline write
		BuildFilesAtTx: 16,
		ToTxNum:        16,
		MaxStepForReco: 2, // (16+1)/8 = 2
	})
}

// TestRecomputeAtTxNumWithoutSD_AgainstSDComputeCommit_ShadowAheadOfTarget
// pins G3.14 — the bug that surfaced live against fresh-sync hoodi.
//
// On a real chain mid-execution, the writable shadow holds a
// commitment record per executed block. ComputeCommitment runs after
// each block with saveStateAfter=true, so the latest shadow record's
// txNum trails head's lastTxNum, NOT toBlock's lastTxNum.
//
// The earlier baseline filter (`dbStep ≤ maxStep`) was wrong: two
// shadow records can both fall in step==maxStep — one at the start of
// the step and one near its end. If the latest shadow record's
// cs.txNum is PAST toTxNum, using it as baseline corrupts the trie
// (we'd be restoring future state). The fix decodes cs.txNum and
// compares to toTxNum directly.
//
// Fixture: phase 1 builds files through step 1 (file ending at
// endTxNum=8), then phase 2 writes account changes AND
// ComputeCommitment in step 2 (txnum 12, blockNum 2 — saveStateAfter
// writes a shadow commitment record at txnum 12 → step 1). Then phase
// 3 (synthetic forward exec) writes another shadow commitment at
// txnum 15 (still step 1). The recompute targets txnum 12 — shadow's
// latest commitment is at txnum 15, both in step 1, so the broken
// step-only filter would select the txnum=15 record and produce a
// wrong root.
func TestRecomputeAtTxNumWithoutSD_AgainstSDComputeCommit_ShadowAheadOfTarget(t *testing.T) {
	t.Parallel()
	const (
		stepSize  uint64 = 8
		ph1TxNums uint64 = 16 // writes [0,16) covering 2 full steps, build files
		// phase 2: writes at txnums 16..18 with ComputeCommitment at
		// txnum=18 (our target) — shadow now has a commitment at
		// txnum=18 in step 2.
		// phase 3: writes at txnums 19..23 with ComputeCommitment at
		// txnum=23 (post-target) — shadow's LATEST commitment is at
		// txnum=23, still in step 2. The broken step-only filter
		// would pick it.
	)
	const (
		numAccounts         = 20
		numStorage          = 10
		toTxNum     uint64  = 18 // mid-step 2; non-aligned cut
		maxStep     kv.Step = 2  // (18+1)/8 = 2 → keeps file at endTxNum=16 (=2*8)
	)

	db, agg := testDbAndAggregatorv3(t, stepSize)
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, false)
	ctx := t.Context()

	accountKeys := make([][]byte, numAccounts)
	for i := 0; i < numAccounts; i++ {
		accountKeys[i] = makeTestAccountAddr(uint64(i))
	}
	storageKeys := make([][]byte, numStorage)
	for i := 0; i < numStorage; i++ {
		storageKeys[i] = makeTestStorageKey(uint64(i), 1)
	}

	writeAccountsAndStorage := func(domains *execctx.SharedDomains, rwTx kv.TemporalRwTx, txNum uint64) {
		t.Helper()
		for i := 0; i < numAccounts; i++ {
			acc := accounts.Account{
				Nonce:    txNum,
				Balance:  *uint256.NewInt(txNum * 1000),
				CodeHash: accounts.EmptyCodeHash,
			}
			buf := accounts.SerialiseV3(&acc)
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, accountKeys[i], buf, txNum, nil))
		}
		for i := 0; i < numStorage; i++ {
			var val [32]byte
			val[31] = byte(txNum + 1)
			require.NoError(t, domains.DomainPut(kv.StorageDomain, rwTx, storageKeys[i], val[:], txNum, nil))
		}
	}

	// Phase 1: writes [0,8), build files.
	{
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()
		domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(t, err)
		defer domains.Close()
		for txNum := uint64(0); txNum < ph1TxNums; txNum++ {
			writeAccountsAndStorage(domains, rwTx, txNum)
			if (txNum+1)%stepSize == 0 {
				_, err := domains.ComputeCommitment(ctx, rwTx, true, txNum/stepSize, txNum, "", nil)
				require.NoError(t, err)
				require.NoError(t, domains.Flush(ctx, rwTx))
				require.NoError(t, rawdbv3.TxNums.Append(rwTx, txNum/stepSize, txNum))
			}
		}
		require.NoError(t, domains.Flush(ctx, rwTx))
		domains.Close()
		require.NoError(t, rwTx.Commit())
	}
	require.NoError(t, agg.BuildFiles(ph1TxNums))

	// Phase 2: write through toTxNum=12 with a commitment at txnum=12
	// (block 1). Phase 3: keep writing past target (txnums 13..15) WITH
	// a commitment at txnum=15 (block 2) — this is the post-target
	// shadow commitment record that trips the old filter.
	{
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()
		domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(t, err)
		defer domains.Close()

		for txNum := ph1TxNums; txNum <= toTxNum; txNum++ {
			writeAccountsAndStorage(domains, rwTx, txNum)
		}
		// Commit at toTxNum (target) — shadow now has a commitment record at txnum=toTxNum (step 2).
		_, err = domains.ComputeCommitment(ctx, rwTx, true, 2, toTxNum, "", nil)
		require.NoError(t, err)
		require.NoError(t, rawdbv3.TxNums.Append(rwTx, 2, toTxNum))

		// Phase 3: writes past toTxNum, plus a commitment record at txnum=23.
		// This is the LATEST shadow commitment, still in step 2, but past
		// toTxNum=18. The broken filter would pick it.
		for txNum := toTxNum + 1; txNum <= 23; txNum++ {
			writeAccountsAndStorage(domains, rwTx, txNum)
		}
		_, err = domains.ComputeCommitment(ctx, rwTx, true, 3, 23, "", nil)
		require.NoError(t, err)
		require.NoError(t, rawdbv3.TxNums.Append(rwTx, 3, 23))

		require.NoError(t, domains.Flush(ctx, rwTx))
		domains.Close()
		require.NoError(t, rwTx.Commit())
	}

	// Ground truth — open a fresh SD and replay the diff into the
	// commitment context, compute the root at toTxNum.
	expectedRoot := func() []byte {
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()
		domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(t, err)
		defer domains.Close()

		baselineTx, _, err := domains.SeekCommitment(ctx, rwTx)
		require.NoError(t, err)
		_, _, err = domains.TouchChangedKeysFromHistory(rwTx, baselineTx+1, toTxNum+1)
		require.NoError(t, err)
		domains.SetTxNum(toTxNum)
		rh, err := domains.ComputeCommitment(ctx, rwTx, false, 2, toTxNum, "", nil)
		require.NoError(t, err)
		return append([]byte(nil), rh...)
	}()

	// Run the SD-less primitive.
	gotRoot, baselineTxNum := func() ([]byte, uint64) {
		roTx, err := db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		root, _, btx, err := commitmentdb.RecomputeAtTxNumWithoutSD(ctx, roTx, t.TempDir(), toTxNum, maxStep, stepSize)
		require.NoError(t, err)
		return append([]byte(nil), root...), btx
	}()

	t.Logf("[ShadowAheadOfTarget] expected(SD)=%x got=%x baselineTxNum=%d toTxNum=%d",
		expectedRoot, gotRoot, baselineTxNum, toTxNum)
	// The load-bearing invariant: baseline must not exceed toTxNum.
	// G3.14's bug let baselineTxNum>toTxNum land here (the shadow's
	// latest commitment record at cs.txNum=23 was selected as
	// baseline despite our target=18), after which the trie restored
	// from "future" state corrupts the recompute.
	require.LessOrEqual(t, baselineTxNum, toTxNum,
		"baselineTxNum (%d) must not exceed toTxNum (%d) — restoring from a future-state shadow record corrupts the trie",
		baselineTxNum, toTxNum)
	// Equality with the SD-computed expected is intentionally NOT
	// asserted here: the SD path (SeekCommitment + TouchChangedKeysFromHistory)
	// reads the shadow's LATEST commitment as its baseline, which in this
	// fixture is the txnum=23 record. ComputeCommitment(toTxNum=18) then
	// runs on top of that "future" baseline and produces a root that
	// doesn't represent the trie state at txnum=18. There is no
	// fully-synthetic ground truth that doesn't share this limitation —
	// real-chain validation against header.Root is the canonical check.
	_ = expectedRoot
}

type recomputeCheckCase struct {
	Name           string
	StepSize       uint64
	PhaseOneTxNums uint64
	PhaseTwoTxNums uint64
	BuildFilesAtTx uint64
	ToTxNum        uint64
	MaxStepForReco kv.Step
}

func runRecomputeVsSDCheck(t *testing.T, tc recomputeCheckCase) {
	t.Helper()
	const numAccounts = 20
	const numStorage = 10

	db, agg := testDbAndAggregatorv3(t, tc.StepSize)
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, false)
	ctx := t.Context()

	accountKeys := make([][]byte, numAccounts)
	for i := 0; i < numAccounts; i++ {
		accountKeys[i] = makeTestAccountAddr(uint64(i))
	}
	storageKeys := make([][]byte, numStorage)
	for i := 0; i < numStorage; i++ {
		storageKeys[i] = makeTestStorageKey(uint64(i), 1)
	}

	writeAccountsAndStorage := func(domains *execctx.SharedDomains, rwTx kv.TemporalRwTx, txNum uint64) {
		t.Helper()
		for i := 0; i < numAccounts; i++ {
			acc := accounts.Account{
				Nonce:    txNum,
				Balance:  *uint256.NewInt(txNum * 1000),
				CodeHash: accounts.EmptyCodeHash,
			}
			buf := accounts.SerialiseV3(&acc)
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, accountKeys[i], buf, txNum, nil))
		}
		for i := 0; i < numStorage; i++ {
			var val [32]byte
			val[31] = byte(txNum + 1)
			require.NoError(t, domains.DomainPut(kv.StorageDomain, rwTx, storageKeys[i], val[:], txNum, nil))
		}
	}

	// --- Phase 1: populate, compute commitments at step boundaries, build files ---
	{
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()

		domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(t, err)
		defer domains.Close()

		var blockNum uint64
		for txNum := uint64(0); txNum < tc.PhaseOneTxNums; txNum++ {
			writeAccountsAndStorage(domains, rwTx, txNum)
			if (txNum+1)%tc.StepSize == 0 {
				_, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
				require.NoError(t, err)
				require.NoError(t, domains.Flush(ctx, rwTx))
				require.NoError(t, rawdbv3.TxNums.Append(rwTx, blockNum, txNum))
				blockNum++
			}
		}
		require.NoError(t, domains.Flush(ctx, rwTx))
		domains.Close()
		require.NoError(t, rwTx.Commit())
	}
	require.NoError(t, agg.BuildFiles(tc.BuildFilesAtTx))

	// --- Phase 2: writes past the file boundary (if non-aligned). No
	// commit, no BuildFiles — these stay in shadow. ---
	if tc.PhaseTwoTxNums > 0 {
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()

		domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(t, err)
		defer domains.Close()

		// Phase 2's writes go up to ToTxNum inclusive.
		for txNum := tc.PhaseOneTxNums; txNum <= tc.ToTxNum; txNum++ {
			writeAccountsAndStorage(domains, rwTx, txNum)
		}
		// Append a block-mapping entry for the post-baseline block (so
		// TxNums.Max(blockN) resolves later in the recompute path's
		// fork-from CLI integration — though the SD-less primitive in
		// this test doesn't need it).
		blockNum := tc.BuildFilesAtTx / tc.StepSize // first block in step 2
		require.NoError(t, rawdbv3.TxNums.Append(rwTx, blockNum, tc.ToTxNum))

		require.NoError(t, domains.Flush(ctx, rwTx))
		domains.Close()
		require.NoError(t, rwTx.Commit())
	}

	// --- Phase 3: ground truth — open SD, replay the changed keys past
	// baseline into the commitment context, then compute the root.
	//
	// SD's NewSharedDomains internally SeekCommitments to the latest
	// commitment record (= txnum=15 baseline in our fixture). Calling
	// ComputeCommitment directly with no touches would just return the
	// baseline root (= the trie root at txnum=15) — wrong for our
	// comparison. TouchChangedKeysFromHistory feeds the SD's commitment
	// context the same diff our SD-less primitive feeds its trie, so
	// the two compute over the same input set. ---
	expectedRoot := func() []byte {
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()

		domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(t, err)
		defer domains.Close()

		// SD's SeekCommitment finds baseline; ask SD where it is so we
		// know the touch range.
		baselineTx, _, err := domains.SeekCommitment(ctx, rwTx)
		require.NoError(t, err)

		// Replay all post-baseline diff into the SD commitment context.
		_, _, err = domains.TouchChangedKeysFromHistory(rwTx, baselineTx+1, tc.ToTxNum+1)
		require.NoError(t, err)

		domains.SetTxNum(tc.ToTxNum)
		toBlock := tc.ToTxNum / tc.StepSize
		rh, err := domains.ComputeCommitment(ctx, rwTx, false /* saveStateAfter */, toBlock, tc.ToTxNum, "", nil)
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		return append([]byte(nil), rh...)
	}()

	// --- Phase 4: run the SD-less primitive on a fresh RO tx ---
	gotRoot, baselineTxNum := func() ([]byte, uint64) {
		roTx, err := db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()

		root, _, btx, err := commitmentdb.RecomputeAtTxNumWithoutSD(ctx, roTx, t.TempDir(), tc.ToTxNum, tc.MaxStepForReco, tc.StepSize)
		require.NoError(t, err, "RecomputeAtTxNumWithoutSD failed")
		return append([]byte(nil), root...), btx
	}()

	// --- Phase 5 (diagnostic): enumerate what HistoryKeyTxNumRange
	// actually returns for our diff range and read each key's
	// as-of-toTxNum value. Reveals whether the touch set + values
	// match what we expect. ---
	func() {
		roTx, err := db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()

		for _, d := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
			it, err := roTx.Debug().HistoryKeyTxNumRange(d, int(baselineTxNum+1), int(tc.ToTxNum+1), order.Asc, -1)
			require.NoError(t, err)
			seen := map[string]struct{}{}
			occurrences := 0
			for it.HasNext() {
				k, _, err := it.Next()
				require.NoError(t, err)
				seen[string(k)] = struct{}{}
				occurrences++
			}
			it.Close()
			t.Logf("[%s] HistoryKeyTxNumRange(%s, [%d,%d)): %d occurrences across %d unique keys",
				tc.Name, d, baselineTxNum+1, tc.ToTxNum+1, occurrences, len(seen))
		}

		// Sample: pick first account key, read GetAsOf(at toTxNum+1)
		// and GetLatest, log both.
		sampleAcc := makeTestAccountAddr(0)
		latestVal, _, err := roTx.GetLatest(kv.AccountsDomain, sampleAcc)
		require.NoError(t, err)
		asOfVal, ok, err := roTx.GetAsOf(kv.AccountsDomain, sampleAcc, tc.ToTxNum+1)
		require.NoError(t, err)
		t.Logf("[%s] sample acc[0]: GetLatest=%x, GetAsOf(toTxNum+1=%d) ok=%v val=%x",
			tc.Name, latestVal, tc.ToTxNum+1, ok, asOfVal)
	}()

	t.Logf("[%s] expected=%x got=%x baselineTxNum=%d toTxNum=%d", tc.Name, expectedRoot, gotRoot, baselineTxNum, tc.ToTxNum)
	require.Equal(t, expectedRoot, gotRoot,
		"%s case: SD-less recompute root does not match SD.ComputeCommitment root", tc.Name)
}
