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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state"
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

// TestRecomputeAtTxNumWithoutSD_MidBlockCS reproduces the hoodi
// production scenario where a single block spans multiple step
// boundaries — so the in-file cs (written at a mid-step CC trigger)
// is a MID-BLOCK partial-execution snapshot, not a block-end state.
// Mode B's recompute must reproduce block-end stateRoot starting
// from this mid-block cs baseline + the partial-block tail's history
// touches.
//
// Existing NonAligned cases all have cs at block-end (their fixtures
// map blockNum = txN/stepSize, so every step boundary IS also a
// block boundary). That coverage gap is what this test fills.
//
// Independent ground truth: two parallel DBs writing the same
// sequence.
//   - dbA: writes with a mid-step CC at the file-end step boundary
//     (analog of rw_v3.go:388's (txN+1)%stepSize==0 trigger fires
//     INSIDE the block). The file ends up with a mid-block cs as its
//     latest CommitmentDomain record. This is the input fixture
//     for RecomputeAtTxNumWithoutSD.
//   - dbB: same writes, NO mid-step CC — only a block-end CC. The
//     block-end CC's root is the canonical reference (R_canonical).
//
// Forward exec without mid-block CC (dbB) and with mid-block CC
// (dbA's block-end CC, if we ran it) MUST produce the same root —
// production correctness depends on this. The bug under test is
// whether the SD-LESS RECOMPUTE PRIMITIVE on dbA reproduces
// R_canonical when fed mid-block cs as baseline.
func TestRecomputeAtTxNumWithoutSD_MidBlockCS(t *testing.T) {
	t.Parallel()
	const (
		stepSize uint64 = 8
		// One "block" spans txN [0, 15] — TWO full steps. Mid-step CCs
		// at txN=7 and txN=15 are both INSIDE block 0.
		blockEndTxN uint64  = 15
		fileEndTxN  uint64  = 16 // BuildFiles boundary — covers steps 0 and 1
		toTxNum     uint64  = blockEndTxN
		maxStep     kv.Step = 2 // (15+1)/8 = 2 → covers files with endStep <= 2
		// numAccounts/numStorage chosen LARGER than the simple cases to
		// give the trie real branching structure (more like hoodi's
		// 85 acct + 96 storage touches).
		numAccounts = 64
		numStorage  = 64
	)

	ctx := t.Context()

	accountKeys := make([][]byte, numAccounts)
	for i := 0; i < numAccounts; i++ {
		accountKeys[i] = makeTestAccountAddr(uint64(i))
	}
	storageKeys := make([][]byte, numStorage)
	for i := 0; i < numStorage; i++ {
		storageKeys[i] = makeTestStorageKey(uint64(i), 1)
	}

	writeAt := func(domains *execctx.SharedDomains, rwTx kv.TemporalRwTx, txNum uint64) {
		t.Helper()
		for i := 0; i < numAccounts; i++ {
			acc := accounts.Account{
				Nonce:    txNum,
				Balance:  *uint256.NewInt(txNum * 1000),
				CodeHash: accounts.EmptyCodeHash,
			}
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, accountKeys[i], accounts.SerialiseV3(&acc), txNum, nil))
		}
		for i := 0; i < numStorage; i++ {
			var val [32]byte
			val[31] = byte(txNum + 1)
			require.NoError(t, domains.DomainPut(kv.StorageDomain, rwTx, storageKeys[i], val[:], txNum, nil))
		}
	}

	// build returns the block-end commitment root computed by SD's
	// ComputeCommitment after the supplied write/CC sequence. The
	// caller controls whether mid-step CCs fire.
	build := func(t *testing.T, midStepCC bool) ([]byte, *kvAggHandles) {
		t.Helper()
		db, agg := testDbAndAggregatorv3(t, stepSize)
		agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, false)

		var blockEndRoot []byte
		{
			rwTx, err := db.BeginTemporalRw(ctx)
			require.NoError(t, err)
			defer rwTx.Rollback()

			domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
			require.NoError(t, err)
			defer domains.Close()

			for txNum := uint64(0); txNum <= blockEndTxN; txNum++ {
				writeAt(domains, rwTx, txNum)
				// Mid-step CC at every step boundary EXCEPT the block
				// end itself. All CCs use blockNum=0 since the entire
				// run is still inside block 0.
				if midStepCC && (txNum+1)%stepSize == 0 && txNum != blockEndTxN {
					_, err := domains.ComputeCommitment(ctx, rwTx, true, 0, txNum, "", nil)
					require.NoError(t, err)
					require.NoError(t, domains.Flush(ctx, rwTx))
				}
			}
			// Block-end CC — canonical root regardless of whether
			// mid-step CCs fired.
			rh, err := domains.ComputeCommitment(ctx, rwTx, true, 0, blockEndTxN, "", nil)
			require.NoError(t, err)
			blockEndRoot = append([]byte(nil), rh...)
			require.NoError(t, rawdbv3.TxNums.Append(rwTx, 0, blockEndTxN))
			require.NoError(t, domains.Flush(ctx, rwTx))
			domains.Close()
			require.NoError(t, rwTx.Commit())
		}
		require.NoError(t, agg.BuildFiles(fileEndTxN))
		return blockEndRoot, &kvAggHandles{db: db, agg: agg}
	}

	// dbB: ground-truth path — block-end CC only, no mid-step CC.
	canonicalRoot, _ := build(t, false /* midStepCC */)
	require.NotEmpty(t, canonicalRoot)

	// dbA: production-like — mid-step CC at txN=7 (still inside block 0)
	// AND a block-end CC at txN=15. The block-end CC anchors block 0's
	// canonical root in the writable shadow / commitment file; the mid-
	// step CC is the partial-block snapshot that mode B's recompute
	// would later load if the block-end record were unavailable.
	dbARoot, handlesA := build(t, true /* midStepCC */)
	require.Equal(t, canonicalRoot, dbARoot,
		"sanity: forward exec must produce the same block-end root regardless of mid-step CCs")

	// Now exercise the recompute primitive on dbA. To force it to read
	// the MID-BLOCK cs (not the block-end cs that build() also wrote),
	// we wipe the writable shadow's commitment records past the mid-
	// step boundary. After this, the only KeyCommitmentState visible
	// is the mid-block cs collated into the file at endStep=2.
	//
	// This mirrors mode B's setup: after admin SetHead unwinds past
	// the block-end's step, the block-end cs is wiped; only the
	// in-file mid-step cs remains as baseline.
	wipeShadowPastMidStep := func(t *testing.T, db kv.TemporalRwDB) {
		t.Helper()
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()
		// Delete every KeyCommitmentState dup at step > 1 (the mid-step
		// boundary is at step 1: (7+1)/stepSize=1). The block-end CC
		// at txN=15 wrote a dup at step 1 too (15/8=1) — so we need
		// finer-grained wiping. Simpler: delete every dup whose value
		// step prefix decodes to step > 1 OR whose tx-encoded txNum > 7.
		c, err := rwTx.RwCursorDupSort("CommitmentVals")
		require.NoError(t, err)
		defer c.Close()
		_, v, err := c.SeekExact(commitmentdb.KeyCommitmentState)
		require.NoError(t, err)
		for v != nil {
			// CommitmentVals dup layout: 8B inv-step prefix + raw cs bytes.
			// Decode inv-step; if step > 1 — we want to KEEP it (the
			// mid-step cs lives at step 1). But we actually want to keep
			// only the smaller-txnum cs at step 1, deleting the larger.
			// Simpler approach: decode raw cs bytes, drop if cs.txNum > 7.
			if len(v) >= 16 {
				txN, _ := commitmentdb.DecodeTxBlockNums(v[8:])
				if txN > 7 {
					require.NoError(t, c.DeleteCurrent())
				}
			}
			_, nv, err := c.NextDup()
			require.NoError(t, err)
			v = nv
		}
		require.NoError(t, rwTx.Commit())
	}
	wipeShadowPastMidStep(t, handlesA.db)

	// Run the recompute against the mid-block cs baseline.
	gotRoot, baselineTxNum := func() ([]byte, uint64) {
		roTx, err := handlesA.db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		root, _, btx, branches, err := commitmentdb.RecomputeAtTxNumWithoutSD(ctx, roTx, t.TempDir(), toTxNum, maxStep, stepSize)
		require.NoError(t, err, "RecomputeAtTxNumWithoutSD failed")
		if branches != nil {
			branches.Close()
		}
		return append([]byte(nil), root...), btx
	}()

	t.Logf("MidBlockCS: canonical=%x recompute=%x baselineTxNum=%d toTxNum=%d", canonicalRoot, gotRoot, baselineTxNum, toTxNum)
	require.Equal(t, uint64(7), baselineTxNum, "recompute must pick the mid-block cs (txN=7), not the block-end cs (txN=15)")
	require.Equal(t, canonicalRoot, gotRoot,
		"mid-block-cs recompute root must match canonical forward-exec root")
}

// kvAggHandles bundles the test DB + aggregator so build() can return
// both for the caller's use.
type kvAggHandles struct {
	db  kv.TemporalRwDB
	agg *state.Aggregator
}

// TestRecomputeAtTxNumWithoutSD_MidBlockCS_HoodiPatterns extends the
// basic MidBlockCS test with patterns observed in the hoodi production
// failure but absent from the simpler tests:
//
//   - Pre-existing untouched keys: a large set of accounts/storage
//     written in step 0 and NEVER touched again. They sit in the
//     baseline trie with leaf hashes from step 0; they must remain
//     correct through the mid-block-cs recompute without re-touching.
//   - DELETEs: storage slots written then later set to all-zeros
//     within the partial-block tail (analog to hoodi's consolidation-
//     contract slot 0 = 0).
//   - Contracts: accounts with non-empty CodeHash + Incarnation > 0
//     (analog to hoodi sys-contract accounts).
//
// If this test fails with the same wrong-root pattern as hoodi while
// the simpler MidBlockCS passes, the bug is in one of these specific
// patterns.
func TestRecomputeAtTxNumWithoutSD_MidBlockCS_HoodiPatterns(t *testing.T) {
	t.Parallel()
	const (
		stepSize uint64 = 8
		// Multi-step pre-existing data:
		//   Step 0 [0, 7]:  preExistingTxNums — write many untouched keys
		//   Step 1 [8, 15]: block 0 first half (mid-block CC at txN=15)
		//   Step 2 [16, 23]: block 0 second half + block-end at txN=23
		// So block 0 spans txN [8, 23] and mid-step CC at txN=15 is
		// MID-BLOCK. Files cover up to step 2.
		preExistingEndTxN uint64  = 7
		blockStartTxN     uint64  = 8
		midBlockCSTxN     uint64  = 15 // (txN+1)%8==0, inside block 0
		blockEndTxN       uint64  = 23
		fileEndTxN        uint64  = 24 // BuildFiles covers steps 0..2
		toTxNum           uint64  = blockEndTxN
		maxStep           kv.Step = 3 // (23+1)/8 = 3 → keeps files endStep <= 3
		// Larger account/storage sets to expand the trie's branching.
		numPreExistingAccts   = 256
		numPreExistingStorage = 256
		numBlockTouchedAccts  = 32
		numBlockTouchedStor   = 32
		numDeletedStorage     = 8 // subset of pre-existing storage that gets deleted
		numContracts          = 8 // contract accounts (non-zero codeHash + incarnation)
	)

	ctx := t.Context()

	// Pre-existing keys — written once at txN=0, never touched again.
	preAccts := make([][]byte, numPreExistingAccts)
	for i := range preAccts {
		preAccts[i] = makeTestAccountAddr(uint64(i + 10000)) // offset to avoid collisions
	}
	preStorage := make([][]byte, numPreExistingStorage)
	for i := range preStorage {
		preStorage[i] = makeTestStorageKey(uint64(i+10000), 1)
	}
	// Block-touched keys — written within block 0 (both halves).
	blockAccts := make([][]byte, numBlockTouchedAccts)
	for i := range blockAccts {
		blockAccts[i] = makeTestAccountAddr(uint64(i))
	}
	blockStor := make([][]byte, numBlockTouchedStor)
	for i := range blockStor {
		blockStor[i] = makeTestStorageKey(uint64(i), 1)
	}
	// Deleted storage — first numDeletedStorage of preStorage, deleted
	// in the partial-block tail at txN=20 (between mid-block CC and
	// block end).
	deletedStor := preStorage[:numDeletedStorage]
	// Contract code hash for first numContracts of blockAccts (the
	// loop checks i < numContracts directly).
	contractCodeHash := func() accounts.CodeHash {
		var h common.Hash
		for i := range h {
			h[i] = byte(0xAB ^ i)
		}
		return accounts.InternCodeHash(h)
	}()

	writePreExisting := func(domains *execctx.SharedDomains, rwTx kv.TemporalRwTx, txNum uint64) {
		t.Helper()
		for i, key := range preAccts {
			acc := accounts.Account{
				Nonce:    1,
				Balance:  *uint256.NewInt(uint64(i + 1)),
				CodeHash: accounts.EmptyCodeHash,
			}
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, key, accounts.SerialiseV3(&acc), txNum, nil))
		}
		for i, key := range preStorage {
			var val [32]byte
			val[31] = byte(i + 1)
			require.NoError(t, domains.DomainPut(kv.StorageDomain, rwTx, key, val[:], txNum, nil))
		}
	}
	writeBlockTouches := func(domains *execctx.SharedDomains, rwTx kv.TemporalRwTx, txNum uint64) {
		t.Helper()
		for i, key := range blockAccts {
			isContract := i < numContracts
			acc := accounts.Account{
				Nonce:   txNum,
				Balance: *uint256.NewInt(txNum * 1000),
			}
			if isContract {
				acc.CodeHash = contractCodeHash
				acc.Incarnation = 1
			} else {
				acc.CodeHash = accounts.EmptyCodeHash
			}
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, key, accounts.SerialiseV3(&acc), txNum, nil))
		}
		for i, key := range blockStor {
			var val [32]byte
			val[31] = byte(txNum + uint64(i))
			require.NoError(t, domains.DomainPut(kv.StorageDomain, rwTx, key, val[:], txNum, nil))
		}
	}
	deleteStorageInTail := func(domains *execctx.SharedDomains, rwTx kv.TemporalRwTx, txNum uint64) {
		t.Helper()
		for _, key := range deletedStor {
			require.NoError(t, domains.DomainDel(kv.StorageDomain, rwTx, key, txNum, nil))
		}
	}

	build := func(t *testing.T, midStepCC bool) ([]byte, *kvAggHandles) {
		t.Helper()
		db, agg := testDbAndAggregatorv3(t, stepSize)
		// Hoodi has ReplaceKeysInValues=true. Match production.
		agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, true)

		var blockEndRoot []byte
		{
			rwTx, err := db.BeginTemporalRw(ctx)
			require.NoError(t, err)
			defer rwTx.Rollback()

			domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
			require.NoError(t, err)
			defer domains.Close()

			// Pre-existing data: write all once at txN=0, then advance txN
			// to fill the step (writing nothing additional). At step-0
			// boundary (txN=7), call CC with blockNum=0 — but this CC
			// happens at a block boundary in our timeline (pre-existing
			// is "block -1" if you will; we treat it as setup).
			writePreExisting(domains, rwTx, 0)
			// CC at end of step 0 anchors pre-existing data in the
			// commitment file. Use blockNum=0 — TxNums entry says block
			// "pre-existing" ends at txN=7.
			_, err = domains.ComputeCommitment(ctx, rwTx, true, 0, preExistingEndTxN, "", nil)
			require.NoError(t, err)
			require.NoError(t, rawdbv3.TxNums.Append(rwTx, 0, preExistingEndTxN))
			require.NoError(t, domains.Flush(ctx, rwTx))

			// Block 0 first half: txN [8, 15]. All block-touched keys
			// written at every txN.
			for txN := blockStartTxN; txN <= midBlockCSTxN; txN++ {
				writeBlockTouches(domains, rwTx, txN)
			}
			// Mid-block CC at txN=15 if requested.
			if midStepCC {
				_, err = domains.ComputeCommitment(ctx, rwTx, true, 1, midBlockCSTxN, "", nil)
				require.NoError(t, err)
				require.NoError(t, domains.Flush(ctx, rwTx))
			}

			// Block 0 second half: txN [16, 23]. Block-touched writes +
			// DELETEs in the tail at txN=20.
			for txN := midBlockCSTxN + 1; txN <= blockEndTxN; txN++ {
				writeBlockTouches(domains, rwTx, txN)
				if txN == 20 {
					deleteStorageInTail(domains, rwTx, txN)
				}
			}
			// Block-end CC — canonical root.
			rh, err := domains.ComputeCommitment(ctx, rwTx, true, 1, blockEndTxN, "", nil)
			require.NoError(t, err)
			blockEndRoot = append([]byte(nil), rh...)
			require.NoError(t, rawdbv3.TxNums.Append(rwTx, 1, blockEndTxN))
			require.NoError(t, domains.Flush(ctx, rwTx))
			domains.Close()
			require.NoError(t, rwTx.Commit())
		}
		require.NoError(t, agg.BuildFiles(fileEndTxN))
		return blockEndRoot, &kvAggHandles{db: db, agg: agg}
	}

	canonicalRoot, _ := build(t, false /* midStepCC */)
	require.NotEmpty(t, canonicalRoot)

	dbARoot, handlesA := build(t, true /* midStepCC */)
	require.Equal(t, canonicalRoot, dbARoot,
		"sanity: forward exec must produce the same block-end root regardless of mid-step CCs")

	// Wipe block-end cs so recompute picks mid-block cs (txN=15).
	wipeShadowPastMidStep := func(t *testing.T, db kv.TemporalRwDB) {
		t.Helper()
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()
		c, err := rwTx.RwCursorDupSort("CommitmentVals")
		require.NoError(t, err)
		defer c.Close()
		_, v, err := c.SeekExact(commitmentdb.KeyCommitmentState)
		require.NoError(t, err)
		for v != nil {
			if len(v) >= 16 {
				txN, _ := commitmentdb.DecodeTxBlockNums(v[8:])
				if txN > midBlockCSTxN {
					require.NoError(t, c.DeleteCurrent())
				}
			}
			_, nv, err := c.NextDup()
			require.NoError(t, err)
			v = nv
		}
		require.NoError(t, rwTx.Commit())
	}
	wipeShadowPastMidStep(t, handlesA.db)

	gotRoot, baselineTxNum := func() ([]byte, uint64) {
		roTx, err := handlesA.db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		root, _, btx, branches, err := commitmentdb.RecomputeAtTxNumWithoutSD(ctx, roTx, t.TempDir(), toTxNum, maxStep, stepSize)
		require.NoError(t, err, "RecomputeAtTxNumWithoutSD failed")
		if branches != nil {
			branches.Close()
		}
		return append([]byte(nil), root...), btx
	}()

	t.Logf("HoodiPatterns: canonical=%x recompute=%x baselineTxNum=%d toTxNum=%d", canonicalRoot, gotRoot, baselineTxNum, toTxNum)
	require.Equal(t, uint64(midBlockCSTxN), baselineTxNum, "recompute must pick mid-block cs")
	require.Equal(t, canonicalRoot, gotRoot,
		"mid-block-cs recompute root must match canonical block-end root with hoodi-like patterns (pre-existing untouched + DELETEs + contracts)")
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
		root, _, btx, branches, err := commitmentdb.RecomputeAtTxNumWithoutSD(ctx, roTx, t.TempDir(), toTxNum, maxStep, stepSize)
		require.NoError(t, err)
		if branches != nil {
			branches.Close()
		}
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

		root, _, btx, branches, err := commitmentdb.RecomputeAtTxNumWithoutSD(ctx, roTx, t.TempDir(), tc.ToTxNum, tc.MaxStepForReco, tc.StepSize)
		require.NoError(t, err, "RecomputeAtTxNumWithoutSD failed")
		if branches != nil {
			branches.Close()
		}
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
