// Copyright 2024 The Erigon Authors
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
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/types/accounts"
	accounts3 "github.com/erigontech/erigon/execution/types/accounts"
)

func NewTest(dirs datadir.Dirs) state.AggOpts { //nolint:gocritic
	return state.NewTest(dirs)
}

func newTestDb(tb testing.TB, stepSize uint64) kv.TemporalRwDB {
	tb.Helper()
	logger := log.New()
	dirs := datadir.New(tb.TempDir())
	db := mdbx.New(dbcfg.ChainDB, logger).InMem(tb, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	tb.Cleanup(db.Close)

	agg := NewTest(dirs).StepSize(stepSize).Logger(logger).MustOpen(tb.Context(), db)
	tb.Cleanup(agg.Close)
	err := agg.OpenFolder()
	require.NoError(tb, err)
	tdb, err := temporal.New(db, agg, nil)
	require.NoError(tb, err)
	return tdb
}

func composite(k, k2 []byte) []byte {
	return append(common.Copy(k), k2...)
}

func TestSharedDomain_Unwind(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	stepSize := uint64(100)
	db := newTestDb(t, stepSize)
	//db := wrapDbWithCtx(_db, agg)

	ctx := t.Context()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	stateChangeset := &changeset.StateChangeSet{}
	domains.SetChangesetAccumulator(stateChangeset)

	maxTx := stepSize
	hashes := make([][]byte, maxTx)
	count := 10

	err = rwTx.Commit()
	require.NoError(t, err)

Loop:
	rwTx, err = db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err = execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	i := 0
	k0 := make([]byte, length.Addr)
	commitStep := 3

	var blockNum uint64
	for ; i < int(maxTx); i++ {
		txNum := uint64(i)
		for accs := 0; accs < 256; accs++ {
			acc := accounts3.Account{
				Nonce:       txNum,
				Balance:     *uint256.NewInt(uint64(i*10e6) + uint64(accs*10e2)),
				CodeHash:    accounts.EmptyCodeHash,
				Incarnation: 0,
			}
			v := accounts3.SerialiseV3(&acc)
			k0[0] = byte(accs)
			pv, _, err := domains.GetLatest(kv.AccountsDomain, rwTx, k0)
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, rwTx, k0, v, uint64(i), pv)
			require.NoError(t, err)
		}

		if i%commitStep == 0 {
			rh, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
			require.NoError(t, err)
			if hashes[uint64(i)] != nil {
				require.Equal(t, hashes[uint64(i)], rh)
			}
			require.NotNil(t, rh)
			hashes[uint64(i)] = rh
		}
	}

	err = domains.Flush(ctx, rwTx)
	require.NoError(t, err)

	unwindTo := uint64(commitStep * rand.IntN(int(maxTx)/commitStep))
	//domains.currentChangesAccumulator = nil

	var a [kv.DomainLen][]kv.DomainEntryDiff
	for idx, d := range stateChangeset.Diffs {
		a[idx] = d.GetDiffSet()
	}
	err = rwTx.Unwind(ctx, unwindTo, &a)
	require.NoError(t, err)

	err = rwTx.Commit()
	require.NoError(t, err)
	if count > 0 {
		count--
	}
	domains.Close()
	if count == 0 {
		return
	}

	goto Loop
}

// TestSharedDomain_UnwindDoesNotRestoreOverlayForNewKey reproduces the
// mainnet block-24898955 gas-used mismatch (diff = -17100 = SSTORE_RESET -
// SSTORE_SET). After a forkchoice-driven unwind, the TemporalMemBatch overlay
// still holds storage writes made INSIDE the unwound txNum range, because:
//
//   - `Unwind` stores `sd.unwindToTxNum` and `sd.unwindChangeset` but does
//     NOT remove post-target entries from `sd.storage` / `sd.domains[...]`.
//   - `getLatest` returns `dataWithTxNums[len(...)-1]` without comparing
//     its txNum against `sd.unwindToTxNum`.
//   - The `unwindChangeset` fallback only fires on an overlay miss.
//
// So a first-time storage write at `txNum=T_write > T_unwind` remains visible
// after `Unwind(T_unwind)`, and a re-executed SSTORE on that slot charges
// SSTORE_RESET (2900) instead of SSTORE_SET (20000) — the observed 17100 diff.
func TestSharedDomain_UnwindDoesNotRestoreOverlayForNewKey(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	stepSize := uint64(100)
	db := newTestDb(t, stepSize)
	ctx := t.Context()

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	stateChangeset := &changeset.StateChangeSet{}
	domains.SetChangesetAccumulator(stateChangeset)

	// Storage key mirrors a USDT balance slot: 20-byte address + 32-byte slot hash.
	addr := common.HexToAddress("0xdac17f958d2ee523a2206206994597c13d831ec7")
	slot := common.HexToHash("0x5ac7102aad1a639901bc2657323aaed9e90e40c550747c49170f1c82fd664e4f")
	key := composite(addr[:], slot[:])
	value := []byte{0x01, 0x02, 0x03, 0x04}

	const unwindTarget uint64 = 50 // pre-write txNum where slot is absent
	const writeTxNum uint64 = 100  // txNum inside a block that will be unwound

	// 1. Precondition: slot is absent at the unwind target.
	domains.SetTxNum(unwindTarget)
	v, _, err := domains.GetLatest(kv.StorageDomain, rwTx, key)
	require.NoError(t, err)
	require.Empty(t, v, "precondition: slot must be absent before any writes")

	// 2. Advance txNum and write the slot for the first time (prevVal = nil).
	domains.SetTxNum(writeTxNum)
	require.NoError(t, domains.DomainPut(kv.StorageDomain, rwTx, key, value, writeTxNum, nil))

	// 3. Sanity: write is visible through the overlay.
	v, _, err = domains.GetLatest(kv.StorageDomain, rwTx, key)
	require.NoError(t, err)
	require.Equal(t, value, v, "sanity: write must be visible pre-unwind")

	// 4. Build the domain diff set from the accumulator, then unwind past the write.
	var diffSet [kv.DomainLen][]kv.DomainEntryDiff
	for idx, d := range stateChangeset.Diffs {
		diffSet[idx] = d.GetDiffSet()
	}
	domains.Unwind(unwindTarget, &diffSet)

	// 5. Simulate re-execution starting at the unwind target.
	domains.SetTxNum(unwindTarget)

	// 6. Failing assertion: the slot must be absent again.
	v, _, err = domains.GetLatest(kv.StorageDomain, rwTx, key)
	require.NoError(t, err)
	require.Empty(t, v,
		"after Unwind(txNum=%d), overlay must not return the write made at txNum=%d (got %x). "+
			"TemporalMemBatch.getLatest returns the latest entry from sd.storage without "+
			"consulting sd.unwindToTxNum, and the unwindChangeset fallback is unreachable "+
			"while the key remains in sd.storage.",
		unwindTarget, writeTxNum, v)
}

// TestNewSharedDomains_StateAheadOfBlocks verifies that when the persisted
// commitment state is ahead of the TxNums index (catch-up scenario),
// NewSharedDomains returns ErrBehindCommitment but the SharedDomains itself
// is fully initialized (txNum set, patricia trie restored). Catch-up handlers
// like ExecModule.InsertBlocks and forkchoice rely on receiving a usable SD
// alongside the signal error.
func TestNewSharedDomains_StateAheadOfBlocks(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	stepSize := uint64(8)
	require := require.New(t)
	db := newTestDb(t, stepSize)

	ctx := t.Context()

	// Phase 1: write some accounts, compute commitment, flush, and append TxNums up to block N.
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(err)
	defer rwTx.Rollback()

	const lastBlock = uint64(10)
	for blockNum := uint64(0); blockNum <= lastBlock; blockNum++ {
		maxTxNum := blockNum*2 + 1
		require.NoError(rawdbv3.TxNums.Append(rwTx, blockNum, maxTxNum))
	}

	doms, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(err)

	addr := make([]byte, length.Addr)
	for i := uint64(0); i < 4; i++ {
		addr[0] = byte(i)
		acc := accounts.Account{
			Nonce:   i,
			Balance: *uint256.NewInt(i + 1),
		}
		require.NoError(doms.DomainPut(kv.AccountsDomain, rwTx, addr, accounts3.SerialiseV3(&acc), uint64(i), nil))
	}
	commitTxNum := lastBlock*2 + 1
	_, err = doms.ComputeCommitment(ctx, rwTx, true, lastBlock, commitTxNum, "", nil)
	require.NoError(err)
	require.NoError(doms.Flush(ctx, rwTx))
	doms.Close()
	require.NoError(rwTx.Commit())

	// Phase 2: truncate TxNums so that lastBn < commitment block — this is the
	// "state ahead of blocks" condition.
	rwTx, err = db.BeginTemporalRw(ctx)
	require.NoError(err)
	defer rwTx.Rollback()
	require.NoError(rawdbv3.TxNums.Truncate(rwTx, lastBlock-3))
	lastBn, _, err := rawdbv3.TxNums.Last(rwTx)
	require.NoError(err)
	require.Less(lastBn, lastBlock, "TxNums must be behind commitment block for the test to be meaningful")

	// Phase 3: NewSharedDomains must return ErrBehindCommitment AND a fully-initialized SD.
	doms, err = execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.ErrorIs(err, commitmentdb.ErrBehindCommitment, "expected ErrBehindCommitment signal")
	require.NotNil(doms, "SD must be returned alongside the signal error")
	defer doms.Close()

	// SD is fully initialized: txNum set to commitment-state's txNum.
	require.Equal(commitTxNum, doms.TxNum(), "SD txNum must be set to the commitment state's txNum (full restore)")

	// Basic domain reads work — no panic, no nil deref.
	addr[0] = 0
	_, _, err = doms.GetLatest(kv.AccountsDomain, rwTx, addr)
	require.NoError(err)
}

// TestSharedDomain_RepeatedUnwindAcrossStepBoundary mimics the FCU-driven
// pattern that left a mainnet node with commitment one step ahead of all
// other domains: three successive unwinds, each followed by forward execution
// that re-advances past the step boundary.
//
// After the final unwind the commitment "state" key and the commitment values
// table must not contain entries at a step beyond the unwind target —
// otherwise the next `SeekCommitment` will return a blockNum > TxNums.Last()
// and execution will skip blocks.
func TestSharedDomain_RepeatedUnwindAcrossStepBoundary(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	stepSize := uint64(10)
	db := newTestDb(t, stepSize)
	ctx := t.Context()
	require := require.New(t)

	// 30 blocks covering 3 steps (0,1,2). Each FCU cycle will unwind back to
	// the middle of step 0, then re-execute forward, mirroring the ~54-block
	// unwinds we saw on mainnet.
	const lastBlock = uint64(29)
	const unwindTarget = uint64(4) // inside step 0

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(err)
	defer rwTx.Rollback()
	for bn := uint64(0); bn <= lastBlock; bn++ {
		require.NoError(rawdbv3.TxNums.Append(rwTx, bn, bn))
	}

	doms, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(err)
	defer doms.Close()
	addr := make([]byte, length.Addr)
	perBlockDiffs := make(map[uint64]*changeset.StateChangeSet)
	blockHashes := make(map[uint64]common.Hash)

	executeRange := func(from, to uint64) {
		for bn := from; bn <= to; bn++ {
			cs := &changeset.StateChangeSet{}
			doms.SetChangesetAccumulator(cs)
			for i := 0; i < 8; i++ {
				addr[0] = byte(i)
				addr[1] = byte(bn)
				acc := accounts3.Account{Nonce: bn, Balance: *uint256.NewInt(bn*1000 + uint64(i))}
				pv, _, err := doms.GetLatest(kv.AccountsDomain, rwTx, addr)
				require.NoError(err)
				require.NoError(doms.DomainPut(kv.AccountsDomain, rwTx, addr, accounts3.SerialiseV3(&acc), bn, pv))
			}
			rh, err := doms.ComputeCommitment(ctx, rwTx, true, bn, bn, "", nil)
			require.NoError(err)
			blockHashes[bn] = common.BytesToHash(rh)
			doms.SavePastChangesetAccumulator(blockHashes[bn], bn, cs)
			perBlockDiffs[bn] = cs
			doms.SetChangesetAccumulator(nil)
		}
	}

	unwindTo := func(target uint64, curBlock uint64) {
		var merged [kv.DomainLen][]kv.DomainEntryDiff
		for bn := curBlock; bn > target; bn-- {
			for idx, d := range perBlockDiffs[bn].Diffs {
				keys := d.GetDiffSet()
				if merged[idx] == nil {
					merged[idx] = keys
				} else {
					merged[idx] = changeset.MergeDiffSets(merged[idx], keys)
				}
			}
		}
		doms.Unwind(target, &merged)
		doms.SetTxNum(target)
	}

	// Cycle 1: execute 0..lastBlock, flush, unwind, flush.
	executeRange(0, lastBlock)
	require.NoError(doms.Flush(ctx, rwTx))
	unwindTo(unwindTarget, lastBlock)
	require.NoError(doms.Flush(ctx, rwTx))

	// Cycle 2: re-execute, unwind, flush.
	executeRange(unwindTarget+1, lastBlock)
	require.NoError(doms.Flush(ctx, rwTx))
	unwindTo(unwindTarget, lastBlock)
	require.NoError(doms.Flush(ctx, rwTx))

	// Cycle 3: re-execute, unwind, flush.
	executeRange(unwindTarget+1, lastBlock)
	require.NoError(doms.Flush(ctx, rwTx))
	unwindTo(unwindTarget, lastBlock)
	require.NoError(doms.Flush(ctx, rwTx))

	// Verify: commitment "state" key is at blockNum ≤ unwindTarget.
	stateVal, _, err := rwTx.GetLatest(kv.CommitmentDomain, commitmentdb.KeyCommitmentState)
	require.NoError(err)
	require.GreaterOrEqual(len(stateVal), 16, "commitment state record must exist post-unwind (was forward-populated by executeRange)")
	postBlock := binary.BigEndian.Uint64(stateVal[8:16])
	require.LessOrEqualf(postBlock, unwindTarget,
		"commitment state blockNum=%d must be ≤ unwindTarget=%d after repeated unwinds",
		postBlock, unwindTarget)
	// Verify: no commitment values table entries with step > unwindTarget/stepSize.
	maxStep := unwindTarget / stepSize
	c, err := rwTx.Cursor(kv.TblCommitmentVals)
	require.NoError(err)
	defer c.Close()
	offending := 0
	var exampleStep uint64
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		require.NoError(err)
		if len(v) < 8 {
			continue
		}
		step := ^binary.BigEndian.Uint64(v[:8])
		if step > maxStep {
			if offending == 0 {
				exampleStep = step
			}
			offending++
		}
	}
	require.Zerof(offending,
		"%d commitment values entries have step > %d after repeated unwinds (e.g. step %d); "+
			"these are the \"orphan\" entries that caused mainnet execution to start at stale commitment state",
		offending, maxStep, exampleStep)
}

// TestSharedDomain_UnwindAcrossStepBoundary reproduces the mainnet corruption
// where the commitment domain's persisted "state" key (and branch entries)
// stayed at a step beyond the unwind target after a forkchoice-driven unwind.
//
// Scenario: write accounts + commitment across two steps (blocks that span
// step boundary), flush, then unwind to a block inside the earlier step, flush.
// After unwind the commitment "state" key should decode to a blockNum ≤ the
// unwind target — otherwise execution will later start at commitment.blockNum+1
// against accounts that are still at the unwind-target's state, producing
// "nonce too high" errors.
func TestSharedDomain_UnwindAcrossStepBoundary(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	stepSize := uint64(10)
	db := newTestDb(t, stepSize)
	ctx := t.Context()
	require := require.New(t)

	// Phase 1: register txNums for blocks 0..14 (spans step 0 [0..9] and step 1 [10..19]).
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(err)
	defer rwTx.Rollback()
	const lastBlock = uint64(14)
	for bn := uint64(0); bn <= lastBlock; bn++ {
		// Each block gets 1 txNum for simplicity: maxTxNum(bn) = bn.
		require.NoError(rawdbv3.TxNums.Append(rwTx, bn, bn))
	}

	// Phase 2: forward execution of all 15 blocks — write accounts + commitment per block.
	doms, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(err)
	stateChangeset := &changeset.StateChangeSet{}
	doms.SetChangesetAccumulator(stateChangeset)

	var perBlockDiffs [lastBlock + 1]*changeset.StateChangeSet
	blockHashes := make([]common.Hash, lastBlock+1)
	addr := make([]byte, length.Addr)
	for bn := uint64(0); bn <= lastBlock; bn++ {
		cs := &changeset.StateChangeSet{}
		doms.SetChangesetAccumulator(cs)
		// Write a handful of account updates so commitment has real branches.
		for i := 0; i < 8; i++ {
			addr[0] = byte(i)
			addr[1] = byte(bn)
			acc := accounts3.Account{Nonce: bn, Balance: *uint256.NewInt(bn*1000 + uint64(i))}
			pv, _, err := doms.GetLatest(kv.AccountsDomain, rwTx, addr)
			require.NoError(err)
			require.NoError(doms.DomainPut(kv.AccountsDomain, rwTx, addr, accounts3.SerialiseV3(&acc), bn, pv))
		}
		rh, err := doms.ComputeCommitment(ctx, rwTx, true, bn, bn, "", nil)
		require.NoError(err)
		require.NotEmpty(rh)
		blockHashes[bn] = common.BytesToHash(rh)
		doms.SavePastChangesetAccumulator(blockHashes[bn], bn, cs)
		perBlockDiffs[bn] = cs
		doms.SetChangesetAccumulator(nil)
	}
	require.NoError(doms.Flush(ctx, rwTx))
	doms.Close()
	require.NoError(rwTx.Commit())

	// Phase 3: read the commitment "state" key — should decode to blockNum=lastBlock.
	rwTx, err = db.BeginTemporalRw(ctx)
	require.NoError(err)
	defer rwTx.Rollback()
	stateVal, _, err := rwTx.GetLatest(kv.CommitmentDomain, commitmentdb.KeyCommitmentState)
	require.NoError(err)
	require.GreaterOrEqual(len(stateVal), 16)
	commitTxNum := binary.BigEndian.Uint64(stateVal[:8])
	commitBlock := binary.BigEndian.Uint64(stateVal[8:16])
	require.Equal(lastBlock, commitBlock, "pre-unwind: commitment state should be at last block")
	require.Equal(lastBlock, commitTxNum, "pre-unwind: commitment state txNum")

	// Phase 4: unwind to block 4 (end of step 0). The per-block diffs for
	// blocks 5..14 are merged and passed to the domain-level Unwind.
	const unwindTarget = uint64(4)
	var merged [kv.DomainLen][]kv.DomainEntryDiff
	for bn := uint64(lastBlock); bn > unwindTarget; bn-- {
		for idx, d := range perBlockDiffs[bn].Diffs {
			currentKeys := d.GetDiffSet()
			if merged[idx] == nil {
				merged[idx] = currentKeys
			} else {
				merged[idx] = changeset.MergeDiffSets(merged[idx], currentKeys)
			}
		}
	}
	require.NoError(rwTx.Unwind(ctx, unwindTarget, &merged))

	// Phase 5: the commitment "state" key should now decode to blockNum ≤ unwindTarget.
	stateVal, _, err = rwTx.GetLatest(kv.CommitmentDomain, commitmentdb.KeyCommitmentState)
	require.NoError(err)
	require.GreaterOrEqual(len(stateVal), 16, "post-unwind: commitment state record must exist (was populated in phase 2)")
	postTxNum := binary.BigEndian.Uint64(stateVal[:8])
	postBlock := binary.BigEndian.Uint64(stateVal[8:16])
	require.LessOrEqualf(postBlock, unwindTarget,
		"post-unwind: commitment state blockNum=%d must be ≤ unwindTarget=%d (txNum=%d)",
		postBlock, unwindTarget, postTxNum)
	// Fix: also confirm no values table entries exist above the unwind-target step.
	maxStep := unwindTarget / stepSize // step 0 for target 4
	c, err := rwTx.Cursor(kv.TblCommitmentVals)
	require.NoError(err)
	defer c.Close()
	offending := 0
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		require.NoError(err)
		if len(v) < 8 {
			continue
		}
		step := ^binary.BigEndian.Uint64(v[:8])
		if step > maxStep {
			offending++
		}
	}
	require.Zerof(offending,
		"post-unwind: %d commitment values entries have step > %d (maxStep for unwindTarget=%d)",
		offending, maxStep, unwindTarget)
}

func TestSharedDomain_StorageIter(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	log.Root().SetHandler(log.LvlFilterHandler(log.LvlWarn, log.StderrHandler))

	stepSize := uint64(4)
	db := newTestDb(t, stepSize)
	//db := wrapDbWithCtx(_db, agg)

	ctx := t.Context()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	maxTx := 3*stepSize + 10
	hashes := make([][]byte, maxTx)

	i := 0
	k0 := make([]byte, length.Addr)
	l0 := make([]byte, length.Hash)
	commitStep := 3
	noaccounts := 1

	var blockNum uint64
	for ; i < int(maxTx); i++ {
		txNum := uint64(i)
		for accs := 0; accs < noaccounts; accs++ {
			acc := accounts3.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i*10e6) + uint64(accs*10e2)),
				CodeHash:    accounts.EmptyCodeHash,
				Incarnation: 0,
			}
			v := accounts3.SerialiseV3(&acc)
			k0[0] = byte(accs)

			pv, _, err := domains.GetLatest(kv.AccountsDomain, rwTx, k0)
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, rwTx, k0, v, txNum, pv)
			require.NoError(t, err)
			binary.BigEndian.PutUint64(l0[16:24], uint64(accs))

			for locs := 0; locs < 1000; locs++ {
				binary.BigEndian.PutUint64(l0[24:], uint64(locs))
				pv, _, err := domains.GetLatest(kv.AccountsDomain, rwTx, append(k0, l0...))
				require.NoError(t, err)

				err = domains.DomainPut(kv.StorageDomain, rwTx, composite(k0, l0), l0[24:], txNum, pv)
				require.NoError(t, err)
			}
		}

		if i%commitStep == 0 {
			rh, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
			require.NoError(t, err)
			if hashes[uint64(i)] != nil {
				require.Equal(t, hashes[uint64(i)], rh)
			}
			require.NotNil(t, rh)
			hashes[uint64(i)] = rh
		}

	}
	fmt.Printf("calling build files step %d\n", maxTx/stepSize)
	err = domains.Flush(ctx, rwTx)
	require.NoError(t, err)
	domains.Close()

	err = rwTx.Commit()
	require.NoError(t, err)

	err = db.(state.HasAgg).Agg().(*state.Aggregator).BuildFiles(maxTx - stepSize)
	require.NoError(t, err)

	{ //prune
		rwTx, err = db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()
		_, err = rwTx.PruneSmallBatches(ctx, 1*time.Minute)
		require.NoError(t, err)
		err = rwTx.Commit()
		require.NoError(t, err)
	}

	rwTx, err = db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	t.Cleanup(rwTx.Rollback)

	domains, err = execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	txNum, _, err := domains.SeekCommitment(ctx, rwTx)
	require.NoError(t, err)
	for accs := 0; accs < noaccounts; accs++ {
		k0[0] = byte(accs)
		pv, _, err := domains.GetLatest(kv.AccountsDomain, rwTx, k0)
		require.NoError(t, err)

		existed := make(map[string]struct{})
		err = domains.IteratePrefix(kv.StorageDomain, k0, rwTx, func(k []byte, v []byte) (bool, error) {
			existed[string(k)] = struct{}{}
			return true, nil
		})
		require.NoError(t, err)

		missed := 0
		err = domains.IteratePrefix(kv.StorageDomain, k0, rwTx, func(k []byte, v []byte) (bool, error) {
			if _, been := existed[string(k)]; !been {
				missed++
			}
			return true, nil
		})
		require.NoError(t, err)
		require.Zero(t, missed)

		err = domains.DomainDel(kv.AccountsDomain, rwTx, k0, txNum, pv)
		require.NoError(t, err)

		notRemoved := 0
		err = domains.IteratePrefix(kv.StorageDomain, k0, rwTx, func(k []byte, v []byte) (bool, error) {
			notRemoved++
			if _, been := existed[string(k)]; !been {
				missed++
			}
			return true, nil
		})
		require.NoError(t, err)
		require.Zero(t, missed)
		require.Zero(t, notRemoved)
	}

	err = domains.Flush(ctx, rwTx)
	require.NoError(t, err)
	rwTx.Rollback()
}

func TestSharedDomain_IteratePrefix(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	stepSize := uint64(8)
	require := require.New(t)
	db := newTestDb(t, stepSize)

	ctx := t.Context()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(err)
	defer rwTx.Rollback()

	iterCount := func(domains *execctx.SharedDomains) int {
		var list [][]byte
		require.NoError(domains.IteratePrefix(kv.StorageDomain, nil, rwTx, func(k []byte, v []byte) (bool, error) {
			list = append(list, k)
			return true, nil
		}))
		return len(list)
	}

	for i := uint64(0); i < stepSize*2; i++ {
		blockNum := i
		maxTxNum := blockNum*2 - 1
		err = rawdbv3.TxNums.Append(rwTx, blockNum, maxTxNum)
		require.NoError(err)
	}

	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(err)
	defer domains.Close()

	acc := func(i uint64) []byte {
		buf := make([]byte, 20)
		binary.BigEndian.PutUint64(buf[20-8:], i)
		return buf
	}
	st := func(i uint64) []byte {
		buf := make([]byte, 32)
		binary.BigEndian.PutUint64(buf[32-8:], i)
		return buf
	}
	addr := acc(1)
	for i := uint64(0); i < stepSize; i++ {
		txNum := i
		if err = domains.DomainPut(kv.AccountsDomain, rwTx, addr, acc(i), txNum, nil); err != nil {
			panic(err)
		}
		if err = domains.DomainPut(kv.StorageDomain, rwTx, composite(addr, st(i)), acc(i), txNum, nil); err != nil {
			panic(err)
		}
	}

	{ // no deletes
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains, err = execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize), iterCount(domains))
	}
	var txNum uint64
	{ // delete marker is in RAM
		require.NoError(domains.Flush(ctx, rwTx))
		domains.Close()
		domains, err = execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize), iterCount(domains))

		txNum = stepSize
		if err := domains.DomainDel(kv.StorageDomain, rwTx, append(addr, st(1)...), txNum, nil); err != nil {
			panic(err)
		}
		if err := domains.DomainDel(kv.StorageDomain, rwTx, append(addr, st(2)...), txNum, nil); err != nil {
			panic(err)
		}
		for i := stepSize; i < stepSize*2+2; i++ {
			txNum = i
			if err = domains.DomainPut(kv.AccountsDomain, rwTx, addr, acc(i), txNum, nil); err != nil {
				panic(err)
			}
			if err = domains.DomainPut(kv.StorageDomain, rwTx, composite(addr, st(i)), acc(i), txNum, nil); err != nil {
				panic(err)
			}
		}
		require.Equal(int(stepSize*2+2-2), iterCount(domains))
	}
	{ // delete marker is in DB
		_, err = domains.ComputeCommitment(ctx, rwTx, true, txNum/2, txNum, "", nil)
		require.NoError(err)
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains, err = execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize*2+2-2), iterCount(domains))
	}
	{ //delete marker is in Files
		domains.Close()
		err = rwTx.Commit() // otherwise agg.BuildFiles will not see data
		require.NoError(err)
		require.NoError(db.(state.HasAgg).Agg().(*state.Aggregator).BuildFiles(stepSize * 2))

		rwTx, err = db.BeginTemporalRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()

		ac := state.AggTx(rwTx)
		require.Equal(int(stepSize*2), int(ac.TxNumsInFiles(kv.StateDomains...)))

		_, err := ac.PruneSmallBatches(ctx, time.Hour, rwTx)
		require.NoError(err)

		domains, err = execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize*2+2-2), iterCount(domains))
	}

	{ // delete/update more keys in RAM
		require.NoError(domains.Flush(ctx, rwTx))
		domains.Close()
		domains, err = execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(err)
		defer domains.Close()

		txNum = stepSize*2 + 1
		if err := domains.DomainDel(kv.StorageDomain, rwTx, append(addr, st(4)...), txNum, nil); err != nil {
			panic(err)
		}
		if err := domains.DomainPut(kv.StorageDomain, rwTx, append(addr, st(5)...), acc(5), txNum, nil); err != nil {
			panic(err)
		}
		require.Equal(int(stepSize*2+2-3), iterCount(domains))
	}
	{ // flush delete/updates to DB
		_, err = domains.ComputeCommitment(ctx, rwTx, true, txNum/2, txNum, "", nil)
		require.NoError(err)
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains, err = execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		require.Equal(int(stepSize*2+2-3), iterCount(domains))
	}
	{ // delete everything - must see 0
		err = domains.Flush(ctx, rwTx)
		require.NoError(err)
		domains.Close()

		domains, err = execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(err)
		defer domains.Close()
		err := domains.DomainDelPrefix(kv.StorageDomain, rwTx, []byte{}, txNum+1)
		require.NoError(err)
		require.Equal(0, iterCount(domains))
	}
}

func TestSharedDomain_HasPrefix_StorageDomain(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	stepSize := uint64(1)
	db := newTestDb(t, stepSize)

	rwTtx1, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	t.Cleanup(rwTtx1.Rollback)
	sd, err := execctx.NewSharedDomains(ctx, rwTtx1, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	acc1 := common.HexToAddress("0x1234567890123456789012345678901234567890")
	acc1slot1 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	storageK1 := append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...)
	acc2 := common.HexToAddress("0x1234567890123456789012345678901234567891")
	acc2slot2 := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002")
	storageK2 := append(append([]byte{}, acc2.Bytes()...), acc2slot2.Bytes()...)

	// --- check 1: non-existing storage ---
	{
		firstKey, firstVal, ok, err := sd.HasPrefix(kv.StorageDomain, acc1.Bytes(), rwTtx1)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, firstKey)
		require.Nil(t, firstVal)
	}

	// --- check 2: storage exists in DB - SharedDomains.HasPrefix should catch this ---
	{
		// write to storage
		err = sd.DomainPut(kv.StorageDomain, rwTtx1, storageK1, []byte{1}, 1, nil)
		require.NoError(t, err)
		// check before flush
		firstKey, firstVal, ok, err := sd.HasPrefix(kv.StorageDomain, acc1.Bytes(), rwTtx1)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), firstKey)
		require.Equal(t, []byte{1}, firstVal)
		// check after flush
		err = sd.Flush(ctx, rwTtx1)
		require.NoError(t, err)
		err = rwTtx1.Commit()
		require.NoError(t, err)

		// make sure it is indeed in db using a db tx
		dbRoTx1, err := db.BeginRo(ctx)
		require.NoError(t, err)
		t.Cleanup(dbRoTx1.Rollback)
		c1, err := dbRoTx1.CursorDupSort(kv.TblStorageVals)
		require.NoError(t, err)
		t.Cleanup(c1.Close)
		k, v, err := c1.Next()
		require.NoError(t, err)
		require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), k)
		wantValueBytes := make([]byte, 8)                      // 8 bytes for uint64 step num
		binary.BigEndian.PutUint64(wantValueBytes, ^uint64(1)) // step num
		wantValueBytes = append(wantValueBytes, byte(1))       // value we wrote to the storage slot
		require.Equal(t, wantValueBytes, v)
		k, v, err = c1.Next()
		require.NoError(t, err)
		require.Nil(t, k)
		require.Nil(t, v)

		// all good
		// now move on to SharedDomains
		roTtx1, err := db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		t.Cleanup(roTtx1.Rollback)

		// make sure there are no files yet and we are only hitting the DB
		require.Equal(t, uint64(0), roTtx1.Debug().TxNumsInFiles(kv.StorageDomain))

		// finally, verify SharedDomains.HasPrefix returns true
		firstKey, firstVal, ok, err = sd.HasPrefix(kv.StorageDomain, acc1.Bytes(), roTtx1)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), firstKey)
		require.Equal(t, []byte{1}, firstVal)

		// check some other non-existing storages for non-existence after write operation
		firstKey, firstVal, ok, err = sd.HasPrefix(kv.StorageDomain, acc2.Bytes(), roTtx1)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, firstKey)
		require.Nil(t, firstVal)

		roTtx1.Rollback()
	}

	// --- check 3: storage exists in files only - SharedDomains.HasPrefix should catch this
	{
		// move data to files and trigger prune (need one more step for prune so write to some other storage)
		rwTtx2, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		t.Cleanup(rwTtx2.Rollback)
		err = sd.DomainPut(kv.StorageDomain, rwTtx2, storageK2, []byte{2}, 2, nil)
		require.NoError(t, err)
		// check before flush
		firstKey, firstVal, ok, err := sd.HasPrefix(kv.StorageDomain, acc1.Bytes(), rwTtx2)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), firstKey)
		require.Equal(t, []byte{1}, firstVal)
		// check after flush
		err = sd.Flush(ctx, rwTtx2)
		require.NoError(t, err)
		err = rwTtx2.Commit()
		require.NoError(t, err)

		// build files
		err = db.(state.HasAgg).Agg().(*state.Aggregator).BuildFiles(2)
		require.NoError(t, err)
		rwTtx3, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		t.Cleanup(rwTtx3.Rollback)

		// prune
		haveMore, err := rwTtx3.PruneSmallBatches(ctx, time.Minute)
		require.NoError(t, err)
		require.False(t, haveMore)
		err = rwTtx3.Commit()
		require.NoError(t, err)

		// double check acc1 storage data not in the mdbx DB
		dbRoTx2, err := db.BeginRo(ctx)
		require.NoError(t, err)
		t.Cleanup(dbRoTx2.Rollback)
		c2, err := dbRoTx2.CursorDupSort(kv.TblStorageVals)
		require.NoError(t, err)
		t.Cleanup(c2.Close)
		k, v, err := c2.Next() // acc2 storage from step 2 will be there
		require.NoError(t, err)
		require.Equal(t, append(append([]byte{}, acc2.Bytes()...), acc2slot2.Bytes()...), k)
		wantValueBytes := make([]byte, 8)                      // 8 bytes for uint64 step num
		binary.BigEndian.PutUint64(wantValueBytes, ^uint64(2)) // step num
		wantValueBytes = append(wantValueBytes, byte(2))       // value we wrote to the storage slot
		require.Equal(t, wantValueBytes, v)
		k, v, err = c2.Next() // acc1 storage from step 1 must not be there
		require.NoError(t, err)
		require.Nil(t, k)
		require.Nil(t, v)

		// double check files for 2 steps have been created
		roTtx2, err := db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		t.Cleanup(roTtx2.Rollback)
		require.Equal(t, uint64(2), roTtx2.Debug().TxNumsInFiles(kv.StorageDomain))

		// finally, verify SharedDomains.HasPrefix returns true
		firstKey, firstVal, ok, err = sd.HasPrefix(kv.StorageDomain, acc1.Bytes(), roTtx2)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), firstKey)
		require.Equal(t, []byte{1}, firstVal)
		roTtx2.Rollback()
	}

	// --- check 4: delete storage - SharedDomains.HasPrefix should catch this and say it does not exist
	{
		rwTtx4, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		t.Cleanup(rwTtx4.Rollback)
		err = sd.DomainDelPrefix(kv.StorageDomain, rwTtx4, acc1.Bytes(), 3)
		require.NoError(t, err)
		// check before flush
		firstKey, firstVal, ok, err := sd.HasPrefix(kv.StorageDomain, acc1.Bytes(), rwTtx4)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, firstKey)
		require.Nil(t, firstVal)
		// check after flush
		err = sd.Flush(ctx, rwTtx4)
		require.NoError(t, err)
		err = rwTtx4.Commit()
		require.NoError(t, err)

		roTtx3, err := db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		t.Cleanup(roTtx3.Rollback)
		firstKey, firstVal, ok, err = sd.HasPrefix(kv.StorageDomain, acc1.Bytes(), roTtx3)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, firstKey)
		require.Nil(t, firstVal)
		roTtx3.Rollback()
	}

	// --- check 5: write to it again after deletion - SharedDomains.HasPrefix should catch
	{
		rwTtx5, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		t.Cleanup(rwTtx5.Rollback)
		err = sd.DomainPut(kv.StorageDomain, rwTtx5, storageK1, []byte{3}, 4, nil)
		require.NoError(t, err)
		// check before flush
		firstKey, firstVal, ok, err := sd.HasPrefix(kv.StorageDomain, acc1.Bytes(), rwTtx5)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), firstKey)
		require.Equal(t, []byte{3}, firstVal)
		// check after flush
		err = sd.Flush(ctx, rwTtx5)
		require.NoError(t, err)
		err = rwTtx5.Commit()
		require.NoError(t, err)

		roTtx4, err := db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		t.Cleanup(roTtx4.Rollback)
		firstKey, firstVal, ok, err = sd.HasPrefix(kv.StorageDomain, acc1.Bytes(), roTtx4)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, append(append([]byte{}, acc1.Bytes()...), acc1slot1.Bytes()...), firstKey)
		require.Equal(t, []byte{3}, firstVal)
		roTtx4.Rollback()
	}
}

// TestDomainPut_HistoryCorrectness is a property test that verifies history invariants
// after random sequences of DomainPut calls:
//  1. GetAsOf returns the correct value at every txNum (history is complete and accurate)
//  2. No history entries are created when value doesn't change (no duplicates)
//
// This catches: duplicate entries, missing entries, wrong previous values, off-by-one errors.
func TestDomainPut_HistoryCorrectness(t *testing.T) {
	t.Parallel()

	type domainCase struct {
		domain          kv.Domain
		makeKey         func() []byte
		makeVal         func(i int) []byte
		historyKeyTable string
	}

	addr := make([]byte, length.Addr)
	addr[0] = 0xAA
	storageKey := composite(addr, make([]byte, length.Hash))

	domainCases := []domainCase{
		{
			domain:  kv.AccountsDomain,
			makeKey: func() []byte { return common.Copy(addr) },
			makeVal: func(i int) []byte {
				acc := accounts3.Account{
					Nonce:    uint64(i),
					Balance:  *uint256.NewInt(uint64(i) * 100),
					CodeHash: accounts.EmptyCodeHash,
				}
				return accounts3.SerialiseV3(&acc)
			},
			historyKeyTable: kv.TblAccountHistoryKeys,
		},
		{
			domain:          kv.StorageDomain,
			makeKey:         func() []byte { return common.Copy(storageKey) },
			makeVal:         func(i int) []byte { return binary.BigEndian.AppendUint64(nil, uint64(i)) },
			historyKeyTable: kv.TblStorageHistoryKeys,
		},
		{
			domain:          kv.CodeDomain,
			makeKey:         func() []byte { return common.Copy(addr) },
			makeVal:         func(i int) []byte { return binary.BigEndian.AppendUint64(nil, uint64(i)) },
			historyKeyTable: kv.TblCodeHistoryKeys,
		},
	}

	for _, dc := range domainCases {
		t.Run(dc.domain.String(), func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			stepSize := uint64(1000)
			db := newTestDb(t, stepSize)

			rwTx, err := db.BeginTemporalRw(ctx)
			require.NoError(t, err)
			defer rwTx.Rollback()

			domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
			require.NoError(t, err)
			defer domains.Close()

			key := dc.makeKey()

			// Generate a random write sequence with deliberate repeated values.
			// valPalette has fewer entries than totalTxs so repeats are guaranteed.
			valPalette := make([][]byte, 5)
			for i := range valPalette {
				valPalette[i] = dc.makeVal(i)
			}

			rng := rand.New(rand.NewPCG(42, 0))
			totalTxs := uint64(30)

			// writes[txNum] = index into valPalette that was written at txNum
			writes := make([]int, totalTxs)
			for i := range writes {
				writes[i] = -1 // -1 means "no write at this txNum"
			}

			// Pick random txNums to write at (not every txNum gets a write)
			numWrites := 15 + rng.IntN(int(totalTxs)-15) // at least 15 writes
			writeTxNums := make([]int, totalTxs)
			for i := range writeTxNums {
				writeTxNums[i] = i
			}
			rng.Shuffle(len(writeTxNums), func(i, j int) {
				writeTxNums[i], writeTxNums[j] = writeTxNums[j], writeTxNums[i]
			})
			writeTxNums = writeTxNums[:numWrites]
			for _, txn := range writeTxNums {
				writes[txn] = rng.IntN(len(valPalette))
			}

			// Execute writes in txNum order
			for txNum := uint64(0); txNum < totalTxs; txNum++ {
				if writes[txNum] < 0 {
					continue
				}
				v := valPalette[writes[txNum]]
				err := domains.DomainPut(dc.domain, rwTx, key, v, txNum, nil)
				require.NoError(t, err)
			}

			// Flush to DB
			err = domains.Flush(ctx, rwTx)
			require.NoError(t, err)

			// Property 1: GetAsOf returns the correct value at every txNum.
			// Build expected state: the value visible at query time ts is the last write with txNum < ts.
			expectedAtTx := make([][]byte, totalTxs+1) // expectedAtTx[ts] = value visible at GetAsOf(ts)
			var currentVal []byte
			for txNum := uint64(0); txNum <= totalTxs; txNum++ {
				expectedAtTx[txNum] = currentVal
				if txNum < totalTxs && writes[txNum] >= 0 {
					currentVal = valPalette[writes[txNum]]
				}
			}

			for ts := uint64(1); ts <= totalTxs; ts++ {
				got, ok, err := rwTx.GetAsOf(dc.domain, key, ts)
				require.NoError(t, err, "ts=%d", ts)
				want := expectedAtTx[ts]
				if want == nil {
					if ok {
						require.Empty(t, got, "ts=%d: expected nil/empty, got %x", ts, got)
					}
				} else {
					require.True(t, ok, "ts=%d: expected value %x but got not-found", ts, want)
					require.Equal(t, want, got, "ts=%d", ts)
				}
			}

			// Property 2: Number of history entries equals number of actual value changes
			// (i.e. no duplicate history entries for consecutive identical values).
			expectedChanges := 0
			var prevWrittenVal []byte
			for txNum := uint64(0); txNum < totalTxs; txNum++ {
				if writes[txNum] < 0 {
					continue
				}
				v := valPalette[writes[txNum]]
				if !bytes.Equal(prevWrittenVal, v) {
					expectedChanges++
					prevWrittenVal = v
				}
			}

			c, err := rwTx.CursorDupSort(dc.historyKeyTable)
			require.NoError(t, err)
			defer c.Close()

			actualEntries := 0
			for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
				require.NoError(t, err)
				actualEntries++
			}
			require.Equal(t, expectedChanges, actualEntries,
				"history entries (%d) should equal actual value changes (%d)", actualEntries, expectedChanges)
		})
	}
}

func TestSharedDomain_TouchChangedKeysFromHistory(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	stepSize := uint64(1)
	db1 := newTestDb(t, stepSize)

	db1RwTx, err := db1.BeginTemporalRw(ctx)
	require.NoError(t, err)
	t.Cleanup(db1RwTx.Rollback)
	sd1, err := execctx.NewSharedDomains(ctx, db1RwTx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd1.Close)

	acc1Addr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	acc1Slot := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	storageK1 := append(append([]byte{}, acc1Addr.Bytes()...), acc1Slot.Bytes()...)
	storageV1 := []byte{1}
	acc1 := accounts.NewAccount()
	acc1.Balance.SetUint64(1)
	acc1Encoded := accounts3.SerialiseV3(&acc1)

	// --- check 1: non-existing account & storage and empty commitment trie ---
	{
		acc1Value, _, err := sd1.GetLatest(kv.AccountsDomain, db1RwTx, acc1Addr.Bytes())
		require.NoError(t, err)
		require.Nil(t, acc1Value)

		acc1SlotValue, _, err := sd1.GetLatest(kv.StorageDomain, db1RwTx, acc1Slot.Bytes())
		require.NoError(t, err)
		require.Nil(t, acc1SlotValue)

		rootHash, err := sd1.GetCommitmentContext().Trie().RootHash()
		require.NoError(t, err)
		require.Equal(t, empty.RootHash.Bytes(), rootHash)
	}

	// --- check 2: write state in db1 and verify is present in history ---
	txNum := uint64(1)
	{
		// write account and storage in db1
		err = sd1.DomainPut(kv.AccountsDomain, db1RwTx, acc1Addr.Bytes(), acc1Encoded, txNum, nil)
		require.NoError(t, err)
		err = sd1.DomainPut(kv.StorageDomain, db1RwTx, storageK1, storageV1, txNum, nil)
		require.NoError(t, err)

		// flush and commit
		err = sd1.Flush(ctx, db1RwTx)
		require.NoError(t, err)
		err = db1RwTx.Commit()
		require.NoError(t, err)

		// verify account and storage are present in history
		db1RoTx, err := db1.BeginRo(ctx)
		require.NoError(t, err)
		t.Cleanup(db1RoTx.Rollback)
		c1, err := db1RoTx.CursorDupSort(kv.TblAccountVals)
		require.NoError(t, err)
		t.Cleanup(c1.Close)
		k, v, err := c1.Next()
		require.NoError(t, err)
		require.Equal(t, acc1Addr.Bytes(), k)
		wantValueBytes := make([]byte, 8)                       // 8 bytes for uint64 step num
		binary.BigEndian.PutUint64(wantValueBytes, ^uint64(1))  // step num
		wantValueBytes = append(wantValueBytes, acc1Encoded...) // value we wrote to the account
		require.Equal(t, wantValueBytes, v)
		k, v, err = c1.Next()
		require.NoError(t, err)
		require.Nil(t, k)
		require.Nil(t, v)
		c2, err := db1RoTx.CursorDupSort(kv.TblStorageVals)
		require.NoError(t, err)
		t.Cleanup(c2.Close)
		k, v, err = c2.Next()
		require.NoError(t, err)
		require.Equal(t, append(append([]byte{}, acc1Addr.Bytes()...), acc1Slot.Bytes()...), k)
		wantValueBytes = make([]byte, 8)                       // 8 bytes for uint64 step num
		binary.BigEndian.PutUint64(wantValueBytes, ^uint64(1)) // step num
		wantValueBytes = append(wantValueBytes, storageV1...)  // value we wrote to the storage slot
		require.Equal(t, wantValueBytes, v)
		k, v, err = c2.Next()
		require.NoError(t, err)
		require.Nil(t, k)
		require.Nil(t, v)
	}

	// --- test: computing commitment by touching changed keys from history *must* produce the same commitment root
	{
		blockNum := uint64(1)
		fromTxNum := txNum
		toTxNum := txNum

		// compute the expected commitment root in db1 using sd1
		db1RoTx, err := db1.BeginTemporalRo(ctx)
		require.NoError(t, err)
		t.Cleanup(db1RoTx.Rollback)
		require.Equal(t, uint64(0), db1RoTx.Debug().TxNumsInFiles(kv.StorageDomain))
		expectedRootHash, err := sd1.ComputeCommitment(ctx, db1RoTx, false, blockNum, toTxNum, "", nil)
		if err != nil {
			return
		}
		require.NoError(t, err)

		// now create a new empty database db2 and SharedDomains sd2 over it
		db2 := newTestDb(t, stepSize)
		roTx2, err := db2.BeginTemporalRo(ctx)
		require.NoError(t, err)
		t.Cleanup(roTx2.Rollback)
		sd2, err := execctx.NewSharedDomains(ctx, roTx2, log.New())
		require.NoError(t, err)
		t.Cleanup(sd2.Close)

		// check that the initial commitment trie is empty in db2
		initialRootHash, err := sd2.GetCommitmentContext().Trie().RootHash()
		require.NoError(t, err)
		require.Equal(t, empty.RootHash.Bytes(), initialRootHash)

		// double-check: if we try to compute the initial commitment trie root in db2, it is empty
		initialRootHash, err = sd2.ComputeCommitment(ctx, roTx2, false, blockNum, toTxNum, "", nil)
		if err != nil {
			return
		}
		require.NoError(t, err)
		require.Equal(t, empty.RootHash.Bytes(), initialRootHash)

		// test core: touch in sd2 the changed keys in [fromTxNum, toTxNum+1) using historical state changes from db1
		accountChanges, storageChanges, err := sd2.TouchChangedKeysFromHistory(db1RoTx, fromTxNum, toTxNum+1)
		require.NoError(t, err)
		require.Equal(t, 1, accountChanges)
		require.Equal(t, 1, storageChanges)

		// compute the commitment trie root in sd2 reading state for touched keys from db1
		sd2.GetCommitmentContext().SetStateReader(commitmentdb.NewLatestStateReader(db1RoTx, sd1))
		rootHash, err := sd2.ComputeCommitment(ctx, roTx2, false, blockNum, toTxNum, "", nil)
		if err != nil {
			return
		}
		require.NoError(t, err)
		require.Equal(t, expectedRootHash, rootHash)
	}
}
