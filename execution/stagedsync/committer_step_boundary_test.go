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

package stagedsync

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/temporal"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestHandleMessage_StepBoundaryCheckpointMidBlock pins the parallel-exec
// step-boundary commitment bug: a block whose txNum range straddles a step
// edge must leave a commitment checkpoint at that edge, otherwise the step's
// commitment .kv lags its account/storage domain .kv. The calculator runs in
// pure batch mode (no per-block compute), so without the step-boundary hook in
// handleMessage's txResult case NO commitment state is written at the mid-block
// step edge: the checkpoint never advances to stepEnd and no step-0 branches
// are produced to verify against the account domain written through that step.
func TestHandleMessage_StepBoundaryCheckpointMidBlock(t *testing.T) {
	ctx := context.Background()
	logger := log.New()
	const stepSize = uint64(16)

	db, tx, doms := setupStepTest(t)

	in := make(chan applyResult, 64)
	out := make(chan commitmentResult, 64)
	cc, err := newCommitmentCalculator(ctx, doms, db, nil, "test", logger, false, 1<<62, in, nil, out)
	require.NoError(t, err)

	// Block 1: txNums 1..10, fully before the step-0 edge (txNum 15).
	// Block 2: txNums 11..20, straddling the edge at txNum 15.
	const block1End = uint64(10)
	const block2End = uint64(20)
	const stepEdgeTxNum = stepSize - 1 // 15, where (txNum+1)%stepSize==0

	rnd := rand.New(rand.NewSource(42))
	accountValues := make(map[string][]byte)

	writeAccount := func(txNum uint64) {
		addrBytes := make([]byte, length.Addr)
		rnd.Read(addrBytes)
		addr := accounts.InternAddress([20]byte(addrBytes))
		bal := *uint256.NewInt(txNum * 1000)
		acc := accounts.Account{Nonce: txNum, Balance: bal, CodeHash: accounts.EmptyCodeHash}
		buf := accounts.SerialiseV3(&acc)
		require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, addrBytes, buf, txNum, nil))
		if txNum <= stepEdgeTxNum {
			accountValues[string(addrBytes)] = buf
		}
		blockNum := uint64(1)
		if txNum > block1End {
			blockNum = 2
		}
		cc.handleMessage(ctx, &txResult{
			blockNum: blockNum,
			txNum:    txNum,
			writes: state.VersionedWrites{
				&state.VersionedWrite{Address: addr, Path: state.NoncePath, Val: txNum},
				&state.VersionedWrite{Address: addr, Path: state.BalancePath, Val: bal},
			},
		})
	}

	for txNum := uint64(1); txNum <= block1End; txNum++ {
		writeAccount(txNum)
	}
	cc.handleMessage(ctx, &blockResult{BlockNum: 1, BlockHash: common.Hash{0x01}, lastTxNum: block1End})

	for txNum := block1End + 1; txNum <= block2End; txNum++ {
		writeAccount(txNum)
	}
	cc.handleMessage(ctx, &blockResult{BlockNum: 2, BlockHash: common.Hash{0x02}, lastTxNum: block2End})

	cc.Stop()

	// Batch mode computes commitment only on an explicit request, which this
	// stream never sends; the sole writer of a checkpoint is the step-boundary
	// hook at txNum 15, so the latest checkpoint must decode to that edge.
	stateBlob, _, err := doms.GetLatest(kv.CommitmentDomain, tx, commitmentdb.KeyCommitmentState)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(stateBlob), 16,
		"no commitment checkpoint was saved at the mid-block step edge — the step-boundary hook in handleMessage's txResult case never ran")
	gotTxNum, gotBlockNum := commitmentdb.DecodeTxBlockNums(stateBlob)
	require.Equal(t, stepEdgeTxNum, gotTxNum,
		"step-boundary checkpoint must reflect the straddling step's last txNum (stepEnd-1), not the last complete block before the edge")
	require.Equal(t, uint64(2), gotBlockNum,
		"the checkpoint sits inside block 2 (the straddling block)")

	requireBranchesConsistentWithAccounts(t, doms, tx, accountValues)
}

// TestHandleMessage_StepCheckpointInPerBlockMode pins that the step-boundary
// checkpoint still fires in per-block compute mode (forcePerBlockCompute), which
// is how the archive snapshot producer runs — it needs step-aligned commitment
// just like batch mode.
func TestHandleMessage_StepCheckpointInPerBlockMode(t *testing.T) {
	ctx := context.Background()
	logger := log.New()
	const stepSize = uint64(16)

	db, tx, doms := setupStepTest(t)

	in := make(chan applyResult, 64)
	out := make(chan commitmentResult, 64)
	// forcePerBlockCompute=true => snapshot-producer mode (per-block at every block).
	cc, err := newCommitmentCalculator(ctx, doms, db, nil, "test", logger, true, 1<<62, in, nil, out)
	require.NoError(t, err)

	const block1End = uint64(10)
	const stepEdgeTxNum = stepSize - 1 // 15

	rnd := rand.New(rand.NewSource(42))
	writeAccount := func(txNum, blockNum uint64) {
		addrBytes := make([]byte, length.Addr)
		rnd.Read(addrBytes)
		addr := accounts.InternAddress([20]byte(addrBytes))
		bal := *uint256.NewInt(txNum * 1000)
		acc := accounts.Account{Nonce: txNum, Balance: bal, CodeHash: accounts.EmptyCodeHash}
		buf := accounts.SerialiseV3(&acc)
		require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, addrBytes, buf, txNum, nil))
		cc.handleMessage(ctx, &txResult{
			blockNum: blockNum,
			txNum:    txNum,
			writes: state.VersionedWrites{
				&state.VersionedWrite{Address: addr, Path: state.NoncePath, Val: txNum},
				&state.VersionedWrite{Address: addr, Path: state.BalancePath, Val: bal},
			},
		})
	}

	for txNum := uint64(1); txNum <= block1End; txNum++ {
		writeAccount(txNum, 1)
	}
	cc.handleMessage(ctx, &blockResult{BlockNum: 1, BlockHash: common.Hash{0x01}, lastTxNum: block1End})

	// Block 2 runs only up to the step edge — no block-2 boundary is sent.
	for txNum := block1End + 1; txNum <= stepEdgeTxNum; txNum++ {
		writeAccount(txNum, 2)
	}

	cc.Stop()

	stateBlob, _, err := doms.GetLatest(kv.CommitmentDomain, tx, commitmentdb.KeyCommitmentState)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(stateBlob), 16, "a commitment checkpoint must exist at the mid-block step edge")
	gotTxNum, gotBlockNum := commitmentdb.DecodeTxBlockNums(stateBlob)
	require.Equal(t, stepEdgeTxNum, gotTxNum,
		"per-block mode must still checkpoint at the mid-block step edge (snapshot producer needs step-aligned commitment)")
	require.Equal(t, uint64(2), gotBlockNum,
		"the step checkpoint sits inside the straddling block")
}

// TestHandleMessage_PartialBlockComputeFailureNotSwallowed pins that when the
// first partial block's commitment computation fails, the calculator does not
// mark the block computed. A swallowed compute error there would let exec
// proceed past a block whose commitment never landed.
func TestHandleMessage_PartialBlockComputeFailureNotSwallowed(t *testing.T) {
	ctx := context.Background()
	logger := log.New()
	const stepSize = uint64(16)

	db, tx, doms := setupStepTest(t)

	in := make(chan applyResult, 64)
	out := make(chan commitmentResult, 64)
	// forcePerBlockCompute=true routes the first partial block to computeWithoutCheck.
	cc, err := newCommitmentCalculator(ctx, doms, db, nil, "test", logger, true, 1<<62, in, nil, out)
	require.NoError(t, err)
	defer cc.Stop()

	rnd := rand.New(rand.NewSource(42))
	for txNum := uint64(1); txNum <= 5; txNum++ {
		addrBytes := make([]byte, length.Addr)
		rnd.Read(addrBytes)
		addr := accounts.InternAddress([20]byte(addrBytes))
		bal := *uint256.NewInt(txNum * 1000)
		acc := accounts.Account{Nonce: txNum, Balance: bal, CodeHash: accounts.EmptyCodeHash}
		buf := accounts.SerialiseV3(&acc)
		require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, addrBytes, buf, txNum, nil))
		cc.handleMessage(ctx, &txResult{
			blockNum: 1,
			txNum:    txNum,
			writes: state.VersionedWrites{
				&state.VersionedWrite{Address: addr, Path: state.NoncePath, Val: txNum},
				&state.VersionedWrite{Address: addr, Path: state.BalancePath, Val: bal},
			},
		})
	}
	require.False(t, cc.hasComputed)

	// A cancelled context makes the partial-block ComputeCommitment fail
	// deterministically (the per-key ctx check in the trie fold, with >=1 update).
	failCtx, cancel := context.WithCancel(ctx)
	cancel()
	cc.handleMessage(failCtx, &blockResult{BlockNum: 1, BlockHash: common.Hash{0x01}, lastTxNum: 5, isPartial: true})

	require.False(t, cc.hasComputed,
		"a failed partial-block commitment must not be marked computed — the error must halt exec, not be swallowed")
	require.Zero(t, cc.lastComputedBlock,
		"lastComputedBlock must not advance past a block whose commitment failed")
}

// requireBranchesConsistentWithAccounts verifies each flushed commitment branch
// hashes consistently with the account values written through the step edge.
func requireBranchesConsistentWithAccounts(t *testing.T, doms *execctx.SharedDomains, tx kv.TemporalRwTx, accountValues map[string][]byte) {
	t.Helper()
	require.NoError(t, doms.Flush(t.Context(), tx))

	it, err := tx.RangeAsOf(kv.CommitmentDomain, nil, nil, 1000, order.Asc, -1)
	require.NoError(t, err)
	defer it.Close()

	storageValues := map[string][]byte{}
	checked := 0
	for it.HasNext() {
		k, v, err := it.Next()
		require.NoError(t, err)
		if bytes.Equal(k, commitmentdb.KeyCommitmentState) {
			continue
		}
		require.NoError(t, commitment.VerifyBranchHashes(k, commitment.BranchData(v), accountValues, storageValues),
			"branch %x at the step edge disagrees with the account domain written through that step", k)
		checked++
	}
	require.Positive(t, checked, "expected at least one commitment branch to verify at the step edge")
}

type stepEdgeOutcome struct {
	commitmentState  []byte
	accountsThruEdge int
}

// runBlockEndingOnStepEdge drives one block, in pure batch mode, whose block-end
// txNum is exactly the step-0 edge (txNum 15, stepSize 16). Regular txs 1..14
// emit a txResult with writes; the block-end system task at txNum 15 is always
// emitted as a regular txResult by the producer (exec3_parallel.go nextResult
// sends every task unconditionally), carrying writes only when the block
// finalizes some — edgeTxHasWrites models that. The step-boundary hook in
// handleMessage's txResult case must checkpoint the edge in both cases.
func runBlockEndingOnStepEdge(t *testing.T, edgeTxHasWrites bool) stepEdgeOutcome {
	t.Helper()
	ctx := context.Background()
	logger := log.New()
	const stepSize = uint64(16)
	const edgeTxNum = stepSize - 1 // 15: (txNum+1)%stepSize == 0

	db, tx, doms := setupStepTest(t)

	in := make(chan applyResult, 64)
	out := make(chan commitmentResult, 64)
	// forcePerBlockCompute=false + huge perBlockFrom => pure batch mode: the
	// blockResult never triggers a per-block compute, so the only thing that can
	// checkpoint the step edge is the step-boundary hook.
	cc, err := newCommitmentCalculator(ctx, doms, db, nil, "test", logger, false, 1<<62, in, nil, out)
	require.NoError(t, err)

	rnd := rand.New(rand.NewSource(42))
	writeAccount := func(txNum uint64, hasWrites bool) {
		var writes state.VersionedWrites
		if hasWrites {
			addrBytes := make([]byte, length.Addr)
			rnd.Read(addrBytes)
			addr := accounts.InternAddress([20]byte(addrBytes))
			bal := *uint256.NewInt(txNum * 1000)
			acc := accounts.Account{Nonce: txNum, Balance: bal, CodeHash: accounts.EmptyCodeHash}
			buf := accounts.SerialiseV3(&acc)
			require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, addrBytes, buf, txNum, nil))
			writes = state.VersionedWrites{
				&state.VersionedWrite{Address: addr, Path: state.NoncePath, Val: txNum},
				&state.VersionedWrite{Address: addr, Path: state.BalancePath, Val: bal},
			}
		}
		cc.handleMessage(ctx, &txResult{blockNum: 1, txNum: txNum, writes: writes})
	}

	for txNum := uint64(1); txNum < edgeTxNum; txNum++ {
		writeAccount(txNum, true)
	}
	// The block-end system task is always published at the block-end txNum, with
	// empty writes when the block has no finalize writes.
	writeAccount(edgeTxNum, edgeTxHasWrites)
	cc.handleMessage(ctx, &blockResult{BlockNum: 1, BlockHash: common.Hash{0x01}, lastTxNum: edgeTxNum})
	cc.Stop()

	stateBlob, _, err := doms.GetLatest(kv.CommitmentDomain, tx, commitmentdb.KeyCommitmentState)
	require.NoError(t, err)

	require.NoError(t, doms.Flush(ctx, tx))
	it, err := tx.RangeAsOf(kv.AccountsDomain, nil, nil, edgeTxNum+1, order.Asc, -1)
	require.NoError(t, err)
	defer it.Close()
	accCount := 0
	for it.HasNext() {
		_, v, err := it.Next()
		require.NoError(t, err)
		if len(v) > 0 {
			accCount++
		}
	}
	return stepEdgeOutcome{commitmentState: stateBlob, accountsThruEdge: accCount}
}

// TestHandleMessage_StepEdgeAtBlockEnd pins that a block whose block-end txNum is
// a step edge leaves a step-aligned commitment checkpoint. The producer always
// emits the block-end system task as a regular txResult, so the step-boundary
// hook in handleMessage's *txResult case checkpoints the edge whether or not that
// task carries finalize writes (it ignores len(writes)). Without it the step's
// commitment .kv lags its account-domain .kv (the mid-block inconsistency).
func TestHandleMessage_StepEdgeAtBlockEnd(t *testing.T) {
	t.Run("with_finalize_writes", func(t *testing.T) {
		res := runBlockEndingOnStepEdge(t, true)
		require.GreaterOrEqual(t, len(res.commitmentState), 16,
			"block-end txResult at the step edge → step-boundary checkpoint written")
		gotTxNum, gotBlockNum := commitmentdb.DecodeTxBlockNums(res.commitmentState)
		require.Equal(t, uint64(15), gotTxNum)
		require.Equal(t, uint64(1), gotBlockNum)
	})

	t.Run("empty_finalize", func(t *testing.T) {
		res := runBlockEndingOnStepEdge(t, false)
		require.Positive(t, res.accountsThruEdge,
			"sanity: the account domain holds the block's writes through the step edge")
		require.GreaterOrEqual(t, len(res.commitmentState), 16,
			"a block-end task with empty finalize writes must still leave a step-aligned "+
				"checkpoint — the hook ignores len(writes); otherwise step-0 commitment .kv "+
				"lags its account .kv")
		gotTxNum, gotBlockNum := commitmentdb.DecodeTxBlockNums(res.commitmentState)
		require.Equal(t, uint64(15), gotTxNum)
		require.Equal(t, uint64(1), gotBlockNum)
	})
}

// TestHandleMessage_StepBoundaryDoesNotPolluteLiveChangeset pins that a
// pre-window mid-block step checkpoint records into NO changeset: the lagging
// calculator's live accumulator can be a later block's, and leaking there
// corrupts that block's unwind.
func TestHandleMessage_StepBoundaryDoesNotPolluteLiveChangeset(t *testing.T) {
	ctx := context.Background()
	logger := log.New()
	const stepSize = uint64(16)

	db, tx, doms := setupStepTest(t)

	// Stand in for the exec loop having advanced into the changeset window and
	// installed a later block's accumulator as live, while the calculator is
	// still on an earlier block whose own changeset was never saved.
	liveCS := &changeset.StateChangeSet{}
	doms.SetChangesetAccumulator(liveCS)

	in := make(chan applyResult, 64)
	out := make(chan commitmentResult, 64)
	cc, err := newCommitmentCalculator(ctx, doms, db, nil, "test", logger, false, 1<<62, in, nil, out)
	require.NoError(t, err)
	defer cc.Stop()

	const stepEdgeTxNum = stepSize - 1 // 15: (txNum+1)%stepSize == 0
	rnd := rand.New(rand.NewSource(42))
	for txNum := uint64(1); txNum <= stepEdgeTxNum; txNum++ {
		addrBytes := make([]byte, length.Addr)
		rnd.Read(addrBytes)
		addr := accounts.InternAddress([20]byte(addrBytes))
		bal := *uint256.NewInt(txNum * 1000)
		acc := accounts.Account{Nonce: txNum, Balance: bal, CodeHash: accounts.EmptyCodeHash}
		buf := accounts.SerialiseV3(&acc)
		require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, addrBytes, buf, txNum, nil))
		cc.handleMessage(ctx, &txResult{
			blockNum: 1,
			txNum:    txNum,
			writes: state.VersionedWrites{
				&state.VersionedWrite{Address: addr, Path: state.NoncePath, Val: txNum},
				&state.VersionedWrite{Address: addr, Path: state.BalancePath, Val: bal},
			},
		})
	}

	require.Zero(t, liveCS.Diffs[kv.CommitmentDomain].Len(),
		"mid-block step-boundary compute leaked CommitmentDomain diffs into the live "+
			"changeset accumulator — on that block's unwind they corrupt commitment state")

	// The checkpoint must still advance to the step edge: the fix isolates the
	// changeset, it does not suppress the checkpoint.
	stateBlob, _, err := doms.GetLatest(kv.CommitmentDomain, tx, commitmentdb.KeyCommitmentState)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(stateBlob), 16,
		"step-boundary checkpoint must still be written to sd, only kept out of the changeset")
	gotTxNum, _ := commitmentdb.DecodeTxBlockNums(stateBlob)
	require.Equal(t, stepEdgeTxNum, gotTxNum)
}

// TestHandleMessage_StepBoundaryRecordsIntoOwnChangesetInWindow pins the cs!=nil
// hash-routing path: with block N's changeset saved and a DIFFERENT accumulator
// live, the mid-block checkpoint must record its commitment writes into N's own
// changeset, not the live one. (The stale-cs==nil TOCTOU itself needs concurrent
// interleaving to trigger and isn't reproducible in this single-goroutine test.)
func TestHandleMessage_StepBoundaryRecordsIntoOwnChangesetInWindow(t *testing.T) {
	ctx := context.Background()
	logger := log.New()
	const stepSize = uint64(16)
	blockHash := common.Hash{0xAB}

	db, tx, doms := setupStepTest(t)

	// Save block 1's own changeset (keyed by its hash) and install a DIFFERENT
	// accumulator as the live one.
	ownCS := &changeset.StateChangeSet{}
	liveCS := &changeset.StateChangeSet{}
	doms.SavePastChangesetAccumulator(blockHash, 1, ownCS)
	doms.SetChangesetAccumulator(liveCS)

	in := make(chan applyResult, 64)
	out := make(chan commitmentResult, 64)
	// perBlockFrom=1 => block 1 is in-window, so it routes through the hash-aware
	// wrap (ownsChangeset(1)=true) rather than the isolated path.
	cc, err := newCommitmentCalculator(ctx, doms, db, nil, "test", logger, false, 1, in, nil, out)
	require.NoError(t, err)
	defer cc.Stop()

	const stepEdgeTxNum = stepSize - 1 // 15
	rnd := rand.New(rand.NewSource(42))
	for txNum := uint64(1); txNum <= stepEdgeTxNum; txNum++ {
		addrBytes := make([]byte, length.Addr)
		rnd.Read(addrBytes)
		addr := accounts.InternAddress([20]byte(addrBytes))
		bal := *uint256.NewInt(txNum * 1000)
		acc := accounts.Account{Nonce: txNum, Balance: bal, CodeHash: accounts.EmptyCodeHash}
		buf := accounts.SerialiseV3(&acc)
		require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, addrBytes, buf, txNum, nil))
		cc.handleMessage(ctx, &txResult{
			blockNum:  1,
			blockHash: blockHash,
			txNum:     txNum,
			writes: state.VersionedWrites{
				&state.VersionedWrite{Address: addr, Path: state.NoncePath, Val: txNum},
				&state.VersionedWrite{Address: addr, Path: state.BalancePath, Val: bal},
			},
		})
	}

	require.Positive(t, ownCS.Diffs[kv.CommitmentDomain].Len(),
		"the checkpoint must hash-route its commitment writes into block N's own saved changeset")
	require.Zero(t, liveCS.Diffs[kv.CommitmentDomain].Len(),
		"the checkpoint must NOT record commitment writes into the different live accumulator")
}

// TestHandleMessage_PreWindowPerBlockComputeDoesNotPolluteLiveChangeset guards
// the block-boundary variant: forcePerBlockCompute makes even a pre-window block
// compute per-block, so that compute must isolate or it leaks into a later
// block's live changeset.
func TestHandleMessage_PreWindowPerBlockComputeDoesNotPolluteLiveChangeset(t *testing.T) {
	ctx := context.Background()
	logger := log.New()

	db, tx, doms := setupStepTest(t)

	liveCS := &changeset.StateChangeSet{}
	doms.SetChangesetAccumulator(liveCS)

	in := make(chan applyResult, 64)
	out := make(chan commitmentResult, 64)
	// forcePerBlockCompute=true + perBlockFrom=5 => block 1 is pre-window yet
	// still computes per-block.
	cc, err := newCommitmentCalculator(ctx, doms, db, nil, "test", logger, true, 5, in, nil, out)
	require.NoError(t, err)
	defer cc.Stop()

	rnd := rand.New(rand.NewSource(42))
	for txNum := uint64(1); txNum <= 5; txNum++ {
		addrBytes := make([]byte, length.Addr)
		rnd.Read(addrBytes)
		addr := accounts.InternAddress([20]byte(addrBytes))
		bal := *uint256.NewInt(txNum * 1000)
		acc := accounts.Account{Nonce: txNum, Balance: bal, CodeHash: accounts.EmptyCodeHash}
		buf := accounts.SerialiseV3(&acc)
		require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, addrBytes, buf, txNum, nil))
		cc.handleMessage(ctx, &txResult{
			blockNum: 1,
			txNum:    txNum,
			writes: state.VersionedWrites{
				&state.VersionedWrite{Address: addr, Path: state.NoncePath, Val: txNum},
				&state.VersionedWrite{Address: addr, Path: state.BalancePath, Val: bal},
			},
		})
	}
	// First partial block => computeWithoutCheck, a per-block compute with no
	// root check; pre-window, so it must isolate its commitment writes.
	cc.handleMessage(ctx, &blockResult{BlockNum: 1, BlockHash: common.Hash{0x01}, lastTxNum: 5, isPartial: true})

	require.Zero(t, liveCS.Diffs[kv.CommitmentDomain].Len(),
		"pre-window per-block compute leaked CommitmentDomain diffs into the live changeset accumulator")
}

// TestOwnsChangeset pins the isolation predicate at the window boundary — a wrong
// answer either leaks a pre-window compute or over-isolates an in-window block.
func TestOwnsChangeset(t *testing.T) {
	cc := &commitmentCalculator{perBlockFrom: 100}
	require.False(t, cc.ownsChangeset(0), "genesis owns no changeset")
	require.False(t, cc.ownsChangeset(99), "pre-window block owns no changeset")
	require.True(t, cc.ownsChangeset(100), "first in-window block owns its changeset")
	require.True(t, cc.ownsChangeset(101), "in-window block owns its changeset")

	// AlwaysGenerateChangesets from genesis => perBlockFrom == startBlockNum (0),
	// but block 0 is still excluded by the exec loop.
	genesisStart := &commitmentCalculator{perBlockFrom: 0}
	require.False(t, genesisStart.ownsChangeset(0), "block 0 is always excluded")
	require.True(t, genesisStart.ownsChangeset(1), "block 1 owns its changeset when the window starts at genesis")
}

// setupStepTest builds the shared in-memory temporal stack (stepSize 16) for the
// step-boundary tests — like setup2CacheTest, but also returns the db the
// commitment calculator needs for its own read tx.
func setupStepTest(t *testing.T) (kv.TemporalRwDB, kv.TemporalRwTx, *execctx.SharedDomains) {
	t.Helper()
	ctx := context.Background()
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	rawDb := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).MustOpen()
	t.Cleanup(rawDb.Close)

	agg, err := dbstate.NewTest(dirs).StepSize(16).Logger(logger).Open(ctx, rawDb)
	require.NoError(t, err)
	t.Cleanup(agg.Close)

	db, err := temporal.New(rawDb, agg)
	require.NoError(t, err)

	tx, err := db.BeginTemporalRw(ctx) //nolint:gocritic
	require.NoError(t, err)
	t.Cleanup(func() { tx.Rollback() })

	doms, err := execctx.NewSharedDomains(ctx, tx, logger)
	require.NoError(t, err)
	t.Cleanup(doms.Close)
	return db, tx, doms
}
