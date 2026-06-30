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

	dirs := datadir.New(t.TempDir())
	rawDb := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).MustOpen()
	defer rawDb.Close()

	agg, err := dbstate.NewTest(dirs).StepSize(stepSize).Logger(logger).Open(ctx, rawDb)
	require.NoError(t, err)
	defer agg.Close()

	db, err := temporal.New(rawDb, agg)
	require.NoError(t, err)

	tx, err := db.BeginTemporalRw(ctx) //nolint:gocritic
	require.NoError(t, err)
	defer tx.Rollback()

	doms, err := execctx.NewSharedDomains(ctx, tx, logger)
	require.NoError(t, err)
	defer doms.Close()

	in := make(chan applyResult, 64)
	out := make(chan commitmentResult, 64)
	cc, err := newCommitmentCalculator(ctx, doms, db, "test", logger, false, 1<<62, in, out)
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

	dirs := datadir.New(t.TempDir())
	rawDb := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).MustOpen()
	defer rawDb.Close()

	agg, err := dbstate.NewTest(dirs).StepSize(stepSize).Logger(logger).Open(ctx, rawDb)
	require.NoError(t, err)
	defer agg.Close()

	db, err := temporal.New(rawDb, agg)
	require.NoError(t, err)

	tx, err := db.BeginTemporalRw(ctx) //nolint:gocritic
	require.NoError(t, err)
	defer tx.Rollback()

	doms, err := execctx.NewSharedDomains(ctx, tx, logger)
	require.NoError(t, err)
	defer doms.Close()

	in := make(chan applyResult, 64)
	out := make(chan commitmentResult, 64)
	// forcePerBlockCompute=true => snapshot-producer mode (per-block at every block).
	cc, err := newCommitmentCalculator(ctx, doms, db, "test", logger, true, 1<<62, in, out)
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

	dirs := datadir.New(t.TempDir())
	rawDb := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).MustOpen()
	defer rawDb.Close()

	agg, err := dbstate.NewTest(dirs).StepSize(stepSize).Logger(logger).Open(ctx, rawDb)
	require.NoError(t, err)
	defer agg.Close()

	db, err := temporal.New(rawDb, agg)
	require.NoError(t, err)

	tx, err := db.BeginTemporalRw(ctx) //nolint:gocritic
	require.NoError(t, err)
	defer tx.Rollback()

	doms, err := execctx.NewSharedDomains(ctx, tx, logger)
	require.NoError(t, err)
	defer doms.Close()

	in := make(chan applyResult, 64)
	out := make(chan commitmentResult, 64)
	// forcePerBlockCompute=true routes the first partial block to computeWithoutCheck.
	cc, err := newCommitmentCalculator(ctx, doms, db, "test", logger, true, 1<<62, in, out)
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
