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
	"github.com/erigontech/erigon/db/kv/temporal"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

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
