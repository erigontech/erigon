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
	"errors"
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
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/temporal"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// setupStepTest with edge records on: AggOpts.NewTest pins the legacy format, so the
// format the binary ships with is only reached by asking for it.
func setupEdgeRecordTest(t *testing.T) (kv.TemporalRwDB, kv.TemporalRwTx, *execctx.SharedDomains) {
	t.Helper()
	ctx := context.Background()
	logger := log.New()
	dirs := datadir.New(t.TempDir())

	rawDB := mdbxtest.InMem(t, mdbx.New(dbcfg.ChainDB, logger), dirs.Chaindata).MustOpen()
	t.Cleanup(rawDB.Close)

	agg, err := dbstate.NewTest(dirs).StepSize(16).Logger(logger).Open(ctx)
	require.NoError(t, err)
	t.Cleanup(agg.Close)
	agg.ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, true)
	require.True(t, agg.Cfg(kv.CommitmentDomain).EdgeRecordsInCommitment)

	db, err := temporal.New(rawDB, agg, nil)
	require.NoError(t, err)

	tx, err := db.BeginTemporalRw(ctx) //nolint:gocritic
	require.NoError(t, err)
	t.Cleanup(func() { tx.Rollback() })

	doms, err := execctx.NewSharedDomains(ctx, tx, logger)
	require.NoError(t, err)
	t.Cleanup(doms.Close)
	doms.SetDisableInlineTouchKey(true) // as parallel exec does: the calculator owns Updates
	return db, tx, doms
}

// Block 2's compute reads back the trie block 1 wrote. A state reader that cannot
// resolve v3 records sees an empty trie and fails on the root's own mask.
func TestHandleMessage_SecondComputeReadsFirstComputesTrie(t *testing.T) {
	ctx := context.Background()
	db, tx, doms := setupEdgeRecordTest(t)

	in := make(chan applyResult, 256)
	out := make(chan commitmentResult, 256)
	cc, err := newCommitmentCalculator(ctx, ctx, doms, db, &chain.Config{}, "test", log.New(), true, 1<<62, in, nil, out)
	require.NoError(t, err)

	rnd := rand.New(rand.NewSource(9))
	txNum := uint64(0)
	writeBlock := func(blockNum uint64, count int) {
		for range count {
			txNum++
			addrBytes := make([]byte, length.Addr)
			rnd.Read(addrBytes)
			addr := accounts.InternAddress([20]byte(addrBytes))
			bal := *uint256.NewInt(txNum * 1000)
			acc := accounts.Account{Nonce: txNum, Balance: bal, CodeHash: accounts.EmptyCodeHash}
			require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, addrBytes, accounts.SerialiseV3(&acc), txNum, nil))
			cc.handleMessage(ctx, &txResult{blockNum: blockNum, txNum: txNum, rules: &chain.Rules{}, writes: nonceBalanceWrites(addr, txNum, bal)})
		}
		cc.handleMessage(ctx, newTestBlockResult(blockNum, common.Hash{byte(blockNum)}, txNum, false))
	}

	// Enough accounts that the root is a branch block 2 must resolve before folding.
	writeBlock(1, 96)
	writeBlock(2, 8)
	cc.Stop()

	for {
		select {
		case r := <-out:
			// The fixture's headers carry no real state root.
			if r.err != nil && !errors.Is(r.err, ErrWrongTrieRoot) {
				require.NoError(t, r.err, "block %d compute", r.blockNum)
			}
			continue
		default:
		}
		break
	}
	require.Equal(t, uint64(2), cc.lastComputedBlock, "block 2's commitment never completed")
}
