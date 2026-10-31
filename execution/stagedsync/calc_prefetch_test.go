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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestAccumulateTouchedPrefetchesFirstTouchPerBlock(t *testing.T) {
	var got [][]byte
	cc := &commitmentCalculator{prefetchFn: func(plainKey []byte) { got = append(got, plainKey) }}
	payer, contract := common.Address{19: 1}, common.Address{19: 2}
	slot := accounts.InternKey(common.Hash{31: 9})
	writes := func(v uint64) *state.WriteSet {
		ws := nonceBalanceWrites(accounts.InternAddress(payer), v, *uint256.NewInt(v))
		ws.SetStorage(accounts.InternAddress(contract), slot, &state.VersionedWrite[uint256.Int]{
			WriteHeader: state.WriteHeader{Address: accounts.InternAddress(contract), Path: state.StoragePath, Key: slot},
			Val:         *uint256.NewInt(v),
		})
		return ws
	}
	slotValue := slot.Value()
	want := [][]byte{payer[:], contract[:], append(contract[:], slotValue[:]...)}

	cc.accumulateTouched(writes(1))
	cc.accumulateTouched(writes(2))
	require.ElementsMatch(t, want, got, "a key touched twice in one block is prefetched once")

	got = nil
	cc.clearTouched()
	cc.accumulateTouched(writes(3))
	require.ElementsMatch(t, want, got, "the next block prefetches the key again")
}

func TestCalculatorRootUnchangedByBranchPrefetch(t *testing.T) {
	ctx := context.Background()
	logger := log.New()
	logger.SetHandler(log.DiscardHandler())
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()), temporaltest.WithStepSize(16))

	addrs := make([]common.Address, 256)
	for i := range addrs {
		addrs[i] = common.Address{0: byte(i * 37), 19: byte(i + 1)}
	}
	putAccount := func(doms *execctx.SharedDomains, tx kv.TemporalTx, addr common.Address, nonce uint64, txNum uint64) {
		acc := accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(nonce * 10), CodeHash: accounts.EmptyCodeHash}
		require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, addr[:], accounts.SerialiseV3(&acc), txNum, nil))
	}

	func() {
		tx, err := db.BeginTemporalRw(ctx) //nolint:gocritic
		require.NoError(t, err)
		defer tx.Rollback()
		doms, err := execctx.NewSharedDomains(ctx, tx, logger, execctx.WithParaTrieDB(db))
		require.NoError(t, err)
		defer doms.Close()
		for _, addr := range addrs {
			putAccount(doms, tx, addr, 1, 1)
		}
		_, err = doms.ComputeCommitment(ctx, tx, true, 0, 1, "", nil)
		require.NoError(t, err)
		require.NoError(t, doms.Flush(ctx, tx))
		require.NoError(t, tx.Commit())
	}()

	root := func(prefetch, amsterdam bool) []byte {
		defer func(readAhead bool, workers int) {
			dbg.ReadAhead, dbg.TrieBALWarmupers = readAhead, workers
		}(dbg.ReadAhead, dbg.TrieBALWarmupers)
		dbg.ReadAhead, dbg.TrieBALWarmupers = true, 0
		if prefetch {
			dbg.TrieBALWarmupers = 2
		}

		tx, err := db.BeginTemporalRw(ctx) //nolint:gocritic
		require.NoError(t, err)
		defer tx.Rollback()
		doms, err := execctx.NewSharedDomains(ctx, tx, logger, execctx.WithParaTrieDB(db))
		require.NoError(t, err)
		defer doms.Close()
		doms.SetDisableInlineTouchKey(true)

		in := make(chan applyResult, 64)
		out := make(chan commitmentResult, 64)
		cc, err := newCommitmentCalculator(ctx, ctx, doms, db, &chain.Config{}, "test", logger, true, 1<<62, in, nil, out, state.NewLayeredDomainReader(doms, nil, nil), nil)
		require.NoError(t, err)

		const lastTxNum = 1 + 64
		vm := newRawViewVM(2)
		for i, addr := range addrs[:64] {
			txNum := uint64(2 + i)
			putAccount(doms, tx, addr, 2, txNum)
			cc.handleMessage(ctx, recordTx(vm, 2, common.Hash{0x02}, txNum, &chain.Rules{IsAmsterdam: amsterdam}, nonceBalanceWrites(accounts.InternAddress(addr), 2, *uint256.NewInt(20))))
		}
		started := cc.prefetch != nil
		cc.handleMessage(ctx, newTestBlockResult(2, common.Hash{0x02}, lastTxNum, false))
		finished := cc.prefetch == nil
		cc.Stop()
		require.Equal(t, prefetch && !amsterdam, started, "the first touched key starts the prefetch, except on BAL blocks the read-ahead covers")
		require.True(t, finished, "the prefetch finishes before the round computes")
		return (<-out).rootHash
	}

	want := root(false, false)
	require.NotEmpty(t, want, "computeAndCheck publishes the root with the header mismatch")
	require.Equal(t, want, root(true, false), "prefetched branches must yield the same root as direct reads")
	root(true, true)
}
