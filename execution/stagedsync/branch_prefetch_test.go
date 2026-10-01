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
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
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
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func drainPrefetchQueue(p *branchPrefetcher) []prefetchItem {
	var got []prefetchItem
	for len(p.work) > 0 {
		got = append(got, <-p.work)
	}
	return got
}

func TestPrefetchedBranchesServeOneRound(t *testing.T) {
	_, tx, doms := setupStepTest(t)
	p := &branchPrefetcher{}
	r := &asOfStateReader{sd: doms, roTx: tx, prefetched: p}
	read := func(key []byte) ([]byte, kv.Step) {
		got, step, err := r.Read(kv.CommitmentDomain, key, 16)
		require.NoError(t, err)
		return got, step
	}

	flushed, untouched, late := []byte{0x40, 0x12, 0x02}, []byte{0x40, 0x34, 0x02}, []byte{0x40, 0x56, 0x02}
	p.put(flushed, []byte("prefetched-flushed"), 3)
	p.put(untouched, []byte("prefetched-untouched"), 3)
	require.NoError(t, doms.DomainPut(kv.CommitmentDomain, tx, flushed, []byte("mem-flushed"), 5, nil))
	p.freeze()
	p.put(late, []byte("prefetched-late"), 3)

	got, _ := read(flushed)
	require.Equal(t, []byte("mem-flushed"), got, "sd.mem wins over a prefetched record")
	got, step := read(untouched)
	require.Equal(t, []byte("prefetched-untouched"), got)
	require.Equal(t, kv.Step(3), step)
	got, _ = read(late)
	require.Empty(t, got, "a record fetched after the round froze its set waits for the next round")

	p.release()
	got, _ = read(untouched)
	require.Empty(t, got, "records do not outlive their round")
}

func TestReaderClonesCarryPrefetcher(t *testing.T) {
	_, tx, doms := setupStepTest(t)
	p := &branchPrefetcher{}
	r := &asOfStateReader{sd: doms, roTx: tx, prefetched: p}
	require.Same(t, p, r.Clone(tx).(*asOfStateReader).prefetched)
	require.Same(t, p, r.CloneForWorker(context.Background(), tx).(*asOfStateReader).prefetched)
}

func TestBranchPrefetcherCountsDroppedAndDrained(t *testing.T) {
	p := &branchPrefetcher{work: make(chan prefetchItem, 1)}
	p.add(accountPrefetch(common.Address{1}))
	p.add(accountPrefetch(common.Address{2}))
	require.Equal(t, uint64(1), p.dropped.Load(), "an item that finds the queue full is dropped")
	p.drain()
	require.Equal(t, uint64(1), p.drained.Load(), "an item still queued when compute starts is drained")
}

func TestHandleBlockRequestQueuesBALWrites(t *testing.T) {
	defer func(prev bool) { dbg.IgnoreBAL = prev }(dbg.IgnoreBAL)
	dbg.IgnoreBAL = false

	p := &branchPrefetcher{work: make(chan prefetchItem, 16)}
	cc := &commitmentCalculator{
		prefetch:      p,
		pending:       map[uint64]*pendingBlock{},
		computedAhead: map[uint64]bool{},
		balRoots:      map[uint64][]byte{},
		hasFirstBlock: true,
		firstBlockNum: 100,
	}
	balanceOnly, storageOnly, readOnly := common.Address{19: 1}, common.Address{19: 2}, common.Address{19: 3}
	slotA, slotB := accounts.InternKey(common.Hash{31: 1}), accounts.InternKey(common.Hash{31: 2})
	change := []*types.StorageChange{{Index: 1, Value: *uint256.NewInt(7)}}
	bal := types.BlockAccessList{
		{Address: balanceOnly, BalanceChanges: []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(1)}}},
		{Address: storageOnly, StorageChanges: []types.SlotChanges{{Slot: slotA, Changes: change}, {Slot: slotB, Changes: change}}},
		{Address: readOnly, StorageReads: []accounts.StorageKey{slotA}},
	}

	cc.handleBlockRequest(context.Background(), &blockRequest{blockNum: 5, bal: bal})

	want := []prefetchItem{
		accountPrefetch(balanceOnly),
		storagePrefetch(storageOnly, slotA.Value()),
		storagePrefetch(storageOnly, slotB.Value()),
	}
	require.ElementsMatch(t, want, drainPrefetchQueue(p), "BAL writes queue their account and slot walks; read-only accounts get nothing")
}

func TestApplyWritesQueuesFirstWritePerBlock(t *testing.T) {
	p := &branchPrefetcher{work: make(chan prefetchItem, 16)}
	cs := &calcState{
		accounts:     map[accounts.Address]*calcAccountState{},
		storageState: map[accounts.Address]map[accounts.StorageKey]uint256.Int{},
		storageDirty: map[accounts.Address]map[accounts.StorageKey]bool{},
		prefetch:     p,
	}
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
	want := []prefetchItem{accountPrefetch(payer), storagePrefetch(contract, slot.Value())}

	cs.ApplyWrites(writes(1), false)
	cs.ApplyWrites(writes(2), false)
	require.ElementsMatch(t, want, drainPrefetchQueue(p), "a key written twice in one block is queued once")

	cs.ResetBlockFlags()
	cs.ApplyWrites(writes(3), false)
	require.ElementsMatch(t, want, drainPrefetchQueue(p), "the next block queues the key again: records last one round")
}

func TestCalculatorServesPrefetchedBranches(t *testing.T) {
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
		doms, err := execctx.NewSharedDomains(ctx, tx, logger)
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

	root := func(prefetch, poisonRoot bool) commitmentResult {
		defer func(prev bool) { dbg.CommitmentPrefetch = prev }(dbg.CommitmentPrefetch)
		dbg.CommitmentPrefetch = prefetch

		tx, err := db.BeginTemporalRw(ctx) //nolint:gocritic
		require.NoError(t, err)
		defer tx.Rollback()
		doms, err := execctx.NewSharedDomains(ctx, tx, logger, execctx.WithParaTrieDB(db))
		require.NoError(t, err)
		defer doms.Close()
		doms.SetDisableInlineTouchKey(true)

		in := make(chan applyResult, 64)
		out := make(chan commitmentResult, 64)
		cc, err := newCommitmentCalculator(ctx, ctx, doms, db, &chain.Config{}, "test", logger, true, 1<<62, in, nil, out)
		require.NoError(t, err)
		p := cc.prefetch
		require.Equal(t, prefetch, p != nil)
		if poisonRoot {
			p.put([]byte{0x00}, nil, 0)
		}

		const lastTxNum = 1 + 64
		for i, addr := range addrs[:64] {
			txNum := uint64(2 + i)
			putAccount(doms, tx, addr, 2, txNum)
			cc.handleMessage(ctx, &txResult{
				blockNum: 2,
				txNum:    txNum,
				rules:    &chain.Rules{},
				writes:   nonceBalanceWrites(accounts.InternAddress(addr), 2, *uint256.NewInt(20)),
			})
		}
		stored := p == nil || poisonRoot || assert.Eventually(t, func() bool { return p.bytes.Load() > 0 }, 10*time.Second, time.Millisecond)
		cc.handleMessage(ctx, newTestBlockResult(2, common.Hash{0x02}, lastTxNum, false))
		cc.Stop()
		require.True(t, stored, "prefetch workers never stored a record")
		return <-out
	}

	want := root(false, false).rootHash
	require.NotEmpty(t, want, "computeAndCheck publishes the root with the header mismatch")
	require.Equal(t, want, root(true, false).rootHash, "the prefetched records must yield the same root as direct reads")
	require.NotEqual(t, want, root(true, true).rootHash, "Process must read the frozen records: an absent root record has to change the result")
}
