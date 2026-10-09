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
	"hash/maphash"
	"runtime/debug"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestPrefetchedBranchesYieldToMemBatch(t *testing.T) {
	_, tx, doms := setupStepTest(t)
	p := &branchPrefetcher{seed: maphash.MakeSeed()}
	for i := range p.shards {
		p.shards[i].records = make(map[string]prefetchedRecord)
	}
	r := &asOfStateReader{sd: doms, roTx: tx, prefetched: p}

	flushed, untouched, absent := []byte{0x40, 0x12, 0x02}, []byte{0x40, 0x34, 0x02}, []byte{0x40, 0x56, 0x02}
	p.put(flushed, []byte("prefetched-flushed"), 3)
	p.put(untouched, []byte("prefetched-untouched"), 3)
	require.NoError(t, doms.DomainPut(kv.CommitmentDomain, tx, flushed, []byte("mem-flushed"), 5, nil))

	got, _, err := r.Read(kv.CommitmentDomain, flushed, 16)
	require.NoError(t, err)
	require.Equal(t, []byte("mem-flushed"), got)

	got, step, err := r.Read(kv.CommitmentDomain, untouched, 16)
	require.NoError(t, err)
	require.Equal(t, []byte("prefetched-untouched"), got)
	require.Equal(t, kv.Step(3), step)

	got, _, err = r.Read(kv.CommitmentDomain, absent, 16)
	require.NoError(t, err)
	require.Empty(t, got)

	require.Equal(t, uint64(1), p.hits.Load(), "only the untouched key is served from the map")
	require.Equal(t, uint64(1), p.misses.Load(), "only the absent key falls through the map")
}

func TestBranchPrefetcherCountsDroppedAndDrained(t *testing.T) {
	p := &branchPrefetcher{work: make(chan prefetchItem, 1)}
	p.add(prefetchItem{account: common.Hash{1}})
	p.add(prefetchItem{account: common.Hash{2}})
	require.Equal(t, uint64(1), p.dropped.Load(), "an item that finds the queue full is dropped")
	p.pause()
	p.resume()
	require.Equal(t, uint64(1), p.drained.Load(), "an item still queued when compute starts is drained")
}

func TestHandleBlockRequestQueuesBALWrites(t *testing.T) {
	defer func(prev bool) { dbg.IgnoreBAL = prev }(dbg.IgnoreBAL)
	dbg.IgnoreBAL = false

	p := &branchPrefetcher{work: make(chan prefetchItem, 16)}
	cc := &commitmentCalculator{
		state:         &calcState{branchPrefetch: p},
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

	hash := keccak.Sum256
	sa, sb := slotA.Value(), slotB.Value()
	want := []prefetchItem{
		{account: hash(balanceOnly[:])},
		{account: hash(storageOnly[:])},
		{account: hash(storageOnly[:]), slot: hash(sa[:]), storage: true},
		{account: hash(storageOnly[:]), slot: hash(sb[:]), storage: true},
	}
	var got []prefetchItem
	for len(p.work) > 0 {
		got = append(got, <-p.work)
	}
	require.ElementsMatch(t, want, got, "BAL writes queue their account and slot walks; storage-only accounts get the account walk, read-only accounts get nothing")
}

func TestComputeAheadReadsPrefetchedBranches(t *testing.T) {
	defer func(v bool) { statecfg.ExperimentalCommitmentV3 = v }(statecfg.ExperimentalCommitmentV3)
	defer func(v bool) { dbg.BALDrivenCommitment = v }(dbg.BALDrivenCommitment)
	defer func(v bool) { dbg.IgnoreBAL = v }(dbg.IgnoreBAL)
	statecfg.ExperimentalCommitmentV3, dbg.BALDrivenCommitment, dbg.IgnoreBAL = true, true, false

	ctx := context.Background()
	logger := log.New()
	logger.SetHandler(log.DiscardHandler())
	db, tx, doms := setupStepTest(t)
	in := make(chan applyResult, 64)
	out := make(chan commitmentResult, 64)
	cc, err := newCommitmentCalculator(ctx, ctx, doms, db, &chain.Config{}, "test", logger, false, 1<<62, in, nil, out)
	require.NoError(t, err)
	defer cc.Stop()
	p := cc.state.branchPrefetch
	require.NotNil(t, p)
	cc.hasFirstBlock, cc.firstBlockNum = true, 1

	addr := common.Address{19: 1}
	acc := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(5), CodeHash: accounts.EmptyCodeHash}
	require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, addr[:], accounts.SerialiseV3(&acc), 1, nil))
	cc.handleBlockRequest(ctx, &blockRequest{
		blockNum:   1,
		firstTxNum: 1,
		lastTxNum:  2,
		stateRoot:  common.Hash{0xde, 0xad},
		bal: types.BlockAccessList{{
			Address:        addr,
			BalanceChanges: []*types.BalanceChange{{Index: 0, Value: *uint256.NewInt(5)}},
			NonceChanges:   []*types.NonceChange{{Index: 0, Value: 1}},
		}},
	})

	require.NotZero(t, p.hits.Load()+p.misses.Load(), "compute-ahead must read commitment records through the prefetch map")
}

func TestRaiseGCPercentRestores(t *testing.T) {
	prev := debug.SetGCPercent(150)
	defer debug.SetGCPercent(prev)
	restore := raiseGCPercent()
	require.Equal(t, computeGCPercent, debug.SetGCPercent(computeGCPercent))
	restore()
	require.Equal(t, 150, debug.SetGCPercent(150))

	debug.SetGCPercent(-1)
	raiseGCPercent()()
	require.Equal(t, -1, debug.SetGCPercent(-1))
}

func TestRaiseGCPercentOverlappingComputesRestore(t *testing.T) {
	prev := debug.SetGCPercent(150)
	defer debug.SetGCPercent(prev)
	first, second := raiseGCPercent(), raiseGCPercent()
	first()
	require.Equal(t, computeGCPercent, debug.SetGCPercent(computeGCPercent), "a compute still running keeps the raised percent")
	second()
	require.Equal(t, 150, debug.SetGCPercent(150), "the last compute to finish restores the percent the first one saw")
}
