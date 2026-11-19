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
	"encoding/binary"
	"testing"
	"time"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestPrefetchedBranchesYieldToMemBatch(t *testing.T) {
	_, tx, doms := setupStepTest(t)
	p := newPrefetchRecords()
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

	outside := []byte{0x40, 0x78, 0x02}
	var unwound [kv.DomainLen][]kv.DomainEntryDiff
	unwound[kv.CommitmentDomain] = []kv.DomainEntryDiff{{Key: string(outside) + string(binary.BigEndian.AppendUint64(nil, ^uint64(2)))}}
	doms.Unwind(32, &unwound)
	p.put(outside, []byte("prefetched-past-unwind"), 3)
	_, maxStep, inMem := doms.GetLatestFromMemory(kv.CommitmentDomain, outside)
	require.False(t, inMem)
	require.NotEqual(t, kv.NoStepBound, maxStep)
	got, _, err = r.Read(kv.CommitmentDomain, outside, 16)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestBranchPrefetcherStopsAtByteCap(t *testing.T) {
	p := newPrefetchRecords()
	p.put([]byte("kept"), []byte("value"), 0)
	p.bytes.Store(branchPrefetchMaxBytes)
	p.put([]byte("overflow"), []byte("dropped"), 0)
	_, _, ok := p.get([]byte("overflow"))
	require.False(t, ok)
	value, _, ok := p.get([]byte("kept"))
	require.True(t, ok)
	require.Equal(t, []byte("value"), value)
}

func TestBranchPrefetcherLeafRefsOnlyForTheStoredRecord(t *testing.T) {
	p := newPrefetchRecords()
	key, data, refs := []byte{0x42, 0x00}, []byte{1, 2, 3}, &commitment.LeafRefs{Mask: 1}
	p.shard(key).records[string(key)] = prefetchedRecord{data: data, refs: refs}
	require.Same(t, refs, p.leafRefs(key, data))
	require.Nil(t, p.leafRefs(key, bytes.Clone(data)))
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

func TestBranchPrefetcherChunkStopsOnPause(t *testing.T) {
	p := &branchPrefetcher{work: make(chan prefetchItem, 2*branchPrefetchPerTx)}
	for i := range 2 * branchPrefetchPerTx {
		p.add(prefetchItem{account: common.Hash{byte(i), byte(i >> 8)}})
	}
	read := func([]byte) []byte { return nil }
	require.Equal(t, branchPrefetchPerTx, p.walkChunk(<-p.work, read), "an unpaused worker walks a full chunk")
	p.pausing.Store(true)
	require.Equal(t, 1, p.walkChunk(<-p.work, read), "a pending pause stops the chunk after the item in hand")
	require.Len(t, p.work, branchPrefetchPerTx-1)
}

func TestParentRecordsOnlyForOneVerifiedChildOfTheReadHead(t *testing.T) {
	t.Cleanup(func() { commitmentdb.PublishParentRecords(nil) })
	p := newPrefetchRecords()
	p.put([]byte{0x12}, []byte("parent-record"), 4)
	head := common.Hash{9}
	child := commitTarget{blockNum: 10, blockHash: common.Hash{10}, parentHash: head}
	calc := func() *commitmentCalculator {
		return &commitmentCalculator{branchPrefetch: p, readAtNum: 9, readAtHash: head}
	}

	cc := calc()
	require.Nil(t, cc.parentRecords(), "nothing verified")
	cc.noteVerified(child)
	r := cc.parentRecords()
	require.NotNil(t, r)
	require.Equal(t, child.blockHash, r.BlockHash)
	data, step, ok := r.Get([]byte{0x12})
	require.True(t, ok)
	require.Equal(t, []byte("parent-record"), data)
	require.Equal(t, kv.Step(4), step)

	cc = calc()
	cc.noteVerified(commitTarget{blockNum: 10, blockHash: common.Hash{10}, parentHash: common.Hash{8}})
	require.Nil(t, cc.parentRecords(), "a block whose parent is not the head the records were read at")

	cc = calc()
	cc.noteVerified(child)
	cc.noteVerified(commitTarget{blockNum: 11, blockHash: common.Hash{11}, parentHash: child.blockHash})
	require.Nil(t, cc.parentRecords(), "a calculator that verified two blocks")

	t.Cleanup(statecfg.SetCommitmentV3(true))
	previous := commitmentdb.ParentReadsWanted()
	commitmentdb.WantParentReads(true)
	t.Cleanup(func() { commitmentdb.WantParentReads(previous) })
	ctx := t.Context()
	db, _, doms := setupStepTest(t)
	out := make(chan commitmentResult, 1)
	cc, err := newCommitmentCalculator(ctx, ctx, doms, db, &chain.Config{}, "test", log.New(), false, 0, make(chan applyResult), nil, out, state.NewLayeredDomainReader(doms, nil, nil), nil)
	require.NoError(t, err)
	defer cc.Stop()
	cc.readAtNum, cc.readAtHash = 9, head
	child.stateRoot = common.Hash{0xde, 0xad}
	doms.SavePastChangesetAccumulator(child.blockHash, child.blockNum, &changeset.StateChangeSet{})
	cc.compute(ctx, child, computeMode{checkRoot: true})
	require.ErrorIs(t, (<-out).err, ErrWrongTrieRoot)
	require.Nil(t, cc.parentRecords(), "a wrong root is never verified")
}

func TestStopDoesNotPublishParentRecordsWhenNotWanted(t *testing.T) {
	previous := commitmentdb.ParentReadsWanted()
	commitmentdb.WantParentReads(false)
	t.Cleanup(func() { commitmentdb.WantParentReads(previous) })
	p := newPrefetchRecords()
	p.work = make(chan prefetchItem, 1)
	cc := &commitmentCalculator{
		branchPrefetch: p,
		done:           make(chan struct{}),
		readAtNum:      9,
		readAtHash:     common.Hash{9},
		logger:         log.New(),
		handoff:        &commitTarget{blockNum: 10, blockHash: common.Hash{10}, parentHash: common.Hash{9}},
	}

	cc.Stop()
	require.Nil(t, commitmentdb.ParentRecordsFor(10, common.Hash{10}, common.Hash{9}))
}

func TestHandleBlockRequestQueuesBALWrites(t *testing.T) {
	defer func(prev bool) { dbg.IgnoreBAL = prev }(dbg.IgnoreBAL)
	dbg.IgnoreBAL = false

	p := &branchPrefetcher{work: make(chan prefetchItem, 16)}
	cc := &commitmentCalculator{
		branchPrefetch: p,
		balBlock:       true,
		signalCtx:      context.Background(),
		pending:        map[uint64]*pendingBlock{},
		computedAhead:  map[uint64]bool{},
		balRoots:       map[uint64][]byte{},
		hasFirstBlock:  true,
		firstBlockNum:  100,
	}
	balanceOnly, storageOnly, readOnly := common.Address{19: 1}, common.Address{19: 2}, common.Address{19: 3}
	slotA, slotB := accounts.InternKey(common.Hash{31: 1}), accounts.InternKey(common.Hash{31: 2})
	change := []*types.StorageChange{{Index: 1, Value: *uint256.NewInt(7)}}
	bal := types.BlockAccessList{
		{Address: balanceOnly, BalanceChanges: []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(1)}}},
		{Address: storageOnly, StorageChanges: []types.SlotChanges{{Slot: slotA, Changes: change}, {Slot: slotB, Changes: change}}},
		{Address: readOnly, StorageReads: []common.Hash{slotA.Value()}},
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
	require.ElementsMatch(t, want, got, "BAL writes queue their account and slot walks at the request, whatever block came before; storage-only accounts get the account walk, read-only accounts get nothing")
}

func TestComputeAheadReadsPrefetchedBranches(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(true))
	defer func(v bool) { dbg.BALDrivenCommitment = v }(dbg.BALDrivenCommitment)
	defer func(v bool) { dbg.IgnoreBAL = v }(dbg.IgnoreBAL)
	dbg.BALDrivenCommitment, dbg.IgnoreBAL = true, false

	ctx := context.Background()
	logger := log.New()
	logger.SetHandler(log.DiscardHandler())
	db, tx, doms := setupStepTest(t)
	in := make(chan applyResult, 64)
	out := make(chan commitmentResult, 64)
	cc, err := newCommitmentCalculator(ctx, ctx, doms, db, &chain.Config{}, "test", logger, false, 1<<62, in, nil, out, state.NewLayeredDomainReader(doms, nil, nil), nil)
	require.NoError(t, err)
	defer cc.Stop()
	p := cc.branchPrefetch
	require.NotNil(t, p)
	cc.hasFirstBlock, cc.firstBlockNum = true, 1

	addr := common.Address{19: 1}
	acc := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(5), CodeHash: accounts.EmptyCodeHash}
	require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, addr[:], accounts.SerialiseV3(&acc), 1, nil))
	p.put([]byte{0x40, 0x00}, []byte{0x0f}, 0)
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

	require.Len(t, out, 1)
	require.ErrorIs(t, (<-out).err, v3.ErrRecordFormat, "compute-ahead must read commitment records through the prefetch map")
}

func TestBranchPrefetcherDrainSortedWaitsForItemsTakenByWorkers(t *testing.T) {
	p := newPrefetchRecords()
	p.work = make(chan prefetchItem, 8)
	p.add(prefetchItem{account: [32]byte{1}})
	<-p.work
	done := make(chan struct{})
	go func() {
		p.drainSorted()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("drainSorted returned with an item in flight")
	case <-time.After(50 * time.Millisecond):
	}
	p.done(1)
	<-done
	require.Zero(t, p.drainTimeout.Load())
}

func TestBranchPrefetcherDrainSortedWalksTheBacklog(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()), temporaltest.WithStepSize(16))
	p := newBranchPrefetcher(context.Background(), db)
	defer p.close()
	p.gate.Lock()
	for i := range 600 {
		p.add(prefetchItem{account: [32]byte{0: byte(i), 1: byte(i >> 8)}})
	}
	p.gate.Unlock()
	p.drainSorted()
	require.Empty(t, p.work)
	require.Zero(t, p.pending.Load())
	require.EqualValues(t, 600, p.queued.Load())
}

func TestBranchPrefetcherQueuesRepeatsWithoutAFollower(t *testing.T) {
	p := newPrefetchRecords()
	p.work = make(chan prefetchItem, 8)
	p.add(prefetchItem{account: [32]byte{1}})
	p.add(prefetchItem{account: [32]byte{1}})
	require.Len(t, p.work, 2)
}

func TestBranchPrefetcherQueuesADroppedItemAgain(t *testing.T) {
	p := newPrefetchRecords()
	p.dedupe = true
	p.work = make(chan prefetchItem, 1)
	p.add(prefetchItem{account: [32]byte{1}})
	p.add(prefetchItem{account: [32]byte{2}})
	require.Equal(t, prefetchItem{account: [32]byte{1}}, <-p.work)
	p.add(prefetchItem{account: [32]byte{2}})
	require.Len(t, p.work, 1, "an item the full queue dropped must be queued when it is touched again")
}

func TestBranchPrefetcherForwardsAQueuePublishedJustBeforeStop(t *testing.T) {
	for i := range 30 {
		p := newPrefetchRecords()
		p.work = make(chan prefetchItem, 8)
		p.blockNum, p.parentHash = uint64(200+i), common.Hash{4}
		p.follow()
		time.Sleep(200 * time.Microsecond)
		touches := &commitmentdb.PreExecTouches{BlockNum: p.blockNum, ParentHash: p.parentHash, C: make(chan []commitmentdb.PathTouch, 2)}
		touches.C <- []commitmentdb.PathTouch{{Account: [32]byte{1}}}
		commitmentdb.PublishPreExecTouches(touches)
		p.stopFollow()
		require.Len(t, p.work, 1)
	}
}

func TestBranchPrefetcherReportsLossesOfItsOwnQueue(t *testing.T) {
	p := newPrefetchRecords()
	p.work = make(chan prefetchItem, 8)
	p.blockNum, p.parentHash = 11, common.Hash{5}
	own := &commitmentdb.PreExecTouches{BlockNum: 11, ParentHash: common.Hash{5}, C: make(chan []commitmentdb.PathTouch, 1)}
	own.Dropped.Store(3)
	commitmentdb.PublishPreExecTouches(own)
	p.follow()
	p.stopFollow()
	commitmentdb.PublishPreExecTouches(&commitmentdb.PreExecTouches{BlockNum: 12, ParentHash: common.Hash{6}})
	dropped, _ := p.preExecLost()
	require.EqualValues(t, 3, dropped)
}

func TestBranchPrefetcherQueuesAnItemOncePerFold(t *testing.T) {
	p := newPrefetchRecords()
	p.dedupe = true
	p.work = make(chan prefetchItem, 8)
	p.add(prefetchItem{account: [32]byte{1}})
	p.add(prefetchItem{account: [32]byte{1}})
	require.Len(t, p.work, 1)
	require.EqualValues(t, 1, p.deduped.Load())
	p.pause()
	p.resume()
	p.add(prefetchItem{account: [32]byte{1}})
	require.Len(t, p.work, 1, "an item discarded at the pause is queued again for the next fold")
}

func TestBranchPrefetcherFoldGetCountsHitsAndMisses(t *testing.T) {
	p := newPrefetchRecords()
	p.put([]byte{1}, []byte{2}, 0)
	_, _, ok := p.foldGet([]byte{1})
	require.True(t, ok)
	_, _, ok = p.foldGet([]byte{3})
	require.False(t, ok)
	require.EqualValues(t, 1, p.foldHits.Load())
	require.EqualValues(t, 1, p.foldMisses.Load())
}

func TestBranchPrefetcherForwardsBatchesBufferedAtStop(t *testing.T) {
	for i := range 30 {
		p := newPrefetchRecords()
		p.work = make(chan prefetchItem, 8)
		p.blockNum, p.parentHash = uint64(100+i), common.Hash{2}
		touches := &commitmentdb.PreExecTouches{BlockNum: p.blockNum, ParentHash: p.parentHash, C: make(chan []commitmentdb.PathTouch, 2)}
		touches.C <- []commitmentdb.PathTouch{{Account: [32]byte{1}}}
		commitmentdb.PublishPreExecTouches(touches)
		p.stopOnce.Do(func() { close(p.stop) })
		p.follow()
		p.stopFollow()
		require.Len(t, p.work, 1)
		require.EqualValues(t, 1, p.followed.Load())
	}
}

func TestBranchPrefetcherFollowsARepublishedQueue(t *testing.T) {
	p := newPrefetchRecords()
	p.work = make(chan prefetchItem, 8)
	p.blockNum, p.parentHash = 9, common.Hash{3}
	commitmentdb.PublishPreExecTouches(&commitmentdb.PreExecTouches{BlockNum: 9, ParentHash: common.Hash{3}, C: make(chan []commitmentdb.PathTouch, 1)})
	p.follow()
	defer p.stopFollow()
	time.Sleep(20 * time.Millisecond)
	retry := &commitmentdb.PreExecTouches{BlockNum: 9, ParentHash: common.Hash{3}, C: make(chan []commitmentdb.PathTouch, 1)}
	retry.C <- []commitmentdb.PathTouch{{Account: [32]byte{5}}}
	commitmentdb.PublishPreExecTouches(retry)
	require.Eventually(t, func() bool { return len(p.work) == 1 }, time.Second, time.Millisecond)
}

func TestBranchPrefetcherFollowsPreExecTouchesUntilStopped(t *testing.T) {
	p := newPrefetchRecords()
	p.work = make(chan prefetchItem, 64)
	p.blockNum, p.parentHash = 7, common.Hash{1}
	touches := &commitmentdb.PreExecTouches{BlockNum: 7, ParentHash: common.Hash{1}, C: make(chan []commitmentdb.PathTouch, 2)}
	commitmentdb.PublishPreExecTouches(touches)
	p.follow()
	touches.C <- []commitmentdb.PathTouch{{Account: [32]byte{1}}, {Account: [32]byte{2}, Slot: [32]byte{3}, Storage: true}}
	require.Eventually(t, func() bool { return len(p.work) == 2 }, time.Second, time.Millisecond)
	require.Equal(t, prefetchItem{account: [32]byte{1}}, <-p.work)
	require.Equal(t, prefetchItem{account: [32]byte{2}, slot: [32]byte{3}, storage: true}, <-p.work)
	p.stopFollow()
	touches.C <- []commitmentdb.PathTouch{{Account: [32]byte{4}}}
	time.Sleep(5 * time.Millisecond)
	require.Empty(t, p.work)
}
