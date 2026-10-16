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
	"fmt"
	"runtime/debug"
	"sort"
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
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	pbt "github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/internal/commitmenttest"
)

func TestBranchPrefetchRejectsChangedRecordBytes(t *testing.T) {
	p := newPrefetchRecords()
	key := []byte{0x08}
	data := []byte{0x01, 0x02}
	rk := prefetchRecordKey(kv.CommitmentDomain, key)
	p.shard(rk).records[rk] = prefetchedRecord{
		data: bytes.Clone(data),
		refs: &commitment.LeafRefs{Mask: 1, Refs: make([][32]byte, 1)},
	}
	changed := bytes.Clone(data)
	changed[0]++
	require.Nil(t, p.leafRefs(key, changed))
}

func TestBranchPrefetchBudgetChargesCompleteLeafRefs(t *testing.T) {
	p := newTestBranchPrefetcher()
	ops := make([]pbt.Op, 16)
	for slot := range ops {
		ops[slot] = pbt.Op{Key: branchPrefetchCodeKey(byte(slot<<4), 0, byte(slot+1)), Value: branchPrefetchValue(byte(slot + 1))}
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	ctx := newBranchPrefetchTrieContext()
	_, err := pbt.NewTrie(ctx).Process(ops)
	require.NoError(t, err)
	var key []byte
	var data []byte
	var refs *commitment.LeafRefs
	for rawKey, rawData := range ctx.Records {
		candidate := pbt.ComputeLeafRefs([]byte(rawKey), rawData)
		if candidate != nil && len(candidate.PBinInternal) != 0 {
			key, data, refs = []byte(rawKey), rawData, candidate
			break
		}
	}
	require.NotEmpty(t, key)
	require.NotNil(t, refs)
	p.putDomain(kv.CommitmentDomain, key, data, 1)
	want := int64(len(key) + len(data) + 32*len(refs.Refs))
	for _, prefix := range refs.Prefixes {
		want += int64(len(prefix))
	}
	for i := range refs.PBinInternal {
		ref := &refs.PBinInternal[i]
		want += int64(2 + 2 + 2 + 32 + 32 + 32 + len(ref.Prefix))
	}
	require.Equal(t, want, p.bytes.Load())
}

func TestBranchPrefetchFoldRejectsStaleRecordRefs(t *testing.T) {
	ops := make([]pbt.Op, 32)
	for slot := range 16 {
		ops[2*slot] = pbt.Op{Key: branchPrefetchCodeKey(byte(slot<<4), 0, 1), Value: branchPrefetchValue(1)}
		ops[2*slot+1] = pbt.Op{Key: branchPrefetchCodeKey(byte(slot<<4), 0, 2), Value: branchPrefetchValue(2)}
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	plain := newBranchPrefetchTrieContext()
	cached := newBranchPrefetchTrieContext()
	plainTrie := pbt.NewTrie(plain)
	cachedTrie := pbt.NewTrie(cached)
	_, err := plainTrie.Process(ops)
	require.NoError(t, err)
	cached.Records = cloneBranchPrefetchRecords(plain.Records)
	p := newTestBranchPrefetcher()
	for key, data := range cached.Records {
		p.putDomain(kv.CommitmentDomain, []byte(key), data, 1)
	}
	cached.prefetcher = p
	insert := pbt.Op{Key: branchPrefetchCodeKey(1, 0, 3), Value: branchPrefetchValue(3)}
	update := pbt.Op{Key: branchPrefetchCodeKey(0xf0, 0, 1), Value: branchPrefetchValue(9)}
	_, err = plainTrie.Process([]pbt.Op{insert})
	require.NoError(t, err)
	_, err = cachedTrie.Process([]pbt.Op{insert})
	require.NoError(t, err)
	plainRoot, err := plainTrie.Process([]pbt.Op{update})
	require.NoError(t, err)
	cachedRoot, err := cachedTrie.Process([]pbt.Op{update})
	require.NoError(t, err)
	require.Equal(t, plainRoot, cachedRoot)
	require.Equal(t, plain.Records, cached.Records)
}

func newTestBranchPrefetcher() *branchPrefetcher {
	p := newPrefetchRecords()
	p.bin = map[kv.Domain]bool{kv.CommitmentDomain: true}
	return p
}

type branchPrefetchTrieContext struct {
	commitmenttest.MapBranchStore
	prefetcher *branchPrefetcher
}

func newBranchPrefetchTrieContext() *branchPrefetchTrieContext {
	return &branchPrefetchTrieContext{MapBranchStore: commitmenttest.NewMapBranchStore()}
}

func (c *branchPrefetchTrieContext) Account([]byte) (*commitment.Update, error) {
	return nil, fmt.Errorf("unexpected account read")
}

func (c *branchPrefetchTrieContext) Storage([]byte) (*commitment.Update, error) {
	return nil, fmt.Errorf("unexpected storage read")
}

func (c *branchPrefetchTrieContext) LeafRefs(key, data []byte) *commitment.LeafRefs {
	if c.prefetcher == nil {
		return nil
	}
	return c.prefetcher.leafRefsDomain(kv.CommitmentDomain, key, data)
}

func cloneBranchPrefetchRecords(records map[string][]byte) map[string][]byte {
	clone := make(map[string][]byte, len(records))
	for key, data := range records {
		clone[key] = bytes.Clone(data)
	}
	return clone
}

func branchPrefetchCodeKey(first, second, seed byte) []byte {
	key := make([]byte, eip8297.CodeKeyLength)
	key[0] = eip8297.CodeZone
	key[1] = first
	key[2] = second
	key[len(key)-1] = seed
	return key
}

func branchPrefetchValue(seed byte) [eip8297.ValueLength]byte {
	var value [eip8297.ValueLength]byte
	value[len(value)-1] = seed
	return value
}

func TestPrefetchedBranchesYieldToMemBatch(t *testing.T) {
	_, tx, doms := setupStepTest(t)
	p := newPrefetchRecords()
	r := &asOfStateReader{sd: doms, roTx: tx, commitmentDomain: kv.CommitmentDomain, prefetched: p}

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

func TestBranchPrefetcherChunkStopsOnPause(t *testing.T) {
	p := &branchPrefetcher{work: make(chan prefetchItem, 2*branchPrefetchPerTx)}
	for i := range 2 * branchPrefetchPerTx {
		p.add(prefetchItem{account: common.Hash{byte(i), byte(i >> 8)}})
	}
	touch := func(prefetchItem) {}
	require.Equal(t, branchPrefetchPerTx, p.walkChunk(<-p.work, touch), "an unpaused worker walks a full chunk")
	p.pausing.Store(true)
	require.Equal(t, 1, p.walkChunk(<-p.work, touch), "a pending pause stops the chunk after the item in hand")
	require.Len(t, p.work, branchPrefetchPerTx-1)
}

func TestParentRecordsOnlyForOneVerifiedChildOfTheReadHead(t *testing.T) {
	p := newPrefetchRecords()
	p.put([]byte{0x12}, []byte("parent-record"), 4)
	head := common.Hash{9}
	child := commitTarget{blockNum: 10, blockHash: common.Hash{10}, parentHash: head}
	calc := func() *commitmentCalculator {
		return &commitmentCalculator{state: &calcState{branchPrefetch: p}, readAtNum: 9, readAtHash: head}
	}

	cc := calc()
	require.Nil(t, cc.parentRecords(), "nothing verified")
	cc.noteVerified(child, nil)
	r := cc.parentRecords()
	require.NotNil(t, r)
	require.Equal(t, child.blockHash, r.BlockHash)
	data, step, ok := r.Get([]byte{0x12})
	require.True(t, ok)
	require.Equal(t, []byte("parent-record"), data)
	require.Equal(t, kv.Step(4), step)

	cc = calc()
	cc.noteVerified(commitTarget{blockNum: 10, blockHash: common.Hash{10}, parentHash: common.Hash{8}}, nil)
	require.Nil(t, cc.parentRecords(), "a block whose parent is not the head the records were read at")

	cc = calc()
	cc.noteVerified(child, nil)
	cc.noteVerified(commitTarget{blockNum: 11, blockHash: common.Hash{11}, parentHash: child.blockHash}, nil)
	require.Nil(t, cc.parentRecords(), "a calculator that verified two blocks")
}

type carryDB struct{ kv.TemporalRoDB }

func TestBranchCarryAdoptsVerifiedDeltasOnlyAtItsHead(t *testing.T) {
	t.Cleanup(dropBranchCarry)
	db := &carryDB{}
	p := newPrefetchRecords()
	p.block = 10
	p.put([]byte{0x01}, []byte("kept"), 3)
	p.put([]byte{0x02}, []byte("old"), 3)
	head := commitTarget{blockNum: 10, blockHash: common.Hash{10}}
	deltas := [][]commitment.BranchDelta{{{Key: []byte{0x02}, Data: []byte("new")}, {Key: []byte{0x03}, Data: []byte("born")}}}

	storeBranchCarry(db, p, head, deltas, 5)
	require.Nil(t, takeBranchCarry(db, 10, common.Hash{9}), "another block at the carry's height")
	require.Nil(t, takeBranchCarry(db, 10, common.Hash{10}), "a mismatch drops the carry")

	storeBranchCarry(db, p, head, deltas, 5)
	require.Nil(t, takeBranchCarry(&carryDB{}, 10, common.Hash{10}), "another database")

	storeBranchCarry(db, p, head, deltas, 5)
	got := takeBranchCarry(db, 10, common.Hash{10})
	require.Same(t, p, got)
	want := map[byte]string{0x01: "kept", 0x02: "new", 0x03: "born"}
	for _, stage := range []string{"attached", "merged"} {
		for key, w := range want {
			data, _, ok := got.get([]byte{key})
			require.True(t, ok, stage)
			require.Equal(t, w, string(data), stage)
		}
		_, step, _ := got.get([]byte{0x02})
		require.Equal(t, kv.Step(5), step, stage)
		got.mergeOverlay()
	}
	require.Nil(t, got.overlay.Load())
	require.Nil(t, takeBranchCarry(db, 10, common.Hash{10}), "a carry is taken once")
}

func TestBranchCarryEvictsRecordsOlderThanTheWindow(t *testing.T) {
	p := newPrefetchRecords()
	p.block = 1
	p.put([]byte{0x01}, []byte("stale"), 0)
	p.put([]byte{0x03}, []byte("refreshed"), 0)
	p.block = 40
	p.put([]byte{0x02}, []byte("recent"), 0)
	p.put([]byte{0x03}, []byte("refreshed"), 0)
	before := p.bytes.Load()

	ov := map[string]prefetchedRecord{}
	p.overlay.Store(&ov)
	p.overlayBlock = 1 + branchCarryBlocks
	p.mergeOverlay()

	_, _, ok := p.get([]byte{0x01})
	require.False(t, ok, "a record last filled more than the window ago is evicted")
	for _, key := range []byte{0x02, 0x03} {
		_, _, ok := p.get([]byte{key})
		require.True(t, ok)
	}
	require.Equal(t, before-int64(1+len("stale")), p.bytes.Load())
}

func TestParentRecordsStopServingOnceTheCarryMovesOn(t *testing.T) {
	p := newPrefetchRecords()
	key := []byte{0x12}
	p.put(key, []byte("parent"), 1)
	head := common.Hash{9}
	cc := &commitmentCalculator{state: &calcState{branchPrefetch: p}, readAtNum: 9, readAtHash: head}
	cc.noteVerified(commitTarget{blockNum: 10, blockHash: common.Hash{10}, parentHash: head}, nil)
	r := cc.parentRecords()
	data, _, ok := r.Get(key)
	require.True(t, ok)
	require.Equal(t, []byte("parent"), data)

	ov := overlayOf([][]commitment.BranchDelta{{{Key: key, Data: []byte("child")}}}, 10, 2)
	p.overlay.Store(&ov)
	data, _, ok = r.Get(key)
	require.True(t, ok)
	require.Equal(t, []byte("parent"), data, "an attached overlay does not reach block 10's witness")
	p.mergeOverlay()
	_, _, ok = r.Get(key)
	require.False(t, ok, "records handed to block 10's witness stop serving once the carry holds block 10's own writes")
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
		{account: hash(balanceOnly[:]), address: balanceOnly},
		{account: hash(storageOnly[:]), address: storageOnly},
		{account: hash(storageOnly[:]), slot: hash(sa[:]), address: storageOnly, plainSlot: sa, storage: true},
		{account: hash(storageOnly[:]), slot: hash(sb[:]), address: storageOnly, plainSlot: sb, storage: true},
	}
	var got []prefetchItem
	for len(p.work) > 0 {
		got = append(got, <-p.work)
	}
	require.ElementsMatch(t, want, got, "BAL writes queue their account and slot walks; storage-only accounts get the account walk, read-only accounts get nothing")
}

func TestHandleBlockRequestQueuesBinaryBALKeys(t *testing.T) {
	previousIgnoreBAL := dbg.IgnoreBAL
	t.Cleanup(func() { dbg.IgnoreBAL = previousIgnoreBAL })
	dbg.IgnoreBAL = false

	_, tx, _ := setupStepTest(t)
	p := newPrefetchRecords()
	p.work = make(chan prefetchItem, 16)
	p.bin = map[kv.Domain]bool{kv.CommitmentDomain: true}
	cc := &commitmentCalculator{
		state:         &calcState{branchPrefetch: p},
		pending:       map[uint64]*pendingBlock{},
		computedAhead: map[uint64]bool{},
		balRoots:      map[uint64][]byte{},
		hasFirstBlock: true,
		firstBlockNum: 100,
	}
	address := common.Address{0x46}
	slot := accounts.InternKey(common.Hash{0x80})
	code := bytes.Repeat([]byte{0x60}, eip8297.ChunkDataLen+1)
	cc.handleBlockRequest(context.Background(), &blockRequest{blockNum: 5, bal: types.BlockAccessList{{
		Address: address,
		StorageChanges: []types.SlotChanges{{
			Slot:    slot,
			Changes: []*types.StorageChange{{Index: 0, Value: *uint256.NewInt(1)}},
		}},
		CodeChanges: []*types.CodeChange{{Index: 0, Bytecode: code}},
	}}})

	for len(p.work) > 0 {
		p.touch(tx, <-p.work)
	}

	plainSlot := slot.Value()
	storageKey := eip8297.TreeKeyStorage(address[:], plainSlot[:])
	storagePath := eip8297.PathFromBits(storageKey, 268)
	storageRowKey, err := pbt.EncodeRowKey(&storagePath)
	require.NoError(t, err)
	_, _, ok := p.getDomain(kv.CommitmentDomain, storageRowKey)
	require.True(t, ok, "BAL storage write must prefetch its binary storage row")

	codeHash := keccak.Sum256(code)
	codeKey := eip8297.TreeKeyCodeChunk(common.BytesToHash(codeHash[:]), 0)
	codePath := eip8297.PathFromBits(codeKey, 268)
	codeRowKey, err := pbt.EncodeRowKey(&codePath)
	require.NoError(t, err)
	_, _, ok = p.getDomain(kv.CommitmentDomain, codeRowKey)
	require.True(t, ok, "BAL code change must prefetch code chunk zero")
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

func TestBranchPrefetchUsesBinaryTreeKeys(t *testing.T) {
	_, tx, _ := setupStepTest(t)
	p := newPrefetchRecords()
	p.bin = map[kv.Domain]bool{kv.CommitmentDomain: true}
	address := [20]byte{0x46}
	slot := [32]byte{0x80}
	p.touch(tx, prefetchItem{address: address, plainSlot: slot, storage: true})

	treeKey := eip8297.TreeKeyStorage(address[:], slot[:])
	path := eip8297.PathFromBits(treeKey, 268)
	rowKey, err := pbt.EncodeRowKey(&path)
	require.NoError(t, err)
	_, _, ok := p.getDomain(kv.CommitmentDomain, rowKey)
	require.True(t, ok)
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
