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
	"encoding/binary"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestPrefetchedBranchesYieldToMemBatch(t *testing.T) {
	_, tx, doms := setupStepTest(t)
	p := exec.NewBranchPrefetcher(context.Background(), nil)
	r := &asOfStateReader{sd: doms, roTx: tx, prefetched: p}

	flushed, untouched, absent := []byte{0x40, 0x12, 0x02}, []byte{0x40, 0x34, 0x02}, []byte{0x40, 0x56, 0x02}
	p.Put(flushed, []byte("prefetched-flushed"), 3)
	p.Put(untouched, []byte("prefetched-untouched"), 3)
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
	p.Put(outside, []byte("prefetched-past-unwind"), 3)
	_, maxStep, inMem := doms.GetLatestFromMemory(kv.CommitmentDomain, outside)
	require.False(t, inMem)
	require.NotEqual(t, kv.NoStepBound, maxStep)
	got, _, err = r.Read(kv.CommitmentDomain, outside, 16)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestParentRecordsOnlyForOneVerifiedChildOfTheReadHead(t *testing.T) {
	t.Cleanup(func() { commitmentdb.PublishParentRecords(nil) })
	p := exec.NewBranchPrefetcher(context.Background(), nil)
	p.Put([]byte{0x12}, []byte("parent-record"), 4)
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
	p := exec.NewBranchPrefetcher(context.Background(), nil)
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

	db := &recordingRoDB{}
	p := exec.NewBranchPrefetcher(context.Background(), db)
	t.Cleanup(p.Close)
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

	require.Equal(t, 4, p.Stats().Queued)
	hash := keccak.Sum256
	sa, sb := slotA.Value(), slotB.Value()
	require.ElementsMatch(t, []exec.PathTouch{
		{Account: hash(balanceOnly[:])},
		{Account: hash(storageOnly[:])},
		{Account: hash(storageOnly[:]), Slot: hash(sa[:]), Storage: true},
		{Account: hash(storageOnly[:]), Slot: hash(sb[:]), Storage: true},
	}, cc.touchBatch[:4], "BAL writes queue their account and slot walks at the request; storage-only accounts get the account walk too")
	want := string(v3.StorageNodeKey(hash(storageOnly[:]), nil, nil))
	require.Eventually(t, func() bool { return slices.Contains(db.recorded(), want) }, 10*time.Second, time.Millisecond)
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
	p.Put([]byte{0x40, 0x00}, []byte{0x0f}, 0)
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

type recordingRoDB struct {
	kv.RoDB
	mu     sync.Mutex
	keys   []string
	limit  int
	opened int
}

func (db *recordingRoDB) BeginRo(context.Context) (kv.Tx, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.limit > 0 && db.opened >= db.limit {
		return nil, errors.New("read tx limit")
	}
	db.opened++
	return &recordingTx{db: db}, nil
}

func (db *recordingRoDB) recorded() []string {
	db.mu.Lock()
	defer db.mu.Unlock()
	return slices.Clone(db.keys)
}

type recordingTx struct {
	kv.TemporalTx
	db *recordingRoDB
}

func (tx *recordingTx) GetLatest(_ kv.Domain, key []byte, _ kv.GetLatestOptions) ([]byte, kv.Step, error) {
	tx.db.mu.Lock()
	tx.db.keys = append(tx.db.keys, string(key))
	tx.db.mu.Unlock()
	return nil, 0, nil
}

func (tx *recordingTx) Rollback() {}

func TestCalculatorQueuesAWriteSetAsOneSortedBatch(t *testing.T) {
	p := exec.NewBranchPrefetcher(context.Background(), &recordingRoDB{limit: 1})
	t.Cleanup(p.Close)
	cc := &commitmentCalculator{branchPrefetch: p}
	for i := 3; i >= 1; i-- {
		owner := [32]byte{0: byte(i)}
		cc.queueBranch(owner[:], []byte{31: byte(i)})
	}
	require.Len(t, cc.touchBatch, 3)
	cc.flushBranches()
	require.Empty(t, cc.touchBatch)
	require.True(t, slices.IsSortedFunc(cc.touchBatch[:3], exec.ComparePathTouch), "the batch is sorted before it is handed over")
	require.Equal(t, 3, p.Stats().Queued)
}
