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
	"math"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	_ "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestCalculatorFeedRootMatchesHexPatricia(t *testing.T) {
	roots := make(map[bool][][]byte)
	for _, v3 := range []bool{false, true} {
		t.Run(map[bool]string{false: "hph", true: "v3"}[v3], func(t *testing.T) {
			t.Cleanup(statecfg.SetCommitmentV3(v3))
			roots[v3] = calculatorRoots(t, v3)
		})
	}
	require.Len(t, roots[false], 2)
	require.Equal(t, roots[false], roots[true])
}

func calculatorRoots(t *testing.T, wantFeed bool) [][]byte {
	ctx := context.Background()
	db, tx, doms := setupStepTest(t)
	require.Equal(t, wantFeed, doms.GetCommitmentContext().AcceptsFeed())
	in := make(chan applyResult, 64)
	out := make(chan commitmentResult, 64)
	cc, err := newCommitmentCalculator(ctx, ctx, doms, db, &chain.Config{}, "test", log.New(), false, 1<<62, in, nil, out, state.NewLayeredDomainReader(doms, nil, nil), nil)
	require.NoError(t, err)
	defer cc.Stop()

	rnd := rand.New(rand.NewSource(7))
	addrs := make([]accounts.Address, 24)
	for i := range addrs {
		var a common.Address
		rnd.Read(a[:])
		addrs[i] = accounts.InternAddress(a)
	}
	slots := make([]accounts.StorageKey, 12)
	for i := range slots {
		var k common.Hash
		rnd.Read(k[:])
		slots[i] = accounts.InternKey(k)
	}
	balances := make(map[accounts.Address]uint64)
	put := func(txNum uint64, ver state.Version, ws *wsb, addr accounts.Address, nonce, balance uint64) {
		acc := accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(balance), CodeHash: accounts.EmptyCodeHash}
		av := addr.Value()
		require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, av[:], accounts.SerialiseV3(&acc), txNum, nil))
		ws.nonce(addr, ver, nonce).bal(addr, ver, *uint256.NewInt(balance)).codeHash(addr, ver, accounts.EmptyCodeHash)
		balances[addr] = balance
	}
	store := func(txNum uint64, ver state.Version, ws *wsb, addr accounts.Address, slot accounts.StorageKey, value uint64) {
		av, sv := addr.Value(), slot.Value()
		v := uint256.NewInt(value)
		require.NoError(t, doms.DomainPut(kv.StorageDomain, tx, append(av[:], sv[:]...), v.Bytes(), txNum, nil))
		ws.stor(addr, slot, ver, *v)
	}
	var roots [][]byte
	txNum := uint64(0)
	for block := uint64(1); block <= 3; block++ {
		vm := newRawViewVM(txNum + 1)
		for i := range 9 {
			txNum++
			ver := state.Version{TxIndex: i}
			ws := newWS()
			for range 3 {
				addr := addrs[rnd.Intn(len(addrs))]
				put(txNum, ver, ws, addr, txNum, uint64(rnd.Intn(1000)+1))
				for range 2 {
					store(txNum, ver, ws, addr, slots[rnd.Intn(len(slots))], uint64(rnd.Intn(3)))
				}
			}
			cc.handleMessage(ctx, recordTx(vm, block, common.Hash{byte(block)}, txNum, &chain.Rules{}, ws.build()))
		}
		cc.handleMessage(ctx, newTestBlockResult(block, common.Hash{byte(block)}, txNum, false))
		if block == 2 || block == 3 {
			cc.handleMessage(ctx, &commitComputeRequest{})
			res := <-out
			require.NotEmpty(t, res.rootHash)
			roots = append(roots, res.rootHash)
		}
	}
	return roots
}

func TestEmitTouchedFeedCarriesWhatEmitTouchedEmits(t *testing.T) {
	commitmentdb.WantParentReads(true)
	t.Cleanup(func() { commitmentdb.WantParentReads(false) })
	const txIdx = 3
	ver := state.Version{TxIndex: txIdx}
	a := func(b byte) accounts.Address { return accounts.InternAddress([20]byte{b}) }
	k := func(b byte) accounts.StorageKey { return accounts.InternKey([32]byte{b}) }
	ws := newWS().
		bal(a(1), ver, *uint256.NewInt(100)).nonce(a(1), ver, 7).
		bal(a(3), ver, *uint256.NewInt(5)).nonce(a(3), ver, 1).
		stor(a(3), k(1), ver, *uint256.NewInt(0x1234)).stor(a(3), k(2), ver, uint256.Int{}).
		emptyRemoval(a(4), ver).
		selfDestruct(a(5), ver, true).bal(a(5), ver, *uint256.NewInt(3)).nonce(a(5), ver, 1).
		stor(a(5), k(3), ver, *uint256.NewInt(7)).
		build()
	vm := state.NewVersionMap(nil)
	vm.FlushVersionedWrites(ws, true)
	view := func() *state.VersionMapView { return vm.View(txIdx+1, nil) }

	plain := &commitmentCalculator{updates: newTestUpdates()}
	plain.accumulateTouched(state.NewVersionMapWriteView(ws, vm, txIdx), nil)
	updates := newTestUpdates()
	require.NoError(t, plain.emitTouched(view(), updates))
	want := map[string]commitment.Update{}
	for key, u := range collectUpdates(t, updates) {
		hash := crypto.Keccak256([]byte(key[:20]))
		if len(key) > 20 {
			hash = append(hash, crypto.Keccak256([]byte(key[20:]))...)
		}
		want[string(hash)] = u
	}

	cc := &commitmentCalculator{branchPrefetch: newPrefetchRecords()}
	cc.accumulateTouched(state.NewVersionMapWriteView(ws, vm, txIdx), nil)
	require.NoError(t, cc.emitTouchedFeed(view()))
	feed := cc.feed.Feed
	got := map[string]commitment.Update{}
	distinct := map[[32]byte]struct{}{}
	for _, account := range feed.Accounts {
		distinct[account.Hash] = struct{}{}
		got[string(account.Hash[:])] = *account.Update
		for _, slot := range account.Slots {
			u := commitment.Update{Flags: commitment.DeleteUpdate}
			if len(slot.Value) != 0 {
				u = commitment.Update{Flags: commitment.StorageUpdate, StorageLen: int8(len(slot.Value))}
				copy(u.Storage[:], slot.Value)
			}
			got[string(append(account.Hash[:], slot.Hash[:]...))] = u
		}
	}
	require.Len(t, want, 7)
	require.Len(t, distinct, len(feed.Accounts))
	require.Equal(t, len(want), feed.Keys)
	require.Equal(t, want, got)
}

func TestComputeAheadFeedRootMatchesHexPatricia(t *testing.T) {
	roots := make(map[bool][]byte)
	for _, v3 := range []bool{false, true} {
		t.Run(map[bool]string{false: "hph", true: "v3"}[v3], func(t *testing.T) {
			t.Cleanup(statecfg.SetCommitmentV3(v3))
			roots[v3] = computeAheadRoot(t, v3)
		})
	}
	require.NotEmpty(t, roots[false])
	require.Equal(t, roots[false], roots[true])
}

func computeAheadRoot(t *testing.T, wantFeed bool) []byte {
	ctx := context.Background()
	db, tx, doms := setupStepTest(t)
	in := make(chan applyResult, 64)
	out := make(chan commitmentResult, 64)
	cc, err := newCommitmentCalculator(ctx, ctx, doms, db, &chain.Config{}, "test", log.New(), false, 1<<62, in, nil, out, state.NewLayeredDomainReader(doms, nil, nil), nil)
	require.NoError(t, err)
	defer cc.Stop()

	var bal types.BlockAccessList
	for i := range 8 {
		addr := common.Address{19: byte(i + 1)}
		acc := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(uint64(i + 1)), CodeHash: accounts.EmptyCodeHash}
		require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, addr[:], accounts.SerialiseV3(&acc), 1, nil))
		ac := types.AccountChanges{
			Address:        addr,
			BalanceChanges: []*types.BalanceChange{{Index: 0, Value: *uint256.NewInt(uint64(100 + i))}},
			NonceChanges:   []*types.NonceChange{{Index: 0, Value: 2}},
		}
		for j := range i % 3 {
			slot := accounts.InternKey(common.Hash{0: byte(i), 31: byte(j + 1)})
			ac.StorageChanges = append(ac.StorageChanges, types.SlotChanges{Slot: slot, Changes: []*types.StorageChange{{Index: 0, Value: *uint256.NewInt(uint64(j + 7))}}})
		}
		bal = append(bal, ac)
	}
	const queued = 3
	if wantFeed {
		cc.branchPrefetch.close()
		p := newPrefetchRecords()
		p.work = make(chan prefetchItem, 64)
		cc.branchPrefetch, cc.asOfReader.prefetched = p, p
		for range queued {
			p.add(prefetchItem{})
		}
	}
	req := &blockRequest{blockNum: 2, firstTxNum: 2, lastTxNum: 3, bal: bal}
	vm := state.NewVersionMap(req.bal)
	vm.SetBaseTxNum(req.firstTxNum)
	rh, _, err := cc.computeRootFromBAL(ctx, req, vm, math.MaxUint32, commitTarget{blockNum: req.blockNum, lastTxNum: req.lastTxNum})
	require.NoError(t, err)
	require.Equal(t, wantFeed, cc.feed.Keys > 0, "v3 compute-ahead folds through the feed")
	if wantFeed {
		require.Equal(t, uint64(queued), cc.branchPrefetch.drained.Load(), "compute-ahead pauses the prefetcher like a block compute, and queues nothing the BAL request already queued")
	}
	return rh
}
