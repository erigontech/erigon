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

package exec

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type commitmentRecordingTx struct {
	kv.TemporalTx
	mu      sync.Mutex
	domains []kv.Domain
}

func (tx *commitmentRecordingTx) GetLatest(domain kv.Domain, _ []byte, _ kv.GetLatestOptions) ([]byte, kv.Step, error) {
	tx.mu.Lock()
	tx.domains = append(tx.domains, domain)
	tx.mu.Unlock()
	return nil, 0, nil
}

func (*commitmentRecordingTx) Rollback() {}

func (*commitmentRecordingTx) AggTx() any { return nil }

type commitmentBeginErrorDB struct {
	kv.RoDB
	errs []error
	next atomic.Uint64
}

func (db *commitmentBeginErrorDB) BeginRo(context.Context) (kv.Tx, error) {
	return nil, db.errs[db.next.Add(1)-1]
}

type commitmentBranchLookupTx struct {
	kv.TemporalTx
	data  []byte
	step  kv.Step
	aggTx any
	opts  kv.GetLatestOptions
	calls int
}

func (tx *commitmentBranchLookupTx) GetLatest(_ kv.Domain, _ []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	tx.opts = opts
	tx.calls++
	return tx.data, tx.step, nil
}

func (tx *commitmentBranchLookupTx) AggTx() any {
	return tx.aggTx
}

func (*commitmentBranchLookupTx) Rollback() {}

type commitmentBranchCacheProvider struct {
	cache *commitment.BranchCache
}

func (p commitmentBranchCacheProvider) BranchCache() *commitment.BranchCache {
	return p.cache
}

func TestBALCommitmentWarmupKeysUseChangesOnly(t *testing.T) {
	readOnlyAddress := accounts.InternAddress(common.Address{19: 1})
	accountAddress := accounts.InternAddress(common.Address{19: 2})
	storageAddress := accounts.InternAddress(common.Address{19: 3})
	readSlot := accounts.InternKey(common.Hash{31: 4})
	changedSlot := accounts.InternKey(common.Hash{31: 5})
	bal := types.BlockAccessList{
		{Address: readOnlyAddress, StorageReads: []accounts.StorageKey{readSlot}},
		{Address: accountAddress, BalanceChanges: []*types.BalanceChange{{Value: *uint256.NewInt(1)}}},
		{Address: storageAddress, StorageChanges: []types.SlotChanges{{
			Slot: changedSlot, Changes: []*types.StorageChange{{Value: *uint256.NewInt(2)}},
		}}},
	}

	accountValue := accountAddress.Value()
	storageValue := storageAddress.Value()
	slotValue := changedSlot.Value()
	storageKey := make([]byte, len(storageValue)+len(slotValue))
	copy(storageKey, storageValue[:])
	copy(storageKey[len(storageValue):], slotValue[:])
	want := [][]byte{
		commitment.KeyToHexNibbleHash(accountValue[:]),
		commitment.KeyToHexNibbleHash(storageKey),
	}
	slices.SortFunc(want, bytes.Compare)

	got := balCommitmentWarmupKeys(bal)
	require.Equal(t, want, got)
	require.True(t, slices.IsSortedFunc(got, bytes.Compare))
}

func TestWarmBALCommitmentReadsCommitmentDomain(t *testing.T) {
	tx := new(commitmentRecordingTx)
	db := &singleTxRoDB{tx: tx}
	bal := types.BlockAccessList{{
		Address:        accounts.InternAddress(common.Address{19: 2}),
		BalanceChanges: []*types.BalanceChange{{Value: *uint256.NewInt(1)}},
	}}

	require.NoError(t, warmBALCommitment(t.Context(), db, bal, 2))

	tx.mu.Lock()
	defer tx.mu.Unlock()
	require.NotEmpty(t, tx.domains)
	for _, domain := range tx.domains {
		require.Equal(t, kv.CommitmentDomain, domain)
	}
}

func TestWarmBALCommitmentCollectsWorkerFactoryErrors(t *testing.T) {
	firstErr := errors.New("first factory error")
	secondErr := errors.New("second factory error")
	db := &commitmentBeginErrorDB{errs: []error{firstErr, secondErr}}
	bal := types.BlockAccessList{
		{
			Address:        accounts.InternAddress(common.Address{19: 1}),
			BalanceChanges: []*types.BalanceChange{{Value: *uint256.NewInt(1)}},
		},
		{
			Address:        accounts.InternAddress(common.Address{19: 2}),
			BalanceChanges: []*types.BalanceChange{{Value: *uint256.NewInt(2)}},
		},
	}

	err := warmBALCommitment(t.Context(), db, bal, 2)
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, secondErr)
}

func TestBALCommitmentContextUsesAvailableBranchCache(t *testing.T) {
	key := []byte{0x12, 0x34, 0x56}

	for _, testCase := range []struct {
		name               string
		cacheAvailable     bool
		cacheHit           bool
		wantData           string
		wantStep           kv.Step
		wantCalls          int
		wantBranchCacheOpt bool
		wantHits           uint64
		wantMisses         uint64
		wantFillRequests   uint64
		wantCallbackMisses int
	}{
		{name: "cache unavailable", wantData: "database", wantStep: 9, wantCalls: 1},
		{name: "cache hit", cacheAvailable: true, cacheHit: true, wantData: "cached", wantStep: 7, wantHits: 1},
		{name: "cache miss", cacheAvailable: true, wantData: "database", wantStep: 9, wantCalls: 1, wantBranchCacheOpt: true, wantMisses: 1, wantFillRequests: 1, wantCallbackMisses: 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var cache *commitment.BranchCache
			if testCase.cacheAvailable {
				cache = commitment.NewBranchCache(100)
				defer cache.Close()
				if testCase.cacheHit {
					cache.Put(key, []byte("cached"), 7, 0)
				}
			}
			callbackMisses := 0
			if cache != nil {
				cache.SetMissCallback(func([]byte) { callbackMisses++ })
			}
			tx := &commitmentBranchLookupTx{data: []byte("database"), step: 9}
			stats := new(balCommitmentCacheStats)
			ctx := &balCommitmentContext{tx: tx, cache: cache, cacheStats: stats}

			got, step, err := ctx.Branch(key)
			require.NoError(t, err)
			require.Equal(t, testCase.wantData, string(got))
			require.Equal(t, testCase.wantStep, step)
			require.Equal(t, testCase.wantCalls, tx.calls)
			require.Equal(t, testCase.wantBranchCacheOpt, tx.opts.BranchCache())
			require.Equal(t, testCase.wantHits, stats.hits.Load())
			require.Equal(t, testCase.wantMisses, stats.misses.Load())
			require.Equal(t, testCase.wantFillRequests, stats.fillRequests.Load())
			require.Equal(t, testCase.wantCallbackMisses, callbackMisses)
		})
	}
}

func TestWarmBALCommitmentUsesAvailableBranchCache(t *testing.T) {
	cache := commitment.NewBranchCache(100)
	defer cache.Close()
	tx := &commitmentBranchLookupTx{
		data:  []byte("database"),
		step:  9,
		aggTx: commitmentBranchCacheProvider{cache: cache},
	}
	db := &singleTxRoDB{tx: tx}
	bal := types.BlockAccessList{{
		Address:        accounts.InternAddress(common.Address{19: 2}),
		BalanceChanges: []*types.BalanceChange{{Value: *uint256.NewInt(1)}},
	}}

	require.NoError(t, warmBALCommitment(t.Context(), db, bal, 1))
	require.Positive(t, tx.calls)
	require.True(t, tx.opts.BranchCache())
}
