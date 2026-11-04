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
	"time"

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

type commitmentBeginFuncDB struct {
	kv.RoDB
	begin func(context.Context) (kv.Tx, error)
	calls atomic.Int64
}

func (db *commitmentBeginFuncDB) BeginRo(ctx context.Context) (kv.Tx, error) {
	db.calls.Add(1)
	return db.begin(ctx)
}

func threeAccountChanges() types.BlockAccessList {
	var bal types.BlockAccessList
	for i := range 3 {
		bal = append(bal, types.AccountChanges{
			Address:        common.Address{19: byte(i + 1)},
			BalanceChanges: []*types.BalanceChange{{Value: *uint256.NewInt(1)}},
		})
	}
	return bal
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
		{Address: readOnlyAddress.Value(), StorageReads: []common.Hash{readSlot.Value()}},
		{Address: accountAddress.Value(), BalanceChanges: []*types.BalanceChange{{Value: *uint256.NewInt(1)}}},
		{Address: storageAddress.Value(), StorageChanges: []types.SlotChanges{{
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
		Address:        common.Address{19: 2},
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
			Address:        common.Address{19: 1},
			BalanceChanges: []*types.BalanceChange{{Value: *uint256.NewInt(1)}},
		},
		{
			Address:        common.Address{19: 2},
			BalanceChanges: []*types.BalanceChange{{Value: *uint256.NewInt(2)}},
		},
	}

	err := warmBALCommitment(t.Context(), db, bal, 2)
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, secondErr)
}

// A warmup whose context is already done does not open any read transaction.
func TestWarmBALCommitmentSkipsAnEndedContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	db := &commitmentBeginFuncDB{begin: func(context.Context) (kv.Tx, error) { return nil, errors.New("unexpected BeginRo") }}

	err := warmBALCommitment(ctx, db, threeAccountChanges(), 3)

	require.Equal(t, context.Canceled, err)
	require.Zero(t, db.calls.Load())
}

// A cancellation reaches every worker; it is reported once, not joined per worker.
func TestWarmBALCommitmentReportsACancellationOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	db := &commitmentBeginFuncDB{begin: func(context.Context) (kv.Tx, error) {
		cancel()
		return nil, context.Canceled
	}}

	err := warmBALCommitment(ctx, db, threeAccountChanges(), 3)

	require.Equal(t, context.Canceled, err)
}

// The first worker error cancels the other workers. Their context errors are consequences, so only
// the cause is reported, once.
func TestWarmBALCommitmentReportsAWorkerErrorOnce(t *testing.T) {
	db := &commitmentBeginFuncDB{begin: func(ctx context.Context) (kv.Tx, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, kv.ErrReadTxLimitExceeded
	}}

	err := warmBALCommitment(t.Context(), db, threeAccountChanges(), 3)

	require.Equal(t, int64(3), db.calls.Load())
	require.Equal(t, kv.ErrReadTxLimitExceeded, err)
}

// Workers that fail with errors allocated per call, such as a closed database's, report the
// message once.
func TestWarmBALCommitmentReportsAnErrorMessageOnce(t *testing.T) {
	db := &commitmentBeginFuncDB{begin: func(context.Context) (kv.Tx, error) { return nil, errors.New("db closed") }}

	err := warmBALCommitment(t.Context(), db, threeAccountChanges(), 3)

	require.EqualError(t, err, "db closed")
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
		Address:        common.Address{19: 2},
		BalanceChanges: []*types.BalanceChange{{Value: *uint256.NewInt(1)}},
	}}

	require.NoError(t, warmBALCommitment(t.Context(), db, bal, 1))
	require.Positive(t, tx.calls)
	require.True(t, tx.opts.BranchCache())
}

func TestBranchPrefetchReadsTouchedKeysThroughBranchCache(t *testing.T) {
	cache := commitment.NewBranchCache(100)
	defer cache.Close()
	tx := &commitmentBranchLookupTx{
		data:  []byte("database"),
		step:  9,
		aggTx: commitmentBranchCacheProvider{cache: cache},
	}
	prefetch := StartBranchPrefetch(t.Context(), &singleTxRoDB{tx: tx}, 1)
	address := common.Address{19: 2}
	prefetch.WarmKey(commitment.KeyToHexNibbleHash(address[:]), 0, 0)
	require.NoError(t, prefetch.WaitBufferFree(0))
	prefetch.CloseAndWait()

	require.Positive(t, tx.calls, "a touched key's path is read")
	require.True(t, tx.opts.BranchCache(), "reads fill the shared BranchCache the calculator consults")
}

type coldPagesPrefetcher struct{ coldPerKey int }

func (p coldPagesPrefetcher) PrefetchLatest(_ context.Context, _ kv.Domain, keys [][]byte) (int, int, time.Duration, time.Duration) {
	return 1, p.coldPerKey * len(keys), 0, 0
}

func TestTrieMostlyResidentSkipsAResidentTrie(t *testing.T) {
	keys := balCommitmentWarmupKeys(threeAccountChanges())
	require.True(t, trieMostlyResident(context.Background(), coldPagesPrefetcher{coldPerKey: 0}, keys))
	require.False(t, trieMostlyResident(context.Background(), coldPagesPrefetcher{coldPerKey: 1}, keys))
}

func TestBALPrefetchKeys(t *testing.T) {
	a1, a2 := common.Address{19: 1}, common.Address{19: 2}
	s1, s2, s3 := common.Hash{31: 1}, common.Hash{31: 2}, common.Hash{31: 3}
	bal := types.BlockAccessList{
		{Address: a1, StorageChanges: []types.SlotChanges{{Slot: accounts.InternKey(s1)}}, StorageReads: []common.Hash{s2}},
		{Address: a2, StorageReads: []common.Hash{s3}},
	}
	accs, slots := balPrefetchKeys(bal)
	require.Equal(t, [][]byte{a1[:], a2[:]}, accs)
	require.Equal(t, [][]byte{append(a1[:], s1[:]...), append(a1[:], s2[:]...), append(a2[:], s3[:]...)}, slots)

	// Keys are independent: appending to one must not overwrite its neighbour.
	_ = append(slots[0], 0xff)
	require.Equal(t, append(a1[:], s2[:]...), slots[1])
}

func TestBALPrefetchKeysOfAnEmptyBAL(t *testing.T) {
	accs, slots := balPrefetchKeys(nil)
	require.Empty(t, accs)
	require.Empty(t, slots)
}

func TestTrieMostlyResidentWithoutKeysWalksNothing(t *testing.T) {
	require.False(t, trieMostlyResident(context.Background(), coldPagesPrefetcher{coldPerKey: 0}, nil))
}

func TestMostlyResidentSkipsResidentStateKeys(t *testing.T) {
	keys := make([][]byte, 10_000)
	for i := range keys {
		keys[i] = []byte{byte(i >> 8), byte(i)}
	}
	sample := sampleEvenly(keys)
	require.LessOrEqual(t, len(sample), residencySampleKeys+1)
	require.Equal(t, keys[0], sample[0])
	require.True(t, mostlyResident(context.Background(), coldPagesPrefetcher{coldPerKey: 0}, kv.StorageDomain, sample))
	require.False(t, mostlyResident(context.Background(), coldPagesPrefetcher{coldPerKey: 1}, kv.StorageDomain, sample))
	require.False(t, mostlyResident(context.Background(), coldPagesPrefetcher{coldPerKey: 0}, kv.StorageDomain, nil))
}

func TestStateSampleColdPicksTheBatchPrefetchOnlyForColdState(t *testing.T) {
	keys := [][]byte{{1}, {2}, {3}}
	require.False(t, stateSampleCold(context.Background(), coldPagesPrefetcher{coldPerKey: 0}, keys, keys))
	require.True(t, stateSampleCold(context.Background(), coldPagesPrefetcher{coldPerKey: 1}, keys, nil))
	require.False(t, stateSampleCold(context.Background(), coldPagesPrefetcher{coldPerKey: 1}, nil, nil), "no state keys: nothing to batch")
}
