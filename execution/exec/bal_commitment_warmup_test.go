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
	"github.com/erigontech/erigon/execution/commitment/nibbles"
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

	require.NoError(t, warmBALCommitment(t.Context(), db, balCommitmentWarmupKeys(bal), 2))

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

	err := warmBALCommitment(t.Context(), db, balCommitmentWarmupKeys(bal), 2)
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, secondErr)
}

// A warmup whose context is already done does not open any read transaction.
func TestWarmBALCommitmentSkipsAnEndedContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	db := &commitmentBeginFuncDB{begin: func(context.Context) (kv.Tx, error) { return nil, errors.New("unexpected BeginRo") }}

	err := warmBALCommitment(ctx, db, balCommitmentWarmupKeys(threeAccountChanges()), 3)

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

	err := warmBALCommitment(ctx, db, balCommitmentWarmupKeys(threeAccountChanges()), 3)

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

	err := warmBALCommitment(t.Context(), db, balCommitmentWarmupKeys(threeAccountChanges()), 3)

	require.Equal(t, int64(3), db.calls.Load())
	require.Equal(t, kv.ErrReadTxLimitExceeded, err)
}

// Workers that fail with errors allocated per call, such as a closed database's, report the
// message once.
func TestWarmBALCommitmentReportsAnErrorMessageOnce(t *testing.T) {
	db := &commitmentBeginFuncDB{begin: func(context.Context) (kv.Tx, error) { return nil, errors.New("db closed") }}

	err := warmBALCommitment(t.Context(), db, balCommitmentWarmupKeys(threeAccountChanges()), 3)

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

	require.NoError(t, warmBALCommitment(t.Context(), db, balCommitmentWarmupKeys(bal), 1))
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

type coldPagesPrefetcher struct {
	coldPerKey int
	err        error
}

func (p coldPagesPrefetcher) PrefetchLatest(_ context.Context, _ kv.Domain, keys [][]byte) (int, error) {
	return p.coldPerKey * len(keys), p.err
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

func TestBALPrefetchSampleIsTheEvenSampleOfAllKeys(t *testing.T) {
	bal := make(types.BlockAccessList, 700)
	for i := range bal {
		bal[i].Address = common.Address{18: byte(i >> 8), 19: byte(i)}
		for j := range i % 5 {
			bal[i].StorageChanges = append(bal[i].StorageChanges, types.SlotChanges{Slot: accounts.InternKey(common.Hash{30: byte(j), 31: byte(i)})})
			bal[i].StorageReads = append(bal[i].StorageReads, common.Hash{29: 1, 30: byte(j), 31: byte(i)})
		}
	}
	accs, slots := balPrefetchKeys(bal)
	sampleAccs, sampleSlots := balPrefetchSample(bal)
	require.Equal(t, sampleEvenly(accs, 2*residencySampleKeys), sampleAccs)
	require.Equal(t, sampleEvenly(slots, 2*residencySampleKeys), sampleSlots)
}

func TestBALPrefetchKeysOfAnEmptyBAL(t *testing.T) {
	accs, slots := balPrefetchKeys(nil)
	require.Empty(t, accs)
	require.Empty(t, slots)
}

// coldStorageTriePrefetcher reports a cold page only for prefixes below the storage root.
type coldStorageTriePrefetcher struct{}

func (coldStorageTriePrefetcher) PrefetchLatest(_ context.Context, _ kv.Domain, keys [][]byte) (int, error) {
	cold := 0
	for _, k := range keys {
		if len(k) > storageTrieDepth/2 {
			cold++
		}
	}
	return cold, nil
}

func TestTrieMostlyResidentSeesAColdStorageTrieOfOneAccount(t *testing.T) {
	var keys [][]byte
	for i := range 64 {
		k := make([]byte, 128)
		k[64], k[65], k[66] = byte(i>>4)&0xf, byte(i)&0xf, 3
		keys = append(keys, k)
	}
	require.False(t, trieMostlyResident(context.Background(), coldStorageTriePrefetcher{}, keys))
}

func TestTrieMostlyResidentWithoutKeysSkipsTheWalk(t *testing.T) {
	require.True(t, trieMostlyResident(context.Background(), coldPagesPrefetcher{coldPerKey: 1}, nil))
}

func TestSampleColdCountsColdPagesPerSampledKey(t *testing.T) {
	keys := make([][]byte, 10_000)
	for i := range keys {
		keys[i] = []byte{byte(i >> 8), byte(i)}
	}
	sample := sampleEvenly(keys, residencySampleKeys)
	require.Len(t, sample, residencySampleKeys)
	require.Equal(t, keys[0], sample[0])
	require.Len(t, sampleEvenly(keys[:2*residencySampleKeys-1], residencySampleKeys), residencySampleKeys)
	require.Len(t, sampleEvenly(keys[:100], residencySampleKeys), 100)
	for _, c := range []struct {
		coldPerKey int
		sample     [][]byte
		want       bool
	}{{0, sample, false}, {1, sample, true}, {1, nil, false}} {
		got, err := sampleCold(context.Background(), coldPagesPrefetcher{coldPerKey: c.coldPerKey}, kv.StorageDomain, c.sample)
		require.NoError(t, err)
		require.Equal(t, c.want, got, "coldPerKey=%d keys=%d", c.coldPerKey, len(c.sample))
	}
}

// sharedPagesPrefetcher finds cold pages only on its first call: pages that all keys share,
// which the first probe reads.
type sharedPagesPrefetcher struct{ calls *atomic.Int32 }

func (p sharedPagesPrefetcher) PrefetchLatest(_ context.Context, _ kv.Domain, keys [][]byte) (int, error) {
	if p.calls.Add(1) == 1 {
		return len(keys), nil
	}
	return 0, nil
}

func TestStateSampleColdIgnoresPagesTheFirstProbeRead(t *testing.T) {
	keys := make([][]byte, 2*residencySampleKeys)
	for i := range keys {
		keys[i] = []byte{byte(i >> 8), byte(i)}
	}
	var calls atomic.Int32
	require.False(t, stateSampleCold(context.Background(), sharedPagesPrefetcher{calls: &calls}, nil, keys, 0, 10*len(keys)).any())
}

func TestStateSampleColdKeepsThePerKeyWarmupWhenTheSampleReadEveryKey(t *testing.T) {
	keys := [][]byte{{1}, {2}, {3}}
	cold := coldPagesPrefetcher{coldPerKey: 1}
	require.False(t, stateSampleCold(context.Background(), cold, nil, keys, 0, len(keys)).any(), "the probes already read every page of the domain")
	require.True(t, stateSampleCold(context.Background(), cold, nil, keys, 0, 10*len(keys)).any())
}

// domainErrPrefetcher reports every page cold and fails its reads in one domain.
type domainErrPrefetcher struct{ failing kv.Domain }

func (p domainErrPrefetcher) PrefetchLatest(_ context.Context, domain kv.Domain, keys [][]byte) (int, error) {
	if domain == p.failing {
		return len(keys), errors.New("read failed")
	}
	return len(keys), nil
}

func TestStateSampleColdKeepsThePerKeyWarmupWhenOneDomainCannotRead(t *testing.T) {
	keys := [][]byte{{1}, {2}, {3}}
	for _, failing := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain} {
		require.False(t, stateSampleCold(context.Background(), domainErrPrefetcher{failing: failing}, keys, keys, 1000, 1000).any(), "failing %s", failing)
	}
}

// domainColdPrefetcher reports every page cold in one domain and none in the others.
type domainColdPrefetcher struct{ cold kv.Domain }

func (p domainColdPrefetcher) PrefetchLatest(_ context.Context, domain kv.Domain, keys [][]byte) (int, error) {
	if domain == p.cold {
		return len(keys), nil
	}
	return 0, nil
}

func TestStateSampleColdGatesEachDomain(t *testing.T) {
	keys := [][]byte{{1}, {2}, {3}}
	require.Equal(t, coldState{storage: true}, stateSampleCold(context.Background(), domainColdPrefetcher{cold: kv.StorageDomain}, keys, keys, 1000, 1000))
	require.Equal(t, coldState{accounts: true}, stateSampleCold(context.Background(), domainColdPrefetcher{cold: kv.AccountsDomain}, keys, keys, 1000, 1000))
}

func TestStateSampleColdKeepsThePerKeyWarmupWhenTheBatchCannotRead(t *testing.T) {
	keys := [][]byte{{1}, {2}, {3}}
	broken := coldPagesPrefetcher{coldPerKey: 1, err: errors.New("no batch ring")}
	require.False(t, stateSampleCold(context.Background(), broken, keys, keys, 1000, 1000).any())
}

func TestStateSampleColdPicksTheBatchPrefetchOnlyForColdState(t *testing.T) {
	keys := [][]byte{{1}, {2}, {3}}
	require.False(t, stateSampleCold(context.Background(), coldPagesPrefetcher{coldPerKey: 0}, keys, keys, 1000, 1000).any())
	require.True(t, stateSampleCold(context.Background(), coldPagesPrefetcher{coldPerKey: 1}, keys, nil, 1000, 0).any())
	require.False(t, stateSampleCold(context.Background(), coldPagesPrefetcher{coldPerKey: 1}, nil, nil, 0, 0).any(), "no state keys: nothing to batch")
}

// prefetcherTx hands p to the code under test as its AggTx.
type prefetcherTx struct {
	kv.TemporalTx
	p latestPrefetcher
}

func (t prefetcherTx) AggTx() any { return t.p }
func (prefetcherTx) Rollback()    {}

// prefetcherDB opens read txs whose AggTx is p.
type prefetcherDB struct {
	kv.RoDB
	p latestPrefetcher
}

func (d prefetcherDB) BeginRo(context.Context) (kv.Tx, error) { return prefetcherTx{p: d.p}, nil }

func TestPrefetchKeysKeepsTheFirstReadError(t *testing.T) {
	keys := make([][]byte, 4*prefetchPar) // spread over every planner tx
	for i := range keys {
		keys[i] = []byte{byte(i)}
	}
	parent := prefetcherTx{p: coldPagesPrefetcher{}}
	failing := prefetcherDB{p: domainErrPrefetcher{failing: kv.CommitmentDomain}}
	require.Error(t, prefetchKeys(context.Background(), failing, parent, kv.CommitmentDomain, keys), "an error from a planner's own tx")
	require.NoError(t, prefetchKeys(context.Background(), prefetcherDB{p: coldPagesPrefetcher{}}, parent, kv.CommitmentDomain, keys))
}

// branchTx serves raw commitment branches from a small trie: compact prefix -> child bitmap.
type branchTx struct {
	kv.TemporalTx
	children map[string]uint16
}

func (t branchTx) AggTx() any { return t }

func (t branchTx) GetLatestBranchRaw(prefix []byte, _ kv.Tx) ([]byte, bool, error) {
	bits, ok := t.children[string(prefix)]
	if !ok {
		return nil, false, nil
	}
	return []byte{0, 0, byte(bits >> 8), byte(bits)}, true, nil
}

func TestKeysWithChildKeepsKeysWhoseBranchHasTheirNextNibble(t *testing.T) {
	compact := func(path ...byte) string { return string(nibbles.HexToCompactInto(nil, path)) }
	tx := branchTx{children: map[string]uint16{
		compact(1): 1 << 2, // branch 1 has only child 2
	}}
	keys := [][]byte{{1, 2, 0}, {1, 5, 0}, {3, 0, 0}, {4}}
	got := keysWithChild(context.Background(), nil, tx, keys, 1)
	require.Equal(t, [][]byte{{1, 2, 0}}, got, "1-5 has no child at branch 1, 3 has no branch, 4 ends above depth 1")
}

func TestFirstFailurePrefersAReadFailureOverAnIncompleteBatch(t *testing.T) {
	failed := errors.New("io_uring read failed")
	require.Equal(t, failed, firstFailure(nil, kv.ErrPrefetchIncomplete, failed))
	require.Equal(t, failed, firstFailure(context.Canceled, failed), "a cancelled chunk does not hide a read failure")
	require.Equal(t, kv.ErrPrefetchIncomplete, firstFailure(nil, kv.ErrPrefetchIncomplete))
	require.NoError(t, firstFailure(nil, nil))
}
