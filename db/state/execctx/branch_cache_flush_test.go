// Copyright 2024 The Erigon Authors
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

package execctx_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/kvmetrics"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/commitment"
)

type temporalTxWithAgg struct {
	kv.TemporalTx
	agg   any
	debug kv.TemporalDebugTx
}

func (tx *temporalTxWithAgg) AggTx() any { return tx.agg }
func (tx *temporalTxWithAgg) Debug() kv.TemporalDebugTx {
	if tx.debug != nil {
		return tx.debug
	}
	return tx.TemporalTx.Debug()
}

type exactVisibleDebug struct{ kv.TemporalDebugTx }

func (*exactVisibleDebug) HasExactDomainVisibleEnd(kv.Domain) bool { return true }

type boundedLatestAgg struct {
	branchCache *commitment.BranchCache
	domain      kv.Domain
	key         []byte
	value       []byte
	step        kv.Step
	maxStep     kv.Step
}

func (a *boundedLatestAgg) BranchCache() *commitment.BranchCache { return a.branchCache }
func (a *boundedLatestAgg) ForbidVisibilityLowering()            {}

func (a *boundedLatestAgg) MeteredGetLatest(domain kv.Domain, key []byte, _ kv.Tx, maxStep kv.Step, _ *kvmetrics.DomainMetrics, _ time.Time) ([]byte, kv.Step, bool, error) {
	if domain != a.domain || !bytes.Equal(key, a.key) {
		return nil, 0, false, nil
	}
	a.maxStep = maxStep
	return a.value, a.step, true, nil
}

type commitErrorTx struct {
	kv.TemporalRwTx
	err error
}

func (tx *commitErrorTx) Commit() error { return tx.err }

type pausedCommitTx struct {
	kv.TemporalRwTx
	entered chan struct{}
	resume  chan struct{}
	err     error
}

func (tx *pausedCommitTx) Commit() error {
	close(tx.entered)
	<-tx.resume
	return tx.err
}

func branchGenerationForTx(t *testing.T, tx kv.TemporalTx) cache.Generation {
	t.Helper()
	stateVersion, err := rawdb.GetStateVersion(tx)
	require.NoError(t, err)
	return cache.BranchGeneration(stateVersion, tx.Debug().TxNumsInFiles(kv.CommitmentDomain))
}

func TestCanonicalCachePublicationRequiresExplicitBinding(t *testing.T) {
	ctx := t.Context()
	db := newTestDb(t, 100)
	tx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer sd.Close()

	statePublisher, branchPublisher := sd.CachePublishersEnabledForTest()
	require.False(t, statePublisher)
	require.False(t, branchPublisher, "construction must not grant authority to publish the process-global branch cache")

	sd.SetCanonicalCaches(nil)
	statePublisher, branchPublisher = sd.CachePublishersEnabledForTest()
	require.False(t, statePublisher)
	require.True(t, branchPublisher, "canonical binding must grant branch publication authority without requiring StateCache")

	stateCache := newSmallStateCache()
	t.Cleanup(stateCache.Close)
	sd.SetCanonicalCachesForTest(stateCache)
	statePublisher, branchPublisher = sd.CachePublishersEnabledForTest()
	require.True(t, statePublisher)
	require.True(t, branchPublisher)
}

func TestCommitRequiresCanonicalCacheBinding(t *testing.T) {
	ctx := t.Context()
	db := newTestDb(t, 100)
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer sd.Close()

	err = sd.Commit(ctx, tx)
	require.ErrorContains(t, err, "SetCanonicalCaches")
}

func TestCommitRequiresCanonicalStateCacheBinding(t *testing.T) {
	ctx := t.Context()
	db := newTestDb(t, 100)
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, tx, log.New(), execctx.WithoutSharedBranchCache())
	require.NoError(t, err)
	defer sd.Close()

	stateCache := newSmallStateCache()
	t.Cleanup(stateCache.Close)
	sd.SetStateCacheReaderForTest(stateCache)

	err = sd.Commit(ctx, tx)
	require.ErrorContains(t, err, "SetCanonicalCaches")
}

func TestCommitRequiresCanonicalStateCacheBindingAfterUnwind(t *testing.T) {
	ctx := t.Context()
	db := newTestDb(t, 100)
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, tx, log.New(), execctx.WithoutSharedBranchCache())
	require.NoError(t, err)
	defer sd.Close()

	stateCache := newSmallStateCache()
	t.Cleanup(stateCache.Close)
	sd.SetStateCacheReaderForTest(stateCache)
	sd.Unwind(0, nil)

	err = sd.Commit(ctx, tx)
	require.ErrorContains(t, err, "SetCanonicalCaches")
}

func TestCommitRequiresCanonicalStateCacheBindingAfterMerge(t *testing.T) {
	ctx := t.Context()
	db := newTestDb(t, 100)
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	parent, err := execctx.NewSharedDomains(ctx, tx, log.New(), execctx.WithoutSharedBranchCache())
	require.NoError(t, err)
	defer parent.Close()
	child, err := execctx.NewSharedDomains(ctx, tx, log.New(), execctx.WithoutSharedBranchCache())
	require.NoError(t, err)
	defer child.Close()

	stateCache := newSmallStateCache()
	t.Cleanup(stateCache.Close)
	child.SetStateCacheReaderForTest(stateCache)
	child.Unwind(0, nil)
	require.NoError(t, parent.Merge(ctx, 0, child, 0))

	err = parent.Commit(ctx, tx)
	require.ErrorContains(t, err, "SetCanonicalCaches")
}

func TestCommitDoesNotHoldCachePublicationDuringDatabaseCommit(t *testing.T) {
	ctx := t.Context()
	db := newTestDb(t, 100)
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	sd.SetCanonicalCaches(nil)

	provider, ok := rwTx.AggTx().(commitment.BranchCacheProvider)
	require.True(t, ok)
	branchCache := provider.BranchCache()
	require.NotNil(t, branchCache)
	filesEnd := rwTx.Debug().TxNumsInFiles(kv.CommitmentDomain) + 1

	pausedTx := &pausedCommitTx{
		TemporalRwTx: rwTx,
		entered:      make(chan struct{}),
		resume:       make(chan struct{}),
		err:          errors.New("injected commit failure"),
	}
	commitDone := make(chan error, 1)
	go func() { commitDone <- sd.Commit(ctx, pausedTx) }()

	select {
	case <-pausedTx.entered:
	case <-time.After(time.Second):
		close(pausedTx.resume)
		t.Fatal("database commit was not reached")
	}

	filesPublished := make(chan struct{})
	go func() {
		branchCache.BeginFilesPublication(filesEnd).Finish()
		close(filesPublished)
	}()

	publicationBlocked := false
	select {
	case <-filesPublished:
	case <-time.After(time.Second):
		publicationBlocked = true
	}
	close(pausedTx.resume)

	select {
	case err := <-commitDone:
		require.ErrorIs(t, err, pausedTx.err)
	case <-time.After(time.Second):
		t.Fatal("database commit did not finish")
	}
	select {
	case <-filesPublished:
	case <-time.After(time.Second):
		t.Fatal("files publication did not finish")
	}
	require.False(t, publicationBlocked, "database commit must not hold cache publication while transaction close may acquire aggregator locks")
}

// Flush changes only the write transaction. The retained memory batch must
// continue to shadow the still-published durable cache for existing getters.
func TestBareFlushRetainsMemoryAsAuthorityOverCacheViews(t *testing.T) {
	accountKey := make([]byte, 20)
	accountKey[0] = 0xaa

	for _, tc := range []struct {
		name          string
		domain        kv.Domain
		key, old, new []byte
	}{
		{"state", kv.AccountsDomain, accountKey, encAccount(1), encAccount(2)},
		{"branch", kv.CommitmentDomain, []byte{0x0a, 0x0b}, []byte("old-branch"), []byte("new-branch")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			db := newTestDb(t, 100)
			stateCache := newSmallStateCache()
			t.Cleanup(stateCache.Close)

			seedTx, err := db.BeginTemporalRw(ctx)
			require.NoError(t, err)
			defer seedTx.Rollback()
			seedDomains, err := execctx.NewSharedDomains(ctx, seedTx, log.New())
			require.NoError(t, err)
			seedDomains.SetCanonicalCachesForTest(stateCache)
			seedDomains.SetTxNum(1)
			require.NoError(t, seedDomains.DomainPut(tc.domain, seedTx, tc.key, tc.old, 1, nil))
			require.NoError(t, seedDomains.Commit(ctx, seedTx))
			seedDomains.Close()

			rwTx, err := db.BeginTemporalRw(ctx)
			require.NoError(t, err)
			defer rwTx.Rollback()
			domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
			require.NoError(t, err)
			defer domains.Close()
			domains.SetCanonicalCachesForTest(stateCache)

			getter := domains.AsGetter(rwTx)
			got, _, err := getter.GetLatest(tc.domain, tc.key)
			require.NoError(t, err)
			require.Equal(t, tc.old, got)

			stateVersion, err := rawdb.GetStateVersion(rwTx)
			require.NoError(t, err)
			var cachedValue func() ([]byte, bool)
			switch tc.domain {
			case kv.AccountsDomain:
				debug := rwTx.Debug()
				view := stateCache.View(cache.StateGeneration(
					stateVersion,
					debug.TxNumsInFiles(kv.AccountsDomain),
					debug.TxNumsInFiles(kv.StorageDomain),
					debug.TxNumsInFiles(kv.CodeDomain),
				))
				cachedValue = func() ([]byte, bool) { return view.Get(tc.domain, tc.key) }
			case kv.CommitmentDomain:
				provider, ok := rwTx.AggTx().(commitment.BranchCacheProvider)
				require.True(t, ok)
				view := provider.BranchCache().View(branchGenerationForTx(t, rwTx))
				cachedValue = func() ([]byte, bool) {
					value, _, ok := view.Get(tc.key)
					return value, ok
				}
			}

			got, ok := cachedValue()
			require.True(t, ok)
			require.Equal(t, tc.old, got)

			domains.SetTxNum(2)
			require.NoError(t, domains.DomainPut(tc.domain, rwTx, tc.key, tc.new, 2, tc.old))
			require.NoError(t, domains.Flush(ctx, rwTx))

			got, ok = cachedValue()
			require.True(t, ok, "bare Flush must not publish an uncommitted cache generation")
			require.Equal(t, tc.old, got)

			got, _, err = getter.GetLatest(tc.domain, tc.key)
			require.NoError(t, err)
			require.Equal(t, tc.new, got, "retained memory must shadow the old durable cache view")
		})
	}
}

func TestBoundedReadDoesNotFillBranchCache(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	db := newTestDb(t, 16)
	branchCache := commitment.NewBranchCache(64)
	t.Cleanup(branchCache.Close)
	key := []byte{0x0a, 0x0b}
	value := []byte("bounded-branch")

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	generation := branchGenerationForTx(t, roTx)
	branchCache.Publisher().Initialize(generation)
	view := branchCache.View(generation)
	agg := &boundedLatestAgg{
		branchCache: branchCache,
		domain:      kv.CommitmentDomain,
		key:         key,
		value:       value,
		step:        1,
	}
	tx := &temporalTxWithAgg{
		TemporalTx: roTx,
		agg:        agg,
		debug:      &exactVisibleDebug{TemporalDebugTx: roTx.Debug()},
	}
	parent, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer parent.Close()
	child, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer child.Close()
	got, step, err := child.GetLatest(kv.CommitmentDomain, tx, key)
	require.NoError(t, err)
	require.Equal(t, value, got)
	require.Equal(t, kv.Step(1), step)
	_, _, ok := view.Get(key)
	require.True(t, ok, "the setup must admit an unbounded read-through fill")
	branchCache.Invalidate(key)
	child.SetParent(parent)

	stepBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(stepBytes, ^uint64(1))
	var diffs [kv.DomainLen][]kv.DomainEntryDiff
	diffs[kv.CommitmentDomain] = []kv.DomainEntryDiff{{Key: string(key) + string(stepBytes)}}
	parent.Unwind(0, &diffs)

	got, step, err = child.GetLatest(kv.CommitmentDomain, tx, key)
	require.NoError(t, err)
	require.Equal(t, value, got)
	require.Equal(t, kv.Step(1), step)
	require.Equal(t, kv.Step(1), agg.maxStep)
	_, _, ok = view.Get(key)
	require.False(t, ok, "a bounded historical branch must not enter the latest-branch cache")
}

// Commit, unlike Flush, publishes the rebuilt branch after the database commit.
func TestBranchCacheCommitRefreshesAfterReadThrough(t *testing.T) {
	stepSize := uint64(100)
	db := newTestDb(t, stepSize)
	ctx := t.Context()
	logger := log.New()

	key := []byte{0x0a, 0x0b}

	writeCommit := func(val []byte, step uint64, prev []byte, readFirst bool) {
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()
		sd, err := execctx.NewSharedDomains(ctx, rwTx, logger)
		require.NoError(t, err)
		defer sd.Close()
		sd.SetCanonicalCaches(nil)

		if readFirst {
			got, _, err := sd.GetLatest(kv.CommitmentDomain, rwTx, key)
			require.NoError(t, err)
			require.Equal(t, prev, got)
		}
		require.NoError(t, sd.DomainPut(kv.CommitmentDomain, rwTx, key, val, step, prev))
		require.NoError(t, sd.Commit(ctx, rwTx))
		sd.Close()
	}

	writeCommit([]byte("v1-branch-bytes"), 1, nil, false)
	writeCommit([]byte("v2-branch-bytes"), 2, []byte("v1-branch-bytes"), true)

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, rwTx, logger)
	require.NoError(t, err)
	defer sd.Close()

	v, _, err := sd.GetLatest(kv.CommitmentDomain, rwTx, key)
	require.NoError(t, err)
	require.Equal(t, []byte("v2-branch-bytes"), v, "fresh SD must read the latest committed branch, not the stale read-through entry")
}

func TestSpeculativeUnwindDetachesWithoutChangingBranchCache(t *testing.T) {
	db := newTestDb(t, 100)
	ctx := t.Context()

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer sd.Close()

	provider, ok := roTx.AggTx().(commitment.BranchCacheProvider)
	require.True(t, ok)
	branchCache := provider.BranchCache()
	require.NotNil(t, branchCache)

	generation := branchGenerationForTx(t, roTx)
	branchCache.Publisher().Initialize(generation)

	key := []byte{0xa0, 0xb0}
	published := branchCache.View(generation)
	published.Fill(key, []byte("canonical-cache-only"), 1)

	sd.Unwind(50, nil)

	value, _, ok := published.Get(key)
	require.True(t, ok, "a speculative unwind must not mutate the process-global branch generation")
	require.Equal(t, []byte("canonical-cache-only"), value)

	value, _, err = sd.GetLatest(kv.CommitmentDomain, roTx, key)
	require.NoError(t, err)
	require.Empty(t, value, "the rewound SharedDomains must detach from the canonical branch generation")
}

func TestCanonicalUnwindClearsBranchCacheOnlyAfterCommit(t *testing.T) {
	db := newTestDb(t, 100)
	ctx := t.Context()
	logger := log.New()
	key := []byte{0xa0, 0xb0}

	seedTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer seedTx.Rollback()
	seedSD, err := execctx.NewSharedDomains(ctx, seedTx, logger)
	require.NoError(t, err)
	seedSD.SetCanonicalCaches(nil)
	require.NoError(t, seedSD.DomainPut(kv.CommitmentDomain, seedTx, key, []byte("durable"), 1, nil))
	require.NoError(t, seedSD.Commit(ctx, seedTx))
	seedSD.Close()

	unwindTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer unwindTx.Rollback()
	unwindSD, err := execctx.NewSharedDomains(ctx, unwindTx, logger)
	require.NoError(t, err)
	defer unwindSD.Close()
	unwindSD.SetCanonicalCaches(nil)

	provider, ok := unwindTx.AggTx().(commitment.BranchCacheProvider)
	require.True(t, ok)
	branchCache := provider.BranchCache()
	oldView := branchCache.View(branchGenerationForTx(t, unwindTx))
	cacheOnlyKey := []byte{0xa0, 0xc0}
	oldView.Fill(cacheOnlyKey, []byte("discarded-fork"), 2)

	var diffs [kv.DomainLen][]kv.DomainEntryDiff
	unwindSD.Unwind(0, &diffs)
	value, _, ok := oldView.Get(cacheOnlyKey)
	require.True(t, ok, "the cache must keep serving the still-durable version before Commit")
	require.Equal(t, []byte("discarded-fork"), value)
	require.NoError(t, unwindSD.Commit(ctx, unwindTx))

	_, _, ok = oldView.Get(cacheOnlyKey)
	require.False(t, ok, "committing the unwind must revoke views of the discarded version")

	readTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer readTx.Rollback()
	_, _, ok = branchCache.View(branchGenerationForTx(t, readTx)).Get(cacheOnlyKey)
	require.False(t, ok, "the unwound generation must not retain a cache-only discarded branch")
}

func TestFailedCommitKeepsBranchCacheGeneration(t *testing.T) {
	db := newTestDb(t, 100)
	ctx := t.Context()
	logger := log.New()

	seedTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer seedTx.Rollback()
	seedSD, err := execctx.NewSharedDomains(ctx, seedTx, logger)
	require.NoError(t, err)
	seedSD.SetCanonicalCaches(nil)
	require.NoError(t, seedSD.Commit(ctx, seedTx))
	seedSD.Close()

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, rwTx, logger)
	require.NoError(t, err)
	defer sd.Close()
	sd.SetCanonicalCaches(nil)

	provider, ok := rwTx.AggTx().(commitment.BranchCacheProvider)
	require.True(t, ok)
	branchCache := provider.BranchCache()
	view := branchCache.View(branchGenerationForTx(t, rwTx))
	key := []byte{0xa0, 0xb0}
	view.Fill(key, []byte("durable"), 1)

	sentinel := errors.New("injected commit failure")
	err = sd.Commit(ctx, &commitErrorTx{TemporalRwTx: rwTx, err: sentinel})
	require.ErrorIs(t, err, sentinel)

	value, _, ok := view.Get(key)
	require.True(t, ok, "a failed database commit must keep the previous branch generation")
	require.Equal(t, []byte("durable"), value)
}
