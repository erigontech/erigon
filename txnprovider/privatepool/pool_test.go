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

package privatepool

import (
	"context"
	"math"
	"sync/atomic"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

type testProvider struct {
	txns     []types.Transaction
	revision atomic.Uint64
}

type zeroRevisionRaceProvider struct {
	txns     []types.Transaction
	revision atomic.Uint64
}

type firstCallBlockingProvider struct {
	txns    []types.Transaction
	calls   atomic.Uint64
	entered chan struct{}
	release chan struct{}
}

func (p *firstCallBlockingProvider) ProvideTxns(ctx context.Context, _ ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	if p.calls.Add(1) == 1 {
		close(p.entered)
		<-p.release
	}
	txnprovider.ObserveTxnRevision(ctx, 0)
	return append([]types.Transaction(nil), p.txns...), nil
}

func (p *zeroRevisionRaceProvider) ProvideTxns(ctx context.Context, _ ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	txnprovider.ObserveTxnRevision(ctx, 0)
	p.revision.Store(1)
	return append([]types.Transaction(nil), p.txns...), nil
}

func (p *zeroRevisionRaceProvider) TransactionSetRevision(uint64, uint64) uint64 {
	return p.revision.Load()
}

func (p *testProvider) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	txnprovider.ObserveTxnRevision(ctx, p.revision.Load())
	return append([]types.Transaction(nil), p.txns...), nil
}

func (p *testProvider) TransactionSetRevision(uint64, uint64) uint64 {
	return p.revision.Load()
}

func testTxn(nonce uint64) types.Transaction {
	return types.NewTransaction(nonce, common.Address{byte(nonce + 1)}, nil, 21_000, nil, nil)
}

func hashes(txns []types.Transaction) []common.Hash {
	result := make([]common.Hash, len(txns))
	for i, txn := range txns {
		result[i] = txn.Hash()
	}
	return result
}

func TestPoolProvidesPrivateTransactionImmediatelyAfterPublicTarget(t *testing.T) {
	target, other, privateTxn := testTxn(1), testTxn(2), testTxn(3)
	base := &testProvider{txns: []types.Transaction{target, other}}
	pool := New(base, 16)

	bundleID, err := pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: privateTxn, TargetSlot: 42})
	require.NoError(t, err)
	require.Equal(t, privateTxn.Hash(), bundleID)

	provided, err := pool.ProvideTxns(t.Context(), txnprovider.WithTargetSlot(42))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{target.Hash(), privateTxn.Hash(), other.Hash()}, hashes(provided))
}

func TestPoolDoesNotLeakPrivateTransactionOutsideTargetSlot(t *testing.T) {
	target, privateTxn := testTxn(1), testTxn(2)
	pool := New(&testProvider{txns: []types.Transaction{target}}, 16)
	_, err := pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: privateTxn, TargetSlot: 42})
	require.NoError(t, err)

	before, err := pool.ProvideTxns(t.Context(), txnprovider.WithTargetSlot(41))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{target.Hash()}, hashes(before))

	after, err := pool.ProvideTxns(t.Context(), txnprovider.WithTargetSlot(43))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{target.Hash()}, hashes(after))
}

func TestPoolDoesNotLeakPrivateTransactionAcrossBuildContexts(t *testing.T) {
	target, privateTxn := testTxn(1), testTxn(2)
	parent := common.Hash{0x11}
	pool := New(&testProvider{txns: []types.Transaction{target}}, 16)
	_, err := pool.Submit(Bundle{
		TargetHash:       target.Hash(),
		TargetParentHash: parent,
		TargetGeneration: 7,
		Transaction:      privateTxn,
		TargetSlot:       42,
	})
	require.NoError(t, err)

	wrongParent, err := pool.ProvideTxns(t.Context(), txnprovider.WithTargetSlot(42), txnprovider.WithTargetParentHash(common.Hash{0x22}), txnprovider.WithTargetGeneration(7))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{target.Hash()}, hashes(wrongParent))

	wrongGeneration, err := pool.ProvideTxns(t.Context(), txnprovider.WithTargetSlot(42), txnprovider.WithTargetParentHash(parent), txnprovider.WithTargetGeneration(8))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{target.Hash()}, hashes(wrongGeneration))

	exact, err := pool.ProvideTxns(t.Context(), txnprovider.WithTargetSlot(42), txnprovider.WithTargetParentHash(parent), txnprovider.WithTargetGeneration(7))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{target.Hash(), privateTxn.Hash()}, hashes(exact))
}

func TestPoolRequiresPublicTargetInProvidedBatch(t *testing.T) {
	target, other, privateTxn := testTxn(1), testTxn(2), testTxn(3)
	pool := New(&testProvider{txns: []types.Transaction{other}}, 16)
	_, err := pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: privateTxn, TargetSlot: 42})
	require.NoError(t, err)

	provided, err := pool.ProvideTxns(t.Context(), txnprovider.WithTargetSlot(42))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{other.Hash()}, hashes(provided))
}

func TestPoolKeepsTargetAndPrivateTransactionTogetherAtBatchBoundary(t *testing.T) {
	other, target, privateTxn := testTxn(1), testTxn(2), testTxn(3)
	pool := New(&testProvider{txns: []types.Transaction{other, target}}, 16)
	_, err := pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: privateTxn, TargetSlot: 42})
	require.NoError(t, err)

	provided, err := pool.ProvideTxns(t.Context(), txnprovider.WithTargetSlot(42), txnprovider.WithAmount(2))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{target.Hash(), privateTxn.Hash()}, hashes(provided))
}

func TestPoolRevisionChangesOnSubmitButNotUnrelatedSlotSelection(t *testing.T) {
	target, privateTxn := testTxn(1), testTxn(2)
	base := &testProvider{txns: []types.Transaction{target}}
	base.revision.Store(7)
	pool := New(base, 16)

	initial := pool.TransactionSetRevision(0, 0)
	_, err := pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: privateTxn, TargetSlot: 42})
	require.NoError(t, err)
	afterSubmit := pool.TransactionSetRevision(0, 0)
	require.NotEqual(t, initial, afterSubmit)

	var observed atomic.Uint64
	ctx := txnprovider.WithTxnRevisionObserver(t.Context(), observed.Store)
	_, err = pool.ProvideTxns(ctx, txnprovider.WithTargetSlot(43))
	require.NoError(t, err)
	require.Equal(t, afterSubmit, observed.Load())
	require.Equal(t, observed.Load(), pool.TransactionSetRevision(0, 0))
}

func TestPoolPreservesObservedZeroBaseRevision(t *testing.T) {
	target := testTxn(1)
	base := &zeroRevisionRaceProvider{txns: []types.Transaction{target}}
	pool := New(base, 16)

	var observed atomic.Uint64
	ctx := txnprovider.WithTxnRevisionObserver(t.Context(), observed.Store)
	provided, err := pool.ProvideTxns(ctx, txnprovider.WithTargetSlot(42))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{target.Hash()}, hashes(provided))
	require.Equal(t, combineRevisions(0, 0), observed.Load())
	require.NotEqual(t, combineRevisions(1, 0), observed.Load())
}

func TestPoolPublishesSelectedTransactionPoliciesAsRequestScopedSnapshot(t *testing.T) {
	target, privateTxn := testTxn(1), testTxn(2)
	pool := New(&testProvider{txns: []types.Transaction{target}}, 16)
	_, err := pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: privateTxn, TargetSlot: 42})
	require.NoError(t, err)

	var observed map[common.Hash]txnprovider.TransactionPolicy
	ctx := txnprovider.WithTxnPolicyObserver(t.Context(), func(policies map[common.Hash]txnprovider.TransactionPolicy) {
		observed = policies
	})
	provided, err := pool.ProvideTxns(ctx, txnprovider.WithTargetSlot(42))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{target.Hash(), privateTxn.Hash()}, hashes(provided))
	require.Equal(t, txnprovider.TransactionPolicy{
		RequiresSuccess: true,
		Dependency:      target.Hash(),
	}, observed[privateTxn.Hash()])
	_, ok := observed[target.Hash()]
	require.False(t, ok)

	_, err = pool.ProvideTxns(t.Context(), txnprovider.WithTargetSlot(43))
	require.NoError(t, err)
	require.Equal(t, target.Hash(), observed[privateTxn.Hash()].Dependency)
}

func TestPoolPolicySnapshotSurvivesConcurrentExpiry(t *testing.T) {
	target, privateTxn := testTxn(1), testTxn(2)
	base := &firstCallBlockingProvider{
		txns:    []types.Transaction{target},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	pool := New(base, 16)
	_, err := pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: privateTxn, TargetSlot: 42})
	require.NoError(t, err)

	var observed map[common.Hash]txnprovider.TransactionPolicy
	ctx := txnprovider.WithTxnPolicyObserver(t.Context(), func(policies map[common.Hash]txnprovider.TransactionPolicy) {
		observed = policies
	})
	done := make(chan error, 1)
	go func() {
		_, provideErr := pool.ProvideTxns(ctx, txnprovider.WithTargetSlot(42))
		done <- provideErr
	}()
	<-base.entered
	_, err = pool.ProvideTxns(t.Context(), txnprovider.WithTargetSlot(43))
	require.NoError(t, err)
	close(base.release)
	require.NoError(t, <-done)
	require.Equal(t, target.Hash(), observed[privateTxn.Hash()].Dependency)
	require.True(t, observed[privateTxn.Hash()].RequiresSuccess)
}

func TestPoolPolicySnapshotSurvivesConcurrentReplacement(t *testing.T) {
	firstTarget, replacementTarget, privateTxn := testTxn(1), testTxn(2), testTxn(3)
	base := &firstCallBlockingProvider{
		txns:    []types.Transaction{firstTarget},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	pool := New(base, 16)
	_, err := pool.Submit(Bundle{TargetHash: firstTarget.Hash(), Transaction: privateTxn, TargetSlot: 42})
	require.NoError(t, err)

	var observed map[common.Hash]txnprovider.TransactionPolicy
	ctx := txnprovider.WithTxnPolicyObserver(t.Context(), func(policies map[common.Hash]txnprovider.TransactionPolicy) {
		observed = policies
	})
	done := make(chan error, 1)
	go func() {
		_, provideErr := pool.ProvideTxns(ctx, txnprovider.WithTargetSlot(42))
		done <- provideErr
	}()
	<-base.entered
	_, err = pool.Submit(Bundle{TargetHash: replacementTarget.Hash(), Transaction: privateTxn, TargetSlot: 42})
	require.NoError(t, err)
	close(base.release)
	require.NoError(t, <-done)
	_, ok := observed[privateTxn.Hash()]
	require.False(t, ok)
}

func TestPoolSelectsBundleSubmittedWhileBaseProviderIsRunning(t *testing.T) {
	target, privateTxn := testTxn(1), testTxn(2)
	base := &firstCallBlockingProvider{
		txns:    []types.Transaction{target},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	pool := New(base, 16)
	var observed map[common.Hash]txnprovider.TransactionPolicy
	ctx := txnprovider.WithTxnPolicyObserver(t.Context(), func(policies map[common.Hash]txnprovider.TransactionPolicy) {
		observed = policies
	})
	type result struct {
		txns []types.Transaction
		err  error
	}
	done := make(chan result, 1)
	go func() {
		txns, err := pool.ProvideTxns(ctx, txnprovider.WithTargetSlot(42))
		done <- result{txns: txns, err: err}
	}()
	<-base.entered
	_, err := pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: privateTxn, TargetSlot: 42})
	require.NoError(t, err)
	close(base.release)
	provided := <-done
	require.NoError(t, provided.err)
	require.Equal(t, []common.Hash{target.Hash(), privateTxn.Hash()}, hashes(provided.txns))
	require.Equal(t, target.Hash(), observed[privateTxn.Hash()].Dependency)
}

func TestPoolProvidesLateBundleAfterTargetWasAlreadyIncluded(t *testing.T) {
	target, privateTxn := testTxn(1), testTxn(2)
	pool := New(&testProvider{}, 16)
	_, err := pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: privateTxn, TargetSlot: 42})
	require.NoError(t, err)
	included := mapset.NewSet[[32]byte](target.Hash())
	var observed map[common.Hash]txnprovider.TransactionPolicy
	ctx := txnprovider.WithTxnPolicyObserver(t.Context(), func(policies map[common.Hash]txnprovider.TransactionPolicy) {
		observed = policies
	})

	provided, err := pool.ProvideTxns(ctx, txnprovider.WithTargetSlot(42), txnprovider.WithIncludedTxnIds(included))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{privateTxn.Hash()}, hashes(provided))
	require.Equal(t, target.Hash(), observed[privateTxn.Hash()].Dependency)
}

func TestPoolRejectsInvalidAndExcessBundles(t *testing.T) {
	target, first, second := testTxn(1), testTxn(2), testTxn(3)
	pool := New(&testProvider{}, 1)

	_, err := pool.Submit(Bundle{TargetHash: target.Hash(), TargetSlot: 42})
	require.ErrorContains(t, err, "transaction")
	_, err = pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: first})
	require.ErrorContains(t, err, "target slot")
	_, err = pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: first, TargetSlot: 42})
	require.NoError(t, err)
	_, err = pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: second, TargetSlot: 42})
	require.ErrorContains(t, err, "capacity")
}

func TestPoolEvictsFarthestFutureBundleForNearerSlot(t *testing.T) {
	target, farOne, farTwo, near := testTxn(1), testTxn(2), testTxn(3), testTxn(4)
	pool := New(&testProvider{txns: []types.Transaction{target}}, 2)
	_, err := pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: farOne, TargetSlot: math.MaxUint64})
	require.NoError(t, err)
	_, err = pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: farTwo, TargetSlot: math.MaxUint64 - 1})
	require.NoError(t, err)
	_, err = pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: near, TargetSlot: 42})
	require.NoError(t, err)

	provided, err := pool.ProvideTxns(t.Context(), txnprovider.WithTargetSlot(42))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{target.Hash(), near.Hash()}, hashes(provided))
}

func TestPoolDoesNotEvictNearerLiveBundleForFartherFutureSlot(t *testing.T) {
	target, nearer, farther := testTxn(1), testTxn(2), testTxn(3)
	pool := New(&testProvider{txns: []types.Transaction{target}}, 1)
	_, err := pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: nearer, TargetSlot: 41})
	require.NoError(t, err)
	_, err = pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: farther, TargetSlot: 42})
	require.ErrorContains(t, err, "capacity")

	provided, err := pool.ProvideTxns(t.Context(), txnprovider.WithTargetSlot(41))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{target.Hash(), nearer.Hash()}, hashes(provided))
}

func TestPoolEvictsBundleKnownExpiredByObservedBuildSlot(t *testing.T) {
	target, expired, current := testTxn(1), testTxn(2), testTxn(3)
	pool := New(&testProvider{txns: []types.Transaction{target}}, 1)
	_, err := pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: expired, TargetSlot: 41})
	require.NoError(t, err)
	_, err = pool.ProvideTxns(t.Context(), txnprovider.WithTargetSlot(42))
	require.NoError(t, err)
	_, err = pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: current, TargetSlot: 42})
	require.NoError(t, err)

	provided, err := pool.ProvideTxns(t.Context(), txnprovider.WithTargetSlot(42))
	require.NoError(t, err)
	require.Equal(t, []common.Hash{target.Hash(), current.Hash()}, hashes(provided))
}

func TestPoolDoesNotEvictBundleForActiveOlderBuild(t *testing.T) {
	target, older, newer := testTxn(1), testTxn(2), testTxn(3)
	var olderActive atomic.Bool
	olderActive.Store(true)
	pool := New(&testProvider{txns: []types.Transaction{target}}, 1, WithSlotActive(func(slot uint64) bool {
		return slot == 42 && olderActive.Load()
	}))
	_, err := pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: older, TargetSlot: 42})
	require.NoError(t, err)
	_, err = pool.ProvideTxns(t.Context(), txnprovider.WithTargetSlot(43))
	require.NoError(t, err)

	_, err = pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: newer, TargetSlot: 43})
	require.ErrorContains(t, err, "capacity")
	olderActive.Store(false)
	_, err = pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: newer, TargetSlot: 43})
	require.NoError(t, err)
}

func TestPoolDoesNotEvictBundleForActiveFartherBuild(t *testing.T) {
	target, farther, nearer := testTxn(1), testTxn(2), testTxn(3)
	var fartherActive atomic.Bool
	fartherActive.Store(true)
	pool := New(&testProvider{txns: []types.Transaction{target}}, 1, WithSlotActive(func(slot uint64) bool {
		return slot == 43 && fartherActive.Load()
	}))
	_, err := pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: farther, TargetSlot: 43})
	require.NoError(t, err)
	_, err = pool.ProvideTxns(t.Context(), txnprovider.WithTargetSlot(43))
	require.NoError(t, err)

	_, err = pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: nearer, TargetSlot: 42})
	require.ErrorContains(t, err, "capacity")
	fartherActive.Store(false)
	_, err = pool.Submit(Bundle{TargetHash: target.Hash(), Transaction: nearer, TargetSlot: 42})
	require.NoError(t, err)
}

func TestPoolReplacesInactiveSameSlotBuildContextAtCapacity(t *testing.T) {
	target, oldPrivate, currentPrivate := testTxn(1), testTxn(2), testTxn(3)
	pool := New(&testProvider{txns: []types.Transaction{target}}, 1, WithContextActive(func(slot, generation uint64) bool {
		return slot == 42 && generation == 2
	}))
	_, err := pool.Submit(Bundle{
		TargetHash:       target.Hash(),
		TargetParentHash: common.Hash{0x11},
		TargetGeneration: 1,
		Transaction:      oldPrivate,
		TargetSlot:       42,
	})
	require.NoError(t, err)
	_, err = pool.Submit(Bundle{
		TargetHash:       target.Hash(),
		TargetParentHash: common.Hash{0x22},
		TargetGeneration: 2,
		Transaction:      currentPrivate,
		TargetSlot:       42,
	})
	require.NoError(t, err)

	provided, err := pool.ProvideTxns(t.Context(),
		txnprovider.WithTargetSlot(42),
		txnprovider.WithTargetParentHash(common.Hash{0x22}),
		txnprovider.WithTargetGeneration(2),
	)
	require.NoError(t, err)
	require.Equal(t, []common.Hash{target.Hash(), currentPrivate.Hash()}, hashes(provided))
}

func TestPoolKeepsActiveSameSlotBuildContextAtCapacity(t *testing.T) {
	target, oldPrivate, currentPrivate := testTxn(1), testTxn(2), testTxn(3)
	pool := New(&testProvider{txns: []types.Transaction{target}}, 1, WithContextActive(func(slot, generation uint64) bool {
		return slot == 42 && (generation == 1 || generation == 2)
	}))
	_, err := pool.Submit(Bundle{TargetHash: target.Hash(), TargetGeneration: 1, Transaction: oldPrivate, TargetSlot: 42})
	require.NoError(t, err)
	_, err = pool.Submit(Bundle{TargetHash: target.Hash(), TargetGeneration: 2, Transaction: currentPrivate, TargetSlot: 42})
	require.ErrorContains(t, err, "capacity")
}
