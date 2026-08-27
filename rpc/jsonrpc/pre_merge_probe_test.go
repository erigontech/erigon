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
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon.  If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"

	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
)

const (
	// probeGrace is how long a test waits for a probe that must not start. It only
	// bounds how quickly a duplicated probe is noticed, so a slow machine costs
	// sensitivity rather than a false failure.
	probeGrace = 100 * time.Millisecond
	// probeMergeHeight is any merge point above the oldest block the stub reports,
	// which is all probePreMergeBlockData needs to reach a verdict.
	probeMergeHeight = 1000
)

var errProbeFailed = errors.New("probe failed")

// probeBlockReader answers MinimumBlockAvailable from fn, which receives the call
// number so a test can hold or fail one probe at a time.
type probeBlockReader struct {
	dbservices.FullBlockReader
	calls atomic.Int64
	fn    func(call int64) (uint64, error)
}

func (r *probeBlockReader) MinimumBlockAvailable(ctx context.Context, tx kv.Tx) (uint64, error) {
	return r.fn(r.calls.Add(1))
}

// CanonicalBodyForStorage answers the early-body leg the probe takes when the oldest
// block reported is genesis: a body without user transactions means the datadir holds
// what pre-merge blocks it has in full.
func (r *probeBlockReader) CanonicalBodyForStorage(ctx context.Context, tx kv.Getter, blockNum uint64) (*types.BodyForStorage, error) {
	return &types.BodyForStorage{}, nil
}

func newProbeAPI(reader dbservices.FullBlockReader) *BaseAPI {
	return &BaseAPI{_blockReader: reader, _preMergeDataTTL: time.Minute}
}

type probeResult struct {
	holds bool
	err   error
}

func callProbe(api *BaseAPI, ctx context.Context, out chan<- probeResult) {
	go func() {
		holds, err := api.holdsPreMergeBlockData(ctx, nil, probeMergeHeight)
		out <- probeResult{holds: holds, err: err}
	}()
}

// TestPreMergeProbeDedupsConcurrentCallers pins that callers arriving while a probe
// is in flight take its result instead of each querying the backend, which on a
// remote rpcdaemon is several RPCs held under a read transaction.
func TestPreMergeProbeDedupsConcurrentCallers(t *testing.T) {
	t.Parallel()

	const callers = 16
	entered := make(chan int64, callers)
	release := make(chan struct{})
	reader := &probeBlockReader{fn: func(call int64) (uint64, error) {
		entered <- call
		<-release
		return 2, nil
	}}
	api := newProbeAPI(reader)

	results := make(chan probeResult, callers)
	for range callers {
		callProbe(api, t.Context(), results)
	}

	require.EqualValues(t, 1, <-entered, "the first caller runs the probe")
	select {
	case call := <-entered:
		close(release)
		t.Fatalf("caller ran its own probe (call %d) while one was in flight", call)
	case <-time.After(probeGrace):
	}
	close(release)

	for range callers {
		res := <-results
		require.NoError(t, res.err)
		require.False(t, res.holds)
	}
	require.EqualValues(t, 1, reader.calls.Load(), "one probe answers every concurrent caller")
}

// TestPreMergeProbeFollowerHonorsContext pins that waiting for another caller's probe
// does not outlive the waiter's own request: the read transaction it holds must be
// released when its client goes away, however long the backend takes.
func TestPreMergeProbeFollowerHonorsContext(t *testing.T) {
	t.Parallel()

	entered := make(chan int64, 2)
	release := make(chan struct{})
	defer close(release)
	reader := &probeBlockReader{fn: func(call int64) (uint64, error) {
		entered <- call
		<-release
		return 2, nil
	}}
	api := newProbeAPI(reader)

	leader := make(chan probeResult, 1)
	callProbe(api, t.Context(), leader)
	require.EqualValues(t, 1, <-entered)

	ctx, cancel := context.WithCancel(t.Context())
	follower := make(chan probeResult, 1)
	callProbe(api, ctx, follower)
	cancel()

	select {
	case res := <-follower:
		require.ErrorIs(t, res.err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("follower waited on the in-flight probe past its own cancellation")
	}
}

// TestPreMergeProbeRetriesAfterLeaderError pins that a failed probe is not shared as a
// verdict: the probe runs on the caller's transaction, so losing it says nothing about
// what the datadir holds and the next caller must ask again on its own.
func TestPreMergeProbeRetriesAfterLeaderError(t *testing.T) {
	t.Parallel()

	const followers = 8
	entered := make(chan int64, followers+1)
	failLeader := make(chan struct{})
	releaseRetry := make(chan struct{})
	reader := &probeBlockReader{fn: func(call int64) (uint64, error) {
		entered <- call
		if call == 1 {
			<-failLeader
			return 0, errProbeFailed
		}
		<-releaseRetry
		return 2, nil
	}}
	api := newProbeAPI(reader)

	leader := make(chan probeResult, 1)
	callProbe(api, t.Context(), leader)
	require.EqualValues(t, 1, <-entered)

	results := make(chan probeResult, followers)
	for range followers {
		callProbe(api, t.Context(), results)
	}
	select {
	case call := <-entered:
		close(failLeader)
		close(releaseRetry)
		t.Fatalf("follower ran its own probe (call %d) while one was in flight", call)
	case <-time.After(probeGrace):
	}

	close(failLeader)
	require.ErrorIs(t, (<-leader).err, errProbeFailed)

	require.EqualValues(t, 2, <-entered, "one follower takes over the failed probe")
	select {
	case call := <-entered:
		close(releaseRetry)
		t.Fatalf("more than one follower retried the failed probe (call %d)", call)
	case <-time.After(probeGrace):
	}
	close(releaseRetry)

	for range followers {
		res := <-results
		require.NoError(t, res.err)
		require.False(t, res.holds)
	}
	require.EqualValues(t, 2, reader.calls.Load(), "a failed probe costs one retry, not one per caller")
}

// TestPreMergeProbeWakesWaitersWithItsAnswer pins the hand-off itself: a caller arriving
// while a probe runs stays parked instead of asking the backend, and the probe's answer
// is what wakes it.
func TestPreMergeProbeWakesWaitersWithItsAnswer(t *testing.T) {
	t.Parallel()

	const waiters = 2
	entered := make(chan int64, waiters+1)
	release := make(chan struct{})
	reader := &probeBlockReader{fn: func(call int64) (uint64, error) {
		entered <- call
		<-release
		return 0, nil
	}}
	api := newProbeAPI(reader)

	results := make(chan probeResult, waiters+1)
	callProbe(api, t.Context(), results)
	require.EqualValues(t, 1, <-entered, "the first caller asks the backend")
	for range waiters {
		callProbe(api, t.Context(), results)
	}

	select {
	case res := <-results:
		close(release)
		t.Fatalf("a caller answered (holds=%v) before the probe did", res.holds)
	case call := <-entered:
		close(release)
		t.Fatalf("waiter asked the backend itself (call %d) instead of waiting", call)
	case <-time.After(probeGrace):
	}
	close(release)

	for range waiters + 1 {
		select {
		case res := <-results:
			require.NoError(t, res.err)
			require.True(t, res.holds, "the probe answer reaches every caller waiting on it")
		case <-time.After(5 * time.Second):
			t.Fatal("a waiter was not woken by the probe answer")
		}
	}
	require.EqualValues(t, 1, reader.calls.Load(), "one backend request answers all three callers")
}

// TestPreMergeProbeReleasesWaitersOnPanic pins that a probe dying without an answer
// still releases the callers waiting on it: the RPC server recovers the request that
// ran it, and the ones parked behind it must not be left waiting on a probe nobody
// will finish.
func TestPreMergeProbeReleasesWaitersOnPanic(t *testing.T) {
	t.Parallel()

	entered := make(chan int64, 2)
	reader := &probeBlockReader{fn: func(call int64) (uint64, error) {
		entered <- call
		if call == 1 {
			panic("probe blew up")
		}
		return 0, nil
	}}
	api := newProbeAPI(reader)

	recovered := make(chan struct{})
	go func() {
		defer close(recovered)
		defer func() { _ = recover() }()
		_, _ = api.holdsPreMergeBlockData(t.Context(), nil, probeMergeHeight)
	}()
	<-recovered
	require.EqualValues(t, 1, <-entered)

	results := make(chan probeResult, 1)
	callProbe(api, t.Context(), results)
	select {
	case res := <-results:
		require.NoError(t, res.err)
		require.True(t, res.holds, "the next caller runs its own probe and gets a verdict")
	case <-time.After(5 * time.Second):
		t.Fatal("a probe that died left the next caller waiting on it forever")
	}
}

// chainProbeBlockReader answers the probe from a chain described by the user
// transactions of each block. unreadable keeps the bodies while taking the
// transactions away, which is what chain history expiry leaves on disk.
type chainProbeBlockReader struct {
	dbservices.FullBlockReader
	userTxns   []int
	inflation  []int
	unreadable bool
	bodyReads  atomic.Int64
}

func (r *chainProbeBlockReader) MinimumBlockAvailable(ctx context.Context, tx kv.Tx) (uint64, error) {
	return 0, nil
}

func (r *chainProbeBlockReader) CanonicalBodyForStorage(ctx context.Context, tx kv.Getter, blockNum uint64) (*types.BodyForStorage, error) {
	r.bodyReads.Add(1)
	if blockNum >= uint64(len(r.userTxns)) {
		return nil, nil
	}
	base := 0
	for block, txns := range r.userTxns[:blockNum] {
		base += systemTxsPerBlock + txns
		if block < len(r.inflation) {
			base += r.inflation[block]
		}
	}
	return &types.BodyForStorage{
		BaseTxnID: types.BaseTxnID(base),
		TxCount:   uint32(systemTxsPerBlock + r.userTxns[blockNum]),
	}, nil
}

func (r *chainProbeBlockReader) TxnByIdxInBlock(ctx context.Context, tx kv.Getter, blockNum uint64, i int) (types.Transaction, bool, error) {
	if r.unreadable || blockNum >= uint64(len(r.userTxns)) || r.userTxns[blockNum] <= i {
		return nil, false, nil
	}
	return types.NewTransaction(0, common.Address{}, uint256.NewInt(1), 21000, nil, nil), true, nil
}

// sparsePreMergeChain has its only user transaction in a block no halving candidate
// below the merge height reaches.
func sparsePreMergeChain() []int {
	chain := make([]int, probeSparseMergeHeight)
	chain[3] = 1
	return chain
}

const probeSparseMergeHeight = 8

// TestPreMergeVerdictFindsATransactionOffTheSampledPath pins that an archive is read as
// one however its transactions are spread: the sampled blocks prove nothing on a sparse
// chain, while the cumulative count says an early transaction is there to be found.
func TestPreMergeVerdictFindsATransactionOffTheSampledPath(t *testing.T) {
	t.Parallel()

	reader := &chainProbeBlockReader{userTxns: sparsePreMergeChain()}
	api := newProbeAPI(reader)

	holds, decided, err := api.probePreMergeBlockData(t.Context(), nil, probeSparseMergeHeight)
	require.NoError(t, err)
	require.True(t, decided, "a chain that holds an early transaction answers the question")
	require.True(t, holds, "the datadir holds pre-merge transactions, sampled or not")
}

// TestPreMergeVerdictReadsASparseExpiryAsExpiry pins the other side of the same chain:
// with the transactions gone the verdict is expiry, and it is settled rather than left
// open, so it is remembered for the TTL like any other observation.
func TestPreMergeVerdictReadsASparseExpiryAsExpiry(t *testing.T) {
	t.Parallel()

	reader := &chainProbeBlockReader{userTxns: sparsePreMergeChain(), unreadable: true}
	api := newProbeAPI(reader)

	holds, decided, err := api.probePreMergeBlockData(t.Context(), nil, probeSparseMergeHeight)
	require.NoError(t, err)
	require.True(t, decided, "bodies on disk without their transactions answer the question")
	require.False(t, holds)
}

// TestPreMergeVerdictStopsAtTheFirstSampledTransaction pins that the search for a
// sparse transaction is the exception: a chain whose sampled block already holds one
// must not pay for it, since this runs on every request that refreshes the verdict.
func TestPreMergeVerdictStopsAtTheFirstSampledTransaction(t *testing.T) {
	t.Parallel()

	dense := make([]int, 64)
	for i := range dense {
		dense[i] = 1
	}
	reader := &chainProbeBlockReader{userTxns: dense}
	api := newProbeAPI(reader)

	holds, decided, err := api.probePreMergeBlockData(t.Context(), nil, uint64(len(dense)))
	require.NoError(t, err)
	require.True(t, decided)
	require.True(t, holds)
	require.LessOrEqual(t, reader.bodyReads.Load(), int64(2), "the count and the first sampled block answer")
}

// TestPreMergeVerdictDoesNotSettleOnAnInflatedCount pins that the cumulative count is
// taken for what it is, an upper bound: the database numbers non-canonical bodies from
// the same sequence, so it can point below the first user transaction. A block that
// holds none cannot say whether the datadir does, and answering from it would settle
// expiry on an archive.
func TestPreMergeVerdictDoesNotSettleOnAnInflatedCount(t *testing.T) {
	t.Parallel()

	userTxns := make([]int, probeSparseMergeHeight)
	userTxns[6] = 1
	inflation := make([]int, probeSparseMergeHeight)
	inflation[0] = 1
	reader := &chainProbeBlockReader{userTxns: userTxns, inflation: inflation}
	api := newProbeAPI(reader)

	holds, decided, err := api.probePreMergeBlockData(t.Context(), nil, probeSparseMergeHeight)
	require.NoError(t, err)
	require.False(t, decided, "a count inflated by non-canonical bodies answers nothing")
	require.False(t, holds)
}
