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
	"fmt"
	"math/rand"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"

	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
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
	api := &BaseAPI{_blockReader: reader}
	api._preMergeData.SetTTL(time.Minute)
	return api
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
	missing    map[uint64]bool
	unreadable bool
	bodyReads  atomic.Int64
}

func (r *chainProbeBlockReader) MinimumBlockAvailable(ctx context.Context, tx kv.Tx) (uint64, error) {
	return 0, nil
}

func (r *chainProbeBlockReader) CanonicalBodyForStorage(ctx context.Context, tx kv.Getter, blockNum uint64) (*types.BodyForStorage, error) {
	r.bodyReads.Add(1)
	if blockNum >= uint64(len(r.userTxns)) || r.missing[blockNum] {
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

// preMergeGateAPI wires the probe reader into the gate, which reads the prune mode and
// the chain config the datadir stores.
func preMergeGateAPI(reader dbservices.FullBlockReader, mergeHeight uint64) *BaseAPI {
	api := newProbeAPI(reader)
	api._pruneMode.Store(&prune.Mode{
		Initialised: true,
		History:     prune.KeepPostMergeBlocksPruneMode,
		Blocks:      prune.KeepPostMergeBlocksPruneMode,
	})
	api._chainConfig.Store(&chain.Config{MergeHeight: &mergeHeight})
	api._genesis.Store(&types.Block{})
	return api
}

// inflatedCountArchive holds a pre-merge transaction the cumulative count cannot point
// at: a non-canonical body numbered from the same sequence moves the bound below it.
func inflatedCountArchive() *chainProbeBlockReader {
	userTxns := make([]int, probeSparseMergeHeight)
	userTxns[6] = 1
	inflation := make([]int, probeSparseMergeHeight)
	inflation[0] = 1
	return &chainProbeBlockReader{userTxns: userTxns, inflation: inflation}
}

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

// TestPreMergeVerdictSearchesPastAnInflatedBound pins how far the cumulative count is
// taken: it is an upper bound, since the database numbers non-canonical bodies from the
// same sequence, so the block it points at can hold no transaction while a later one
// does. The search continues past that block instead of settling the question there.
func TestPreMergeVerdictSearchesPastAnInflatedBound(t *testing.T) {
	t.Parallel()

	api := newProbeAPI(inflatedCountArchive())

	holds, decided, err := api.probePreMergeBlockData(t.Context(), nil, probeSparseMergeHeight)
	require.NoError(t, err)
	require.True(t, holds, "the search reaches the transaction the bound points below")
	require.True(t, decided)
}

// TestPreMergeGateServesAnArchiveOffTheCountedBound pins the gate against the shape the
// count points below: the datadir serves a pre-merge transaction, so rejecting its blocks
// as expired is the one answer that costs the caller data it has.
func TestPreMergeGateServesAnArchiveOffTheCountedBound(t *testing.T) {
	t.Parallel()

	reader := inflatedCountArchive()
	txn, ok, err := reader.TxnByIdxInBlock(t.Context(), nil, 6, 0)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, txn, "the fixture must hold a readable pre-merge transaction")

	api := preMergeGateAPI(reader, probeSparseMergeHeight)

	expiry, _, err := api.blocksFollowChainHistoryExpiry(t.Context(), nil)
	require.NoError(t, err)
	require.False(t, expiry, "a count that confirms nothing is not an observation of expiry")

	for block := uint64(1); block < probeSparseMergeHeight; block++ {
		require.NoError(t, api.checkPruneBlocks(t.Context(), nil, block))
	}
}

// TestPreMergeGateRefusesExpiredDataItsCountCannotConfirm pins the expiry reading of the
// shape the count points below: the bodies record transactions and none of them can be
// read, so the search has to reach the block holding one rather than trust the count.
func TestPreMergeGateRefusesExpiredDataItsCountCannotConfirm(t *testing.T) {
	t.Parallel()

	reader := inflatedCountArchive()
	reader.unreadable = true
	api := preMergeGateAPI(reader, probeSparseMergeHeight)

	for block := uint64(1); block < probeSparseMergeHeight; block++ {
		require.ErrorIs(t, api.checkPruneBlocks(t.Context(), nil, block), state.PrunedError,
			"bodies without their transactions are expiry, whatever the count says")
	}
}

// TestPreMergeGateLeavesAMissingSearchBodyUnanswered pins that block data the search
// cannot read leaves the question open: it is not evidence of an archive, and it is not
// an observation to remember either, so the gate refuses and asks again.
func TestPreMergeGateLeavesAMissingSearchBodyUnanswered(t *testing.T) {
	t.Parallel()

	reader := inflatedCountArchive()
	reader.missing = map[uint64]bool{6: true}
	api := preMergeGateAPI(reader, probeSparseMergeHeight)

	require.ErrorIs(t, api.checkPruneBlocks(t.Context(), nil, 1), state.PrunedError,
		"a body the search needs and cannot read does not open the gate")
	_, observed, _ := api._preMergeData.Load()
	require.False(t, observed, "a question left open is not an observation")
}

// TestPreMergeSearchStopsAtItsReadBudget pins that a search walking past its budget
// leaves the question open. A count inflated on every block and no block confirming one
// is the shape that walks furthest.
func TestPreMergeSearchStopsAtItsReadBudget(t *testing.T) {
	t.Parallel()

	const length = 1024
	reader := &chainProbeBlockReader{userTxns: make([]int, length), inflation: slices.Repeat([]int{1}, length)}
	api := newProbeAPI(reader)

	holds, decided, err := api.probePreMergeBlockData(t.Context(), nil, length)
	require.NoError(t, err)
	require.False(t, holds, "a search that stopped short has not observed an archive")
	require.False(t, decided, "and has nothing to remember")
	require.LessOrEqual(t, reader.bodyReads.Load(), int64(earlyTxnSearchBudget+16), "the budget bounds the walk")
}

// TestPreMergeVerdictReadsAnInflationOnlyCountAsArchive pins the other end the search
// can reach: every block the count leaves room for a transaction in is readable and none
// records one, so the count is inflation alone and there is no transaction to be missing.
func TestPreMergeVerdictReadsAnInflationOnlyCountAsArchive(t *testing.T) {
	t.Parallel()

	reader := inflatedCountArchive()
	reader.userTxns[6] = 0
	api := newProbeAPI(reader)

	holds, decided, err := api.probePreMergeBlockData(t.Context(), nil, probeSparseMergeHeight)
	require.NoError(t, err)
	require.True(t, holds, "a chain with no pre-merge transaction has none the datadir could be missing")
	require.True(t, decided, "every body the count pointed into was read")
}

// TestPreMergeVerdictHoldsAcrossChainShapes sweeps the shapes the named cases pin one at
// a time: transactions anywhere, counts inflated anywhere, bodies missing anywhere. A
// datadir whose transactions cannot be read is never an archive, and one holding every
// block is never refused.
func TestPreMergeVerdictHoldsAcrossChainShapes(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(11))
	for range 20000 {
		length := 1 + rng.Intn(24)
		userTxns, inflation := make([]int, length), make([]int, length)
		missing := map[uint64]bool{}
		for block := range userTxns {
			if rng.Intn(4) == 0 {
				userTxns[block] = 1 + rng.Intn(3)
			}
			if rng.Intn(5) == 0 {
				inflation[block] = 1 + rng.Intn(3)
			}
			if rng.Intn(8) == 0 {
				missing[uint64(block)] = true
			}
		}
		reader := &chainProbeBlockReader{
			userTxns:   userTxns,
			inflation:  inflation,
			missing:    missing,
			unreadable: rng.Intn(2) == 0,
		}
		holds, decided, err := newProbeAPI(reader).probePreMergeBlockData(t.Context(), nil, uint64(length))
		require.NoError(t, err)
		shape := func() string {
			return fmt.Sprintf("user=%v inflation=%v missing=%v unreadable=%v", userTxns, inflation, missing, reader.unreadable)
		}

		anyTxn := slices.ContainsFunc(userTxns, func(txns int) bool { return txns > 0 })
		if reader.unreadable && anyTxn && holds {
			t.Fatalf("expired transaction data read as an archive: %s", shape())
		}
		if len(missing) == 0 && !reader.unreadable {
			if !holds {
				t.Fatalf("a datadir holding every block refused: %s", shape())
			}
			if !decided {
				t.Fatalf("a datadir holding every block left unanswered: %s", shape())
			}
		}
	}
}
