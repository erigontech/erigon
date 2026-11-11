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

package follower_test

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/execution/types"
)

// queuedLogs is an L1 whose next log reads fail with the queued errors, in
// order.
type queuedLogs struct {
	*recording.MemoryChain
	mu   sync.Mutex
	errs []error
}

func (r *queuedLogs) fail(errs ...error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, errs...)
}

func (r *queuedLogs) Logs(ctx context.Context, from, to uint64, address common.Address, topics [][]common.Hash) ([]*types.Log, error) {
	r.mu.Lock()
	if len(r.errs) > 0 {
		err := r.errs[0]
		r.errs = r.errs[1:]
		r.mu.Unlock()
		return nil, err
	}
	r.mu.Unlock()
	return r.MemoryChain.Logs(ctx, from, to, address, topics)
}

// lines returns the node's log lines that hold msg.
func (n *node) lines(msg string) []string {
	var out []string
	for line := range strings.SplitSeq(n.logs.String(), "\n") {
		if strings.Contains(line, msg) {
			out = append(out, line)
		}
	}
	return out
}

// TestRetryLogLevels: a running follower whose step fails on a missing blob
// logs its retries and its recovery at debug level; any other failure logs
// its retries at warning level and its recovery at info level.
func TestRetryLogLevels(t *testing.T) {
	for _, tc := range []struct {
		name              string
		err               error
		failed, recovered string
	}{
		{"blob missing", fmt.Errorf("batch 01…02: %w: slot 7 of L1 block 9", l1.ErrBlobUnavailable), "dbug", "dbug"},
		{"L1 failure", errors.New("L1 unreachable"), "warn", "info"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := derivertest.NewComposer(t)
			composeBatches(t, c, 1, 3)
			src := &queuedLogs{MemoryChain: c.L1}
			n := newNode(t, src)
			n.run(t)
			require.Eventually(t, func() bool { return n.follower.Status().Phase == follower.PhaseFollowing }, 10*time.Second, 5*time.Millisecond)

			src.fail(tc.err, tc.err)
			composeBatches(t, c, 1, 3)
			require.Eventually(t, func() bool { return len(n.lines("follower recovered")) > 0 }, 10*time.Second, 5*time.Millisecond)
			failed := n.lines("follower step failed, retrying")
			require.Len(t, failed, 2)
			for _, line := range failed {
				require.Contains(t, line, "lvl="+tc.failed+" ")
				require.Contains(t, line, tc.err.Error())
			}
			recovered := n.lines("follower recovered")
			require.Len(t, recovered, 1)
			require.Contains(t, recovered[0], "lvl="+tc.recovered+" ")
		})
	}
}

// TestRecoveryLevelPerStreak: each failing streak logs its recovery at debug
// level only when every retry of that streak did.
func TestRecoveryLevelPerStreak(t *testing.T) {
	missing := fmt.Errorf("batch 01…02: %w: slot 7 of L1 block 9", l1.ErrBlobUnavailable)
	other := errors.New("L1 unreachable")
	c := derivertest.NewComposer(t)
	composeBatches(t, c, 1, 3)
	src := &queuedLogs{MemoryChain: c.L1}
	n := newNode(t, src)
	n.run(t)
	require.Eventually(t, func() bool { return n.follower.Status().Phase == follower.PhaseFollowing }, 10*time.Second, 5*time.Millisecond)

	for i, streak := range []struct {
		errs []error
		lvl  string
	}{
		{[]error{other}, "info"},
		{[]error{missing}, "dbug"},
		{[]error{missing, other}, "info"},
		{[]error{missing, missing}, "dbug"},
	} {
		src.fail(streak.errs...)
		composeBatches(t, c, 1, 3)
		require.Eventually(t, func() bool { return len(n.lines("follower recovered")) > i }, 10*time.Second, 5*time.Millisecond)
		require.Contains(t, n.lines("follower recovered")[i], "lvl="+streak.lvl+" ", "streak %d", i)
	}
}

// retryPauses returns the pause of each retry the node logged with msg.
func (n *node) retryPauses(msg string) []string {
	var out []string
	for _, line := range n.lines(msg) {
		out = append(out, regexp.MustCompile(` in=(\S+)`).FindStringSubmatch(line)[1])
	}
	return out
}

// composeInOne composes two batches of three blocks for one L1 block, the
// second settling on the first.
func composeInOne(c *derivertest.Composer) (first, second *derivertest.Batch) {
	first = composeBatch(c, 3)
	c.Settle(first.Blocks[len(first.Blocks)-1].Header())
	return first, composeBatch(c, 3)
}

// TestRetryAfterProgress: a step that fails after deriving a batch starts a
// new failing streak, retried after the first pause again; one that fails
// without progress doubles the pause.
func TestRetryAfterProgress(t *testing.T) {
	c := derivertest.NewComposer(t)
	composeBatches(t, c, 1, 3)
	src := &queuedLogs{MemoryChain: c.L1}
	n := newNode(t, src)
	n.run(t)
	require.Eventually(t, func() bool { return n.follower.Status().Phase == follower.PhaseFollowing }, 10*time.Second, 5*time.Millisecond)

	// The step fails on L1, then derives the first batch and fails on the
	// second, which both arrive in one L1 block, then fails without progress.
	first, second := composeInOne(c)
	missing := fmt.Errorf("batch 03…04: %w: slot 9 of L1 block 11", l1.ErrBlobUnavailable)
	n.chain.failOnce(second.Blocks[0].NumberU64(), missing)
	src.fail(errors.New("L1 unreachable"))
	c.Post(derivertest.Posting{Batch: first}, derivertest.Posting{Batch: second})
	n.requireBlocks(t, blocksOf(first, second)...)
	require.Equal(t, []string{"5ms", "5ms"}, n.retryPauses("follower step failed, retrying"))

	src.fail(errors.New("L1 unreachable"), errors.New("L1 unreachable"))
	composeBatches(t, c, 1, 3)
	require.Eventually(t, func() bool { return len(n.lines("follower recovered")) == 2 }, 10*time.Second, 5*time.Millisecond)
	require.Equal(t, []string{"5ms", "5ms", "5ms", "10ms"}, n.retryPauses("follower step failed, retrying"))
}

// TestBootRetryAfterProgress: a boot attempt that fails after deriving a
// batch is retried after the first pause again.
func TestBootRetryAfterProgress(t *testing.T) {
	c := derivertest.NewComposer(t)
	first, second := composeInOne(c)
	c.Post(derivertest.Posting{Batch: first}, derivertest.Posting{Batch: second})
	src := &queuedLogs{MemoryChain: c.L1}
	n := newNode(t, src)
	n.chain.failOnce(second.Blocks[0].NumberU64(), fmt.Errorf("batch 03…04: %w: slot 9 of L1 block 11", l1.ErrBlobUnavailable))
	src.fail(errors.New("L1 unreachable"))
	require.NoError(t, n.follower.Boot(t.Context()))
	n.requireBlocks(t, blocksOf(first, second)...)
	require.Equal(t, []string{"5ms", "5ms"}, n.retryPauses("boot catch-up failed, retrying"))
}
