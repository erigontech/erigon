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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/execution/types"
)

// pruned returns the composer's L1 from block first on, a source that no
// longer serves the blocks below it.
func pruned(t *testing.T, c *derivertest.Composer, first uint64) *recording.MemoryChain {
	t.Helper()
	blocks := c.L1.Canonical()
	chain, err := recording.NewMemoryChain(derivertest.Config().L2.(*config.L2Config).L1ChainID, blocks[first])
	require.NoError(t, err)
	for _, b := range blocks[first+1:] {
		require.NoError(t, chain.Append(b))
	}
	return chain
}

// TestL1OnlyHistoryIncomplete: an L1-only boot stops when L1 no longer
// serves a block it needs more than 64 blocks below the L1 head, and
// retries while the block is 64 or fewer below, which a lagging source may
// yet serve.
func TestL1OnlyHistoryIncomplete(t *testing.T) {
	c := derivertest.NewComposer(t)
	composeBatches(t, c, 1, 3)
	deploy := derivertest.Config().L2.(*config.L2Config).RegistryDeployBlock
	c.Advance(63)
	head, err := c.L1.Head(t.Context())
	require.NoError(t, err)
	require.Equal(t, deploy+64, head.Number, "the deploy block, unserved, 64 below the head")
	err = newNode(t, pruned(t, c, deploy+1)).follower.Boot(t.Context())
	require.ErrorIs(t, err, l1.ErrNotFound)
	require.NotErrorIs(t, err, follower.ErrL1HistoryIncomplete)

	c.Advance(1)
	n := newNode(t, pruned(t, c, deploy+1))
	require.ErrorIs(t, n.follower.Boot(t.Context()), follower.ErrL1HistoryIncomplete)
	require.False(t, n.logged("boot catch-up failed"), "boot stops without retrying")
}

// logsBelow is an L1 that no longer serves the logs of the blocks below
// from, once set.
type logsBelow struct {
	*recording.MemoryChain
	from atomic.Uint64
}

func (r *logsBelow) Logs(ctx context.Context, from, to uint64, address common.Address, topics [][]common.Hash) ([]*types.Log, error) {
	if from < r.from.Load() {
		return nil, fmt.Errorf("%w: logs of [%d, %d]", l1.ErrNotFound, from, to)
	}
	return r.MemoryChain.Logs(ctx, from, to, address, topics)
}

// unserveNext waits until n follows the head of c's L1, then makes src stop
// serving the logs of the next L1 block, which it returns.
func unserveNext(t *testing.T, n *node, c *derivertest.Composer, src *logsBelow) uint64 {
	t.Helper()
	require.Eventually(t, func() bool { return n.follower.Status().Phase == follower.PhaseFollowing }, 10*time.Second, 5*time.Millisecond)
	head, err := c.L1.Head(t.Context())
	require.NoError(t, err)
	require.Equal(t, head.Number, n.follower.Status().L1Scanned)
	src.from.Store(head.Number + 2)
	return head.Number + 1
}

// TestL1OnlyHistoryIncompleteRunning: a running L1-only follower stops, as
// its boot does, once L1 no longer serves a block it needs more than 64
// blocks below the L1 head, and retries while the block is 64 or fewer
// below.
func TestL1OnlyHistoryIncompleteRunning(t *testing.T) {
	c := derivertest.NewComposer(t)
	composeBatches(t, c, 1, 3)
	src := &logsBelow{MemoryChain: c.L1}
	n := newNode(t, src)
	_, done := n.run(t)
	next := unserveNext(t, n, c, src)

	c.Advance(65)
	unserved := fmt.Sprintf("L1 block %d not served", next)
	require.Eventually(t, func() bool { return n.logged("follower step failed, retrying", unserved) }, 10*time.Second, 5*time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("stopped with block %d 64 below the head: %v", next, err)
	case <-time.After(200 * time.Millisecond):
	}

	c.Advance(1)
	select {
	case err := <-done:
		require.ErrorIs(t, err, follower.ErrL1HistoryIncomplete)
		var u *l1.UnservedError
		require.ErrorAs(t, err, &u)
		require.Equal(t, next, u.Block)
	case <-time.After(10 * time.Second):
		t.Fatalf("still retrying block %d 65 below the head", next)
	}
}

// TestPeersModeRetriesUnservedHistory: a follower in peers mode, whose
// history below L1 finality comes from its peers, keeps retrying a block L1
// no longer serves, however deep below the L1 head.
func TestPeersModeRetriesUnservedHistory(t *testing.T) {
	c := derivertest.NewComposer(t)
	composeBatches(t, c, 2, 3)
	finalizeHead(t, c)
	src := &logsBelow{MemoryChain: c.L1}
	n, _, _ := newPeersNode(t, c, src, true)
	_, done := n.run(t)
	next := unserveNext(t, n, c, src)

	c.Advance(66)
	unserved := fmt.Sprintf("L1 block %d not served", next)
	require.Eventually(t, func() bool { return n.logged("follower step failed, retrying", unserved) }, 10*time.Second, 5*time.Millisecond)
	head, err := c.L1.Head(t.Context())
	require.NoError(t, err)
	require.Greater(t, head.Number, next+64)
	select {
	case err := <-done:
		t.Fatalf("stopped with block %d %d below the head: %v", next, head.Number-next, err)
	case <-time.After(500 * time.Millisecond):
	}
	require.False(t, n.logged("L1 no longer serves a block"))
}

// rootAt is an L1 whose registry records root for the rollup at every block.
type rootAt struct {
	*recording.MemoryChain
	root common.Hash
}

func (r rootAt) CallAt(context.Context, l1.Call, common.Hash) ([]byte, error) {
	out := make([]byte, 96)
	copy(out[12:32], derivertest.RollupContract[:])
	copy(out[32:64], r.root[:])
	return out, nil
}

// TestL1OnlyRootCheck: an L1-only boot stops when its scan ends without the
// root L1's finalized block held before it, and checks nothing on a source
// without state reads.
func TestL1OnlyRootCheck(t *testing.T) {
	c := derivertest.NewComposer(t)
	composeBatches(t, c, 1, 3)
	finalizeHead(t, c)
	require.NoError(t, newNode(t, c.L1).follower.Boot(t.Context()))
	n := newNode(t, rootAt{c.L1, common.Hash{0x01}})
	require.ErrorIs(t, n.follower.Boot(t.Context()), follower.ErrL1HistoryIncomplete)
	require.False(t, n.logged("boot catch-up failed"), "boot stops without retrying")
	require.NoError(t, newNode(t, struct{ l1.ChainReader }{c.L1}).follower.Boot(t.Context()))
}
