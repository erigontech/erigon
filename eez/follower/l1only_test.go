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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
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
