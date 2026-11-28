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

package forkchoice

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
)

type executionHeadGraph struct {
	*getFinalizedExecutionHashForkGraph
	verified bool
}

func (g *executionHeadGraph) PayloadAccepted(common.Hash) (bool, bool) { return g.verified, true }

func executionHeadFixture(t *testing.T) (*ForkChoiceStore, *executionHeadGraph, ForkChoiceNode) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch = 0, 0, 0
	cfg.DenebForkEpoch, cfg.ElectraForkEpoch, cfg.FuluForkEpoch = 0, 0, 0
	cfg.GloasForkEpoch = 0
	cfg.InitializeForkSchedule()
	head := ForkChoiceNode{Root: common.Hash{1}, PayloadStatus: cltypes.PayloadStatusFull}
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	block.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockHash = common.Hash{2}
	block.Block.Body.SignedExecutionPayloadBid.Message.BlockHash = common.Hash{3}
	graph := &executionHeadGraph{
		getFinalizedExecutionHashForkGraph: &getFinalizedExecutionHashForkGraph{blocks: map[common.Hash]*cltypes.SignedBeaconBlock{head.Root: block}},
		verified:                           true,
	}
	store := &ForkChoiceStore{beaconCfg: &cfg, forkGraph: graph}
	return store, graph, head
}

func TestGetExecutionHeadResolvesNode(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*ForkChoiceStore, *executionHeadGraph, *ForkChoiceNode)
		want   common.Hash
		ready  bool
	}{
		{
			name:  "Gloas FULL verified",
			want:  common.Hash{3},
			ready: true,
		},
		{
			name: "Gloas FULL unverified",
			mutate: func(_ *ForkChoiceStore, graph *executionHeadGraph, _ *ForkChoiceNode) {
				graph.verified = false
			},
			want:  common.Hash{3},
			ready: true,
		},
		{
			name: "Gloas EMPTY",
			mutate: func(_ *ForkChoiceStore, _ *executionHeadGraph, head *ForkChoiceNode) {
				head.PayloadStatus = cltypes.PayloadStatusEmpty
			},
			want:  common.Hash{2},
			ready: true,
		},
		{
			name: "Gloas PENDING",
			mutate: func(_ *ForkChoiceStore, _ *executionHeadGraph, head *ForkChoiceNode) {
				head.PayloadStatus = cltypes.PayloadStatusPending
			},
			want:  common.Hash{2},
			ready: true,
		},
		{
			name: "pre-Gloas block",
			mutate: func(store *ForkChoiceStore, graph *executionHeadGraph, head *ForkChoiceNode) {
				graph.blocks[head.Root] = cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.FuluVersion)
			},
		},
		{
			name: "block not in graph",
			mutate: func(_ *ForkChoiceStore, graph *executionHeadGraph, head *ForkChoiceNode) {
				delete(graph.blocks, head.Root)
			},
		},
		{
			name: "Gloas block without bid",
			mutate: func(store *ForkChoiceStore, graph *executionHeadGraph, head *ForkChoiceNode) {
				block := cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.GloasVersion)
				block.Block.Body.SignedExecutionPayloadBid = nil
				graph.blocks[head.Root] = block
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, graph, head := executionHeadFixture(t)
			if tc.mutate != nil {
				tc.mutate(store, graph, &head)
			}
			hash, ready := store.GetExecutionHead(head)
			require.Equal(t, tc.ready, ready)
			require.Equal(t, tc.want, hash)
		})
	}
}

func TestGetHeadExecutionHashFallsBackToEth1Hash(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*ForkChoiceStore, *executionHeadGraph, *ForkChoiceNode)
		want   common.Hash
	}{
		{name: "Gloas FULL", want: common.Hash{3}},
		{
			name: "Gloas EMPTY",
			mutate: func(_ *ForkChoiceStore, _ *executionHeadGraph, head *ForkChoiceNode) {
				head.PayloadStatus = cltypes.PayloadStatusEmpty
			},
			want: common.Hash{2},
		},
		{
			name: "Gloas PENDING",
			mutate: func(_ *ForkChoiceStore, _ *executionHeadGraph, head *ForkChoiceNode) {
				head.PayloadStatus = cltypes.PayloadStatusPending
			},
			want: common.Hash{2},
		},
		{
			name: "pre-Gloas block",
			mutate: func(store *ForkChoiceStore, graph *executionHeadGraph, head *ForkChoiceNode) {
				graph.blocks[head.Root] = cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.FuluVersion)
			},
			want: common.Hash{9},
		},
		{
			name: "block without bid",
			mutate: func(_ *ForkChoiceStore, graph *executionHeadGraph, head *ForkChoiceNode) {
				delete(graph.blocks, head.Root)
			},
			want: common.Hash{9},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, graph, head := executionHeadFixture(t)
			var err error
			store.eth2Roots, err = lru.New[common.Hash, common.Hash](4)
			require.NoError(t, err)
			store.eth2Roots.Add(head.Root, common.Hash{9})
			if tc.mutate != nil {
				tc.mutate(store, graph, &head)
			}
			require.Equal(t, tc.want, store.GetHeadExecutionHash(head))
		})
	}
}

func TestLockExecutionHeadHonoursContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &ForkChoiceStore{executionHeadLock: make(chan struct{}, 1)}
		unlock, err := store.LockExecutionHead(t.Context())
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() {
			unlockWaiter, err := store.LockExecutionHead(ctx)
			if err == nil {
				unlockWaiter()
			}
			result <- err
		}()
		synctest.Wait()
		require.Empty(t, result, "LockExecutionHead returned while the lock was held")

		cancel()
		synctest.Wait()
		require.ErrorIs(t, <-result, context.Canceled)

		unlock()
		again, err := store.LockExecutionHead(t.Context())
		require.NoError(t, err, "the cancelled waiter must not hold the lock")
		again()
	})
}

func TestLockExecutionHeadRejectsCancelledContextWhenFree(t *testing.T) {
	store := &ForkChoiceStore{executionHeadLock: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for range 200 {
		unlock, err := store.LockExecutionHead(ctx)
		if err == nil {
			unlock()
		}
		require.ErrorIs(t, err, context.Canceled)
	}
}

func TestRecordExecutionHead(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want common.Hash
	}{
		{name: "success", want: common.Hash{2}},
		{name: "timeout", err: execution_client.ErrForkChoiceUpdateTimeout, want: common.Hash{2}},
		{name: "busy", err: execution_client.ErrForkChoiceUpdateBusy, want: common.Hash{1}},
		{name: "other error", err: errors.New("rejected"), want: common.Hash{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &ForkChoiceStore{}
			store.RecordExecutionHead(common.Hash{1}, nil)
			store.RecordExecutionHead(common.Hash{2}, tc.err)
			require.Equal(t, tc.want, store.LastExecutionHead())
		})
	}
}

func TestTryLockExecutionHead(t *testing.T) {
	store := &ForkChoiceStore{executionHeadLock: make(chan struct{}, 1)}
	unlock, ok := store.TryLockExecutionHead()
	require.True(t, ok)

	_, ok = store.TryLockExecutionHead()
	require.False(t, ok, "the lock is held")

	unlock()
	unlock, ok = store.TryLockExecutionHead()
	require.True(t, ok, "unlock frees the lock")
	unlock()
}
