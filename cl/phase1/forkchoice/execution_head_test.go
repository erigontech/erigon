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
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
)

type executionHeadGraph struct {
	*getFinalizedExecutionHashForkGraph
	verified bool
	envelope bool
}

func (g *executionHeadGraph) PayloadAccepted(common.Hash) (bool, bool) { return g.verified, true }
func (g *executionHeadGraph) HasEnvelope(common.Hash) bool             { return g.envelope }

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
		verified:                           true, envelope: true,
	}
	hashes, err := lru.New[common.Hash, common.Hash](8)
	require.NoError(t, err)
	hashes.Add(head.Root, common.Hash{3})
	hashes.Add(common.Hash{}, common.Hash{2})
	store := &ForkChoiceStore{beaconCfg: &cfg, forkGraph: graph, headHash: head.Root, headPayloadStatus: head.PayloadStatus, eth2Roots: hashes}
	store.justifiedCheckpoint.Store(solid.Checkpoint{})
	store.finalizedCheckpoint.Store(solid.Checkpoint{})
	return store, graph, head
}

func TestGetExecutionHeadUsesCanonicalPayloadStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status cltypes.PayloadStatus
		want   common.Hash
	}{
		{"FULL", cltypes.PayloadStatusFull, common.Hash{3}},
		{"EMPTY", cltypes.PayloadStatusEmpty, common.Hash{2}},
		{"PENDING", cltypes.PayloadStatusPending, common.Hash{2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, _, head := executionHeadFixture(t)
			head.PayloadStatus, store.headPayloadStatus = tc.status, tc.status
			hash, ready := store.GetExecutionHead(head)
			require.True(t, ready)
			require.Equal(t, tc.want, hash)
		})
	}
}

func TestGetExecutionHeadRejectsStaleOrUnverifiedFull(t *testing.T) {
	for _, reason := range []string{"root changed", "status changed", "cache invalidated", "not verified", "no envelope", "different payload", "unverified anchor"} {
		t.Run(reason, func(t *testing.T) {
			store, graph, head := executionHeadFixture(t)
			switch reason {
			case "root changed":
				store.headHash = common.Hash{9}
			case "status changed":
				store.headPayloadStatus = cltypes.PayloadStatusEmpty
			case "cache invalidated":
				store.headHash = common.Hash{}
			case "not verified":
				graph.verified = false
			case "no envelope":
				graph.envelope = false
			case "different payload":
				store.eth2Roots.Add(head.Root, common.Hash{9})
			case "unverified anchor":
				graph.anchorRoot = head.Root
				delete(graph.blocks, head.Root)
				graph.verified = false
			}
			_, ready := store.GetExecutionHead(head)
			require.False(t, ready)
		})
	}
}

func TestUpdateExecutionHeadDiscardsChangedDecision(t *testing.T) {
	store, _, head := executionHeadFixture(t)
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	store.engine = engine
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), common.Hash{2}, common.Hash{2}, common.Hash{3}, nil, clparams.GloasVersion).
		DoAndReturn(func(ctx context.Context, _, _, _ common.Hash, _ *engine_types.PayloadAttributes, _ clparams.StateVersion) ([]byte, error) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.LessOrEqual(t, time.Until(deadline), time.Second)
			require.True(t, store.mu.TryLock(), "execution work must not hold the fork-choice lock")
			store.headHash = common.Hash{}
			store.mu.Unlock()
			return nil, nil
		})
	hash, err := store.UpdateExecutionHead(t.Context(), head)
	require.NoError(t, err)
	require.Zero(t, hash, "a superseded decision must remain eligible for canonical retry")
}

func TestUpdateExecutionHeadWithoutEngine(t *testing.T) {
	store, _, head := executionHeadFixture(t)
	hash, err := store.UpdateExecutionHead(t.Context(), head)
	require.NoError(t, err)
	require.Equal(t, common.Hash{3}, hash, "an absent engine must not leave the canonical decision pending for retry")
}

func TestUpdateExecutionHeadBoundsSlowExecution(t *testing.T) {
	store, _, head := executionHeadFixture(t)
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	store.engine = engine
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), nil, clparams.GloasVersion).
		DoAndReturn(func(ctx context.Context, _, _, _ common.Hash, _ *engine_types.PayloadAttributes, _ clparams.StateVersion) ([]byte, error) {
			require.True(t, store.mu.TryLock())
			store.mu.Unlock()
			<-ctx.Done()
			return nil, ctx.Err()
		})
	hash, err := store.UpdateExecutionHead(t.Context(), head)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, hash)
}
