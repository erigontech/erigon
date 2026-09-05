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

package handler

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/beacon/builder"
	builder_mock "github.com/erigontech/erigon/cl/beacon/builder/mock_services"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	gossip_topic "github.com/erigontech/erigon/cl/gossip"
	blob_storage_mock "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	gossip_mock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	sync_pool_mock "github.com/erigontech/erigon/cl/validator/sync_contribution_pool/mock_services"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

type updateFailingDB struct {
	kv.RwDB
}

func (db updateFailingDB) Update(context.Context, func(kv.RwTx) error) error {
	return errors.New("stop after persistence")
}

func matchedSelfBuildEnvelope(t *testing.T, cfg *clparams.BeaconChainConfig, block *cltypes.SignedBeaconBlock) *cltypes.SignedExecutionPayloadEnvelope {
	t.Helper()
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	payload := cltypes.NewEth1Block(clparams.GloasVersion, cfg)
	payload.Extra = solid.NewExtraData()
	payload.Transactions = solid.NewProgressiveTransactionsSSZ()
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	payload.SlotNumber = block.Block.Slot
	payload.ParentHash = bid.Message.ParentBlockHash
	payload.PrevRandao = bid.Message.PrevRandao
	payload.FeeRecipient = bid.Message.FeeRecipient
	payload.GasLimit = bid.Message.GasLimit
	requests := cltypes.NewExecutionRequestsWithVersion(cfg, clparams.GloasVersion)
	requestsRoot, err := requests.HashSSZ()
	require.NoError(t, err)
	bid.Message.ExecutionRequestsRoot = requestsRoot
	requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(cfg, requests))
	payload.BlockHash, err = payload.ComputeBlockHash(&block.Block.ParentRoot, requestsHash, nil)
	require.NoError(t, err)
	bid.Message.BlockHash = payload.BlockHash
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	return &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			Payload:               payload,
			ExecutionRequests:     requests,
			BuilderIndex:          bid.Message.BuilderIndex,
			BeaconBlockRoot:       blockRoot,
			ParentBeaconBlockRoot: block.Block.ParentRoot,
		},
		Signature: common.Bytes96{1},
	}
}

func TestBroadcastSelfBuildEnvelopeStopsPermanentApplyErrorBeforeGossip(t *testing.T) {
	_, _, _, _, _, h, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	h.gossipManager = gossipManager
	fcu.OnExecutionPayloadErr = errors.New("invalid payload")

	block := cltypes.NewSignedBeaconBlock(h.beaconChainCfg, clparams.GloasVersion)
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	bid.Message.BuilderIndex = clparams.BuilderIndexSelfBuild
	bid.Message.BlockHash = common.HexToHash("0x1234")
	envelope := matchedSelfBuildEnvelope(t, h.beaconChainCfg, block)
	h.selfBuildPayloads.Add(bid.Message.BlockHash, &selfBuildPayload{Payload: cltypes.NewEth1Block(clparams.GloasVersion, h.beaconChainCfg)})

	err := h.broadcastSelfBuildEnvelope(context.Background(), block, envelope)
	require.ErrorContains(t, err, "invalid payload")
	_, cached := h.selfBuildPayloads.Get(bid.Message.BlockHash)
	require.True(t, cached)
}

func TestBroadcastSelfBuildEnvelopeDoesNotPublishQueuedUnverifiedEnvelope(t *testing.T) {
	_, _, _, _, _, h, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	h.gossipManager = gossipManager
	fcu.OnExecutionPayloadErr = forkchoice.ErrIgnore

	block := cltypes.NewSignedBeaconBlock(h.beaconChainCfg, clparams.GloasVersion)
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	bid.Message.BuilderIndex = clparams.BuilderIndexSelfBuild
	bid.Message.BlockHash = common.HexToHash("0x1234")
	envelope := matchedSelfBuildEnvelope(t, h.beaconChainCfg, block)
	h.selfBuildPayloads.Add(bid.Message.BlockHash, &selfBuildPayload{Payload: cltypes.NewEth1Block(clparams.GloasVersion, h.beaconChainCfg)})
	require.ErrorIs(t, h.broadcastSelfBuildEnvelope(context.Background(), block, envelope), forkchoice.ErrIgnore)
	_, cached := h.selfBuildPayloads.Get(bid.Message.BlockHash)
	require.True(t, cached)
}

func TestBroadcastSelfBuildEnvelopeRejectsUnsignedLocalFallback(t *testing.T) {
	_, _, _, _, _, h, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	h.gossipManager = gossip_mock.NewMockGossip(ctrl)
	fcu.ApplyLocalSelfBuildEnvelopeErr = errors.New("local apply must not be called")

	block := cltypes.NewSignedBeaconBlock(h.beaconChainCfg, clparams.GloasVersion)
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	bid.Message.BuilderIndex = clparams.BuilderIndexSelfBuild
	bid.Message.BlockHash = common.HexToHash("0x1234")
	payload := cltypes.NewEth1Block(clparams.GloasVersion, h.beaconChainCfg)
	payload.BlockHash = bid.Message.BlockHash
	payload.SlotNumber = block.Block.Slot
	requests := cltypes.NewExecutionRequestsWithVersion(h.beaconChainCfg, clparams.GloasVersion)
	requestsRoot, err := requests.HashSSZ()
	require.NoError(t, err)
	bid.Message.ExecutionRequestsRoot = requestsRoot
	h.selfBuildPayloads.Add(bid.Message.BlockHash, &selfBuildPayload{Payload: payload, ExecutionRequests: requests})

	require.ErrorContains(t, h.broadcastSelfBuildEnvelope(context.Background(), block, nil), "validator-signed envelope")
	_, cached := h.selfBuildPayloads.Get(bid.Message.BlockHash)
	require.True(t, cached)
}

func TestBroadcastSelfBuildEnvelopeRejectsQueuedEnvelopeForDifferentBlock(t *testing.T) {
	tests := []struct {
		name    string
		wantErr string
		mutate  func(t *testing.T, block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope)
	}{
		{name: "beacon block root", wantErr: "beacon block root", mutate: func(_ *testing.T, _ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) {
			envelope.Message.BeaconBlockRoot = common.HexToHash("0xdead")
		}},
		{name: "parent beacon block root", wantErr: "parent beacon block root", mutate: func(_ *testing.T, _ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) {
			envelope.Message.ParentBeaconBlockRoot = common.HexToHash("0xdead")
		}},
		{name: "payload slot", wantErr: "payload slot", mutate: func(_ *testing.T, _ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) {
			envelope.Message.Payload.SlotNumber++
		}},
		{name: "builder index", wantErr: "builder index", mutate: func(_ *testing.T, _ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) {
			envelope.Message.BuilderIndex++
		}},
		{name: "payload block hash", wantErr: "block hash", mutate: func(_ *testing.T, _ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) {
			envelope.Message.Payload.BlockHash = common.HexToHash("0xdead")
		}},
		{name: "prev randao", wantErr: "prev randao", mutate: func(_ *testing.T, _ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) {
			envelope.Message.Payload.PrevRandao = common.HexToHash("0xdead")
		}},
		{name: "execution requests root", wantErr: "execution requests root", mutate: func(t *testing.T, block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) {
			block.Block.Body.GetSignedExecutionPayloadBid().Message.ExecutionRequestsRoot = common.HexToHash("0xdead")
			blockRoot, err := block.Block.HashSSZ()
			require.NoError(t, err)
			envelope.Message.BeaconBlockRoot = blockRoot
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, _, _, h, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
			ctrl := gomock.NewController(t)
			h.gossipManager = gossip_mock.NewMockGossip(ctrl)
			fcu.OnExecutionPayloadErr = forkchoice.ErrIgnore

			block := cltypes.NewSignedBeaconBlock(h.beaconChainCfg, clparams.GloasVersion)
			bid := block.Block.Body.GetSignedExecutionPayloadBid()
			bid.Message.BuilderIndex = clparams.BuilderIndexSelfBuild
			bid.Message.BlockHash = common.HexToHash("0x1234")
			envelope := matchedSelfBuildEnvelope(t, h.beaconChainCfg, block)
			h.selfBuildPayloads.Add(bid.Message.BlockHash, &selfBuildPayload{Payload: cltypes.NewEth1Block(clparams.GloasVersion, h.beaconChainCfg)})
			test.mutate(t, block, envelope)

			err := h.broadcastSelfBuildEnvelope(context.Background(), block, envelope)
			require.ErrorContains(t, err, test.wantErr)
			_, cached := h.selfBuildPayloads.Get(bid.Message.BlockHash)
			require.True(t, cached)
		})
	}
}

func TestBroadcastSelfBuildEnvelopeContinuesWhenIndicesArePending(t *testing.T) {
	_, _, _, _, _, h, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	h.gossipManager = gossipManager
	fcu.OnExecutionPayloadErr = fmt.Errorf("index write failed: %w", forkchoice.ErrExecutionPayloadEnvelopeIndicesPending)

	block := cltypes.NewSignedBeaconBlock(h.beaconChainCfg, clparams.GloasVersion)
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	bid.Message.BuilderIndex = clparams.BuilderIndexSelfBuild
	bid.Message.BlockHash = common.HexToHash("0x1234")
	envelope := matchedSelfBuildEnvelope(t, h.beaconChainCfg, block)
	h.selfBuildPayloads.Add(bid.Message.BlockHash, &selfBuildPayload{Payload: cltypes.NewEth1Block(clparams.GloasVersion, h.beaconChainCfg)})
	gossipManager.EXPECT().Publish(gomock.Any(), gossip_topic.TopicNameExecutionPayload, gomock.Any()).Return(nil)

	require.NoError(t, h.broadcastSelfBuildEnvelope(context.Background(), block, envelope))
	_, cached := h.selfBuildPayloads.Get(bid.Message.BlockHash)
	require.False(t, cached)
}

func TestBroadcastSelfBuildEnvelopeUsesFullyValidatedIngress(t *testing.T) {
	_, _, _, _, _, h, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	h.gossipManager = gossipManager
	fcu.ApplyLocalSelfBuildEnvelopeErr = errors.New("local apply must not be called")

	block := cltypes.NewSignedBeaconBlock(h.beaconChainCfg, clparams.GloasVersion)
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	bid.Message.BuilderIndex = clparams.BuilderIndexSelfBuild
	bid.Message.BlockHash = common.HexToHash("0x1234")
	envelope := matchedSelfBuildEnvelope(t, h.beaconChainCfg, block)
	h.selfBuildPayloads.Add(bid.Message.BlockHash, &selfBuildPayload{Payload: cltypes.NewEth1Block(clparams.GloasVersion, h.beaconChainCfg)})
	gossipManager.EXPECT().Publish(gomock.Any(), gossip_topic.TopicNameExecutionPayload, gomock.Any()).Return(nil)

	require.NoError(t, h.broadcastSelfBuildEnvelope(context.Background(), block, envelope))
	require.True(t, fcu.OnExecutionPayloadCheckBlobData)
	_, cached := h.selfBuildPayloads.Get(bid.Message.BlockHash)
	require.False(t, cached)
}

func TestIntegrateBlockPersistsGeneratedColumnsBeforeForkchoice(t *testing.T) {
	_, _, _, _, _, h, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	columnStorage := blob_storage_mock.NewMockDataColumnStorage(ctrl)
	h.columnStorage = columnStorage
	block := cltypes.NewSignedBeaconBlock(h.beaconChainCfg, clparams.GloasVersion)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	column := &cltypes.DataColumnSidecar{Index: 3}
	written := false
	columnStorage.EXPECT().WriteColumnSidecars(gomock.Any(), blockRoot, int64(column.Index), column).DoAndReturn(
		func(context.Context, common.Hash, int64, *cltypes.DataColumnSidecar) error {
			written = true
			return nil
		},
	)
	fcu.OnBlockFunc = func(context.Context, *cltypes.SignedBeaconBlock, bool, bool, bool) error {
		require.True(t, written)
		return nil
	}
	fcu.OnTickFunc = func(uint64) {}

	_, err = h.integrateBlockAndBlobs(t.Context(), block, nil, []*cltypes.DataColumnSidecar{column})

	require.NoError(t, err)
}

func TestPublishBlockAndSidecarsReturnsPublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	h := &ApiHandler{gossipManager: gossipManager}
	injected := errors.New("injected publish failure")
	gossipManager.EXPECT().Publish(gomock.Any(), gossip_topic.TopicNameBeaconBlock, []byte{1}).Return(injected)

	err := h.publishBlockAndSidecars(t.Context(), []byte{1}, nil, nil, clparams.Phase0Version)

	require.ErrorIs(t, err, injected)
}

func TestPublishDataColumnSidecarsReturnsPublishFailure(t *testing.T) {
	if clparams.GetBeaconConfig() == nil {
		clparams.InitGlobalStaticConfig(&clparams.MainnetBeaconConfig, &clparams.CaplinConfig{})
	}
	_, _, _, _, _, h, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	h.gossipManager = gossip_mock.NewMockGossip(ctrl)
	injected := errors.New("column publish failed")
	column := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
	h.gossipManager.(*gossip_mock.MockGossip).EXPECT().Publish(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(injected)

	err := h.publishDataColumnSidecars(t.Context(), []*cltypes.DataColumnSidecar{column})

	require.ErrorIs(t, err, injected)
}

func TestBroadcastSelfBuildEnvelopeRetriesGossipAfterPublishFailure(t *testing.T) {
	_, _, _, _, _, h, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	h.gossipManager = gossipManager

	block := cltypes.NewSignedBeaconBlock(h.beaconChainCfg, clparams.GloasVersion)
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	bid.Message.BuilderIndex = clparams.BuilderIndexSelfBuild
	bid.Message.BlockHash = common.HexToHash("0x1234")
	envelope := matchedSelfBuildEnvelope(t, h.beaconChainCfg, block)
	h.selfBuildPayloads.Add(bid.Message.BlockHash, &selfBuildPayload{Payload: cltypes.NewEth1Block(clparams.GloasVersion, h.beaconChainCfg)})
	var publishes atomic.Int32
	gossipManager.EXPECT().Publish(gomock.Any(), gossip_topic.TopicNameExecutionPayload, gomock.Any()).Times(2).DoAndReturn(
		func(context.Context, string, []byte) error {
			if publishes.Add(1) == 1 {
				return errors.New("publish failed")
			}
			return nil
		},
	)

	require.ErrorContains(t, h.broadcastSelfBuildEnvelope(context.Background(), block, envelope), "publish failed")
	_, cached := h.selfBuildPayloads.Get(bid.Message.BlockHash)
	require.True(t, cached)
	require.NoError(t, h.broadcastSelfBuildEnvelope(context.Background(), block, envelope))
	require.EqualValues(t, 2, publishes.Load())
	_, cached = h.selfBuildPayloads.Get(bid.Message.BlockHash)
	require.False(t, cached)
}

func TestBroadcastSelfBuildEnvelopePublishesValidatedEnvelopeBeforePersistenceRetry(t *testing.T) {
	_, _, _, _, _, h, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	h.gossipManager = gossipManager
	fcu.OnExecutionPayloadErr = fmt.Errorf("disk unavailable: %w", forkchoice.ErrExecutionPayloadEnvelopePersistenceFailed)

	block := cltypes.NewSignedBeaconBlock(h.beaconChainCfg, clparams.GloasVersion)
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	bid.Message.BuilderIndex = clparams.BuilderIndexSelfBuild
	bid.Message.BlockHash = common.HexToHash("0x1234")
	envelope := matchedSelfBuildEnvelope(t, h.beaconChainCfg, block)
	h.selfBuildPayloads.Add(bid.Message.BlockHash, &selfBuildPayload{Payload: cltypes.NewEth1Block(clparams.GloasVersion, h.beaconChainCfg)})
	gossipManager.EXPECT().Publish(gomock.Any(), gossip_topic.TopicNameExecutionPayload, gomock.Any()).Return(nil)

	require.ErrorIs(t, h.broadcastSelfBuildEnvelope(context.Background(), block, envelope), forkchoice.ErrExecutionPayloadEnvelopePersistenceFailed)
	_, cached := h.selfBuildPayloads.Get(bid.Message.BlockHash)
	require.True(t, cached)
}

func TestBroadcastSelfBuildEnvelopeCoalescesConcurrentPublication(t *testing.T) {
	_, _, _, _, _, h, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	h.gossipManager = gossipManager
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	var publishes atomic.Int32
	fcu.OnExecutionPayloadFunc = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	}
	gossipManager.EXPECT().Publish(gomock.Any(), gossip_topic.TopicNameExecutionPayload, gomock.Any()).DoAndReturn(
		func(context.Context, string, []byte) error {
			publishes.Add(1)
			return nil
		},
	).AnyTimes()
	block := cltypes.NewSignedBeaconBlock(h.beaconChainCfg, clparams.GloasVersion)
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	bid.Message.BuilderIndex = clparams.BuilderIndexSelfBuild
	envelope := matchedSelfBuildEnvelope(t, h.beaconChainCfg, block)

	done := make(chan error, 2)
	go func() { done <- h.broadcastSelfBuildEnvelope(t.Context(), block, envelope) }()
	<-started
	go func() { done <- h.broadcastSelfBuildEnvelope(t.Context(), block, envelope) }()
	close(release)

	require.NoError(t, <-done)
	require.NoError(t, <-done)
	require.EqualValues(t, 1, calls.Load())
	require.EqualValues(t, 1, publishes.Load())
}

func TestPublishSelfBuildEnvelopeAfterIntegration(t *testing.T) {
	t.Run("optional envelope absent does not wait for integration", func(t *testing.T) {
		integrated := make(chan error, 1)
		done := make(chan error, 1)
		called := false
		go func() {
			done <- publishSelfBuildEnvelopeAfterIntegration(t.Context(), integrated, false, time.Second, 2*time.Second, func(context.Context) error {
				called = true
				return nil
			})
		}()

		select {
		case err := <-done:
			require.NoError(t, err)
			require.False(t, called)
		case <-time.After(100 * time.Millisecond):
			integrated <- nil
			require.NoError(t, <-done)
			t.Fatal("canonical block publication waited for asynchronous integration without an envelope")
		}
	})

	t.Run("optional envelope absent", func(t *testing.T) {
		integrated := make(chan error, 1)
		integrated <- nil
		called := false
		require.NoError(t, publishSelfBuildEnvelopeAfterIntegration(t.Context(), integrated, false, time.Second, 2*time.Second, func(context.Context) error {
			called = true
			return nil
		}))
		require.False(t, called)
	})

	t.Run("integration failure", func(t *testing.T) {
		integrated := make(chan error, 1)
		integrated <- errors.New("integration failed")
		called := false
		err := publishSelfBuildEnvelopeAfterIntegration(t.Context(), integrated, true, time.Second, 2*time.Second, func(context.Context) error {
			called = true
			return nil
		})
		require.ErrorContains(t, err, "integration failed")
		require.False(t, called)
	})

	t.Run("integration success", func(t *testing.T) {
		integrated := make(chan error, 1)
		integrated <- nil
		called := false
		require.NoError(t, publishSelfBuildEnvelopeAfterIntegration(t.Context(), integrated, true, time.Second, 2*time.Second, func(context.Context) error {
			called = true
			return nil
		}))
		require.True(t, called)
	})

	t.Run("canceled", func(t *testing.T) {
		integrated := make(chan error, 1)
		integrated <- nil
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		called := false
		err := publishSelfBuildEnvelopeAfterIntegration(ctx, integrated, true, time.Second, 2*time.Second, func(publicationCtx context.Context) error {
			called = true
			require.NoError(t, publicationCtx.Err())
			return nil
		})
		require.NoError(t, err)
		require.True(t, called)
	})
}

func TestPublishSelfBuildEnvelopeAfterIntegrationRetainsOwnerAfterRequestTimeout(t *testing.T) {
	integrated := make(chan error, 1)
	published := make(chan error, 2)

	err := publishSelfBuildEnvelopeAfterIntegration(t.Context(), integrated, true, 10*time.Millisecond, time.Second, func(ctx context.Context) error {
		published <- ctx.Err()
		return nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)

	integrated <- nil
	select {
	case err := <-published:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("detached envelope owner did not publish after integration completed")
	}

	select {
	case <-published:
		t.Fatal("detached envelope owner published more than once")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestPublishSelfBuildEnvelopeAfterIntegrationOwnerExpiresWithoutIntegration(t *testing.T) {
	integrated := make(chan error, 1)
	published := make(chan struct{}, 1)

	err := publishSelfBuildEnvelopeAfterIntegration(t.Context(), integrated, true, time.Second, 10*time.Millisecond, func(context.Context) error {
		published <- struct{}{}
		return nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)

	integrated <- nil
	select {
	case <-published:
		t.Fatal("expired envelope owner published after late integration")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestRunBlockStoragePhasesSignalsIntegrationBeforeSuffixFailure(t *testing.T) {
	integrated := make(chan error, 1)
	finishStarted := make(chan struct{})
	finishRelease := make(chan struct{})
	done := make(chan error, 1)
	suffixErr := errors.New("forkchoice update failed")

	go func() {
		done <- runBlockStoragePhases(integrated, func() error {
			return nil
		}, func() error {
			close(finishStarted)
			<-finishRelease
			return suffixErr
		})
	}()

	require.NoError(t, <-integrated)
	<-finishStarted
	close(finishRelease)
	require.ErrorIs(t, <-done, suffixErr)
}

func TestRunBlockStoragePhasesDoesNotRunSuffixAfterIntegrationFailure(t *testing.T) {
	integrated := make(chan error, 1)
	integrationErr := errors.New("on block failed")
	finished := false

	err := runBlockStoragePhases(integrated, func() error {
		return integrationErr
	}, func() error {
		finished = true
		return nil
	})

	require.ErrorIs(t, err, integrationErr)
	require.ErrorIs(t, <-integrated, integrationErr)
	require.False(t, finished)
}

func TestBlockBuilderWindowPreGloas(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:   12,
		IntervalsPerSlot: 3,
	}
	slotStart := time.Unix(100, 0)
	now := slotStart

	window := computeBlockBuilderWindow(now, slotStart, cfg, clparams.ElectraVersion, false)

	// Attestation deadline is 4s; polling stops a quarter of it (1s) earlier, at 3s.
	require.Equal(t, slotStart.Add(3*time.Second).Add(-minPayloadPollingWindow), window.firstGetAt)
	require.Equal(t, slotStart.Add(3*time.Second), window.pollUntil)
}

func TestBlockBuilderWindowGloas(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:   12,
		IntervalsPerSlot: 3,
	}
	slotStart := time.Unix(100, 0)
	now := slotStart

	window := computeBlockBuilderWindow(now, slotStart, cfg, clparams.GloasVersion, false)

	// Attestation deadline is 3s; polling stops a quarter of it (750ms) earlier, at 2.25s.
	require.Equal(t, slotStart.Add(2250*time.Millisecond).Add(-minPayloadPollingWindow), window.firstGetAt)
	require.Equal(t, slotStart.Add(2250*time.Millisecond), window.pollUntil)
}

func TestPublishBlindedBlocksRejectsGloas(t *testing.T) {
	_, _, _, _, _, h, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blinded_blocks", bytes.NewReader(nil))
	req.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())

	_, err := h.publishBlindedBlocks(httptest.NewRecorder(), req, 2)
	require.Error(t, err)
	require.Contains(t, err.Error(), cltypes.ErrGloasCannotBlind.Error())
}

func TestParseGloasRequestBeaconBlockAcceptsConfiguredWrappedJSON(t *testing.T) {
	h := &ApiHandler{beaconChainCfg: &clparams.MainnetBeaconConfig}
	wrapper := cltypes.NewDenebSignedBeaconBlock(h.beaconChainCfg, clparams.GloasVersion)
	wrapper.SignedBlock.Block.Slot = 123
	wrapper.SignedExecutionPayloadEnvelope = &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelopeWithVersion(h.beaconChainCfg, clparams.GloasVersion),
	}
	payload := wrapper.SignedExecutionPayloadEnvelope.Message.Payload
	payload.Extra = solid.NewExtraData()
	payload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{{1}})
	payload.Withdrawals = solid.NewStaticListSSZFromList(
		[]*cltypes.Withdrawal{{Index: 1}},
		int(h.beaconChainCfg.MaxWithdrawalsPerPayload),
		44,
	)
	body, err := json.Marshal(wrapper)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blocks", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")

	var parsed *cltypes.DenebSignedBeaconBlock
	require.NotPanics(t, func() {
		parsed, err = h.parseGloasRequestBeaconBlock(clparams.GloasVersion, request)
	})
	require.NoError(t, err)
	require.Equal(t, uint64(123), parsed.SignedBlock.Block.Slot)
	require.NotNil(t, parsed.SignedExecutionPayloadEnvelope)
	require.Equal(t, clparams.GloasVersion, parsed.SignedExecutionPayloadEnvelope.Message.Payload.Version())
	require.Equal(t, clparams.GloasVersion, parsed.SignedExecutionPayloadEnvelope.Message.ExecutionRequests.Version())
	require.Equal(t, 1, parsed.SignedExecutionPayloadEnvelope.Message.Payload.Withdrawals.Len())
}

func TestPostBeaconBlocksReturnsAcceptedAfterPublishedBlockIntegrationFailure(t *testing.T) {
	_, _, _, _, _, h, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	injected := errors.New("injected block integration failure")
	fcu.OnBlockFunc = func(context.Context, *cltypes.SignedBeaconBlock, bool, bool, bool) error {
		return injected
	}
	fcu.OnTickFunc = func(uint64) {}
	block := cltypes.NewSignedBeaconBlock(h.beaconChainCfg, clparams.GloasVersion)
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	bid.Message.BuilderIndex = clparams.BuilderIndexSelfBuild
	envelope := matchedSelfBuildEnvelope(t, h.beaconChainCfg, block)
	wrapper := cltypes.NewDenebSignedBeaconBlock(h.beaconChainCfg, clparams.GloasVersion)
	wrapper.SignedBlock = block
	wrapper.SignedExecutionPayloadEnvelope = envelope
	body, err := json.Marshal(wrapper)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blocks", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())

	_, err = h.postBeaconBlocks(httptest.NewRecorder(), request, 2)

	var endpointErr *beaconhttp.EndpointError
	require.ErrorAs(t, err, &endpointErr)
	require.Equal(t, http.StatusAccepted, endpointErr.Code)
	require.ErrorContains(t, err, injected.Error())
}

func TestParseGloasRequestBeaconBlockRejectsMalformedWrappedEnvelope(t *testing.T) {
	h := &ApiHandler{beaconChainCfg: &clparams.MainnetBeaconConfig}
	request := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"/eth/v2/beacon/blocks",
		strings.NewReader(`{"signed_block":{},"signed_execution_payload_envelope":1}`),
	)
	request.Header.Set("Content-Type", "application/json")

	parsed, err := h.parseGloasRequestBeaconBlock(clparams.GloasVersion, request)
	require.Error(t, err)
	require.Nil(t, parsed)
}

func TestParseGloasRequestBeaconBlockRejectsNonCanonicalSSZ(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)

	const requestOffsetsSize = 20
	requestsStart := len(encoded) - requestOffsetsSize
	for offset := requestsStart; offset < len(encoded); offset += 4 {
		binary.LittleEndian.PutUint32(encoded[offset:], requestOffsetsSize+1)
	}
	encoded = append(encoded, 0)

	lax := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	require.NoError(t, lax.DecodeSSZ(encoded, int(clparams.GloasVersion)))
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blocks", bytes.NewReader(encoded))
	request.Header.Set("Content-Type", "application/octet-stream")

	parsed, err := (&ApiHandler{beaconChainCfg: cfg}).parseGloasRequestBeaconBlock(clparams.GloasVersion, request)
	require.Error(t, err)
	require.Nil(t, parsed)
}

func TestParseGloasRequestBeaconBlockAcceptsCanonicalSSZ(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	block.Block.Slot = 123
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blocks", bytes.NewReader(encoded))
	request.Header.Set("Content-Type", "application/octet-stream")

	parsed, err := (&ApiHandler{beaconChainCfg: cfg}).parseGloasRequestBeaconBlock(clparams.GloasVersion, request)
	require.NoError(t, err)
	require.Equal(t, uint64(123), parsed.SignedBlock.Block.Slot)
}

func TestPublishBlindedBlocksRejectsPreBellatrix(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.Phase0Version, clparams.AltairVersion} {
		t.Run(version.String(), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			h := &ApiHandler{
				beaconChainCfg: &clparams.MainnetBeaconConfig,
				builderClient:  builder_mock.NewMockBuilderClient(ctrl),
			}
			block := cltypes.NewSignedBlindedBeaconBlock(&clparams.MainnetBeaconConfig, version)
			body, err := json.Marshal(block)
			require.NoError(t, err)
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blinded_blocks", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Eth-Consensus-Version", version.String())

			_, err = h.publishBlindedBlocks(httptest.NewRecorder(), req, 2)
			require.ErrorContains(t, err, "blinded blocks are unsupported before Bellatrix")
		})
	}

	require.NoError(t, validateBlindedBlockRequest(
		cltypes.NewSignedBlindedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.BellatrixVersion),
		clparams.BellatrixVersion,
	))
}

func TestPublishBlindedBlocksRejectsUnsupportedContentType(t *testing.T) {
	h := &ApiHandler{beaconChainCfg: &clparams.MainnetBeaconConfig}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blinded_blocks", nil)
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Eth-Consensus-Version", clparams.FuluVersion.String())

	_, err := h.publishBlindedBlocks(httptest.NewRecorder(), req, 2)
	var endpointErr *beaconhttp.EndpointError
	require.True(t, errors.As(err, &endpointErr))
	require.Equal(t, http.StatusUnsupportedMediaType, endpointErr.Code)
	require.ErrorContains(t, err, "unsupported content type")
}

func TestPublishBlindedBlocksAcceptsEmptyFuluBuilderResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	builderClient := builder_mock.NewMockBuilderClient(ctrl)
	builderClient.EXPECT().SubmitBlindedBlocks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, block *cltypes.SignedBlindedBeaconBlock) (*cltypes.Eth1Block, *engine_types.BlobsBundle, *cltypes.ExecutionRequests, error) {
			require.Equal(t, cltypes.NewEth1Header(clparams.FuluVersion).EncodingSizeSSZ(), block.Block.Body.ExecutionPayload.EncodingSizeSSZ())
			return nil, nil, nil, nil
		},
	)
	h := &ApiHandler{
		beaconChainCfg: &clparams.MainnetBeaconConfig,
		builderClient:  builderClient,
	}
	block := cltypes.NewSignedBlindedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	body, err := json.Marshal(block)
	require.NoError(t, err)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blinded_blocks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Eth-Consensus-Version", clparams.FuluVersion.String())

	resp, err := h.publishBlindedBlocks(httptest.NewRecorder(), req, 2)
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestPublishBlindedBlocksRejectsMissingPreFuluPayload(t *testing.T) {
	ctrl := gomock.NewController(t)
	builderClient := builder_mock.NewMockBuilderClient(ctrl)
	builderClient.EXPECT().SubmitBlindedBlocks(gomock.Any(), gomock.Any()).Return(nil, nil, nil, nil)
	h := &ApiHandler{
		beaconChainCfg: &clparams.MainnetBeaconConfig,
		builderClient:  builderClient,
	}
	block := cltypes.NewSignedBlindedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.ElectraVersion)
	body, err := json.Marshal(block)
	require.NoError(t, err)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blinded_blocks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Eth-Consensus-Version", clparams.ElectraVersion.String())

	_, err = h.publishBlindedBlocks(httptest.NewRecorder(), req, 2)
	require.ErrorContains(t, err, "builder returned nil execution payload")
}

func TestPublishBlindedBlocksRejectsMalformedRequest(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mutate     func(*cltypes.SignedBlindedBeaconBlock)
		mutateJSON func(*testing.T, []byte) []byte
		wantErr    string
	}{
		{
			name: "missing block",
			mutate: func(block *cltypes.SignedBlindedBeaconBlock) {
				block.Block = nil
			},
			wantErr: "missing block",
		},
		{
			name: "missing body",
			mutate: func(block *cltypes.SignedBlindedBeaconBlock) {
				block.Block.Body = nil
			},
			wantErr: "missing block body",
		},
		{
			name: "null execution payload header",
			mutate: func(block *cltypes.SignedBlindedBeaconBlock) {
				block.Block.Body.ExecutionPayload = nil
			},
			mutateJSON: func(t *testing.T, body []byte) []byte {
				var request map[string]any
				require.NoError(t, json.Unmarshal(body, &request))
				message := request["message"].(map[string]any)
				blockBody := message["body"].(map[string]any)
				blockBody["execution_payload_header"] = nil
				encoded, err := json.Marshal(request)
				require.NoError(t, err)
				return encoded
			},
			wantErr: "missing execution payload header",
		},
		{
			name: "omitted execution payload header",
			mutate: func(block *cltypes.SignedBlindedBeaconBlock) {
				block.Block.Body.ExecutionPayload = nil
			},
			wantErr: "missing execution payload header",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			builderClient := builder_mock.NewMockBuilderClient(ctrl)
			h := &ApiHandler{
				beaconChainCfg: &clparams.MainnetBeaconConfig,
				builderClient:  builderClient,
			}
			block := cltypes.NewSignedBlindedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
			tc.mutate(block)
			body, err := json.Marshal(block)
			require.NoError(t, err)
			if tc.mutateJSON != nil {
				body = tc.mutateJSON(t, body)
			}
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blinded_blocks", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Eth-Consensus-Version", clparams.FuluVersion.String())

			_, err = h.publishBlindedBlocks(httptest.NewRecorder(), req, 2)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestValidateBuilderPayload(t *testing.T) {
	validPayload := func(version clparams.StateVersion) *cltypes.Eth1Block {
		payload := cltypes.NewEth1Block(version, &clparams.MainnetBeaconConfig)
		payload.Extra = solid.NewExtraData()
		payload.Transactions = &solid.TransactionsSSZ{}
		if version.AfterOrEqual(clparams.CapellaVersion) {
			payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(clparams.MainnetBeaconConfig.MaxWithdrawalsPerPayload), 44)
		}
		return payload
	}
	validBellatrix := validPayload(clparams.BellatrixVersion)
	require.NoError(t, validateBuilderPayload(validBellatrix, nil, clparams.BellatrixVersion))

	for _, tc := range []struct {
		name    string
		payload *cltypes.Eth1Block
		version clparams.StateVersion
		wantErr string
	}{
		{name: "missing payload", version: clparams.BellatrixVersion, wantErr: "nil execution payload"},
		{
			name: "missing extra data",
			payload: func() *cltypes.Eth1Block {
				payload := validPayload(clparams.BellatrixVersion)
				payload.Extra = nil
				return payload
			}(),
			version: clparams.BellatrixVersion,
			wantErr: "missing extra data",
		},
		{
			name: "missing transactions",
			payload: func() *cltypes.Eth1Block {
				payload := validPayload(clparams.BellatrixVersion)
				payload.Transactions = nil
				return payload
			}(),
			version: clparams.BellatrixVersion,
			wantErr: "missing transactions",
		},
		{
			name: "missing withdrawals",
			payload: func() *cltypes.Eth1Block {
				payload := validPayload(clparams.CapellaVersion)
				payload.Withdrawals = nil
				return payload
			}(),
			version: clparams.CapellaVersion,
			wantErr: "missing withdrawals",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, validateBuilderPayload(tc.payload, nil, tc.version), tc.wantErr)
		})
	}
}

func TestValidateBuilderPayloadRejectsOlderResponseVersion(t *testing.T) {
	payload := cltypes.NewEth1Block(clparams.BellatrixVersion, &clparams.MainnetBeaconConfig)
	payload.Extra = solid.NewExtraData()
	payload.Transactions = &solid.TransactionsSSZ{}

	require.ErrorContains(t, validateBuilderPayload(payload, nil, clparams.ElectraVersion), "version mismatch")
}

func TestValidateBuilderPayloadRejectsMissingElectraExecutionRequests(t *testing.T) {
	payload := cltypes.NewEth1Block(clparams.ElectraVersion, &clparams.MainnetBeaconConfig)
	payload.Extra = solid.NewExtraData()
	payload.Transactions = &solid.TransactionsSSZ{}
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(clparams.MainnetBeaconConfig.MaxWithdrawalsPerPayload), 44)

	require.ErrorContains(t, validateBuilderPayload(payload, nil, clparams.ElectraVersion), "missing execution requests")
	require.NoError(t, validateBuilderPayload(payload, cltypes.NewExecutionRequestsWithVersion(&clparams.MainnetBeaconConfig, clparams.ElectraVersion), clparams.ElectraVersion))
}

func TestBlockBuilderWindowLateStartKeepsPublicationMargin(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:   12,
		IntervalsPerSlot: 3,
	}
	slotStart := time.Unix(100, 0)
	now := slotStart.Add(2950 * time.Millisecond)

	window := computeBlockBuilderWindow(now, slotStart, cfg, clparams.ElectraVersion, false)

	// A late request clamps the first poll up to now but still stops at 3s, preserving the margin.
	require.Equal(t, now, window.firstGetAt)
	require.Equal(t, slotStart.Add(3*time.Second), window.pollUntil)
}

func TestBlockBuilderWindowLateRequestGrabsImmediately(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:   12,
		IntervalsPerSlot: 3,
	}
	slotStart := time.Unix(100, 0)
	now := slotStart.Add(5 * time.Second)

	window := computeBlockBuilderWindow(now, slotStart, cfg, clparams.GloasVersion, false)

	require.Equal(t, now, window.firstGetAt)
	require.Equal(t, now, window.pollUntil)
}

func TestBlockBuilderWindowReservesPublicationMargin(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:   12,
		IntervalsPerSlot: 3,
	}
	slotStart := time.Unix(100, 0)

	for _, tc := range []struct {
		name          string
		version       clparams.StateVersion
		deadline      time.Duration
		wantPollUntil time.Duration
	}{
		{"pre-gloas", clparams.ElectraVersion, 4 * time.Second, 3 * time.Second},
		{"gloas", clparams.GloasVersion, 3 * time.Second, 2250 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			window := computeBlockBuilderWindow(slotStart, slotStart, cfg, tc.version, false)
			require.Equal(t, slotStart.Add(tc.wantPollUntil), window.pollUntil)
			require.True(t, window.pollUntil.Before(slotStart.Add(tc.deadline)),
				"polling must stop before the attestation deadline to leave publication margin")
		})
	}
}

func TestShouldRetryGetPayloadStopsAtDeadline(t *testing.T) {
	deadline := time.Unix(100, 0)

	require.True(t, shouldRetryGetPayload(deadline.Add(-time.Nanosecond), deadline))
	require.False(t, shouldRetryGetPayload(deadline, deadline))
	require.False(t, shouldRetryGetPayload(deadline.Add(time.Nanosecond), deadline))
}

func TestPollAssembledPayloadReturnsReadyPayload(t *testing.T) {
	ctx := t.Context()
	now := time.Now()
	window := blockBuilderWindow{firstGetAt: now.Add(-time.Millisecond), pollUntil: now.Add(time.Second)}
	want := &cltypes.Eth1Block{}
	calls := 0
	payload, _, _, _, err := pollAssembledPayload(ctx, window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			return want, nil, nil, nil, nil
		})
	require.NoError(t, err)
	require.Same(t, want, payload)
	require.Equal(t, 1, calls)
}

func TestPollAssembledPayloadRetriesWhileBusy(t *testing.T) {
	ctx := t.Context()
	now := time.Now()
	window := blockBuilderWindow{firstGetAt: now, pollUntil: now.Add(time.Second)}
	want := &cltypes.Eth1Block{}
	calls := 0
	payload, _, _, _, err := pollAssembledPayload(ctx, window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			if calls < 3 {
				return nil, nil, nil, nil, nil
			}
			return want, nil, nil, nil, nil
		})
	require.NoError(t, err)
	require.Same(t, want, payload)
	require.Equal(t, 3, calls)
}

func TestPollAssembledPayloadRetriesOnError(t *testing.T) {
	ctx := t.Context()
	now := time.Now()
	window := blockBuilderWindow{firstGetAt: now, pollUntil: now.Add(time.Second)}
	want := &cltypes.Eth1Block{}
	calls := 0
	payload, _, _, _, err := pollAssembledPayload(ctx, window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			if calls == 1 {
				return nil, nil, nil, nil, errors.New("EL busy")
			}
			return want, nil, nil, nil, nil
		})
	require.NoError(t, err)
	require.Same(t, want, payload)
	require.Equal(t, 2, calls)
}

func TestPollAssembledPayloadStopsOnUnknownPayload(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"direct execution client", fmt.Errorf("get payload: %w", chainreader.ErrUnknownPayload)},
		{"remote execution client", fmt.Errorf("get payload: %w", &engine_helpers.UnknownPayloadErr)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			window := blockBuilderWindow{firstGetAt: now, pollUntil: now.Add(50 * time.Millisecond)}
			calls := 0
			payload, _, _, _, err := pollAssembledPayload(context.Background(), window, time.Millisecond,
				func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
					calls++
					return nil, nil, nil, nil, tc.err
				})
			require.True(t, execution_client.IsUnknownPayloadError(err))
			require.Nil(t, payload)
			require.Equal(t, 1, calls)
		})
	}
}

func TestPollAssembledPayloadStopsOnInvalidResponse(t *testing.T) {
	now := time.Now()
	window := blockBuilderWindow{firstGetAt: now, pollUntil: now.Add(50 * time.Millisecond)}
	calls := 0

	payload, _, _, _, err := pollAssembledPayload(t.Context(), window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			return nil, nil, nil, nil, fmt.Errorf("get payload: %w", execution_client.ErrInvalidGetPayloadResponse)
		})

	require.ErrorIs(t, err, execution_client.ErrInvalidGetPayloadResponse)
	require.Nil(t, payload)
	require.Equal(t, 1, calls)
}

func TestProductionReportsUnknownPayloadOnce(t *testing.T) {
	logs := captureProductionLogs(t)

	err := produceBlockWithFailingCollection(t, t.Context(), &engine_helpers.UnknownPayloadErr)
	require.Error(t, err)

	captured := logs()
	require.Equal(t, 1, strings.Count(captured, "execution payload is unknown"), "records:\n"+captured)
	require.Contains(t, captured, "lvl=warn")
	require.NotContains(t, captured, "lvl=eror")
}

func TestPollAssembledPayloadStopsAtDeadline(t *testing.T) {
	ctx := t.Context()
	now := time.Now()
	window := blockBuilderWindow{firstGetAt: now, pollUntil: now.Add(50 * time.Millisecond)}
	calls := 0
	payload, _, _, _, err := pollAssembledPayload(ctx, window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			return nil, nil, nil, nil, nil
		})
	require.Error(t, err)
	require.Nil(t, payload)
	require.NotZero(t, calls)
}

func TestPollAssembledPayloadLateRequestGrabsOnce(t *testing.T) {
	ctx := t.Context()
	past := time.Now().Add(-time.Second)
	window := blockBuilderWindow{firstGetAt: past, pollUntil: past}
	calls := 0
	_, _, _, _, err := pollAssembledPayload(ctx, window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			return nil, nil, nil, nil, nil
		})
	require.Error(t, err)
	require.Equal(t, 1, calls)
}

func TestPollAssembledPayloadReturnsOnContextCancel(t *testing.T) {
	now := time.Now()
	window := blockBuilderWindow{firstGetAt: now.Add(time.Hour), pollUntil: now.Add(time.Hour)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	_, _, _, _, err := pollAssembledPayload(ctx, window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			return nil, nil, nil, nil, nil
		})
	require.Error(t, err)
	require.Zero(t, calls)
}

func TestSetupHeaderResponseForBlockProductionGloasPayloadIncluded(t *testing.T) {
	h := &ApiHandler{}
	rr := httptest.NewRecorder()

	h.setupHeaderReponseForBlockProduction(rr, clparams.GloasVersion, false, true, big.NewInt(123), big.NewInt(456))

	require.Equal(t, "gloas", rr.Header().Get("Eth-Consensus-Version"))
	require.Equal(t, "123", rr.Header().Get("Eth-Execution-Payload-Value"))
	require.Equal(t, "456", rr.Header().Get("Eth-Consensus-Block-Value"))
	require.Equal(t, "false", rr.Header().Get("Eth-Execution-Payload-Blinded"))
	require.Equal(t, "true", rr.Header().Get("Eth-Execution-Payload-Included"))
}

func TestSetupHeaderResponseForBlockProductionPreGloasOmitsPayloadIncluded(t *testing.T) {
	h := &ApiHandler{}
	rr := httptest.NewRecorder()

	h.setupHeaderReponseForBlockProduction(rr, clparams.ElectraVersion, false, true, big.NewInt(123), big.NewInt(456))

	require.Empty(t, rr.Header().Get("Eth-Execution-Payload-Included"))
}

func TestProduceBeaconBodyRejectsInvalidFuluCellProofLength(t *testing.T) {
	proofIndexes := []struct {
		name  string
		index int
	}{
		{name: "first", index: 0},
		{name: "last", index: 2*int(clparams.MainnetBeaconConfig.NumberOfColumns) - 1},
	}
	for _, proofIndex := range proofIndexes {
		for _, proofLength := range []int{length.Bytes48 - 1, length.Bytes48 + 1} {
			t.Run(fmt.Sprintf("%s proof length %d", proofIndex.name, proofLength), func(t *testing.T) {
				body, err := produceFuluBodyWithProofLength(t, proofIndex.index, proofLength)

				require.Nil(t, body)
				require.ErrorContains(t, err, "invalid proof length")
			})
		}
	}
}

func TestProduceBeaconBodyAcceptsExactFuluCellProofLength(t *testing.T) {
	body, err := produceFuluBodyWithProofLength(t, 0, length.Bytes48)

	require.NoError(t, err)
	require.NotNil(t, body)
}

func TestProduceBeaconBodyPreservesPreFuluValidationOrder(t *testing.T) {
	bundle := &engine_types.BlobsBundle{
		Blobs:       []hexutil.Bytes{make([]byte, cltypes.BYTES_PER_BLOB)},
		Commitments: []hexutil.Bytes{make([]byte, length.Bytes48-1)},
		Proofs:      []hexutil.Bytes{make([]byte, length.Bytes48-1)},
	}

	body, err := produceBodyWithBundle(t, clparams.ElectraVersion, bundle)

	require.Nil(t, body)
	require.ErrorContains(t, err, "invalid commitment length")
}

func produceFuluBodyWithProofLength(t *testing.T, proofIndex, proofLength int) (*cltypes.BeaconBody, error) {
	t.Helper()
	proofs := make([]hexutil.Bytes, 2*int(clparams.MainnetBeaconConfig.NumberOfColumns))
	for i := range proofs {
		proofs[i] = make([]byte, length.Bytes48)
	}
	proofs[proofIndex] = make([]byte, proofLength)
	bundle := &engine_types.BlobsBundle{
		Blobs: []hexutil.Bytes{
			make([]byte, cltypes.BYTES_PER_BLOB),
			make([]byte, cltypes.BYTES_PER_BLOB),
		},
		Commitments: []hexutil.Bytes{
			make([]byte, length.Bytes48),
			make([]byte, length.Bytes48),
		},
		Proofs: proofs,
	}
	return produceBodyWithBundle(t, clparams.FuluVersion, bundle)
}

func produceBodyWithBundle(t *testing.T, version clparams.StateVersion, bundle *engine_types.BlobsBundle) (*cltypes.BeaconBody, error) {
	t.Helper()
	_, blocks, _, _, postState, h, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	if version.AfterOrEqual(clparams.FuluVersion) {
		h.beaconChainCfg.FuluForkEpoch = 1
		h.beaconChainCfg.InitializeForkSchedule()
	}

	payload := cltypes.NewEth1Block(version, h.beaconChainCfg)
	payload.Extra = solid.NewExtraData()
	payload.Transactions = solid.NewTransactionsSSZFromTransactions(nil)
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(h.beaconChainCfg.MaxWithdrawalsPerPayload), 44)

	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]byte{1}, nil)
	engine.EXPECT().GetAssembledBlock(gomock.Any(), []byte{1}, version).Return(payload, bundle, nil, nil, nil)
	h.engine = engine

	baseBlock := blocks[len(blocks)-1].Block
	baseBlockRoot, err := baseBlock.HashSSZ()
	require.NoError(t, err)

	body, _, err := h.produceBeaconBody(
		t.Context(), 3, baseBlock.Slot, baseBlockRoot, postState, baseBlock.Slot+1,
		common.Bytes96{0xc0}, common.Hash{},
	)
	return body, err
}

func TestProduceBeaconBodyAcceptsMissingBlobsBundleBeforeDeneb(t *testing.T) {
	_, blocks, _, _, postState, h, _, _, _, _ := setupTestingHandler(t, clparams.CapellaVersion, log.Root(), true)

	payload := cltypes.NewEth1BlockFromExecutionHeader(postState.LatestExecutionPayloadHeader(), clparams.CapellaVersion, h.beaconChainCfg)
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]byte{1}, nil)
	engine.EXPECT().GetAssembledBlock(gomock.Any(), []byte{1}, clparams.CapellaVersion).
		Return(payload, nil, nil, nil, nil)
	h.engine = engine

	baseBlock := blocks[len(blocks)-1].Block
	baseBlockRoot, err := baseBlock.HashSSZ()
	require.NoError(t, err)

	body, _, err := h.produceBeaconBody(
		t.Context(), 3, baseBlock.Slot, baseBlockRoot, postState, baseBlock.Slot+1,
		common.Bytes96{0xc0}, common.Hash{},
	)

	require.NoError(t, err)
	require.NotNil(t, body)
	require.Zero(t, body.BlobKzgCommitments.Len())
}

func TestProduceBeaconBodyRejectsMissingBlobsBundleAtDeneb(t *testing.T) {
	_, blocks, _, _, postState, h, _, _, _, _ := setupTestingHandler(t, clparams.CapellaVersion, log.Root(), true)

	baseBlock := blocks[len(blocks)-1].Block
	targetSlot := baseBlock.Slot + 1
	h.beaconChainCfg.DenebForkEpoch = targetSlot / h.beaconChainCfg.SlotsPerEpoch

	payload := cltypes.NewEth1BlockFromExecutionHeader(postState.LatestExecutionPayloadHeader(), clparams.DenebVersion, h.beaconChainCfg)
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]byte{1}, nil)
	engine.EXPECT().GetAssembledBlock(gomock.Any(), []byte{1}, clparams.DenebVersion).
		Return(payload, nil, nil, nil, nil)
	h.engine = engine

	baseBlockRoot, err := baseBlock.HashSSZ()
	require.NoError(t, err)

	body, _, err := h.produceBeaconBody(
		t.Context(), 3, baseBlock.Slot, baseBlockRoot, postState, targetSlot,
		common.Bytes96{0xc0}, common.Hash{},
	)

	require.Nil(t, body)
	require.ErrorIs(t, err, execution_client.ErrInvalidGetPayloadResponse)
	require.ErrorContains(t, err, "missing blobs bundle")
}

func TestSelectHigherGloasP2PBidValueUsesWei(t *testing.T) {
	t.Run("higher bid", func(t *testing.T) {
		localValueWei := gweiToWei(big.NewInt(2))
		externalBid := &cltypes.SignedExecutionPayloadBid{
			Message: &cltypes.ExecutionPayloadBid{Value: 3},
		}

		selectedValueWei, selected := selectHigherGloasP2PBidValue(localValueWei, externalBid)

		require.True(t, selected)
		require.Equal(t, "3000000000", selectedValueWei.String())
	})

	t.Run("equal bid", func(t *testing.T) {
		localValueWei := gweiToWei(big.NewInt(2))
		externalBid := &cltypes.SignedExecutionPayloadBid{
			Message: &cltypes.ExecutionPayloadBid{Value: 2},
		}

		selectedValueWei, selected := selectHigherGloasP2PBidValue(localValueWei, externalBid)

		require.False(t, selected)
		require.Same(t, localValueWei, selectedValueWei)
	})

	t.Run("maximum bid", func(t *testing.T) {
		externalBid := &cltypes.SignedExecutionPayloadBid{
			Message: &cltypes.ExecutionPayloadBid{Value: ^uint64(0)},
		}
		wantWei := gweiToWei(new(big.Int).SetUint64(^uint64(0)))

		selectedValueWei, selected := selectHigherGloasP2PBidValue(new(big.Int), externalBid)

		require.True(t, selected)
		require.Equal(t, wantWei, selectedValueWei)
	})
}

func TestPreferLocalExecutionValueRejectsNilBuilderValue(t *testing.T) {
	require.True(t, preferLocalExecutionValue(big.NewInt(1), nil, 100))
}

func TestShouldRequestBuilderHeader(t *testing.T) {
	require.True(t, shouldRequestBuilderHeader(clparams.FuluVersion))
	require.False(t, shouldRequestBuilderHeader(clparams.GloasVersion))
}

func TestGetBuilderPayloadRejectsInvalidBlockValue(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "empty"},
		{name: "not_a_number", value: "not-a-number"},
		{name: "leading_plus", value: "+1"},
		{name: "negative", value: "-1"},
		{name: "over_uint256", value: new(big.Int).Lsh(big.NewInt(1), 256).String()},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			_, _, _, _, postState, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
			builderClient := builder_mock.NewMockBuilderClient(ctrl)
			builderClient.EXPECT().GetHeader(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&builder.ExecutionHeader{
				Version: postState.Version().String(),
				Data: builder.ExecutionHeaderData{Message: builder.ExecutionHeaderMessage{
					Value: test.value,
				}},
			}, nil)
			handler.builderClient = builderClient

			_, _, err := handler.getBuilderPayload(t.Context(), postState, postState.Slot()+1)

			require.ErrorContains(t, err, "invalid builder block value")
		})
	}
}

func TestProcessProducedBlockFallsBackWithoutCandidateStateLeak(t *testing.T) {
	fixture := newGloasBidSelectionFixture(t, gloasBidSelectionOptions{exitBuilder: true})
	selfBid := fixture.block.BeaconBody.SignedExecutionPayloadBid
	expectedState, err := fixture.productionState.Copy()
	require.NoError(t, err)
	_, err = processBlockForProduction(expectedState, fixture.block)
	require.NoError(t, err)
	expectedRoot, err := expectedState.HashSSZ()
	require.NoError(t, err)
	logs := captureProductionLogs(t)
	handler := &ApiHandler{epbsPool: pool.NewEpbsPool()}
	handler.epbsPool.StoreHighestBid(fixture.bidKey, fixture.externalBid)

	selectedState, _, err := handler.processProducedBlock(fixture.productionState, fixture.block)

	require.NoError(t, err)
	require.Same(t, fixture.productionState, selectedState)
	require.Same(t, selfBid, fixture.block.BeaconBody.SignedExecutionPayloadBid)
	require.Equal(t, "1000000000", fixture.block.ExecutionValue.String())
	require.Len(t, fixture.block.Blobs, 1)
	require.Len(t, fixture.block.KzgProofs, 1)
	selectedRoot, err := selectedState.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expectedRoot, selectedRoot)
	_, found := handler.epbsPool.GetHighestBid(fixture.bidKey)
	require.False(t, found)
	require.Contains(t, logs(), "builderIndex=0")
	require.Contains(t, logs(), "bidValueGwei=3")
}

func TestProcessProducedBlockRetainsBidAfterUnclassifiedTransitionFailure(t *testing.T) {
	fixture := newGloasBidSelectionFixture(t, gloasBidSelectionOptions{})
	selfBid := fixture.block.BeaconBody.SignedExecutionPayloadBid
	handler := &ApiHandler{epbsPool: pool.NewEpbsPool()}
	handler.epbsPool.StoreHighestBid(fixture.bidKey, fixture.externalBid)
	transitionErr := errors.New("temporary transition failure")
	processBlock := func(
		productionState *state.CachingBeaconState,
		block *cltypes.BlindOrExecutionBeaconBlock,
	) (*eth2.Impl, error) {
		bid := block.BeaconBody.GetSignedExecutionPayloadBid()
		if bid.Message.BuilderIndex != clparams.BuilderIndexSelfBuild {
			return nil, transitionErr
		}
		return processBlockForProduction(productionState, block)
	}

	selectedState, _, err := handler.processProducedBlockWithProcessor(
		fixture.productionState,
		fixture.block,
		processBlock,
	)

	require.NoError(t, err)
	require.Same(t, fixture.productionState, selectedState)
	require.Same(t, selfBid, fixture.block.BeaconBody.SignedExecutionPayloadBid)
	storedBid, found := handler.epbsPool.GetHighestBid(fixture.bidKey)
	require.True(t, found)
	require.Same(t, fixture.externalBid, storedBid)
}

func TestProcessProducedBlockEvictsInvalidBidWhenSelfBuildFails(t *testing.T) {
	fixture := newGloasBidSelectionFixture(t, gloasBidSelectionOptions{})
	handler := &ApiHandler{epbsPool: pool.NewEpbsPool()}
	handler.epbsPool.StoreHighestBid(fixture.bidKey, fixture.externalBid)
	invalidBidErr := fmt.Errorf("%w: rejected candidate", eth2.ErrInvalidExecutionPayloadBid)
	selfBuildErr := errors.New("self-build failed")
	processBlock := func(
		_ *state.CachingBeaconState,
		block *cltypes.BlindOrExecutionBeaconBlock,
	) (*eth2.Impl, error) {
		bid := block.BeaconBody.GetSignedExecutionPayloadBid()
		if bid.Message.BuilderIndex == clparams.BuilderIndexSelfBuild {
			return nil, selfBuildErr
		}
		return nil, invalidBidErr
	}

	_, _, err := handler.processProducedBlockWithProcessor(
		fixture.productionState,
		fixture.block,
		processBlock,
	)

	require.ErrorIs(t, err, invalidBidErr)
	require.ErrorIs(t, err, selfBuildErr)
	require.ErrorContains(t, err, "bid evicted: true")
	_, found := handler.epbsPool.GetHighestBid(fixture.bidKey)
	require.False(t, found)
}

func TestProcessProducedBlockSelectsExternalBidWithoutLegacyBuilderBoost(t *testing.T) {
	fixture := newGloasBidSelectionFixture(t, gloasBidSelectionOptions{})
	originalRoot, err := fixture.productionState.HashSSZ()
	require.NoError(t, err)
	handler := &ApiHandler{epbsPool: pool.NewEpbsPool()}
	handler.epbsPool.StoreHighestBid(fixture.bidKey, fixture.externalBid)

	selectedState, blockMachine, err := handler.processProducedBlock(fixture.productionState, fixture.block)

	require.NoError(t, err)
	require.NotSame(t, fixture.productionState, selectedState)
	require.NotNil(t, blockMachine.BlockRewardsCollector)
	require.Same(t, fixture.externalBid, fixture.block.BeaconBody.SignedExecutionPayloadBid)
	require.Equal(t, "3000000000", fixture.block.ExecutionValue.String())
	require.Nil(t, fixture.block.Blobs)
	require.Nil(t, fixture.block.KzgProofs)
	afterRoot, err := fixture.productionState.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, originalRoot, afterRoot)
	require.Equal(t, fixture.externalBid.Message.BlockHash, selectedState.GetLatestExecutionPayloadBid().BlockHash)
}

func TestGetEthV3ValidatorBlockClearsStaleSelfBuildEnvelopeForExternalBid(t *testing.T) {
	fixture := newGloasBidSelectionFixture(t, gloasBidSelectionOptions{slotOffset: 1})
	_, _, _, _, _, handler, _, _, forkchoiceStore, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	handler.beaconChainCfg = fixture.block.Cfg
	forkEpoch := state.Epoch(fixture.productionState)
	handler.beaconChainCfg.AltairForkEpoch = forkEpoch
	handler.beaconChainCfg.BellatrixForkEpoch = forkEpoch
	handler.beaconChainCfg.CapellaForkEpoch = forkEpoch
	handler.beaconChainCfg.DenebForkEpoch = forkEpoch
	handler.beaconChainCfg.ElectraForkEpoch = forkEpoch
	handler.beaconChainCfg.FuluForkEpoch = forkEpoch
	handler.beaconChainCfg.GloasForkEpoch = forkEpoch
	handler.beaconChainCfg.InitializeForkSchedule()
	headState, err := fixture.productionState.Copy()
	require.NoError(t, err)
	require.NoError(t, headState.SetSlot(fixture.block.Slot-1))
	headStateRoot, err := headState.HashSSZ()
	require.NoError(t, err)
	headHeader := headState.LatestBlockHeader()
	headHeader.Root = common.Hash(headStateRoot)
	headState.SetLatestBlockHeader(&headHeader)
	headRootRaw, err := headHeader.HashSSZ()
	require.NoError(t, err)
	headRoot := common.Hash(headRootRaw)
	fixture.externalBid.Message.ParentBlockRoot = headRoot
	fixture.bidKey.ParentBlockRoot = headRoot
	domain, err := headState.GetDomain(handler.beaconChainCfg.DomainBeaconBuilder, state.Epoch(headState))
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(fixture.externalBid.Message, domain)
	require.NoError(t, err)
	copy(fixture.externalBid.Signature[:], fixture.builderKey.Sign(signingRoot[:]).Bytes())
	syncedData := synced_data.NewSyncedDataManager(handler.beaconChainCfg, true)
	require.NoError(t, syncedData.OnHeadStateWithBlockRoot(headState, headRoot))
	handler.syncedData = syncedData
	forkchoiceStore.HeadVal = headRoot
	forkchoiceStore.HeadSlotVal = headState.Slot()
	handler.epbsPool = pool.NewEpbsPool()
	syncPool := sync_pool_mock.NewMockSyncContributionPool(gomock.NewController(t))
	syncPool.EXPECT().GetSyncAggregate(gomock.Any(), gomock.Any()).
		Return(cltypes.NewSyncAggregateWithSize(int(handler.beaconChainCfg.SyncCommitteeSize/8)), nil).
		Times(2)
	handler.syncMessagePool = syncPool

	payload := cltypes.NewEth1Block(clparams.GloasVersion, handler.beaconChainCfg)
	payload.ParentHash = fixture.externalBid.Message.ParentBlockHash
	payload.BlockHash = common.Hash{0x66}
	payload.PrevRandao = fixture.externalBid.Message.PrevRandao
	payload.FeeRecipient = common.Address{0x77}
	payload.GasLimit = 30_000_000
	payload.Extra = solid.NewExtraData()
	payload.Transactions = solid.NewTransactionsSSZFromTransactions(nil)
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(handler.beaconChainCfg.MaxWithdrawalsPerPayload), 44)
	payload.SlotNumber = fixture.block.Slot

	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), clparams.GloasVersion).
		Return([]byte{1}, nil).
		Times(2)
	engine.EXPECT().GetAssembledBlock(gomock.Any(), []byte{1}, clparams.GloasVersion).
		Return(payload, &engine_types.BlobsBundle{}, nil, big.NewInt(1), nil).
		Times(2)
	handler.engine = engine

	produce := func() *cltypes.BeaconBlock {
		request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf(
			"/eth/v3/validator/blocks/%d?randao_reveal=%s&skip_randao_verification=true&graffiti=0x01",
			fixture.block.Slot,
			(common.Bytes96{}).String(),
		), http.NoBody)
		routeContext := chi.NewRouteContext()
		routeContext.URLParams.Add("slot", fmt.Sprint(fixture.block.Slot))
		request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeContext))
		response, err := handler.GetEthV3ValidatorBlock(httptest.NewRecorder(), request)
		require.NoError(t, err)
		producedBlock, ok := response.Data.(*cltypes.BeaconBlock)
		require.True(t, ok)
		return producedBlock
	}

	selfBuiltBlock := produce()
	require.Equal(t, uint64(clparams.BuilderIndexSelfBuild), selfBuiltBlock.Body.SignedExecutionPayloadBid.Message.BuilderIndex)
	_, found := handler.selfBuildEnvelopes.Get(fixture.block.Slot)
	require.True(t, found)

	handler.epbsPool.StoreHighestBid(fixture.bidKey, fixture.externalBid)
	externalBlock := produce()
	require.Equal(t, fixture.externalBid.Message.BuilderIndex, externalBlock.Body.SignedExecutionPayloadBid.Message.BuilderIndex)
	_, found = handler.selfBuildEnvelopes.Get(fixture.block.Slot)
	require.False(t, found)
}

func TestUpdateSelfBuildEnvelopeCacheRetainsMatchingEnvelope(t *testing.T) {
	cache, err := lru.New[uint64, *cltypes.ExecutionPayloadEnvelope]("testSelfBuildEnvelopes", 4)
	require.NoError(t, err)
	handler := &ApiHandler{selfBuildEnvelopes: cache}
	slot := uint64(1)
	blockRoot := common.Hash{0x01}
	existing := &cltypes.ExecutionPayloadEnvelope{
		BuilderIndex:    clparams.BuilderIndexSelfBuild,
		BeaconBlockRoot: blockRoot,
	}
	handler.selfBuildEnvelopes.Add(slot, existing)

	handler.updateSelfBuildEnvelopeCache(slot, &blockRoot, nil)

	got, found := handler.selfBuildEnvelopes.Get(slot)
	require.True(t, found)
	require.Same(t, existing, got)
}

func TestUpdateSelfBuildEnvelopeCacheRemovesMismatchedEnvelope(t *testing.T) {
	cache, err := lru.New[uint64, *cltypes.ExecutionPayloadEnvelope]("testSelfBuildEnvelopes", 4)
	require.NoError(t, err)
	handler := &ApiHandler{selfBuildEnvelopes: cache}
	slot := uint64(1)
	existing := &cltypes.ExecutionPayloadEnvelope{
		BuilderIndex:    clparams.BuilderIndexSelfBuild,
		BeaconBlockRoot: common.Hash{0x01},
	}
	handler.selfBuildEnvelopes.Add(slot, existing)
	producedBlockRoot := common.Hash{0x02}

	handler.updateSelfBuildEnvelopeCache(slot, &producedBlockRoot, nil)

	_, found := handler.selfBuildEnvelopes.Get(slot)
	require.False(t, found)
}

type coordinatedSelfBuildEnvelopeStore struct {
	mu               sync.Mutex
	envelope         *cltypes.ExecutionPayloadEnvelope
	removeStarted    chan struct{}
	replacementAdded chan struct{}
	releaseRemoval   chan struct{}
}

func (s *coordinatedSelfBuildEnvelopeStore) Add(_ uint64, envelope *cltypes.ExecutionPayloadEnvelope) bool {
	s.mu.Lock()
	s.envelope = envelope
	s.mu.Unlock()
	close(s.replacementAdded)
	return false
}

func (s *coordinatedSelfBuildEnvelopeStore) Get(_ uint64) (*cltypes.ExecutionPayloadEnvelope, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.envelope, s.envelope != nil
}

func (s *coordinatedSelfBuildEnvelopeStore) Remove(_ uint64) bool {
	close(s.removeStarted)
	select {
	case <-s.replacementAdded:
	case <-s.releaseRemoval:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	found := s.envelope != nil
	s.envelope = nil
	return found
}

func TestUpdateSelfBuildEnvelopeCacheSerializesReplacementWithRemoval(t *testing.T) {
	existing := &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: common.Hash{0x01}}
	replacement := &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: common.Hash{0x02}}
	store := &coordinatedSelfBuildEnvelopeStore{
		envelope:         existing,
		removeStarted:    make(chan struct{}),
		replacementAdded: make(chan struct{}),
		releaseRemoval:   make(chan struct{}),
	}
	handler := &ApiHandler{selfBuildEnvelopes: store}
	producedBlockRoot := common.Hash{0x03}
	removalDone := make(chan struct{})
	go func() {
		handler.updateSelfBuildEnvelopeCache(1, &producedBlockRoot, nil)
		close(removalDone)
	}()
	<-store.removeStarted

	replacementDone := make(chan struct{})
	go func() {
		handler.updateSelfBuildEnvelopeCache(1, nil, replacement)
		close(replacementDone)
	}()
	select {
	case <-replacementDone:
	case <-time.After(250 * time.Millisecond):
		close(store.releaseRemoval)
	}
	<-removalDone
	<-replacementDone

	got, found := store.Get(1)
	require.True(t, found)
	require.Same(t, replacement, got)
}

func TestProcessProducedBlockTreatsNilExecutionValueAsZero(t *testing.T) {
	fixture := newGloasBidSelectionFixture(t, gloasBidSelectionOptions{})
	fixture.block.ExecutionValue = nil
	handler := &ApiHandler{epbsPool: pool.NewEpbsPool()}
	handler.epbsPool.StoreHighestBid(fixture.bidKey, fixture.externalBid)

	_, _, err := handler.processProducedBlock(fixture.productionState, fixture.block)

	require.NoError(t, err)
	require.Same(t, fixture.externalBid, fixture.block.BeaconBody.SignedExecutionPayloadBid)
	require.Equal(t, "3000000000", fixture.block.GetExecutionValue().String())
}

func TestProcessProducedBlockRejectsBlindedGloasBlock(t *testing.T) {
	fixture := newGloasBidSelectionFixture(t, gloasBidSelectionOptions{})
	block := &cltypes.BlindOrExecutionBeaconBlock{
		BlindedBeaconBody: cltypes.NewBlindedBeaconBody(fixture.block.Cfg, clparams.GloasVersion),
		Cfg:               fixture.block.Cfg,
	}
	handler := &ApiHandler{epbsPool: pool.NewEpbsPool()}

	_, _, err := handler.processProducedBlock(fixture.productionState, block)

	require.ErrorContains(t, err, "cannot process blinded Gloas block")
}

func TestProcessProducedBlockRejectsNilBlock(t *testing.T) {
	fixture := newGloasBidSelectionFixture(t, gloasBidSelectionOptions{})
	handler := &ApiHandler{epbsPool: pool.NewEpbsPool()}

	_, _, err := handler.processProducedBlock(fixture.productionState, nil)

	require.ErrorContains(t, err, "cannot process nil block")
}

func TestProcessProducedBlockRejectsBlockWithoutBody(t *testing.T) {
	fixture := newGloasBidSelectionFixture(t, gloasBidSelectionOptions{})
	handler := &ApiHandler{epbsPool: pool.NewEpbsPool()}
	block := &cltypes.BlindOrExecutionBeaconBlock{Cfg: fixture.block.Cfg}
	processed := false
	processBlock := func(
		_ *state.CachingBeaconState,
		_ *cltypes.BlindOrExecutionBeaconBlock,
	) (*eth2.Impl, error) {
		processed = true
		return &eth2.Impl{}, nil
	}

	_, _, err := handler.processProducedBlockWithProcessor(
		fixture.productionState,
		block,
		processBlock,
	)

	require.ErrorContains(t, err, "cannot process block without body")
	require.False(t, processed)
}

func TestProcessProducedBlockRejectsInvalidExternalBidGuards(t *testing.T) {
	tests := []struct {
		name    string
		options gloasBidSelectionOptions
	}{
		{
			name: "randao mismatch",
			options: gloasBidSelectionOptions{mutateBid: func(bid *cltypes.ExecutionPayloadBid) {
				bid.PrevRandao[0] ^= 0xff
			}},
		},
		{
			name: "builder version mismatch",
			options: gloasBidSelectionOptions{mutateBuilder: func(builder *cltypes.Builder) {
				builder.Version++
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newGloasBidSelectionFixture(t, test.options)
			selfBid := fixture.block.BeaconBody.SignedExecutionPayloadBid
			handler := &ApiHandler{epbsPool: pool.NewEpbsPool()}
			handler.epbsPool.StoreHighestBid(fixture.bidKey, fixture.externalBid)

			_, _, err := handler.processProducedBlock(fixture.productionState, fixture.block)

			require.NoError(t, err)
			require.Same(t, selfBid, fixture.block.BeaconBody.SignedExecutionPayloadBid)
			_, found := handler.epbsPool.GetHighestBid(fixture.bidKey)
			require.False(t, found)
		})
	}
}

func TestConsensusBlockValueUsesWeiWithoutOverflow(t *testing.T) {
	rewards := &eth2.BlockRewardsCollector{
		Attestations:      ^uint64(0),
		AttesterSlashings: 2,
		ProposerSlashings: 3,
		SyncAggregate:     4,
	}
	wantGwei := new(big.Int).Add(new(big.Int).SetUint64(^uint64(0)), big.NewInt(9))
	wantWei := gweiToWei(wantGwei)

	require.Equal(t, wantWei, consensusBlockValueWei(rewards))
}

type gloasBidSelectionOptions struct {
	exitBuilder   bool
	slotOffset    uint64
	mutateBuilder func(*cltypes.Builder)
	mutateBid     func(*cltypes.ExecutionPayloadBid)
}

type gloasBidSelectionFixture struct {
	productionState *state.CachingBeaconState
	block           *cltypes.BlindOrExecutionBeaconBlock
	externalBid     *cltypes.SignedExecutionPayloadBid
	bidKey          pool.HighestBidKey
	builderKey      *bls.PrivateKey
}

func newGloasBidSelectionFixture(t *testing.T, options gloasBidSelectionOptions) gloasBidSelectionFixture {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	cfg.PayloadBuilderVersion = 7
	productionState := state.New(&cfg)
	productionState.SetVersion(clparams.GloasVersion)
	slot := cfg.SlotsPerEpoch + options.slotOffset
	require.NoError(t, productionState.SetSlot(slot))
	productionState.SetFinalizedCheckpoint(solid.Checkpoint{Epoch: 1})
	productionState.SetGenesisValidatorsRoot(common.Hash{0x91})
	productionState.SetFork(&cltypes.Fork{
		PreviousVersion: utils.Uint32ToBytes4(uint32(cfg.FuluForkVersion)),
		CurrentVersion:  utils.Uint32ToBytes4(uint32(cfg.GloasForkVersion)),
		Epoch:           state.Epoch(productionState),
	})
	require.NoError(t, productionState.SetRandaoMixAt(
		int(state.Epoch(productionState)%cfg.EpochsPerHistoricalVector),
		common.Hash{0xa1},
	))

	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	pubkey := common.Bytes48(bls.CompressPublicKey(privateKey.PublicKey()))
	require.NoError(t, productionState.AddValidator(solid.NewValidatorFromParameters(
		pubkey,
		common.Hash{},
		cfg.MaxEffectiveBalance,
		false,
		0,
		0,
		cfg.FarFutureEpoch,
		cfg.FarFutureEpoch,
	), cfg.MaxEffectiveBalance))
	committee := make([]common.Bytes48, int(cfg.SyncCommitteeSize))
	for i := range committee {
		committee[i] = pubkey
	}
	require.NoError(t, productionState.SetCurrentSyncCommittee(
		solid.NewSyncCommitteeFromParameters(committee, pubkey),
	))

	executionAddress := common.Address{0x42}
	builders := solid.NewStaticListSSZ[*cltypes.Builder](int(cfg.BuilderRegistryLimit), new(cltypes.Builder).EncodingSizeSSZ())
	payloadBuilder := &cltypes.Builder{
		Pubkey:            pubkey,
		Version:           cfg.PayloadBuilderVersion,
		ExecutionAddress:  executionAddress,
		Balance:           cfg.MinDepositAmount + 100,
		DepositEpoch:      0,
		WithdrawableEpoch: cfg.FarFutureEpoch,
	}
	if options.mutateBuilder != nil {
		options.mutateBuilder(payloadBuilder)
	}
	builders.Append(payloadBuilder)
	productionState.SetBuilders(builders)

	parentHeader := productionState.LatestBlockHeader()
	parentRootRaw, err := (&parentHeader).HashSSZ()
	require.NoError(t, err)
	parentRoot := common.Hash(parentRootRaw)
	parentHash := common.Hash{0x22}
	require.NoError(t, productionState.SetBlockRootAt(int((slot-1)%cfg.SlotsPerHistoricalRoot), parentRoot))
	parentRequests := cltypes.NewExecutionRequestsWithVersion(&cfg, clparams.GloasVersion)
	if options.exitBuilder {
		parentRequests.BuilderExits.Append(&solid.BuilderExitRequest{
			SourceAddress: executionAddress,
			PubKey:        pubkey,
		})
	}
	parentRequestsRoot, err := parentRequests.HashSSZ()
	require.NoError(t, err)
	productionState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
		BlockHash:             parentHash,
		Slot:                  slot - 1,
		ExecutionRequestsRoot: parentRequestsRoot,
	})
	productionState.SetLatestBlockHash(parentHash)

	commitments := solid.NewStaticProgressiveListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48)
	commitments.Append(&cltypes.KZGCommitment{0x33})
	externalBid := &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
		ParentBlockHash:    parentHash,
		ParentBlockRoot:    parentRoot,
		BlockHash:          common.Hash{0x44},
		PrevRandao:         productionState.GetRandaoMixes(state.Epoch(productionState)),
		FeeRecipient:       common.Address{0x55},
		BuilderIndex:       0,
		Slot:               slot,
		Value:              3,
		BlobKzgCommitments: *commitments,
	}}
	if options.mutateBid != nil {
		options.mutateBid(externalBid.Message)
	}
	domain, err := productionState.GetDomain(cfg.DomainBeaconBuilder, state.Epoch(productionState))
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(externalBid.Message, domain)
	require.NoError(t, err)
	copy(externalBid.Signature[:], privateKey.Sign(signingRoot[:]).Bytes())

	selfCommitments := solid.NewStaticProgressiveListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48)
	body := cltypes.NewBeaconBody(&cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{
			ParentBlockHash:    parentHash,
			ParentBlockRoot:    parentRoot,
			BlockHash:          common.Hash{0x66},
			PrevRandao:         productionState.GetRandaoMixes(state.Epoch(productionState)),
			BuilderIndex:       clparams.BuilderIndexSelfBuild,
			Slot:               slot,
			BlobKzgCommitments: *selfCommitments,
		},
		Signature: common.Bytes96(bls.InfiniteSignature),
	}
	body.ParentExecutionRequests = parentRequests

	block := &cltypes.BlindOrExecutionBeaconBlock{
		Slot:           slot,
		ProposerIndex:  0,
		ParentRoot:     parentRoot,
		BeaconBody:     body,
		Blobs:          []*cltypes.Blob{{0x77}},
		KzgProofs:      []common.Bytes48{{0x88}},
		ExecutionValue: gweiToWei(big.NewInt(1)),
		Cfg:            &cfg,
	}
	return gloasBidSelectionFixture{
		productionState: productionState,
		block:           block,
		externalBid:     externalBid,
		builderKey:      privateKey,
		bidKey: pool.HighestBidKey{
			Slot:            slot,
			ParentBlockHash: parentHash,
			ParentBlockRoot: parentRoot,
		},
	}
}

func TestSetupHeaderResponsePreservesLargeExecutionValue(t *testing.T) {
	h := &ApiHandler{}
	rr := httptest.NewRecorder()
	valueWei := gweiToWei(new(big.Int).SetUint64(^uint64(0)))

	h.setupHeaderReponseForBlockProduction(rr, clparams.GloasVersion, false, true, valueWei, new(big.Int))

	require.Equal(t, valueWei.String(), rr.Header().Get("Eth-Execution-Payload-Value"))
}

func TestProduceBlockPreservesLargeExecutionValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, h, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	h.routerCfg.Builder = false
	payload := cltypes.NewEth1Block(clparams.ElectraVersion, h.beaconChainCfg)
	payload.Transactions = &solid.TransactionsSSZ{}
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(h.beaconChainCfg.MaxWithdrawalsPerPayload), 44)
	valueWei := new(big.Int).Add(new(big.Int).SetUint64(^uint64(0)), big.NewInt(1))
	wantValueWei := valueWei.String()

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]byte{1, 2, 3, 4, 5, 6, 7, 8}, nil).AnyTimes()
	engine.EXPECT().GetAssembledBlock(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(payload, &engine_types.BlobsBundle{}, nil, valueWei, nil).AnyTimes()
	engine.EXPECT().SupportInsertion().Return(true).AnyTimes()
	h.engine = engine

	block, err := h.produceBlock(t.Context(), 1, postState.Slot(), common.Hash{0x41}, postState,
		postState.Slot()+1, common.Bytes96{}, common.Hash{})

	require.NoError(t, err)
	require.Equal(t, wantValueWei, block.ExecutionValue.String())
}

func TestProduceBlockUsesLocalPayloadWithoutBuilderClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, h, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	require.True(t, h.routerCfg.Builder)
	require.Nil(t, h.builderClient)
	payload := cltypes.NewEth1Block(clparams.ElectraVersion, h.beaconChainCfg)
	payload.Transactions = &solid.TransactionsSSZ{}
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(h.beaconChainCfg.MaxWithdrawalsPerPayload), 44)
	valueWei := big.NewInt(1)

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]byte{1, 2, 3, 4, 5, 6, 7, 8}, nil).AnyTimes()
	engine.EXPECT().GetAssembledBlock(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(payload, &engine_types.BlobsBundle{}, nil, valueWei, nil).AnyTimes()
	engine.EXPECT().SupportInsertion().Return(true).AnyTimes()
	h.engine = engine

	block, err := h.produceBlock(t.Context(), 1, postState.Slot(), common.Hash{0x41}, postState,
		postState.Slot()+1, common.Bytes96{}, common.Hash{})

	require.NoError(t, err)
	require.False(t, block.IsBlinded())
	require.Equal(t, valueWei, block.ExecutionValue)
}

func TestBroadcastExternalGloasBidDoesNotRequireLocalBlobBundles(t *testing.T) {
	logs := captureAllProductionLogs(t)
	_, _, _, _, _, h, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	h.indiciesDB = updateFailingDB{RwDB: h.indiciesDB}
	block := cltypes.NewSignedBeaconBlock(h.beaconChainCfg, clparams.GloasVersion)
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	require.NotNil(t, bid)
	require.NotNil(t, bid.Message)
	bid.Message.BuilderIndex = 1
	bid.Message.BlobKzgCommitments.Append(&cltypes.KZGCommitment{0x01})

	require.NoError(t, h.broadcastBlock(t.Context(), block))

	// The persistence error is logged at the end of the background store goroutine.
	require.Eventually(t, func() bool {
		return strings.Contains(logs(), "stop after persistence")
	}, 5*time.Second, 10*time.Millisecond)
	require.Contains(t, logs(), "blobSidecars=0")
	require.Contains(t, logs(), "columnSidecars=0")
}

// TestCaplinBlockProductionWithWithdrawalRequest tests Caplin's produceBeaconBody
// against a real Erigon execution layer. A withdrawal request transaction is
// submitted to the EIP-7002 system contract, and then Caplin's actual block
// production code builds the beacon body — calling ForkChoiceUpdate,
// GetAssembledBlock, and decoding the execution requests. This is the code path
// that was broken in issue #14319 and fixed in PR #14326.
func TestCaplinBlockProductionWithWithdrawalRequest(t *testing.T) {
	ctx := context.Background()

	// --- Set up real execution layer ---

	m := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))

	// Insert 1 initial block so we have a chain head.
	chainPack, err := m.GenerateChain(1, func(i int, gen *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(gen.TxNonce(m.Address), common.Address{1}, uint256.NewInt(10_000), params.TxGas, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key,
		)
		require.NoError(t, err)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)

	// Submit a withdrawal request transaction (EIP-7002).
	var pubkey [48]byte
	for i := range pubkey {
		pubkey[i] = 0x01
	}
	var calldata []byte
	calldata = append(calldata, pubkey[:]...)
	calldata = append(calldata, make([]byte, 8)...) // amount=0 → full exit

	baseFee := chainPack.TopBlock.BaseFee().Uint64()
	withdrawalAddr := params.WithdrawalRequestAddress.Value()
	withdrawalTx, err := types.SignTx(
		&types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce:    1,
				GasLimit: 1_000_000,
				To:       &withdrawalAddr,
				Value:    *uint256.NewInt(500_000_000_000_000_000), // 0.5 ETH
				Data:     calldata,
			},
			GasPrice: *uint256.NewInt(baseFee),
		},
		*types.LatestSignerForChainID(m.ChainConfig.ChainID),
		m.Key,
	)
	require.NoError(t, err)

	var txBuf bytes.Buffer
	err = withdrawalTx.EncodeRLP(&txBuf)
	require.NoError(t, err)
	addResp, err := m.TxPoolGrpcServer.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{txBuf.Bytes()}})
	require.NoError(t, err)
	require.Equal(t, "success", addResp.Errors[0])

	// --- Wire real EL into Caplin's ApiHandler ---

	chainRW := chainreader.NewChainReaderEth1(
		m.ChainConfig,
		m.ExecModule,
		time.Hour,
	)
	engine, err := execution_client.NewExecutionClientDirect(chainRW, nil)
	require.NoError(t, err)

	// Set up handler with Electra test data (provides validator set, RANDAO, etc.)
	// and our real execution engine.
	_, blocks, _, _, postState, h, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	h.engine = engine

	// Patch the beacon state's execution payload header to point at the real
	// EL chain head — this is how produceBeaconBody knows what hash to send
	// in ForkChoiceUpdate.
	elHead := chainPack.TopBlock.Header()
	elHeader := cltypes.NewEth1Header(clparams.ElectraVersion)
	elHeader.BlockHash = elHead.Hash()
	elHeader.BlockNumber = elHead.Number.Uint64()
	elHeader.Time = elHead.Time
	elHeader.BaseFeePerGas = common.BigToHash(elHead.BaseFee.ToBig())
	postState.SetLatestExecutionPayloadHeader(elHeader)

	// Make GetEth1Hash return the EL head hash for any checkpoint root —
	// produceBeaconBody falls back to head when the hash is zero, but we
	// set it explicitly for clarity.
	elHeadHash := elHead.Hash()
	fcu.Eth1Hashes[postState.FinalizedCheckpoint().Root] = elHeadHash
	fcu.Eth1Hashes[postState.CurrentJustifiedCheckpoint().Root] = elHeadHash

	// --- Call Caplin's actual block production ---

	baseBlock := blocks[len(blocks)-1].Block
	targetSlot := baseBlock.Slot + 1
	baseBlockRoot, err := baseBlock.HashSSZ()
	require.NoError(t, err)

	beaconBody, execValue, err := h.produceBeaconBody(
		ctx, 3, baseBlock.Slot, baseBlockRoot, postState, targetSlot,
		common.Bytes96{0xc0}, // infinity BLS signature (skip RANDAO verification)
		common.Hash{},
	)
	require.NoError(t, err)
	require.NotNil(t, beaconBody)
	require.Positive(t, execValue.Sign())

	// --- Verify execution requests were decoded by Caplin ---

	require.NotNil(t, beaconBody.ExecutionRequests,
		"ExecutionRequests must not be nil — this was the bug in issue #14319")
	require.Greater(t, beaconBody.ExecutionRequests.Withdrawals.Len(), 0,
		"expected at least 1 withdrawal request from the EL system contract")

	gotWithdrawal := beaconBody.ExecutionRequests.Withdrawals.Get(0)
	require.Equal(t, common.Bytes48(pubkey), gotWithdrawal.ValidatorPubKey,
		"withdrawal request pubkey should match what was submitted")
	require.Equal(t, uint64(0), gotWithdrawal.Amount,
		"withdrawal request amount should be 0 (full exit)")
}

// fcuSpy wraps an ExecutionEngine and captures the PayloadAttributes from
// the most recent ForkChoiceUpdate call.
type fcuSpy struct {
	execution_client.ExecutionEngine
	lastAttributes *engine_types.PayloadAttributes
}

func (s *fcuSpy) ForkChoiceUpdate(ctx context.Context, finalized, safe, head common.Hash, attributes *engine_types.PayloadAttributes, version clparams.StateVersion) ([]byte, error) {
	s.lastAttributes = attributes
	return s.ExecutionEngine.ForkChoiceUpdate(ctx, finalized, safe, head, attributes, version)
}

// TestCaplinBlockProductionGlamsterdamSlotNumber verifies that Caplin passes
// the slot number to the execution engine in PayloadAttributes when the
// Glamsterdam (Gloas) fork is active, per EIP-7843.
func TestCaplinBlockProductionGlamsterdamSlotNumber(t *testing.T) {
	ctx := context.Background()

	// --- Set up real execution layer with Amsterdam activated ---

	m := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))

	// Insert 1 initial block so we have a chain head.
	chainPack, err := m.GenerateChain(1, func(i int, gen *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(gen.TxNonce(m.Address), common.Address{1}, uint256.NewInt(10_000), params.TxGas, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key,
		)
		require.NoError(t, err)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)

	// --- Wire real EL into Caplin's ApiHandler ---

	chainRW := chainreader.NewChainReaderEth1(
		m.ChainConfig,
		m.ExecModule,
		time.Hour,
	)
	engine, err := execution_client.NewExecutionClientDirect(chainRW, nil)
	require.NoError(t, err)

	// Wrap the real engine with a spy to capture PayloadAttributes.
	spy := &fcuSpy{ExecutionEngine: engine}

	// Set up handler with Electra test data (provides validator set, RANDAO,
	// etc.) and plug in our spy engine.
	_, blocks, _, _, postState, h, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	h.engine = spy

	// Activate Fulu and Gloas at epoch 1 (same as the other forks in the
	// Electra fixture setup) so GetCurrentStateVersion returns GloasVersion.
	h.beaconChainCfg.FuluForkEpoch = 1
	h.beaconChainCfg.GloasForkEpoch = 1
	h.beaconChainCfg.InitializeForkSchedule()

	// Patch the beacon state's execution payload header to point at the real
	// EL chain head.
	elHead := chainPack.TopBlock.Header()
	elHeader := cltypes.NewEth1Header(clparams.ElectraVersion)
	elHeader.BlockHash = elHead.Hash()
	elHeader.BlockNumber = elHead.Number.Uint64()
	elHeader.Time = elHead.Time
	elHeader.BaseFeePerGas = common.BigToHash(elHead.BaseFee.ToBig())
	postState.SetLatestExecutionPayloadHeader(elHeader)
	// GLOAS uses GetLatestBlockHash() instead of LatestExecutionPayloadHeader().BlockHash
	postState.SetLatestBlockHash(elHead.Hash())
	// GLOAS deferred payload: set LatestExecutionPayloadBid so that GetHeadPayloadStatus()==FULL &&
	// ShouldBuildOnFull (both returning true in the mock) select bid.BlockHash as the EL head.
	postState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
		BlockHash:       elHead.Hash(),
		ParentBlockHash: elHead.Hash(),
	})

	elHeadHash := elHead.Hash()
	fcu.Eth1Hashes[postState.FinalizedCheckpoint().Root] = elHeadHash
	fcu.Eth1Hashes[postState.CurrentJustifiedCheckpoint().Root] = elHeadHash

	// --- Call Caplin's actual block production ---

	baseBlock := blocks[len(blocks)-1].Block
	targetSlot := baseBlock.Slot + 1
	baseBlockRoot, err := baseBlock.HashSSZ()
	require.NoError(t, err)

	// GLOAS deferred payload: the mock returns GetHeadPayloadStatus=FULL and ShouldBuildOnFull=true,
	// so block production expects an envelope on disk. Provide one with empty ExecutionRequests.
	fcu.SetEnvelope(baseBlockRoot, &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			ExecutionRequests: cltypes.NewExecutionRequestsWithVersion(h.beaconChainCfg, clparams.GloasVersion),
		},
	})

	beaconBody, _, err := h.produceBeaconBody(
		ctx, 3, baseBlock.Slot, baseBlockRoot, postState, targetSlot,
		common.Bytes96{0xc0}, // infinity BLS signature (skip RANDAO verification)
		common.Hash{},
	)
	require.NoError(t, err)
	require.NotNil(t, beaconBody)

	// --- Verify the slot number was passed to the EL (EIP-7843) ---

	require.NotNil(t, spy.lastAttributes,
		"ForkChoiceUpdate should have been called with PayloadAttributes")
	require.NotNil(t, spy.lastAttributes.SlotNumber,
		"PayloadAttributes.SlotNumber must be set for Glamsterdam (EIP-7843)")
	require.Equal(t, hexutil.Uint64(targetSlot), *spy.lastAttributes.SlotNumber,
		"SlotNumber should equal the target slot")
}

func TestExpectedWithdrawalsReadsTheRightSourcePerFork(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch = 0, 0, 0
	a := &ApiHandler{beaconChainCfg: &cfg}

	capellaState := state.New(&cfg)
	capellaState.SetVersion(clparams.CapellaVersion)

	// Before Gloas the expectation is computed from the head state itself, and the list is present
	// even when empty: the execution layer rejects a nil one after Shanghai.
	withdrawals, err := a.expectedWithdrawals(capellaState, nil, clparams.CapellaVersion, 0)
	require.NoError(t, err)
	require.NotNil(t, withdrawals)
	require.Empty(t, withdrawals)

	gloasState := state.New(&cfg)
	gloasState.SetVersion(clparams.GloasVersion)

	// A Gloas head whose payload was revealed is read from the state copy carrying that payload,
	// not from the head state. Only that copy carries a pending builder withdrawal, so reading the
	// wrong one comes back empty rather than merely equal.
	withParentPayload := state.New(&cfg)
	withParentPayload.SetVersion(clparams.GloasVersion)
	pending := solid.NewDynamicListSSZ[*cltypes.BuilderPendingWithdrawal](int(cfg.MaxWithdrawalsPerPayload))
	pending.Append(&cltypes.BuilderPendingWithdrawal{FeeRecipient: common.Address{0xbb}, Amount: 12, BuilderIndex: 3})
	withParentPayload.SetBuilderPendingWithdrawals(pending)

	withdrawals, err = a.expectedWithdrawals(gloasState, withParentPayload, clparams.GloasVersion, 0)
	require.NoError(t, err)
	require.Equal(t, []*types.Withdrawal{{
		Index:     0,
		Validator: state.ConvertBuilderIndexToValidatorIndex(3),
		Address:   common.Address{0xbb},
		Amount:    12,
	}}, withdrawals)

	// An EMPTY Gloas head uses the expectation the state already cached rather than computing a
	// fresh one, so what it returns is whatever was cached.
	withdrawals, err = a.expectedWithdrawals(gloasState, nil, clparams.GloasVersion, 0)
	require.NoError(t, err)
	require.Empty(t, withdrawals)

	cached := solid.NewDynamicListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload))
	cached.Append(&cltypes.Withdrawal{Index: 7, Validator: 8, Address: common.Address{0xaa}, Amount: 9})
	gloasState.SetPayloadExpectedWithdrawals(cached)
	withdrawals, err = a.expectedWithdrawals(gloasState, nil, clparams.GloasVersion, 0)
	require.NoError(t, err)
	require.Equal(t, []*types.Withdrawal{
		{Index: 7, Validator: 8, Address: common.Address{0xaa}, Amount: 9},
	}, withdrawals)
}

func TestPayloadAttributesOmitFieldsTheChosenVersionCannotCarry(t *testing.T) {
	root := common.Hash{0xaa}
	withdrawals := []*types.Withdrawal{{Index: 1}}
	slotNumber := hexutil.Uint64(64)
	targetGasLimit := hexutil.Uint64(36_000_000)

	for _, tc := range []struct {
		version         clparams.StateVersion
		wantWithdrawals bool
		wantParentRoot  bool
		wantGloasFields bool
	}{
		{clparams.BellatrixVersion, false, false, false},
		{clparams.CapellaVersion, true, false, false},
		{clparams.DenebVersion, true, true, false},
		{clparams.FuluVersion, true, true, false},
		{clparams.GloasVersion, true, true, true},
	} {
		t.Run(tc.version.String(), func(t *testing.T) {
			attrs := payloadAttributes(tc.version, 1, common.Hash{0xbb}, common.Address{0xcc},
				withdrawals, &root, &slotNumber, &targetGasLimit)

			// A version that does not define a field must not have it populated: V1 carries no
			// withdrawals, V1 and V2 no parent beacon block root, and supplying one is rejected
			// rather than ignored.
			require.Equal(t, tc.wantWithdrawals, attrs.Withdrawals != nil)
			require.Equal(t, tc.wantParentRoot, attrs.ParentBeaconBlockRoot != nil)

			// The values have to arrive, not merely be non-nil: dropping either Gloas field leaves
			// every Gloas proposal rejected with -38003.
			if tc.wantGloasFields {
				require.Equal(t, &slotNumber, attrs.SlotNumber)
				require.Equal(t, &targetGasLimit, attrs.TargetGasLimit)
			} else {
				require.Nil(t, attrs.SlotNumber)
				require.Nil(t, attrs.TargetGasLimit)
			}
			require.Equal(t, hexutil.Uint64(1), attrs.Timestamp)
			require.Equal(t, common.Hash{0xbb}, attrs.PrevRandao)
			require.Equal(t, common.Address{0xcc}, attrs.SuggestedFeeRecipient)
		})
	}
}

// syncedBuffer is a writer the log package can hand to several goroutines at once, which
// StreamHandler requires and a bare bytes.Buffer does not provide.
type syncedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncedBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncedBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func captureAllProductionLogs(t *testing.T) func() string {
	t.Helper()
	output := &syncedBuffer{}
	previous := log.Root().GetHandler()
	log.Root().SetHandler(log.StreamHandler(output, log.LogfmtFormat()))
	t.Cleanup(func() { log.Root().SetHandler(previous) })
	return output.String
}

// captureProductionLogs redirects the root logger for one test and returns everything written at
// warning level or above. It deliberately does not filter by message: a record this package emits
// under another name is exactly what a test asserting silence needs to see.
func captureProductionLogs(t *testing.T) func() string {
	t.Helper()
	allLogs := captureAllProductionLogs(t)
	return func() string {
		var loud []string
		for line := range strings.SplitSeq(allLogs(), "\n") {
			if strings.Contains(line, "lvl=eror") || strings.Contains(line, "lvl=warn") {
				loud = append(loud, line)
			}
		}
		return strings.Join(loud, "\n")
	}
}

func TestPollAssembledPayloadStaysQuietWhenAFailedPollRecovers(t *testing.T) {
	ctx := t.Context()
	logs := captureProductionLogs(t)
	window := blockBuilderWindow{firstGetAt: time.Now(), pollUntil: time.Now().Add(time.Second)}

	calls := 0
	payload, _, _, _, err := pollAssembledPayload(ctx, window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			if calls == 1 {
				return nil, nil, nil, nil, errors.New("execution module is busy")
			}
			return &cltypes.Eth1Block{}, &engine_types.BlobsBundle{}, nil, big.NewInt(1), nil
		})

	require.NoError(t, err)
	require.NotNil(t, payload)
	// Contention that clears is a healthy slot, so nothing may be reported at error level.
	require.NotContains(t, logs(), "lvl=eror")
}

func TestPollAssembledPayloadReportsAWindowThatNeverProducedOnce(t *testing.T) {
	ctx := t.Context()
	logs := captureProductionLogs(t)
	window := blockBuilderWindow{firstGetAt: time.Now(), pollUntil: time.Now().Add(50 * time.Millisecond)}

	boom := errors.New("boom")
	calls := 0
	_, _, _, _, err := pollAssembledPayload(ctx, window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			return nil, nil, nil, nil, boom
		})

	require.NotZero(t, calls)

	// The reason goes to the caller, which knows the slot and owns the record, carrying the first
	// failure - the one that says what went wrong - and how many there were.
	require.ErrorIs(t, err, boom)
	require.Contains(t, err.Error(), "attempt")
	require.Empty(t, logs(), "the poll does not report; its caller does")
}

func TestPollAssembledPayloadStaysQuietWhenTheCallerGoesAway(t *testing.T) {
	logs := captureProductionLogs(t)
	ctx, cancel := context.WithCancel(t.Context())
	window := blockBuilderWindow{firstGetAt: time.Now(), pollUntil: time.Now().Add(time.Minute)}

	_, _, _, _, err := pollAssembledPayload(ctx, window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			cancel()
			return nil, nil, nil, nil, context.Canceled
		})

	// A validator client that times out, or a node shutting down, takes the slot with it. Nothing
	// failed that anyone can act on.
	require.Error(t, err)
	require.NotContains(t, logs(), "lvl=eror")
}

func TestPollAssembledPayloadStillReportsFailuresThatPrecededTheCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	window := blockBuilderWindow{firstGetAt: time.Now(), pollUntil: time.Now().Add(time.Minute)}

	calls := 0
	_, _, _, _, err := pollAssembledPayload(ctx, window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			if calls == 1 {
				return nil, nil, nil, nil, errors.New("boom")
			}
			cancel()
			return nil, nil, nil, nil, context.Canceled
		})

	// The client may well have given up because production was failing. Reporting only the
	// cancellation would lose the only sign of it.
	require.NotErrorIs(t, err, context.Canceled)
	require.Contains(t, err.Error(), "boom")
}

func TestFeeRecipientWarnsOncePerProposer(t *testing.T) {
	logs := captureProductionLogs(t)
	warned, err := lru.New[uint64, struct{}]("unregisteredProposers", 8)
	require.NoError(t, err)
	params := validator_params.NewValidatorParams()
	a := &ApiHandler{validatorParams: params, unregisteredProposers: warned}

	registered := common.Address{0x11}
	params.SetFeeRecipient(7, registered)
	require.Equal(t, registered, a.feeRecipientForProposal(7, 1))
	require.NotContains(t, logs(), "lvl=warn", "a registered proposer must stay quiet")

	// Giving the fees away is worth saying, but only once: a chain whose validator never registers
	// one would otherwise warn on every proposal.
	require.Equal(t, common.Address{}, a.feeRecipientForProposal(9, 2))
	require.Equal(t, common.Address{}, a.feeRecipientForProposal(9, 3))
	require.Equal(t, 1, strings.Count(logs(), "lvl=warn"))

	require.Equal(t, common.Address{}, a.feeRecipientForProposal(10, 4))
	require.Equal(t, 2, strings.Count(logs(), "lvl=warn"), "a different proposer is worth saying again")

	// Alternating proposers must not each reset the other: 9 has already been reported.
	require.Equal(t, common.Address{}, a.feeRecipientForProposal(9, 5))
	require.Equal(t, 2, strings.Count(logs(), "lvl=warn"))
}

func TestFeeRecipientWarnsOncePerProposerUnderConcurrentRequests(t *testing.T) {
	logs := captureProductionLogs(t)
	warned, err := lru.New[uint64, struct{}]("unregisteredProposers", 8)
	require.NoError(t, err)
	a := &ApiHandler{validatorParams: validator_params.NewValidatorParams(), unregisteredProposers: warned}

	// Several block template requests for the same slot arrive together, and each would otherwise
	// find the proposer absent and report it.
	var wg sync.WaitGroup
	for range 128 {
		wg.Go(func() { a.feeRecipientForProposal(9, 2) })
	}
	wg.Wait()

	require.Equal(t, 1, strings.Count(logs(), "lvl=warn"))
}

func TestPollAssembledPayloadDoesNotCollectAfterTheCallerHasGone(t *testing.T) {
	logs := captureProductionLogs(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	past := time.Now().Add(-time.Second)
	window := blockBuilderWindow{firstGetAt: past, pollUntil: past}

	calls := 0
	_, _, _, _, err := pollAssembledPayload(ctx, window, time.Microsecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			// The execution module takes its semaphore before it looks at a context, so a request
			// made after the caller has gone comes back as contention rather than cancellation.
			return nil, nil, nil, nil, errors.New("execution module is busy")
		})

	require.Error(t, err)
	require.Zero(t, calls, "collection must not be started for a caller that has gone")
	require.NotContains(t, logs(), "lvl=eror")
}

// produceBlockWithFailingCollection drives a real production through to the payload collection and
// makes that collection fail the given way, so the records the whole request emits are observable
// rather than only those of the polling loop.
func produceBlockWithFailingCollection(t *testing.T, ctx context.Context, collect error) error {
	t.Helper()
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]byte{1, 2, 3, 4, 5, 6, 7, 8}, nil).AnyTimes()
	engine.EXPECT().GetAssembledBlock(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil, nil, nil, collect).AnyTimes()
	engine.EXPECT().SupportInsertion().Return(true).AnyTimes()
	handler.engine = engine

	_, err := handler.produceBlock(ctx, 1, postState.Slot(), common.Hash{0x41}, postState,
		postState.Slot()+1, common.Bytes96{}, common.Hash{})
	return err
}

func TestProductionReportsAFailedCollectionExactlyOnce(t *testing.T) {
	ctx := t.Context()
	logs := captureProductionLogs(t)

	err := produceBlockWithFailingCollection(t, ctx, errors.New("boom"))
	require.Error(t, err)

	// One record for the whole request, and it carries the cause: the generic failure the caller
	// used to see said only that production failed.
	captured := logs()
	require.Equal(t, 1, strings.Count(captured, "lvl=eror"), "records:\n"+captured)
	require.Contains(t, captured, "boom")
}

func TestProductionReportsMissingPayloadIDExactlyOnce(t *testing.T) {
	ctx := t.Context()
	logs := captureProductionLogs(t)
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x42})

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil).AnyTimes()
	engine.EXPECT().SupportInsertion().Return(true).AnyTimes()
	handler.engine = engine

	_, err = handler.produceBlock(ctx, 1, postState.Slot(), common.Hash{0x41}, postState,
		targetSlot, common.Bytes96{}, common.Hash{})
	require.ErrorContains(t, err, "forkchoice update returned no payload ID")

	captured := logs()
	require.Equal(t, 1, strings.Count(captured, "forkchoice update returned no payload ID"), "records:\n"+captured)
	require.Equal(t, 1, strings.Count(captured, "lvl=eror"), "records:\n"+captured)
	require.NotContains(t, captured, "lvl=warn", "records:\n"+captured)
	require.NotContains(t, captured, "failed to produce execution payload")
}

func TestProductionCollectsTwoFailingBodyStepsWithoutRacing(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]byte{1, 2, 3, 4, 5, 6, 7, 8}, nil).AnyTimes()
	engine.EXPECT().GetAssembledBlock(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil, nil, nil, errors.New("boom")).AnyTimes()
	engine.EXPECT().SupportInsertion().Return(true).AnyTimes()
	handler.engine = engine

	// The body steps run concurrently, so each needs somewhere of its own to put its failure.
	syncPool := sync_pool_mock.NewMockSyncContributionPool(ctrl)
	syncPool.EXPECT().GetSyncAggregate(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("no aggregate")).AnyTimes()
	handler.syncMessagePool = syncPool

	_, err := handler.produceBlock(t.Context(), 1, postState.Slot(), common.Hash{0x41}, postState,
		postState.Slot()+1, common.Bytes96{}, common.Hash{})
	require.Error(t, err)
}

func TestProductionSaysNothingWhenTheRequestWasAbandoned(t *testing.T) {
	logs := captureProductionLogs(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := produceBlockWithFailingCollection(t, ctx, context.Canceled)
	require.Error(t, err)

	// A validator client that disconnects, or a node shutting down, is routine. Nothing about it is
	// actionable, at any layer. The unregistered fee recipient this fixture also warns about is a
	// separate matter and not what this measures.
	require.NotContains(t, logs(), "lvl=eror", "records:\n"+logs())
}

func TestLocalBlockIntegrationWaitsForMatchingBlock(t *testing.T) {
	handler := &ApiHandler{}
	root := common.HexToHash("0x1234")
	integration := handler.startLocalBlockIntegration(root)
	waited := make(chan error, 1)
	go func() {
		waited <- handler.waitForLocalBlockIntegration(t.Context(), root)
	}()

	select {
	case err := <-waited:
		require.Failf(t, "wait returned early", "error: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	handler.finishLocalBlockIntegration(root, integration, nil)
	require.NoError(t, <-waited)
	require.NoError(t, handler.waitForLocalBlockIntegration(t.Context(), common.HexToHash("0x5678")))
}

func TestLocalBlockIntegrationPropagatesFailure(t *testing.T) {
	handler := &ApiHandler{}
	root := common.HexToHash("0x1234")
	integration := handler.startLocalBlockIntegration(root)
	want := errors.New("block integration failed")
	waitCtx := &signalingContext{Context: t.Context(), entered: make(chan struct{})}
	waited := make(chan error, 1)
	go func() { waited <- handler.waitForLocalBlockIntegration(waitCtx, root) }()
	<-waitCtx.entered
	source := make(chan error, 1)
	destination := make(chan error, 1)
	go handler.relayLocalBlockIntegration(root, integration, source, destination)
	source <- want

	require.ErrorIs(t, <-waited, want)
	require.ErrorIs(t, <-destination, want)
}

func TestLocalBlockIntegrationCompletesBeforeStorageSuffix(t *testing.T) {
	handler := &ApiHandler{}
	root := common.HexToHash("0x1234")
	integration := handler.startLocalBlockIntegration(root)
	storageIntegrated := make(chan error, 1)
	blockIntegrated := make(chan error, 1)
	go handler.relayLocalBlockIntegration(root, integration, storageIntegrated, blockIntegrated)
	finishRelease := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		finished <- runBlockStoragePhases(storageIntegrated, func() error { return nil }, func() error {
			<-finishRelease
			return errors.New("storage suffix failed")
		})
	}()

	require.NoError(t, handler.waitForLocalBlockIntegration(t.Context(), root))
	require.NoError(t, <-blockIntegrated)
	close(finishRelease)
	require.ErrorContains(t, <-finished, "storage suffix failed")
}

func TestBroadcastBlockRegistersLocalIntegrationBeforeReturning(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	block := cltypes.NewSignedBeaconBlock(handler.beaconChainCfg, clparams.GloasVersion)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	integrationEntered := make(chan struct{})
	releaseIntegration := make(chan struct{})
	fcu.OnTickFunc = func(uint64) {}
	fcu.OnBlockFunc = func(context.Context, *cltypes.SignedBeaconBlock, bool, bool, bool) error {
		close(integrationEntered)
		<-releaseIntegration
		return nil
	}
	handler.gossipManager.(*gossip_mock.MockGossip).EXPECT().Publish(
		gomock.Any(), gossip_topic.TopicNameBeaconBlock, gomock.Any(),
	).Return(nil)

	require.NoError(t, handler.broadcastBlock(t.Context(), block))
	<-integrationEntered
	waitCtx := &signalingContext{Context: t.Context(), entered: make(chan struct{})}
	waited := make(chan error, 1)
	go func() { waited <- handler.waitForLocalBlockIntegration(waitCtx, root) }()
	<-waitCtx.entered
	select {
	case err := <-waited:
		require.Failf(t, "wait returned before block integration", "error: %v", err)
	default:
	}

	close(releaseIntegration)
	require.NoError(t, <-waited)
}
