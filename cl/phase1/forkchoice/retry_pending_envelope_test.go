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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	das_mock "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
	"github.com/hashicorp/golang-lru/v2"
)

type retryPendingForkGraph struct {
	payloadVoteForkGraph
	block *cltypes.SignedBeaconBlock
}

func (g retryPendingForkGraph) GetBlock(common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	return g.block, g.block != nil
}

func newRetryPendingStore(t *testing.T, peerDas *das_mock.MockPeerDas) (*ForkChoiceStore, *lru.Cache[common.Hash, *cltypes.SignedExecutionPayloadEnvelope]) {
	t.Helper()
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](2)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](2)
	require.NoError(t, err)
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 7, Body: &cltypes.BeaconBody{
		Version: clparams.GloasVersion,
		SignedExecutionPayloadBid: &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](4, 48),
		}},
	}}}
	block.Block.Body.SignedExecutionPayloadBid.Message.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	f := &ForkChoiceStore{
		forkGraph:                      retryPendingForkGraph{block: block},
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: local,
		peerDas:                        peerDas,
	}
	return f, pending
}

func TestRetryPendingExecutionPayloadEnvelopeIgnoresRootsWithoutPendingEnvelope(t *testing.T) {
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	f, _ := newRetryPendingStore(t, peerDas)

	f.RetryPendingExecutionPayloadEnvelope(t.Context(), common.HexToHash("0x1"))
}

func TestRetryPendingExecutionPayloadEnvelopeWaitsForColumnData(t *testing.T) {
	root := common.HexToHash("0x1")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(uint64(7), root).Return(false, nil)
	f, pending := newRetryPendingStore(t, peerDas)
	pending.Add(root, &cltypes.SignedExecutionPayloadEnvelope{})

	f.RetryPendingExecutionPayloadEnvelope(t.Context(), root)

	require.True(t, pending.Contains(root))
}

func TestRetryPendingExecutionPayloadEnvelopeAppliesOnceColumnDataIsAvailable(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(cfg, envelope.Message.ExecutionRequests))
	payloadHash, err := envelope.Message.Payload.ComputeBlockHash(&envelope.Message.ParentBeaconBlockRoot, requestsHash, nil)
	require.NoError(t, err)
	envelope.Message.Payload.BlockHash = payloadHash
	parentBid := block.Block.Body.GetSignedExecutionPayloadBid().Message
	parentBid.BlockHash = payloadHash
	parentBid.GasLimit = envelope.Message.Payload.GasLimit
	bodyRoot, err := block.Block.Body.HashSSZ()
	require.NoError(t, err)
	blockState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{
		Slot:          block.Block.Slot,
		ProposerIndex: block.Block.ProposerIndex,
		ParentRoot:    block.Block.ParentRoot,
		BodyRoot:      bodyRoot,
	})
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	root := common.Hash(blockRoot)
	envelope.Message.BeaconBlockRoot = root
	resignAdmissionEnvelope(t, cfg, blockState, envelope)

	f := newPayloadVoteTestStore(t, root, false, false)
	gasLimits, err := lru.New[common.Hash, uint64](1)
	require.NoError(t, err)
	f.executionPayloadGasLimit = gasLimits
	f.beaconCfg = cfg
	graph := &persistedEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block}}
	f.forkGraph = graph
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](2)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](2)
	require.NoError(t, err)
	pending.Add(root, envelope)
	f.pendingEnvelopes = pending
	f.pendingLocalSelfBuildEnvelopes = local
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(true, nil).AnyTimes()
	f.peerDas = peerDas

	f.RetryPendingExecutionPayloadEnvelope(t.Context(), root)

	require.True(t, graph.HasEnvelope(root))
	require.True(t, f.isPayloadAvailable(root))
	status, ok := f.GetRecentExecutionPayloadStatusByRoot(root)
	require.True(t, ok)
	require.Equal(t, execution_client.PayloadStatus(execution_client.PayloadStatusNotValidated), status)
	require.False(t, pending.Contains(root))
}

func TestRetryPendingExecutionPayloadEnvelopeWaitsForInFlightRetry(t *testing.T) {
	root := common.HexToHash("0x1")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	release := make(chan struct{})
	peerDas.EXPECT().IsDataAvailable(uint64(7), root).DoAndReturn(func(uint64, common.Hash) (bool, error) {
		<-release
		return false, nil
	}).Times(1)
	f, pending := newRetryPendingStore(t, peerDas)
	pending.Add(root, &cltypes.SignedExecutionPayloadEnvelope{})

	first := make(chan struct{})
	go func() {
		f.RetryPendingExecutionPayloadEnvelope(t.Context(), root)
		close(first)
	}()
	for {
		if _, busy := f.retryingEnvelopes.Load(root); busy {
			break
		}
		time.Sleep(time.Millisecond)
	}
	second := make(chan struct{})
	go func() {
		f.RetryPendingExecutionPayloadEnvelope(t.Context(), root)
		close(second)
	}()
	select {
	case <-second:
		t.Fatal("second retry returned while the first was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-first
	<-second
}
