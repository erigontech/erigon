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
	"sync/atomic"
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
	gate  chan struct{} // HasEnvelope blocks on it, inside the coordinated section
}

func (g retryPendingForkGraph) GetBlock(common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	return g.block, g.block != nil
}

// With a gate, HasEnvelope blocks until it opens and then reports the envelope as present, so
// the apply ends on the already-persisted path without touching a config the stub lacks.
func (g retryPendingForkGraph) HasEnvelope(common.Hash) bool {
	if g.gate != nil {
		<-g.gate
		return true
	}
	return false
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
	peerDas.EXPECT().IsDataAvailable(uint64(7), root).Return(true, nil).AnyTimes()
	f, pending := newRetryPendingStore(t, peerDas)
	release := make(chan struct{})
	f.forkGraph = retryPendingForkGraph{block: f.forkGraph.(retryPendingForkGraph).block, gate: release}
	pending.Add(root, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root}})

	first := make(chan struct{})
	go func() {
		f.RetryPendingExecutionPayloadEnvelope(t.Context(), root)
		close(first)
	}()
	require.Eventually(t, func() bool {
		_, busy := f.retryingEnvelopes.Load(root)
		return busy
	}, 5*time.Second, time.Millisecond)
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
	requireClosed(t, first)
	requireClosed(t, second)
}

func requireClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}

func TestEnterPendingEnvelopeApplyAdmitsAWaiterAfterTheHolderLeaves(t *testing.T) {
	f := &ForkChoiceStore{}
	root := common.HexToHash("0x1")
	require.True(t, f.enterPendingEnvelopeApply(context.Background(), root))

	waiter := make(chan bool, 1)
	go func() {
		admitted := f.enterPendingEnvelopeApply(context.Background(), root)
		waiter <- admitted
		f.leavePendingEnvelopeApply(root)
	}()
	select {
	case <-waiter:
		t.Fatal("a waiter was admitted while the holder was still applying")
	case <-time.After(50 * time.Millisecond):
	}
	f.leavePendingEnvelopeApply(root)
	select {
	case admitted := <-waiter:
		require.True(t, admitted, "the waiter runs itself after the holder leaves")
	case <-time.After(5 * time.Second):
		t.Fatal("the waiter was not admitted after the holder left")
	}

	// A waiter whose context ends first is not admitted.
	require.True(t, f.enterPendingEnvelopeApply(context.Background(), root))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.False(t, f.enterPendingEnvelopeApply(ctx, root))
	f.leavePendingEnvelopeApply(root)
}

// A caller whose budget already expired must not take the gate even when it is idle: the apply
// would run commitments, BLS and the state transition before the EL rejects the dead context.
func TestEnterPendingEnvelopeApplyRefusesADeadContextAtAnIdleGate(t *testing.T) {
	f := &ForkChoiceStore{}
	root := common.HexToHash("0x1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.False(t, f.enterPendingEnvelopeApply(ctx, root))
	_, busy := f.retryingEnvelopes.Load(root)
	require.False(t, busy)
}

// Every caller of applyPendingEnvelope, block import included, holds the per-root gate.
func TestApplyPendingEnvelopeHoldsThePerRootGate(t *testing.T) {
	root := common.HexToHash("0x1")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(uint64(7), root).Return(true, nil).AnyTimes()
	f, pending := newRetryPendingStore(t, peerDas)
	release := make(chan struct{})
	f.forkGraph = retryPendingForkGraph{block: f.forkGraph.(retryPendingForkGraph).block, gate: release}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root}}
	pending.Add(root, envelope)

	done := make(chan struct{})
	go func() {
		f.applyPendingEnvelope(t.Context(), root, envelope, false, false)
		close(done)
	}()
	require.Eventually(t, func() bool {
		_, busy := f.retryingEnvelopes.Load(root)
		return busy
	}, 5*time.Second, time.Millisecond)
	close(release)
	requireClosed(t, done)
	_, busy := f.retryingEnvelopes.Load(root)
	require.False(t, busy)
}

type countingForkGraph struct {
	retryPendingForkGraph
	hasEnvelopeCalls *atomic.Int32
}

func (g countingForkGraph) HasEnvelope(root common.Hash) bool {
	g.hasEnvelopeCalls.Add(1)
	return g.retryPendingForkGraph.HasEnvelope(root)
}

// A waiter admitted after the holder settled the same parked copy must not apply it again.
func TestApplyPendingEnvelopeSkipsACopyTheHolderSettled(t *testing.T) {
	root := common.HexToHash("0x1")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(uint64(7), root).Return(true, nil).AnyTimes()
	f, pending := newRetryPendingStore(t, peerDas)
	calls := &atomic.Int32{}
	opened := make(chan struct{})
	close(opened) // an unexpected apply ends on the persisted path instead of the stub's missing config
	f.forkGraph = countingForkGraph{retryPendingForkGraph: retryPendingForkGraph{block: f.forkGraph.(retryPendingForkGraph).block, gate: opened}, hasEnvelopeCalls: calls}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root}}
	pending.Add(root, envelope)

	require.True(t, f.enterPendingEnvelopeApply(t.Context(), root))
	done := make(chan struct{})
	go func() {
		f.applyPendingEnvelope(t.Context(), root, envelope, false, false)
		close(done)
	}()
	pending.Remove(root) // the holder settled the copy
	f.leavePendingEnvelopeApply(root)
	requireClosed(t, done)

	require.Zero(t, calls.Load(), "the waiter must return without touching the envelope")
}
