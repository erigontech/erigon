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

package stages

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

// forwardSyncTipSentinel serves the tip for the first block range request and empty ranges after it. The first
// envelope request, which asks for the anchor's envelope, gets an envelope that fails the anchor bid check, so the
// anchor stays EMPTY; later envelope requests get the tip's envelope.
type forwardSyncTipSentinel struct {
	sentinelproto.SentinelClient
	blockResponse          []byte
	anchorEnvelopeResponse []byte
	envelopeResponse       []byte
	blockCalls             atomic.Int32
	envelopeCalls          atomic.Int32
}

func (s *forwardSyncTipSentinel) SendRequest(_ context.Context, request *sentinelproto.RequestData, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	if strings.Contains(request.Topic, communication.BeaconBlocksByRangeTopic) {
		call := s.blockCalls.Add(1)
		response := []byte(nil)
		if call == 1 {
			response = s.blockResponse
		}
		return &sentinelproto.ResponseData{Data: response, Peer: &sentinelproto.Peer{Pid: fmt.Sprintf("peer-%d", call)}}, nil
	}
	response := s.envelopeResponse
	if s.envelopeCalls.Add(1) == 1 {
		response = s.anchorEnvelopeResponse
	}
	return &sentinelproto.ResponseData{Data: response, Peer: &sentinelproto.Peer{Pid: "envelope-peer"}}, nil
}

func (*forwardSyncTipSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{}, nil
}

func (*forwardSyncTipSentinel) GetPeers(context.Context, *sentinelproto.EmptyMessage, ...grpc.CallOption) (*sentinelproto.PeerCount, error) {
	return &sentinelproto.PeerCount{Active: 1}, nil
}

func (*forwardSyncTipSentinel) SetStatus(context.Context, *sentinelproto.Status, ...grpc.CallOption) (*sentinelproto.EmptyMessage, error) {
	return &sentinelproto.EmptyMessage{}, nil
}

// newForwardSyncTipFixture returns a node whose peers serve a FULL Gloas tip and no block in the slotsAfterTip slots
// after it.
func newForwardSyncTipFixture(t *testing.T, slotsAfterTip uint64, tipInForkChoice bool) (*Cfg, common.Hash, uint64) {
	t.Helper()

	cfg, graph, tipRoot, _, _, _ := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	tip := graph.parents[tipRoot]
	envelope := graph.envelopes[tipRoot]
	delete(graph.parents, tipRoot)
	delete(graph.envelopes, tipRoot)
	graph.insertOnAdd = true
	if tipInForkChoice {
		require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), tip, false, false, false))
	}

	currentSlot := tip.Block.Slot + slotsAfterTip
	genesisTime := uint64(time.Now().Unix()) - currentSlot*cfg.beaconCfg.SecondsPerSlot
	clock := eth_clock.NewEthereumClock(genesisTime, common.Hash{}, cfg.beaconCfg)
	digest, err := clock.ComputeForkDigest(tip.Block.Slot / cfg.beaconCfg.SlotsPerEpoch)
	require.NoError(t, err)
	var blockResponse bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&blockResponse, tip, digest[:]...))
	anchorEnvelope := *envelope
	anchorMessage := *envelope.Message
	anchorMessage.BeaconBlockRoot = cfg.forkChoice.AnchorRoot()
	anchorEnvelope.Message = &anchorMessage
	var anchorEnvelopeResponse bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&anchorEnvelopeResponse, &anchorEnvelope, digest[:]...))
	var envelopeResponse bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&envelopeResponse, envelope, digest[:]...))
	sentinel := &forwardSyncTipSentinel{
		blockResponse:          blockResponse.Bytes(),
		anchorEnvelopeResponse: anchorEnvelopeResponse.Bytes(),
		envelopeResponse:       envelopeResponse.Bytes(),
	}
	cfg.ethClock = clock
	cfg.rpc = rpc.NewBeaconRpcP2P(t.Context(), sentinel, cfg.beaconCfg, clock)
	cfg.syncedData = synced_data.NewSyncedDataManager(cfg.beaconCfg, true)
	cfg.hasDownloaded = true
	return cfg, tipRoot, currentSlot
}

func TestForwardSyncEndsAtGloasTipWithoutSuccessor(t *testing.T) {
	for _, slotsAfterTip := range []uint64{1, 2, 33, 40} {
		t.Run(fmt.Sprint(slotsAfterTip), func(t *testing.T) {
			cfg, tipRoot, currentSlot := newForwardSyncTipFixture(t, slotsAfterTip, true)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			require.NoError(t, forwardSync(ctx, log.Root(), cfg, Args{}))
			require.Equal(t, tipRoot, cfg.forwardSyncTipRoot)
			require.Equal(t, currentSlot, cfg.forwardSyncTipSlot)
			require.True(t, cfg.forkChoice.HasEnvelope(tipRoot))
			headRoot, _, err := cfg.forkChoice.GetHead(nil)
			require.NoError(t, err)
			require.Equal(t, tipRoot, headRoot)
		})
	}
}

// After a restart the held tip is not in fork choice yet: ForwardSync imports it with its envelope, so the head
// that ForkChoice publishes is the tip and not its parent.
func TestForwardSyncImportsHeldGloasTip(t *testing.T) {
	for _, slotsAfterTip := range []uint64{1, 2, 33} {
		t.Run(fmt.Sprint(slotsAfterTip), func(t *testing.T) {
			cfg, tipRoot, currentSlot := newForwardSyncTipFixture(t, slotsAfterTip, false)
			cfg.forkChoice.OnTick((currentSlot + 1) * cfg.beaconCfg.SecondsPerSlot * 1000)
			_, known := cfg.forkChoice.GetHeader(tipRoot)
			require.False(t, known)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			require.NoError(t, forwardSync(ctx, log.Root(), cfg, Args{}))
			_, known = cfg.forkChoice.GetHeader(tipRoot)
			require.True(t, known)
			require.True(t, cfg.forkChoice.HasEnvelope(tipRoot))
			headRoot, _, err := cfg.forkChoice.GetHead(nil)
			require.NoError(t, err)
			require.Equal(t, tipRoot, headRoot)
			require.Equal(t, tipRoot, cfg.forwardSyncTipRoot)
		})
	}
}

// More than an epoch of empty slots after the tip keeps the seen epoch behind the target epoch. The stage loop must
// still reach ForkChoice and CleanupAndPruning instead of going back to ForwardSync.
func TestStageLoopAfterGloasTipWithEmptyEpochs(t *testing.T) {
	cfg, _, _ := newForwardSyncTipFixture(t, 2*32, true)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stageErr := forwardSync(ctx, log.Root(), cfg, Args{})
	require.NoError(t, stageErr)

	graph := ConsensusClStages()
	stage := ForwardSync
	var visited []string
	for range 8 {
		args := graph.ArgsFunc(ctx, cfg)
		require.Less(t, args.seenEpoch, args.targetEpoch)
		stage = graph.Stages[stage].TransitionFunc(cfg, args, stageErr)
		visited = append(visited, stage)
		stageErr = nil
		if stage == ChainTipSync {
			stageErr = context.DeadlineExceeded
		}
	}
	require.NotContains(t, visited, ForwardSync)
	require.Equal(t, ForkChoice, visited[0])
	require.Contains(t, visited, CleanupAndPruning)
}
