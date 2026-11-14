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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	das_mock_services "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/ssz"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

// forwardSyncTipSentinel serves firstBlocks, then laterBlocks (empty by default), and envelopeResponse for every
// envelope request.
type forwardSyncTipSentinel struct {
	sentinelproto.SentinelClient
	firstBlocks      []byte
	laterBlocks      []byte
	envelopeResponse []byte
	blockCalls       atomic.Int32
}

func (s *forwardSyncTipSentinel) SendRequest(_ context.Context, request *sentinelproto.RequestData, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	if strings.Contains(request.Topic, communication.BeaconBlocksByRangeTopic) {
		call := s.blockCalls.Add(1)
		response := s.laterBlocks
		if call == 1 {
			response = s.firstBlocks
		}
		return &sentinelproto.ResponseData{Data: response, Peer: &sentinelproto.Peer{Pid: fmt.Sprintf("peer-%d", call)}}, nil
	}
	return &sentinelproto.ResponseData{Data: s.envelopeResponse, Peer: &sentinelproto.Peer{Pid: "envelope-peer"}}, nil
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

type forwardSyncTipFixture struct {
	cfg         *Cfg
	tip         *cltypes.SignedBeaconBlock
	tipRoot     common.Hash
	envelope    *cltypes.SignedExecutionPayloadEnvelope
	successor   *cltypes.SignedBeaconBlock
	emptyChild  *cltypes.SignedBeaconBlock
	currentSlot uint64
	sentinel    *forwardSyncTipSentinel
	digest      common.Bytes4
	clock       *steppedClock
}

// newForwardSyncTipFixture returns a node whose peers serve a FULL Gloas tip and no block in the slotsAfterTip slots
// after it. Its clock stays at the current slot until a test moves it.
func newForwardSyncTipFixture(t *testing.T, slotsAfterTip uint64, tipInForkChoice bool) *forwardSyncTipFixture {
	return newForwardSyncTipFixtureWithBlob(t, slotsAfterTip, tipInForkChoice, false)
}

func newForwardSyncTipFixtureWithBlob(t *testing.T, slotsAfterTip uint64, tipInForkChoice, withBlob bool) *forwardSyncTipFixture {
	t.Helper()

	cfg, graph, tipRoot, successor, emptyChild, _ := newChainTipBatchFixtureWithBlob(t, execution_client.PayloadStatusValidated, false, withBlob)
	tip := graph.parents[tipRoot]
	envelope := graph.envelopes[tipRoot]
	delete(graph.parents, tipRoot)
	delete(graph.envelopes, tipRoot)
	graph.insertOnAdd = true
	if tipInForkChoice {
		require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), tip, false, false, false))
	}

	currentSlot := tip.Block.Slot + slotsAfterTip
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, cfg.beaconCfg)
	digest, err := clock.ComputeForkDigest(tip.Block.Slot / cfg.beaconCfg.SlotsPerEpoch)
	require.NoError(t, err)
	f := &forwardSyncTipFixture{cfg: cfg, tip: tip, tipRoot: tipRoot, envelope: envelope, successor: successor, emptyChild: emptyChild, currentSlot: currentSlot, digest: digest}
	anchorEnvelope := *envelope
	anchorMessage := *envelope.Message
	anchorMessage.BeaconBlockRoot = cfg.forkChoice.AnchorRoot()
	anchorEnvelope.Message = &anchorMessage
	// An anchor envelope that fails the anchor bid check keeps the anchor EMPTY without a network fetch.
	graph.envelopesMu.Lock()
	graph.envelopes[cfg.forkChoice.AnchorRoot()] = &anchorEnvelope
	graph.envelopesMu.Unlock()
	f.sentinel = &forwardSyncTipSentinel{
		firstBlocks:      f.encode(t, tip),
		envelopeResponse: f.encode(t, envelope),
	}
	f.clock = &steppedClock{EthereumClock: clock, slotsPerEpoch: cfg.beaconCfg.SlotsPerEpoch}
	f.clock.slot.Store(currentSlot)
	cfg.ethClock = f.clock
	cfg.rpc = rpc.NewBeaconRpcP2P(t.Context(), f.sentinel, cfg.beaconCfg, clock)
	cfg.syncedData = synced_data.NewSyncedDataManager(cfg.beaconCfg, true)
	cfg.hasDownloaded = true
	return f
}

func (f *forwardSyncTipFixture) encode(t *testing.T, object ssz.Marshaler) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&buf, object, f.digest[:]...))
	return buf.Bytes()
}

func (f *forwardSyncTipFixture) head(t *testing.T) common.Hash {
	t.Helper()
	headRoot, _, err := f.cfg.forkChoice.GetHead(nil)
	require.NoError(t, err)
	return headRoot
}

// ForwardSync imports the held tip with its envelope, also after a restart when the tip is not in fork choice yet,
// so the head that ForkChoice publishes is the tip and not its parent.
func TestForwardSyncEndsAtGloasTipWithoutSuccessor(t *testing.T) {
	for _, tipInForkChoice := range []bool{true, false} {
		for _, slotsAfterTip := range []uint64{1, 2, 33, 40} {
			t.Run(fmt.Sprintf("known=%t/%d", tipInForkChoice, slotsAfterTip), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					f := newForwardSyncTipFixture(t, slotsAfterTip, tipInForkChoice)
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()

					require.NoError(t, forwardSync(ctx, log.Root(), f.cfg, Args{}))
					require.Equal(t, f.tipRoot, f.cfg.forwardSyncTipRoot)
					require.Equal(t, f.currentSlot, f.cfg.forwardSyncTipSlot)
					require.True(t, f.cfg.forkChoice.HasEnvelope(f.tipRoot))
					require.Equal(t, f.tipRoot, f.head(t))
				})
			})
		}
	}
}

// The tip has no child that built on its payload, so its envelope must pass the data availability check before it
// takes effect; a miss queues it for the retry.
func TestForwardSyncQueuesTipEnvelopeUntilDataIsAvailable(t *testing.T) {
	f := newForwardSyncTipFixtureWithBlob(t, 2, false, true)
	var available atomic.Bool
	peerDas := das_mock_services.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(gomock.Any(), gomock.Any()).DoAndReturn(func(uint64, common.Hash) (bool, error) {
		return available.Load(), nil
	}).AnyTimes()
	peerDas.EXPECT().SyncColumnDataLater(gomock.Any()).Return(nil).AnyTimes()
	peerDas.EXPECT().BackfillCustodyColumns(gomock.Any(), gomock.Any()).AnyTimes()
	f.cfg.forkChoice.InitPeerDas(peerDas)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	require.NoError(t, forwardSync(ctx, log.Root(), f.cfg, Args{}))
	require.Equal(t, f.tipRoot, f.cfg.forwardSyncTipRoot)
	require.False(t, f.cfg.forkChoice.HasEnvelope(f.tipRoot))
	require.True(t, f.cfg.forkChoice.HasPendingExecutionPayloadEnvelope(f.tipRoot))
	require.Equal(t, f.tipRoot, f.head(t))

	available.Store(true)
	require.Eventually(t, func() bool {
		f.cfg.forkChoice.RetryDataAvailablePendingExecutionPayloadEnvelopes(ctx, f.tip.Block.Slot)
		return f.cfg.forkChoice.HasEnvelope(f.tipRoot)
	}, 3*time.Second, 50*time.Millisecond)
}

// Fork choice does not schedule a column download while the head is unpublished, so ForwardSync schedules the tip's.
func TestForwardSyncSchedulesTipColumnsWhenDataIsMissing(t *testing.T) {
	f := newForwardSyncTipFixtureWithBlob(t, 0, false, true)
	var scheduled []common.Hash
	peerDas := das_mock_services.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
	peerDas.EXPECT().SyncColumnDataLater(gomock.Any()).DoAndReturn(func(block *cltypes.SignedBeaconBlock) error {
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		scheduled = append(scheduled, root)
		return nil
	}).AnyTimes()
	peerDas.EXPECT().BackfillCustodyColumns(gomock.Any(), gomock.Any()).AnyTimes()
	f.cfg.forkChoice.InitPeerDas(peerDas)
	f.cfg.peerDas = peerDas
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	require.NoError(t, forwardSync(ctx, log.Root(), f.cfg, Args{}))
	require.True(t, f.cfg.forkChoice.HasPendingExecutionPayloadEnvelope(f.tipRoot))
	require.Contains(t, scheduled, f.tipRoot)
}

// With an execution client that inserts blocks, the tip's envelope skips EL validation. Its arrival still counts for
// the payload attestation of the tip's slot, but only after its builder signature and bid commitments are checked.
func TestForwardSyncRecordsTipEnvelopeArrivalWithInsertion(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		f := newForwardSyncTipFixture(t, 0, false)
		f.cfg.executionClient.(*testExecutionEngine).supportInsertion = true
		f.cfg.blockCollector = &gloasCollectorTest{}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		started := time.Now()

		require.NoError(t, forwardSync(ctx, log.Root(), f.cfg, Args{}))
		require.True(t, f.cfg.forkChoice.HasEnvelope(f.tipRoot))
		require.False(t, f.cfg.forkChoice.ExecutionPayloadReceivedBefore(f.tipRoot, started))
		require.True(t, f.cfg.forkChoice.ExecutionPayloadReceivedBefore(f.tipRoot, time.Now()))
	})
	t.Run("data missing", func(t *testing.T) {
		f := newForwardSyncTipFixtureWithBlob(t, 0, false, true)
		f.cfg.executionClient.(*testExecutionEngine).supportInsertion = true
		f.cfg.blockCollector = &gloasCollectorTest{}
		missingDataPeerDas(t, f.cfg)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		require.NoError(t, forwardSync(ctx, log.Root(), f.cfg, Args{}))
		require.True(t, f.cfg.forkChoice.HasPendingExecutionPayloadEnvelope(f.tipRoot))
		require.True(t, f.cfg.forkChoice.ExecutionPayloadReceivedBefore(f.tipRoot, time.Now()))
	})
	t.Run("signed bid mismatch", func(t *testing.T) {
		f := newForwardSyncTipFixture(t, 0, false)
		f.cfg.executionClient.(*testExecutionEngine).supportInsertion = true
		f.cfg.blockCollector = &gloasCollectorTest{}
		message := *f.envelope.Message
		payload := *message.Payload
		payload.GasLimit++
		message.Payload = &payload
		badEnvelope := *f.envelope
		badEnvelope.Message = &message
		parentState, err := f.cfg.forkChoice.GetStateAtBlockRoot(f.tip.Block.ParentRoot)
		require.NoError(t, err)
		privKey, err := bls.NewPrivateKeyFromIKM([]byte("01234567890123456789012345678901"))
		require.NoError(t, err)
		signAnchorEnvelope(t, parentState, privKey, &badEnvelope, f.tip.Block.Slot)
		f.sentinel.envelopeResponse = nil
		envelopeBytes, err := badEnvelope.EncodeSSZ(nil)
		require.NoError(t, err)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(envelopeBytes)
		}))
		t.Cleanup(server.Close)
		previousURLs := clparams.ConfigurableCheckpointsURLs
		clparams.ConfigurableCheckpointsURLs = []string{server.URL}
		t.Cleanup(func() { clparams.ConfigurableCheckpointsURLs = previousURLs })
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		// The tip can be imported without an envelope; discarded envelopes must not count as payload arrivals.
		require.NoError(t, forwardSync(ctx, log.Root(), f.cfg, Args{}))
		require.Equal(t, f.tipRoot, f.cfg.forwardSyncTipRoot)
		require.False(t, f.cfg.forkChoice.HasEnvelope(f.tipRoot))
		require.False(t, f.cfg.forkChoice.ExecutionPayloadReceivedBefore(f.tipRoot, time.Now()))
	})
	for _, withBlob := range []bool{false, true} {
		t.Run(fmt.Sprintf("bad signature/blob=%t", withBlob), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newForwardSyncTipFixtureWithBlob(t, 0, false, withBlob)
				f.cfg.executionClient.(*testExecutionEngine).supportInsertion = true
				f.cfg.blockCollector = &gloasCollectorTest{}
				if withBlob {
					missingDataPeerDas(t, f.cfg)
				}
				badEnvelope := *f.envelope
				badEnvelope.Signature[0] ^= 0xff
				f.sentinel.envelopeResponse = f.encode(t, &badEnvelope)
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
				defer cancel()

				require.ErrorIs(t, forwardSync(ctx, log.Root(), f.cfg, Args{}), context.DeadlineExceeded)
				require.Zero(t, f.cfg.forwardSyncTipRoot)
				require.False(t, f.cfg.forkChoice.HasEnvelope(f.tipRoot))
				require.False(t, f.cfg.forkChoice.HasPendingExecutionPayloadEnvelope(f.tipRoot))
				require.False(t, f.cfg.forkChoice.ExecutionPayloadReceivedBefore(f.tipRoot, time.Now()))
			})
		})
	}
}

func missingDataPeerDas(t *testing.T, cfg *Cfg) {
	t.Helper()
	peerDas := das_mock_services.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
	peerDas.EXPECT().SyncColumnDataLater(gomock.Any()).Return(nil).AnyTimes()
	peerDas.EXPECT().BackfillCustodyColumns(gomock.Any(), gomock.Any()).AnyTimes()
	cfg.forkChoice.InitPeerDas(peerDas)
}

// The answer that brings the tip covers no slot after it, so ForwardSync asks again and finds the successor.
func TestForwardSyncAsksForSuccessorAfterAnswerWithTip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newForwardSyncTipFixture(t, 1, false)
		f.sentinel.laterBlocks = f.encode(t, f.successor)
		successorRoot, err := f.successor.Block.HashSSZ()
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		require.NoError(t, forwardSync(ctx, log.Root(), f.cfg, Args{}))
		require.Equal(t, common.Hash(successorRoot), f.cfg.forwardSyncTipRoot)
		require.Equal(t, common.Hash(successorRoot), f.head(t))
		require.True(t, f.cfg.forkChoice.HasEnvelope(f.tipRoot))
	})
}

// Processing skips a tip that is not on the finalized chain without an error, even when the tip is in the chain tip
// slot that ends ForwardSync.
func TestForwardSyncKeepsSyncingWhenForkChoiceRejectsGloasTip(t *testing.T) {
	for _, slotsAfterTip := range []uint64{0, 2} {
		t.Run(fmt.Sprint(slotsAfterTip), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newForwardSyncTipFixture(t, slotsAfterTip, false)
				orphan := cltypes.NewSignedBeaconBlock(f.cfg.beaconCfg, clparams.GloasVersion)
				orphan.Block.Slot = f.tip.Block.Slot
				orphan.Block.ParentRoot = common.HexToHash("0x01")
				f.sentinel.firstBlocks = f.encode(t, orphan)
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
				defer cancel()

				require.ErrorIs(t, forwardSync(ctx, log.Root(), f.cfg, Args{}), context.DeadlineExceeded)
				require.Zero(t, f.cfg.forwardSyncTipRoot)
			})
		})
	}
}

func TestForwardSyncKeepsSyncingWhenTipEnvelopeIsRejected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newForwardSyncTipFixture(t, 2, false)
		badEnvelope := *f.envelope
		badEnvelope.Signature[0] ^= 0xff
		f.sentinel.envelopeResponse = f.encode(t, &badEnvelope)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()

		require.ErrorIs(t, forwardSync(ctx, log.Root(), f.cfg, Args{}), context.DeadlineExceeded)
		require.Zero(t, f.cfg.forwardSyncTipRoot)
		require.False(t, f.cfg.forkChoice.HasEnvelope(f.tipRoot))
	})
}

// The marker holds for less than an epoch after it was recorded, and only while no newer block is seen.
func TestForwardSyncTipMarkerExpires(t *testing.T) {
	f := newForwardSyncTipFixture(t, 2, true)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, forwardSync(ctx, log.Root(), f.cfg, Args{}))

	expirySlot := f.currentSlot + f.cfg.beaconCfg.SlotsPerEpoch
	argsAt := func(slot uint64) Args {
		f.clock.slot.Store(slot)
		return ConsensusClStages().ArgsFunc(ctx, f.cfg)
	}
	require.True(t, argsAt(expirySlot-1).atForwardSyncTip)
	require.False(t, argsAt(expirySlot).atForwardSyncTip)

	require.NoError(t, f.cfg.forkChoice.OnBlock(ctx, f.successor, false, false, false))
	require.False(t, argsAt(expirySlot-1).atForwardSyncTip)
}

// Fork choice has seen a block above the tip that the peers do not serve. The marker records the highest seen block,
// so the stage loop does not go back to ForwardSync.
func TestForwardSyncMarkerHoldsWhenHighestSeenIsAboveTip(t *testing.T) {
	f := newForwardSyncTipFixture(t, 2*32, true)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, f.cfg.forkChoice.OnBlock(ctx, f.emptyChild, false, false, false))
	childRoot, err := f.emptyChild.Block.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(childRoot), f.cfg.forkChoice.HighestSeenRoot())

	require.NoError(t, forwardSync(ctx, log.Root(), f.cfg, Args{}))
	args := ConsensusClStages().ArgsFunc(ctx, f.cfg)
	require.True(t, args.atForwardSyncTip)
	require.Equal(t, ForkChoice, ConsensusClStages().Stages[ForwardSync].TransitionFunc(f.cfg, args, nil))
}

type steppedClock struct {
	eth_clock.EthereumClock
	slot          atomic.Uint64
	slotsPerEpoch uint64
}

func (c *steppedClock) GetCurrentSlot() uint64  { return c.slot.Load() }
func (c *steppedClock) GetCurrentEpoch() uint64 { return c.slot.Load() / c.slotsPerEpoch }

type flushHookCollector struct {
	gloasCollectorTest
	onFlush func()
}

func (c *flushHookCollector) Flush(ctx context.Context) error {
	c.onFlush()
	return c.gloasCollectorTest.Flush(ctx)
}

// The marker's epoch starts when ForwardSync returns, after the block collector flush, which can take long.
func TestForwardSyncDatesTipMarkerAfterFlush(t *testing.T) {
	f := newForwardSyncTipFixture(t, 2, true)
	f.cfg.executionClient.(*testExecutionEngine).supportInsertion = true
	afterFlush := f.currentSlot + f.cfg.beaconCfg.SlotsPerEpoch
	f.cfg.blockCollector = &flushHookCollector{onFlush: func() { f.clock.slot.Store(afterFlush) }}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	require.NoError(t, forwardSync(ctx, log.Root(), f.cfg, Args{}))
	require.Equal(t, afterFlush, f.cfg.forwardSyncTipSlot)
}

// More than an epoch of empty slots after the tip keeps the seen epoch behind the target epoch. The stage loop must
// still reach ForkChoice and CleanupAndPruning instead of going back to ForwardSync.
func TestStageLoopAfterGloasTipWithEmptyEpochs(t *testing.T) {
	f := newForwardSyncTipFixture(t, 2*32, true)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stageErr := forwardSync(ctx, log.Root(), f.cfg, Args{})
	require.NoError(t, stageErr)

	graph := ConsensusClStages()
	stage := ForwardSync
	var visited []string
	for range 8 {
		args := graph.ArgsFunc(ctx, f.cfg)
		require.Less(t, args.seenEpoch, args.targetEpoch)
		stage = graph.Stages[stage].TransitionFunc(f.cfg, args, stageErr)
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
