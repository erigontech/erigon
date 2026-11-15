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

package network

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type noPeersSentinel struct {
	sentinelproto.SentinelClient
	calls atomic.Int32
}

func (s *noPeersSentinel) SendRequest(context.Context, *sentinelproto.RequestData, ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	s.calls.Add(1)
	return nil, errors.New("no peers")
}

func (*noPeersSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{}, nil
}

func newHeldTipDownloader(t *testing.T, cfg *clparams.BeaconChainConfig, client *rpc.BeaconRpcP2P, tip *cltypes.SignedBeaconBlock, tipPeer string) *ForwardBeaconDownloader {
	downloader := NewForwardBeaconDownloader(t.Context(), client, cfg)
	downloader.SetHighestProcessedSlot(tip.Block.Slot - 1)
	downloader.gloasLookahead = tip
	downloader.gloasLookaheadPeer = tipPeer
	downloader.gloasNextUnscanned = tip.Block.Slot + 1
	return downloader
}

func newNoPeersRPC(t *testing.T, cfg *clparams.BeaconChainConfig) (*rpc.BeaconRpcP2P, *noPeersSentinel) {
	cfg.InitializeForkSchedule()
	sentinel := &noPeersSentinel{}
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, cfg)
	return rpc.NewBeaconRpcP2P(t.Context(), sentinel, cfg, clock), sentinel
}

func newEnvelopeServer(t *testing.T, root common.Hash, envelopeBytes []byte) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != fmt.Sprintf("/eth/v1/beacon/execution_payload_envelopes/0x%x", root) {
			http.NotFound(w, r)
			return
		}
		if envelopeBytes == nil {
			<-r.Context().Done()
			return
		}
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(envelopeBytes)
	}))
	t.Cleanup(server.Close)
	return server
}

func makeFullGloasTipAndEnvelope(t *testing.T, cfg *clparams.BeaconChainConfig, slot uint64) (*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope, common.Hash) {
	t.Helper()
	block := makeGloasBlock(slot, common.Hash{}, common.Hash{})
	bid := block.Block.Body.SignedExecutionPayloadBid.Message
	bid.Slot = slot
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	envelope.Message.Payload.SlotNumber = slot
	_, err := envelope.Message.Payload.EncodeSSZ(nil)
	require.NoError(t, err)
	bid.ExecutionRequestsRoot, err = envelope.Message.ExecutionRequests.HashSSZ()
	require.NoError(t, err)
	requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(cfg, envelope.Message.ExecutionRequests))
	envelope.Message.Payload.BlockHash = common.HexToHash("0xefd641f283ac9ad546ad48be6207db401e3f70fe94adc9a10d6c858d845b67e3")
	header, err := envelope.Message.Payload.RlpHeader(&envelope.Message.ParentBeaconBlockRoot, requestsHash, nil)
	require.NoError(t, err)
	envelope.Message.Payload.BlockHash = header.Hash()
	bid.BlockHash = header.Hash()
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope.Message.BeaconBlockRoot = root
	require.NoError(t, ValidateDownloadedGloasEnvelope(cfg, block, envelope))
	return block, envelope, root
}

func acceptAnyTip(*cltypes.SignedBeaconBlock, common.Hash, *cltypes.SignedExecutionPayloadEnvelope, time.Time) bool {
	return true
}

func requestMoreWithin(t *testing.T, downloader *ForwardBeaconDownloader) bool {
	t.Helper()
	done := make(chan bool, 1)
	go func() { done <- downloader.RequestMore(t.Context()) }()
	select {
	case atTip := <-done:
		return atTip
	case <-time.After(time.Second):
		t.Fatal("RequestMore did not return")
		return false
	}
}

func TestForwardBeaconDownloaderReturnsAtScannedGloasTip(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	forwardBeaconRequestInterval = time.Millisecond
	t.Cleanup(func() { forwardBeaconRequestInterval = previousInterval })

	for _, currentSlot := range []uint64{11, 12, 50} {
		t.Run(fmt.Sprint(currentSlot), func(t *testing.T) {
			tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
			downloader := newHeldTipDownloader(t, gloasFromGenesisConfig(), nil, tip, "tip-peer")
			downloader.SetCurrentSlotSampler(func() uint64 { return currentSlot })
			var rangeCalls atomic.Int32
			downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
				return []*cltypes.SignedBeaconBlock{}, fmt.Sprintf("range-peer-%d", rangeCalls.Add(1)), nil
			}
			var processed atomic.Bool
			downloader.SetProcessFunction(func(uint64, []*cltypes.SignedBeaconBlock, map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
				processed.Store(true)
				return 10, nil
			})

			require.True(t, requestMoreWithin(t, downloader))
			require.False(t, processed.Load())
			require.Equal(t, uint64(9), downloader.GetHighestProcessedSlot())
			require.Same(t, tip, downloader.gloasLookahead)
		})
	}
}

// The answer that brings the tip says nothing about the slots after it, so it does not report the tip.
func TestForwardBeaconDownloaderNeedsEmptyAnswerAfterTip(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	forwardBeaconRequestInterval = time.Millisecond
	t.Cleanup(func() { forwardBeaconRequestInterval = previousInterval })

	tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
	downloader := NewForwardBeaconDownloader(t.Context(), nil, gloasFromGenesisConfig())
	downloader.SetHighestProcessedSlot(9)
	downloader.SetCurrentSlotSampler(func() uint64 { return 11 })
	response := []*cltypes.SignedBeaconBlock{tip}
	downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		return response, "range-peer", nil
	}
	downloader.SetProcessFunction(func(highest uint64, _ []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		return highest, nil
	})

	require.False(t, requestMoreWithin(t, downloader))
	require.Same(t, tip, downloader.gloasLookahead)
	response = []*cltypes.SignedBeaconBlock{}
	require.True(t, requestMoreWithin(t, downloader))
}

// The scan already reached the current slot. One probe answers empty while another is still bringing the successor,
// so RequestMore must wait for that probe instead of reporting the tip.
func TestForwardBeaconDownloaderWaitsForProbeInFlightAfterEmptyAnswer(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	forwardBeaconRequestInterval = time.Millisecond
	t.Cleanup(func() { forwardBeaconRequestInterval = previousInterval })

	tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
	successor := makeGloasBlock(11, hash(0xbb), common.Hash{})
	linkBeaconBlocks(t, tip, successor)
	downloader := newHeldTipDownloader(t, gloasFromGenesisConfig(), nil, tip, "tip-peer")
	downloader.SetCurrentSlotSampler(func() uint64 { return 11 })
	emptyAnswered := make(chan struct{})
	var calls atomic.Int32
	downloader.requestBlocksByRange = func(ctx context.Context, _ uint64, _ uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		switch calls.Add(1) {
		case 1:
			select {
			case <-emptyAnswered:
			case <-ctx.Done():
			}
			time.Sleep(100 * time.Millisecond)
			return []*cltypes.SignedBeaconBlock{successor}, "slow-peer", nil
		case 2:
			close(emptyAnswered)
		}
		return []*cltypes.SignedBeaconBlock{}, "empty-peer", nil
	}
	var processedTip atomic.Bool
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		for _, block := range blocks {
			if block == tip {
				processedTip.Store(true)
				highest = max(highest, block.Block.Slot)
			}
		}
		return highest, nil
	})

	require.False(t, requestMoreWithin(t, downloader))
	require.True(t, processedTip.Load())
}

// No completed slot follows a tip in the current slot, so there is nothing to ask about.
func TestForwardBeaconDownloaderReportsGloasTipInCurrentSlot(t *testing.T) {
	tip := makeGloasBlock(11, hash(0xaa), common.Hash{})
	downloader := newHeldTipDownloader(t, gloasFromGenesisConfig(), nil, tip, "tip-peer")
	downloader.SetCurrentSlotSampler(func() uint64 { return 11 })
	downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		t.Error("requested a range after a tip in the current slot")
		return nil, "", errors.New("unexpected request")
	}

	require.True(t, requestMoreWithin(t, downloader))
}

// A tip that came from the beacon API gets its envelope from there, without waiting for P2P.
func TestForwardBeaconDownloaderImportsHeldGloasTipWithHTTPEnvelope(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	tip, envelope, root := makeFullGloasTipAndEnvelope(t, cfg, 10)
	envelopeBytes, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	client, sentinel := newNoPeersRPC(t, cfg)
	downloader := newHeldTipDownloader(t, cfg, client, tip, "http-fallback")
	downloader.SetHTTPFallbackURL(newEnvelopeServer(t, root, envelopeBytes).URL)
	var processedBlocks []*cltypes.SignedBeaconBlock
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processedBlocks = blocks
		require.Empty(t, envelopes)
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	var importedTip *cltypes.SignedBeaconBlock
	var importedRoot common.Hash
	var importedEnvelope *cltypes.SignedExecutionPayloadEnvelope
	require.True(t, downloader.ImportHeldGloasTip(t.Context(), time.Second, func(tip *cltypes.SignedBeaconBlock, root common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope, _ time.Time) bool {
		importedTip, importedRoot, importedEnvelope = tip, root, envelope
		return true
	}))
	require.Same(t, tip, importedTip)
	require.Equal(t, root, importedRoot)
	require.Zero(t, sentinel.calls.Load())
	require.Equal(t, []*cltypes.SignedBeaconBlock{tip}, processedBlocks)
	require.NotNil(t, importedEnvelope)
	processedEnvelopeBytes, err := importedEnvelope.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, envelopeBytes, processedEnvelopeBytes)
	require.Equal(t, uint64(10), downloader.GetHighestProcessedSlot())
}

func TestForwardBeaconDownloaderImportsHeldGloasTipWithoutEnvelope(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
	client, _ := newNoPeersRPC(t, cfg)
	downloader := newHeldTipDownloader(t, cfg, client, tip, "tip-peer")
	processed := 0
	downloader.SetProcessFunction(func(_ uint64, blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processed++
		require.Equal(t, []*cltypes.SignedBeaconBlock{tip}, blocks)
		require.Empty(t, envelopes)
		return 10, nil
	})

	require.True(t, downloader.ImportHeldGloasTip(t.Context(), 50*time.Millisecond, acceptAnyTip))
	require.Equal(t, 1, processed)
}

// P2P and the beacon API share one envelope budget.
func TestForwardBeaconDownloaderBoundsHeldGloasTipEnvelopeFetch(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	tip, _, root := makeFullGloasTipAndEnvelope(t, cfg, 10)
	client, _ := newNoPeersRPC(t, cfg)
	downloader := newHeldTipDownloader(t, cfg, client, tip, "tip-peer")
	downloader.SetHTTPFallbackURL(newEnvelopeServer(t, root, nil).URL)
	downloader.SetProcessFunction(func(_ uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	const budget = 200 * time.Millisecond
	start := time.Now()
	require.True(t, downloader.ImportHeldGloasTip(t.Context(), budget, acceptAnyTip))
	require.Less(t, time.Since(start), budget+budget/2)
}

// A tip that processing did not reach, or that fork choice does not know after processing, is kept for the next slot.
func TestForwardBeaconDownloaderRetriesHeldGloasTipInNextSlot(t *testing.T) {
	for name, test := range map[string]struct {
		processedSlot func(highest uint64) uint64
		accepted      bool
	}{
		"not reached":  {processedSlot: func(highest uint64) uint64 { return highest }, accepted: true},
		"not accepted": {processedSlot: func(uint64) uint64 { return 10 }},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := gloasFromGenesisConfig()
			tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
			client, _ := newNoPeersRPC(t, cfg)
			downloader := newHeldTipDownloader(t, cfg, client, tip, "tip-peer")
			var currentSlot atomic.Uint64
			currentSlot.Store(11)
			downloader.SetCurrentSlotSampler(currentSlot.Load)
			processed := 0
			downloader.SetProcessFunction(func(highest uint64, _ []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
				processed++
				return test.processedSlot(highest), nil
			})
			importTip := func() bool {
				return downloader.ImportHeldGloasTip(t.Context(), 50*time.Millisecond, func(*cltypes.SignedBeaconBlock, common.Hash, *cltypes.SignedExecutionPayloadEnvelope, time.Time) bool {
					return test.accepted
				})
			}

			require.False(t, importTip())
			require.False(t, importTip())
			require.Equal(t, 1, processed)
			currentSlot.Store(12)
			require.False(t, importTip())
			require.Equal(t, 2, processed)
			require.Equal(t, uint64(9), downloader.GetHighestProcessedSlot())
			require.Same(t, tip, downloader.gloasLookahead)
		})
	}
}

// A tip that processing rejected is dropped like a rejected batch: its peer is banned unless the error cannot be
// attributed to it.
func TestForwardBeaconDownloaderDropsRejectedHeldGloasTip(t *testing.T) {
	for name, test := range map[string]struct {
		err        error
		wantBanned []string
	}{
		"invalid block":    {err: errors.New("invalid block"), wantBanned: []string{"tip-peer"}},
		"invalid envelope": {err: fmt.Errorf("%w: invalid envelope", ErrUnattributableProcess)},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := gloasFromGenesisConfig()
			tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
			client, _ := newNoPeersRPC(t, cfg)
			downloader := newHeldTipDownloader(t, cfg, client, tip, "tip-peer")
			downloader.SetCurrentSlotSampler(func() uint64 { return 11 })
			var banned []string
			downloader.banPeer = func(peer string) { banned = append(banned, peer) }
			downloader.SetProcessFunction(func(highest uint64, _ []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
				return highest, test.err
			})

			require.False(t, downloader.ImportHeldGloasTip(t.Context(), 50*time.Millisecond, acceptAnyTip))
			require.Equal(t, test.wantBanned, banned)
			require.Nil(t, downloader.gloasLookahead)
			require.Equal(t, uint64(9), downloader.GetHighestProcessedSlot())
		})
	}
}

// With a cancelled stage context, processing fails with the context error, which says nothing about the tip's peer.
func TestForwardBeaconDownloaderKeepsHeldGloasTipWhenContextIsCancelled(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	tip := makeGloasBlock(11, hash(0xaa), common.Hash{})
	client, _ := newNoPeersRPC(t, cfg)
	downloader := newHeldTipDownloader(t, cfg, client, tip, "tip-peer")
	var banned []string
	downloader.banPeer = func(peer string) { banned = append(banned, peer) }
	processed := false
	downloader.SetProcessFunction(func(highest uint64, _ []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processed = true
		return highest, fmt.Errorf("failed to read signed header: %w", context.Canceled)
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.False(t, downloader.ImportHeldGloasTip(ctx, 50*time.Millisecond, acceptAnyTip))
	require.False(t, processed)
	require.Empty(t, banned)
	require.Same(t, tip, downloader.gloasLookahead)
}

func TestForwardBeaconDownloaderKeepsPeersWhenContextIsCancelledDuringRequestMore(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	forwardBeaconRequestInterval = time.Millisecond
	t.Cleanup(func() { forwardBeaconRequestInterval = previousInterval })

	for name, holdTip := range map[string]bool{"plain batch": false, "held tip": true} {
		t.Run(name, func(t *testing.T) {
			tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
			successor := makeGloasBlock(11, hash(0xbb), common.Hash{})
			grandchild := makeGloasBlock(12, hash(0xcc), common.Hash{})
			linkBeaconBlocks(t, tip, successor, grandchild)
			response := []*cltypes.SignedBeaconBlock{successor, grandchild}
			downloader := newHeldTipDownloader(t, gloasFromGenesisConfig(), nil, tip, "tip-peer")
			if !holdTip {
				downloader.clearGloasScan()
				response = []*cltypes.SignedBeaconBlock{tip, successor}
			}
			downloader.SetCurrentSlotSampler(func() uint64 { return 50 })
			downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
				return response, "range-peer", nil
			}
			var banned []string
			downloader.banPeer = func(peer string) { banned = append(banned, peer) }
			ctx, cancel := context.WithCancel(t.Context())
			var processed bool
			downloader.SetProcessFunction(func(highest uint64, _ []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
				processed = true
				cancel()
				return highest, fmt.Errorf("failed to read signed header: %w", context.Canceled)
			})

			downloader.RequestMore(ctx)
			require.True(t, processed)
			require.Empty(t, banned)
			if holdTip {
				require.Same(t, tip, downloader.gloasLookahead)
				require.Equal(t, "tip-peer", downloader.gloasLookaheadPeer)
			}
		})
	}
}

func TestForwardBeaconDownloaderKeepsHeldGloasTipWhenContextIsCancelledDuringProcessing(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	tip := makeGloasBlock(11, hash(0xaa), common.Hash{})
	client, _ := newNoPeersRPC(t, cfg)
	downloader := newHeldTipDownloader(t, cfg, client, tip, "tip-peer")
	downloader.SetCurrentSlotSampler(func() uint64 { return 12 })
	var banned []string
	downloader.banPeer = func(peer string) { banned = append(banned, peer) }
	ctx, cancel := context.WithCancel(t.Context())
	downloader.SetProcessFunction(func(highest uint64, _ []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		cancel()
		return highest, fmt.Errorf("failed to read signed header: %w", context.Canceled)
	})

	require.False(t, downloader.ImportHeldGloasTip(ctx, 50*time.Millisecond, acceptAnyTip))
	require.Empty(t, banned)
	require.Same(t, tip, downloader.gloasLookahead)
	require.Equal(t, "tip-peer", downloader.gloasLookaheadPeer)

	downloader.SetProcessFunction(func(_ uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		return blocks[len(blocks)-1].Block.Slot, nil
	})
	require.True(t, downloader.ImportHeldGloasTip(t.Context(), 50*time.Millisecond, acceptAnyTip), "the next run imports the tip in the same slot")
}

// When P2P does not serve the tip's envelope, the beacon API still gets part of the budget.
func TestForwardBeaconDownloaderFetchesHeldGloasTipEnvelopeFromHTTPAfterP2P(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	tip, envelope, root := makeFullGloasTipAndEnvelope(t, cfg, 10)
	envelopeBytes, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	client, sentinel := newNoPeersRPC(t, cfg)
	downloader := newHeldTipDownloader(t, cfg, client, tip, "tip-peer")
	downloader.SetHTTPFallbackURL(newEnvelopeServer(t, root, envelopeBytes).URL)
	downloader.SetProcessFunction(func(_ uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	var importedEnvelope *cltypes.SignedExecutionPayloadEnvelope
	require.True(t, downloader.ImportHeldGloasTip(t.Context(), 400*time.Millisecond, func(_ *cltypes.SignedBeaconBlock, _ common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope, _ time.Time) bool {
		importedEnvelope = envelope
		return true
	}))
	require.NotZero(t, sentinel.calls.Load())
	require.NotNil(t, importedEnvelope)
}

// In HTTP-preferred mode every empty beacon API answer waits like a P2P retry, so a tip that cannot be imported, or a
// gap that is scanned again and again, does not flood the beacon API. At the current slot the answer reports the tip.
func TestForwardBeaconDownloaderHTTPPreferredPacesEmptyAnswers(t *testing.T) {
	for _, test := range []struct {
		currentSlot uint64
		wantAtTip   bool
	}{{currentSlot: 12, wantAtTip: true}, {currentSlot: 100}} {
		t.Run(fmt.Sprint(test.currentSlot), func(t *testing.T) {
			tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
			server := httptest.NewServer(http.NotFoundHandler())
			t.Cleanup(server.Close)
			downloader := newHeldTipDownloader(t, gloasFromGenesisConfig(), nil, tip, "http-fallback")
			downloader.SetCurrentSlotSampler(func() uint64 { return test.currentSlot })
			downloader.SetHTTPFallbackURL(server.URL)
			downloader.httpPreferred.Store(true)

			start := time.Now()
			require.Equal(t, test.wantAtTip, requestMoreWithin(t, downloader))
			require.GreaterOrEqual(t, time.Since(start), forwardRequestRetryInterval)
			require.Greater(t, downloader.gloasNextUnscanned, uint64(11))
		})
	}
}

// The P2P requests fail, and the HTTP fallback answers empty for the slots after the tip.
func TestForwardBeaconDownloaderHTTPFallbackReportsGloasTip(t *testing.T) {
	previousInterval, previousDelay := forwardBeaconRequestInterval, forwardBeaconFallbackDelay
	forwardBeaconRequestInterval, forwardBeaconFallbackDelay = time.Millisecond, time.Millisecond
	t.Cleanup(func() { forwardBeaconRequestInterval, forwardBeaconFallbackDelay = previousInterval, previousDelay })

	tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)
	downloader := newHeldTipDownloader(t, gloasFromGenesisConfig(), nil, tip, "tip-peer")
	downloader.SetCurrentSlotSampler(func() uint64 { return 12 })
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		return nil, "", errors.New("no peers")
	}

	require.True(t, requestMoreWithin(t, downloader))
}

// The scan reached the current slot and one probe answered empty while another is still running, so the beacon API
// fallback must not start a request that RequestMore would then wait for.
func TestForwardBeaconDownloaderStartsNoHTTPFallbackAfterEmptyAnswerAtCurrentSlot(t *testing.T) {
	previousInterval, previousDelay := forwardBeaconRequestInterval, forwardBeaconFallbackDelay
	forwardBeaconRequestInterval, forwardBeaconFallbackDelay = time.Millisecond, 150*time.Millisecond
	t.Cleanup(func() { forwardBeaconRequestInterval, forwardBeaconFallbackDelay = previousInterval, previousDelay })

	var httpCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpCalls.Add(1)
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
	downloader := newHeldTipDownloader(t, gloasFromGenesisConfig(), nil, tip, "tip-peer")
	downloader.SetCurrentSlotSampler(func() uint64 { return 11 })
	downloader.SetHTTPFallbackURL(server.URL)
	var calls atomic.Int32
	downloader.requestBlocksByRange = func(ctx context.Context, _ uint64, _ uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		if calls.Add(1) == 1 {
			select {
			case <-ctx.Done():
			case <-time.After(500 * time.Millisecond):
			}
		}
		return []*cltypes.SignedBeaconBlock{}, "range-peer", nil
	}

	require.True(t, requestMoreWithin(t, downloader))
	require.Zero(t, httpCalls.Load(), "HTTP fallback started after the empty answer at the current slot")
}

func TestForwardBeaconDownloaderReportsNoTipWithoutGloasLookahead(t *testing.T) {
	previousInterval, previousTimeout := forwardBeaconRequestInterval, forwardBeaconRequestTimeout
	forwardBeaconRequestInterval, forwardBeaconRequestTimeout = time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() { forwardBeaconRequestInterval, forwardBeaconRequestTimeout = previousInterval, previousTimeout })

	downloader := NewForwardBeaconDownloader(t.Context(), nil, &clparams.MainnetBeaconConfig)
	downloader.SetHighestProcessedSlot(9)
	downloader.SetCurrentSlotSampler(func() uint64 { return 12 })
	downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		return []*cltypes.SignedBeaconBlock{}, "range-peer", nil
	}

	require.False(t, requestMoreWithin(t, downloader))
}

// A beacon API batch asked through the current slot; its block can appear later, so the scan asks for that slot
// again even after the clock moved on.
func TestForwardBeaconDownloaderRescansSlotInProgressAtHTTPRequest(t *testing.T) {
	first := makeGloasBlock(9, hash(0x09), hash(0x01))
	tip := makeGloasBlock(10, hash(0x10), hash(0x02))
	successor := makeGloasBlock(12, hash(0x12), hash(0x03))
	linkBeaconBlocks(t, first, tip, successor)
	encoded := map[uint64][]byte{}
	for _, block := range []*cltypes.SignedBeaconBlock{first, tip, successor} {
		data, err := block.EncodeSSZ(nil)
		require.NoError(t, err)
		encoded[block.Block.Slot] = data
	}
	var successorPublished atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var slot uint64
		_, _ = fmt.Sscanf(r.URL.Path, "/eth/v2/beacon/blocks/%d", &slot)
		if data, ok := encoded[slot]; ok && (slot != successor.Block.Slot || successorPublished.Load()) {
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(data)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)

	var currentSlot atomic.Uint64
	currentSlot.Store(12)
	downloader := NewForwardBeaconDownloader(t.Context(), nil, gloasFromGenesisConfig())
	downloader.SetHighestProcessedSlot(8)
	downloader.SetCurrentSlotSampler(currentSlot.Load)
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.httpPreferred.Store(true)
	var processedSlots []uint64
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		for _, block := range blocks {
			processedSlots = append(processedSlots, block.Block.Slot)
			highest = block.Block.Slot
		}
		currentSlot.Store(14)
		successorPublished.Store(true)
		return highest, nil
	})

	require.False(t, requestMoreWithin(t, downloader))
	require.Equal(t, tip.Block.Slot, downloader.gloasLookahead.Block.Slot)
	require.False(t, requestMoreWithin(t, downloader))
	require.Equal(t, []uint64{9, 10}, processedSlots)
	require.Equal(t, successor.Block.Slot, downloader.gloasLookahead.Block.Slot)
}

// A block held back for its missing envelope after its successor arrived is not imported as a tip; the batch path imports it.
func TestForwardBeaconDownloaderKeepsFullFrontierAfterEmptyAnswer(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	empty := makeGloasBlock(9, hash(0xa0), common.Hash{})
	full, envelope, _ := makeFullGloasTipAndEnvelope(t, cfg, 10)
	linkBeaconBlocks(t, empty, full)
	fullRoot, err := full.Block.HashSSZ()
	require.NoError(t, err)
	envelope.Message.BeaconBlockRoot = fullRoot
	envelopeBytes, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	successor := makeGloasBlock(11, hash(0xcc), full.Block.Body.SignedExecutionPayloadBid.Message.BlockHash)
	linkBeaconBlocks(t, full, successor)
	encoded := map[string][]byte{}
	for _, block := range []*cltypes.SignedBeaconBlock{empty, full, successor} {
		b, err := block.EncodeSSZ(nil)
		require.NoError(t, err)
		encoded[fmt.Sprintf("/eth/v2/beacon/blocks/%d", block.Block.Slot)] = b
	}
	envelopePath := fmt.Sprintf("/eth/v1/beacon/execution_payload_envelopes/0x%x", fullRoot)
	var serveEnvelope atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, ok := encoded[r.URL.Path]
		if r.URL.Path == envelopePath && serveEnvelope.Load() {
			b, ok = envelopeBytes, true
		}
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(b)
	}))
	t.Cleanup(server.Close)

	downloader := NewForwardBeaconDownloader(t.Context(), nil, cfg)
	downloader.SetHighestProcessedSlot(8)
	downloader.SetCurrentSlotSampler(func() uint64 { return 13 })
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.httpPreferred.Store(true)
	var processed []uint64
	var processedEnvelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		for _, block := range blocks {
			processed = append(processed, block.Block.Slot)
			highest = block.Block.Slot
		}
		if len(blocks) > 0 {
			processedEnvelopes = envelopes
		}
		return highest, nil
	})

	require.False(t, requestMoreWithin(t, downloader))
	require.Equal(t, []uint64{9}, processed)
	require.Equal(t, full.Block.Slot, downloader.gloasLookahead.Block.Slot)

	downloader.httpPreferred.Store(false)
	answerEmpty := func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		return []*cltypes.SignedBeaconBlock{}, "lagging-peer", nil
	}
	downloader.requestBlocksByRange = answerEmpty
	require.True(t, requestMoreWithin(t, downloader))
	require.False(t, downloader.ImportHeldGloasTip(t.Context(), 50*time.Millisecond, acceptAnyTip))

	downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		return []*cltypes.SignedBeaconBlock{full}, "duplicate-peer", nil
	}
	require.False(t, requestMoreWithin(t, downloader))
	downloader.requestBlocksByRange = answerEmpty
	require.True(t, requestMoreWithin(t, downloader))
	require.False(t, downloader.ImportHeldGloasTip(t.Context(), 50*time.Millisecond, acceptAnyTip))
	require.Equal(t, []uint64{9}, processed)
	require.Equal(t, uint64(9), downloader.GetHighestProcessedSlot())

	serveEnvelope.Store(true)
	downloader.httpPreferred.Store(true)
	require.False(t, requestMoreWithin(t, downloader))
	require.Equal(t, []uint64{9, 10}, processed)
	require.NotNil(t, processedEnvelopes[fullRoot])

	require.True(t, requestMoreWithin(t, downloader))
	require.True(t, downloader.ImportHeldGloasTip(t.Context(), 50*time.Millisecond, acceptAnyTip))
	require.Equal(t, []uint64{9, 10, 11}, processed)
}

// A held block whose successor was downloaded is never imported as a tip, however its successor was dropped.
func TestForwardBeaconDownloaderDoesNotImportTipAfterSuccessorWasDropped(t *testing.T) {
	const (
		heldSlot    = 10
		currentSlot = 13
	)
	tests := []struct {
		name string
		// drop runs while the block is held and leaves it not held; nil means the first download already did.
		drop            func(t *testing.T, d *ForwardBeaconDownloader, failNextProcess *atomic.Bool)
		parentProcessed uint64
	}{
		{
			name:            "successor re-sent while the envelope is missing",
			parentProcessed: 8,
			drop: func(t *testing.T, d *ForwardBeaconDownloader, failNextProcess *atomic.Bool) {
				downloadFromHTTP(t, d)
			},
		},
		{
			name:            "no processable prefix on first observation",
			parentProcessed: 9,
		},
		{
			name:            "disconnected retry while held",
			parentProcessed: 8,
			drop: func(t *testing.T, d *ForwardBeaconDownloader, failNextProcess *atomic.Bool) {
				other := makeGloasBlock(heldSlot-1, hash(0xdd), common.Hash{})
				other.Block.ParentRoot = hash(0xee)
				downloadFromPeer(t, d, other)
			},
		},
		{
			name:            "process error while held",
			parentProcessed: 8,
			drop: func(t *testing.T, d *ForwardBeaconDownloader, failNextProcess *atomic.Bool) {
				failNextProcess.Store(true)
				downloadFromHTTP(t, d)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := gloasFromGenesisConfig()
			a := makeGloasBlock(heldSlot-1, hash(0xa0), common.Hash{})
			b := makeGloasBlock(heldSlot, hash(0xb0), common.Hash{})
			c := makeGloasBlock(heldSlot+1, hash(0xc0), hash(0xb0))
			linkBeaconBlocks(t, a, b, c)
			server := newBlocksServer(t, a, b, c)

			var failNextProcess atomic.Bool
			client, _ := newNoPeersRPC(t, cfg)
			downloader := NewForwardBeaconDownloader(t.Context(), client, cfg)
			downloader.SetHighestProcessedSlot(test.parentProcessed)
			downloader.SetCurrentSlotSampler(func() uint64 { return currentSlot })
			downloader.SetHTTPFallbackURL(server.URL)
			var processedHeld bool
			downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
				if failNextProcess.CompareAndSwap(true, false) {
					return highest, errors.New("process failed")
				}
				for _, block := range blocks {
					processedHeld = processedHeld || block.Block.Slot == heldSlot
					highest = block.Block.Slot
				}
				return highest, nil
			})

			downloadFromHTTP(t, downloader)
			if test.drop != nil {
				require.NotNil(t, downloader.gloasLookahead)
				require.Equal(t, uint64(heldSlot), downloader.gloasLookahead.Block.Slot)
				test.drop(t, downloader, &failNextProcess)
			}
			require.Nil(t, downloader.gloasLookahead)

			downloadFromPeer(t, downloader, b)
			require.Equal(t, uint64(heldSlot), downloader.gloasLookahead.Block.Slot)
			downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
				return []*cltypes.SignedBeaconBlock{}, "lagging-peer", nil
			}
			require.True(t, requestMoreWithin(t, downloader))
			require.False(t, downloader.ImportHeldGloasTip(t.Context(), 50*time.Millisecond, acceptAnyTip))
			require.False(t, processedHeld)
			require.Equal(t, uint64(heldSlot-1), downloader.GetHighestProcessedSlot())
		})
	}
}

func downloadFromHTTP(t *testing.T, d *ForwardBeaconDownloader) {
	t.Helper()
	d.httpPreferred.Store(true)
	require.False(t, requestMoreWithin(t, d))
}

func downloadFromPeer(t *testing.T, d *ForwardBeaconDownloader, blocks ...*cltypes.SignedBeaconBlock) {
	t.Helper()
	d.httpPreferred.Store(false)
	d.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		return blocks, "peer", nil
	}
	require.False(t, requestMoreWithin(t, d))
}

func newBlocksServer(t *testing.T, blocks ...*cltypes.SignedBeaconBlock) *httptest.Server {
	t.Helper()
	encoded := map[string][]byte{}
	for _, block := range blocks {
		bytes, err := block.EncodeSSZ(nil)
		require.NoError(t, err)
		encoded[fmt.Sprintf("/eth/v2/beacon/blocks/%d", block.Block.Slot)] = bytes
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bytes, ok := encoded[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(bytes)
	}))
	t.Cleanup(server.Close)
	return server
}

func answerEmpty(t *testing.T, d *ForwardBeaconDownloader) {
	t.Helper()
	d.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		return []*cltypes.SignedBeaconBlock{}, "lagging-peer", nil
	}
	require.True(t, requestMoreWithin(t, d))
}

// A higher block that does not descend from the held block is no successor, so the held block is still a tip.
func TestForwardBeaconDownloaderImportsTipAfterUnrelatedHigherBlock(t *testing.T) {
	for _, heldWhenUnrelatedArrives := range []bool{true, false} {
		name := "before the block is held"
		if heldWhenUnrelatedArrives {
			name = "while the block is held"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				a := makeGloasBlock(9, hash(0xa0), common.Hash{})
				b := makeGloasBlock(10, hash(0xb0), common.Hash{})
				linkBeaconBlocks(t, a, b)
				unrelated := makeGloasBlock(12, hash(0xdd), common.Hash{})
				unrelated.Block.ParentRoot = hash(0xee)

				cfg := gloasFromGenesisConfig()
				client, _ := newNoPeersRPC(t, cfg)
				downloader := NewForwardBeaconDownloader(t.Context(), client, cfg)
				downloader.SetHighestProcessedSlot(9)
				downloader.SetCurrentSlotSampler(func() uint64 { return 13 })
				var processedHeld bool
				downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
					for _, block := range blocks {
						processedHeld = processedHeld || block == b
						highest = block.Block.Slot
					}
					return highest, nil
				})

				if heldWhenUnrelatedArrives {
					downloadFromPeer(t, downloader, b)
					require.Same(t, b, downloader.gloasLookahead)
				}
				downloadFromPeer(t, downloader, unrelated)
				if !heldWhenUnrelatedArrives {
					require.Same(t, unrelated, downloader.gloasLookahead)
				}
				downloadFromPeer(t, downloader, b)
				if !heldWhenUnrelatedArrives {
					downloadFromPeer(t, downloader, b)
				}
				require.Same(t, b, downloader.gloasLookahead)
				answerEmpty(t, downloader)
				require.True(t, downloader.ImportHeldGloasTip(t.Context(), 50*time.Millisecond, acceptAnyTip))
				require.True(t, processedHeld)
			})
		})
	}
}

// An unvalidated block naming the held block as parent must not displace the real child's record.
func TestForwardBeaconDownloaderKeepsChildRecordAfterForgedChildAtLowSlot(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	a := makeGloasBlock(9, hash(0xa0), common.Hash{})
	b := makeGloasBlock(10, hash(0xb0), common.Hash{})
	c := makeGloasBlock(11, hash(0xc0), hash(0xb0))
	linkBeaconBlocks(t, a, b, c)
	rootB, err := b.Block.HashSSZ()
	require.NoError(t, err)
	forged := makeGloasBlock(9, hash(0xdd), common.Hash{})
	forged.Block.ParentRoot = rootB
	server := newBlocksServer(t, a, b, c)

	client, _ := newNoPeersRPC(t, cfg)
	downloader := NewForwardBeaconDownloader(t.Context(), client, cfg)
	downloader.SetHighestProcessedSlot(8)
	downloader.SetCurrentSlotSampler(func() uint64 { return 13 })
	downloader.SetHTTPFallbackURL(server.URL)
	var processedHeld bool
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		for _, block := range blocks {
			processedHeld = processedHeld || block.Block.Slot == 10
			highest = block.Block.Slot
		}
		return highest, nil
	})

	downloadFromHTTP(t, downloader)
	require.Equal(t, uint64(10), downloader.gloasLookahead.Block.Slot)
	downloadFromPeer(t, downloader, forged)
	require.Nil(t, downloader.gloasLookahead)
	downloadFromPeer(t, downloader, b)
	require.Equal(t, uint64(10), downloader.gloasLookahead.Block.Slot)
	answerEmpty(t, downloader)
	require.False(t, downloader.ImportHeldGloasTip(t.Context(), 50*time.Millisecond, acceptAnyTip))
	require.False(t, processedHeld)
}

// Every block of a response marks its parent as having a downloaded child, not only the last block.
func TestForwardBeaconDownloaderRecordsParentOfEveryDownloadedBlock(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	a := makeGloasBlock(9, hash(0xa0), common.Hash{})
	b := makeGloasBlock(10, hash(0xb0), common.Hash{})
	c := makeGloasBlock(11, hash(0xc0), hash(0xb0))
	d := makeGloasBlock(12, hash(0xd0), hash(0xc0))
	linkBeaconBlocks(t, a, b, c, d)
	other := makeGloasBlock(9, hash(0xdd), common.Hash{})
	other.Block.ParentRoot = hash(0xee)
	server := newBlocksServer(t, a, b, c, d)

	client, _ := newNoPeersRPC(t, cfg)
	downloader := NewForwardBeaconDownloader(t.Context(), client, cfg)
	downloader.SetHighestProcessedSlot(8)
	downloader.SetCurrentSlotSampler(func() uint64 { return 14 })
	downloader.SetHTTPFallbackURL(server.URL)
	var processedHeld bool
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		for _, block := range blocks {
			processedHeld = processedHeld || block.Block.Slot == 10
			highest = block.Block.Slot
		}
		return highest, nil
	})

	downloadFromHTTP(t, downloader)
	require.Equal(t, uint64(10), downloader.gloasLookahead.Block.Slot)
	downloadFromPeer(t, downloader, other)
	require.Nil(t, downloader.gloasLookahead)
	downloadFromPeer(t, downloader, b)
	require.Equal(t, uint64(10), downloader.gloasLookahead.Block.Slot)
	answerEmpty(t, downloader)
	require.False(t, downloader.ImportHeldGloasTip(t.Context(), 50*time.Millisecond, acceptAnyTip))
	require.False(t, processedHeld)
}

// A child outside the requested range that decides the held block is FULL also counts as a downloaded child.
func TestForwardBeaconDownloaderRecordsChildOutsideRequestedRange(t *testing.T) {
	previousExpiration := requestEnvelopeBatchExpiration
	requestEnvelopeBatchExpiration = 10 * time.Millisecond
	t.Cleanup(func() { requestEnvelopeBatchExpiration = previousExpiration })
	cfg := gloasFromGenesisConfig()
	a := makeGloasBlock(9, hash(0xa0), common.Hash{})
	b := makeGloasBlock(10, hash(0xb0), common.Hash{})
	c := makeGloasBlock(30, hash(0xc0), hash(0xb0))
	linkBeaconBlocks(t, a, b, c)
	other := makeGloasBlock(9, hash(0xdd), common.Hash{})
	other.Block.ParentRoot = hash(0xee)

	client, _ := newNoPeersRPC(t, cfg)
	downloader := NewForwardBeaconDownloader(t.Context(), client, cfg)
	downloader.SetHighestProcessedSlot(8)
	downloader.SetCurrentSlotSampler(func() uint64 { return 14 })
	var processedHeld bool
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		for _, block := range blocks {
			processedHeld = processedHeld || block.Block.Slot == 10
			highest = block.Block.Slot
		}
		return highest, nil
	})

	downloadFromPeer(t, downloader, a, b, c)
	require.Equal(t, uint64(10), downloader.gloasLookahead.Block.Slot)
	downloadFromPeer(t, downloader, other)
	require.Nil(t, downloader.gloasLookahead)
	downloadFromPeer(t, downloader, b)
	require.Equal(t, uint64(10), downloader.gloasLookahead.Block.Slot)
	answerEmpty(t, downloader)
	require.False(t, downloader.ImportHeldGloasTip(t.Context(), 50*time.Millisecond, acceptAnyTip))
	require.False(t, processedHeld)
}

// Only the blocks that one response can use are recorded as children, so a peer cannot grow the record without bound.
func TestForwardBeaconDownloaderRecordsChildrenOnlyOfUsableBlocks(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	blocks := make([]*cltypes.SignedBeaconBlock, 0, 100)
	for slot := uint64(9); slot < 109; slot++ {
		blocks = append(blocks, makeGloasBlock(slot, hash(byte(slot)), common.Hash{}))
	}
	linkBeaconBlocks(t, blocks...)
	client, _ := newNoPeersRPC(t, cfg)
	downloader := NewForwardBeaconDownloader(t.Context(), client, cfg)
	downloader.SetHighestProcessedSlot(8)
	downloader.SetCurrentSlotSampler(func() uint64 { return 13 })
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		for _, block := range blocks {
			highest = block.Block.Slot
		}
		return highest, nil
	})
	downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		return blocks, "peer", nil
	}

	requestMoreWithin(t, downloader)
	require.LessOrEqual(t, len(downloader.gloasDownloadedParents), 34)
}

// In a response longer than a batch, the block after the held block is its downloaded child.
func TestForwardBeaconDownloaderRecordsChildOfHeldBlockInLongResponse(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	blocks := make([]*cltypes.SignedBeaconBlock, 0, 42)
	for slot := uint64(9); slot < 51; slot++ {
		blocks = append(blocks, makeGloasBlock(slot, hash(byte(slot)), common.Hash{}))
	}
	linkBeaconBlocks(t, blocks...)
	held := blocks[32]
	other := makeGloasBlock(9, hash(0xdd), common.Hash{})
	other.Block.ParentRoot = hash(0xee)

	client, _ := newNoPeersRPC(t, cfg)
	downloader := NewForwardBeaconDownloader(t.Context(), client, cfg)
	downloader.SetHighestProcessedSlot(8)
	downloader.SetCurrentSlotSampler(func() uint64 { return 60 })
	var processedHeld bool
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		for _, block := range blocks {
			processedHeld = processedHeld || block == held
			highest = block.Block.Slot
		}
		return highest, nil
	})

	downloadFromPeer(t, downloader, blocks...)
	require.Same(t, held, downloader.gloasLookahead)
	downloadFromPeer(t, downloader, other)
	require.Nil(t, downloader.gloasLookahead)
	downloadFromPeer(t, downloader, held)
	require.Same(t, held, downloader.gloasLookahead)
	answerEmpty(t, downloader)
	require.False(t, downloader.ImportHeldGloasTip(t.Context(), 50*time.Millisecond, acceptAnyTip))
	require.False(t, processedHeld)
}

// Neither rejected responses nor far-future children of processed blocks accumulate in the downloaded-child record.
func TestForwardBeaconDownloaderKeepsDownloadedParentRecordBounded(t *testing.T) {
	newDownloader := func(t *testing.T) *ForwardBeaconDownloader {
		cfg := gloasFromGenesisConfig()
		client, _ := newNoPeersRPC(t, cfg)
		downloader := NewForwardBeaconDownloader(t.Context(), client, cfg)
		downloader.SetHighestProcessedSlot(8)
		downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
			for _, block := range blocks {
				highest = block.Block.Slot
			}
			return highest, nil
		})
		return downloader
	}
	farBlock := func(i int) *cltypes.SignedBeaconBlock {
		block := makeGloasBlock(1000+uint64(i), hash(0xdd), common.Hash{})
		block.Block.ParentRoot = hash(byte(i))
		return block
	}

	t.Run("rejected responses", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			downloader := newDownloader(t)
			parent := makeGloasBlock(9, hash(0xa0), common.Hash{})
			held := makeGloasBlock(10, hash(0xb0), common.Hash{})
			linkBeaconBlocks(t, parent, held)
			downloadFromPeer(t, downloader, parent, held)
			for i := range 5 {
				require.Same(t, held, downloader.gloasLookahead)
				downloadFromPeer(t, downloader, farBlock(i))
				require.Nil(t, downloader.gloasLookahead)
				downloadFromPeer(t, downloader, held)
			}
			require.LessOrEqual(t, len(downloader.gloasDownloadedParents), 2)
		})
	})

	t.Run("far children of processed blocks", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			downloader := newDownloader(t)
			var previous *cltypes.SignedBeaconBlock
			for i := range 5 {
				block := makeGloasBlock(9+uint64(i), hash(byte(0x80+i)), common.Hash{})
				child := makeGloasBlock(2000+uint64(i), hash(0xcc), common.Hash{})
				if previous != nil {
					linkBeaconBlocks(t, previous, block)
				}
				linkBeaconBlocks(t, block, child)
				downloadFromPeer(t, downloader, block, child)
				require.Same(t, child, downloader.gloasLookahead)
				downloadFromPeer(t, downloader, farBlock(i))
				require.Nil(t, downloader.gloasLookahead)
				previous = block
			}
			require.Equal(t, uint64(13), downloader.GetHighestProcessedSlot())
			require.LessOrEqual(t, len(downloader.gloasDownloadedParents), 2)
		})
	})

	t.Run("accepted but unprocessed responses", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			downloader := newDownloader(t)
			downloader.gloasDownloadedParents = map[common.Hash]uint64{}
			for i := range maxGloasDownloadedParents {
				downloader.gloasDownloadedParents[common.Hash{0xee, byte(i), byte(i >> 8)}] = 5000
			}
			parent := makeGloasBlock(9, hash(0xa0), common.Hash{})
			held := makeGloasBlock(10, hash(0xb0), common.Hash{})
			linkBeaconBlocks(t, parent, held)
			downloadFromPeer(t, downloader, parent, held)
			require.Same(t, held, downloader.gloasLookahead)
			require.LessOrEqual(t, len(downloader.gloasDownloadedParents), 2)
			require.Contains(t, downloader.gloasDownloadedParents, held.Block.ParentRoot)
		})
	})
}
