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
}

func (noPeersSentinel) SendRequest(context.Context, *sentinelproto.RequestData, ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	return nil, errors.New("no peers")
}

func (noPeersSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{}, nil
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

// The tip is held at 10 and every range after it is empty: RequestMore returns once the scan reached the current
// slot, without processing the tip and without waiting for its request timeout.
func TestForwardBeaconDownloaderReturnsAtScannedGloasTip(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	forwardBeaconRequestInterval = time.Millisecond
	t.Cleanup(func() { forwardBeaconRequestInterval = previousInterval })

	for _, currentSlot := range []uint64{11, 12, 50} {
		t.Run(fmt.Sprint(currentSlot), func(t *testing.T) {
			tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
			downloader := NewForwardBeaconDownloader(t.Context(), nil, gloasFromGenesisConfig())
			downloader.SetHighestProcessedSlot(9)
			downloader.gloasLookahead = tip
			downloader.gloasLookaheadPeer = "tip-peer"
			downloader.gloasNextUnscanned = 11
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

			done := make(chan struct{})
			go func() {
				defer close(done)
				downloader.RequestMore(t.Context())
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("RequestMore did not return after the scan reached the current slot")
			}
			require.False(t, processed.Load())
			require.Equal(t, uint64(9), downloader.GetHighestProcessedSlot())
			require.Same(t, tip, downloader.gloasLookahead)
			require.GreaterOrEqual(t, downloader.gloasNextUnscanned, currentSlot)
		})
	}
}

// A node without peers gets the tip's envelope from the beacon API.
func TestForwardBeaconDownloaderImportsHeldGloasTipWithHTTPEnvelope(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, cfg)
	tip, envelope, root := makeFullGloasTipAndEnvelope(t, cfg, 10)
	envelopeBytes, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != fmt.Sprintf("/eth/v1/beacon/execution_payload_envelopes/0x%x", root) {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(envelopeBytes)
	}))
	t.Cleanup(server.Close)

	downloader := NewForwardBeaconDownloader(t.Context(), rpc.NewBeaconRpcP2P(t.Context(), noPeersSentinel{}, cfg, clock), cfg)
	downloader.SetHighestProcessedSlot(9)
	downloader.gloasLookahead = tip
	downloader.gloasLookaheadPeer = "http-fallback"
	downloader.gloasNextUnscanned = 11
	downloader.SetHTTPFallbackURL(server.URL)
	var processedBlocks []*cltypes.SignedBeaconBlock
	var processedEnvelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processedBlocks, processedEnvelopes = blocks, envelopes
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	require.True(t, downloader.ImportHeldGloasTip(t.Context(), 100*time.Millisecond))
	require.Equal(t, []*cltypes.SignedBeaconBlock{tip}, processedBlocks)
	require.NotNil(t, processedEnvelopes[root])
	processedEnvelopeBytes, err := processedEnvelopes[root].EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, envelopeBytes, processedEnvelopeBytes)
	require.Equal(t, uint64(10), downloader.GetHighestProcessedSlot())
}

// An EMPTY tip has no envelope: it is imported without one once the envelope budget ends.
func TestForwardBeaconDownloaderImportsHeldGloasTipWithoutEnvelope(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, cfg)
	tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
	downloader := NewForwardBeaconDownloader(t.Context(), rpc.NewBeaconRpcP2P(t.Context(), noPeersSentinel{}, cfg, clock), cfg)
	downloader.SetHighestProcessedSlot(9)
	downloader.gloasLookahead = tip
	downloader.gloasLookaheadPeer = "tip-peer"
	downloader.gloasNextUnscanned = 11
	processed := 0
	downloader.SetProcessFunction(func(_ uint64, blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processed++
		require.Equal(t, []*cltypes.SignedBeaconBlock{tip}, blocks)
		require.Empty(t, envelopes)
		return 10, nil
	})

	start := time.Now()
	require.True(t, downloader.ImportHeldGloasTip(t.Context(), 50*time.Millisecond))
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, 1, processed)
}

// A tip that processing did not reach, for example because its parent state is missing, is not reported as imported.
func TestForwardBeaconDownloaderReportsHeldGloasTipNotImported(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
	downloader := NewForwardBeaconDownloader(t.Context(), nil, cfg)
	downloader.SetHighestProcessedSlot(9)
	downloader.gloasLookahead = tip
	downloader.gloasLookaheadPeer = "tip-peer"
	downloader.gloasNextUnscanned = 11
	downloader.SetProcessFunction(func(highest uint64, _ []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		return highest, nil
	})

	require.False(t, downloader.ImportHeldGloasTip(t.Context(), 50*time.Millisecond))
	require.Equal(t, uint64(9), downloader.GetHighestProcessedSlot())
	require.Same(t, tip, downloader.gloasLookahead)
}
