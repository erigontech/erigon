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
	"bytes"
	"context"
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
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type invalidEnvelopeSentinel struct {
	sentinelproto.SentinelClient
	response []byte
	calls    atomic.Int32
}

func (s *invalidEnvelopeSentinel) SendRequest(context.Context, *sentinelproto.RequestData, ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	s.calls.Add(1)
	return &sentinelproto.ResponseData{Data: s.response, Peer: &sentinelproto.Peer{Pid: "envelope-peer"}}, nil
}

func (s *invalidEnvelopeSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{}, nil
}

func TestForwardInvalidEnvelopeUsesAvailableFallback(t *testing.T) {
	for _, httpAvailable := range []bool{false, true} {
		t.Run(fmt.Sprint(httpAvailable), func(t *testing.T) {
			cfg := gloasFromGenesisConfig()
			cfg.InitializeForkSchedule()
			clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, cfg)
			digest, err := clock.ComputeForkDigest(0)
			require.NoError(t, err)
			first := makeGloasBlock(10, common.Hash{}, common.Hash{})
			bid := first.Block.Body.SignedExecutionPayloadBid.Message
			bid.Slot = 10
			good := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
			good.Message.Payload.SlotNumber = 10
			_, err = good.Message.Payload.EncodeSSZ(nil)
			require.NoError(t, err)
			bid.ExecutionRequestsRoot, err = good.Message.ExecutionRequests.HashSSZ()
			require.NoError(t, err)
			requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(cfg, good.Message.ExecutionRequests))
			good.Message.Payload.BlockHash = common.HexToHash("0xefd641f283ac9ad546ad48be6207db401e3f70fe94adc9a10d6c858d845b67e3")
			header, err := good.Message.Payload.RlpHeader(&good.Message.ParentBeaconBlockRoot, requestsHash, nil)
			require.NoError(t, err)
			good.Message.Payload.BlockHash = header.Hash()
			bid.BlockHash = header.Hash()
			root, err := first.Block.HashSSZ()
			require.NoError(t, err)
			good.Message.BeaconBlockRoot = root
			require.NoError(t, ValidateDownloadedGloasEnvelope(cfg, first, good))
			goodBytes, err := good.EncodeSSZ(nil)
			require.NoError(t, err)
			bad := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
			require.NoError(t, bad.DecodeSSZ(goodBytes, int(clparams.GloasVersion)))
			bad.Message.Payload.BlockHash = hash(0xff)
			require.ErrorContains(t, ValidateDownloadedGloasEnvelope(cfg, first, bad), "block hash mismatch")
			var wire bytes.Buffer
			require.NoError(t, ssz_snappy.EncodeAndWrite(&wire, bad, digest[:]...))
			sentinel := &invalidEnvelopeSentinel{response: wire.Bytes()}
			client := rpc.NewBeaconRpcP2P(t.Context(), sentinel, cfg, clock, nil)
			second := makeGloasBlock(11, hash(2), bid.BlockHash)
			linkBeaconBlocks(t, first, second)
			var httpCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				httpCalls.Add(1)
				w.Header().Set("Eth-Consensus-Version", "gloas")
				_, _ = w.Write(goodBytes)
			}))
			defer server.Close()
			probe := make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope)
			httpResult := fetchEnvelopesFromBeaconAPI(t.Context(), server.URL, []*cltypes.SignedBeaconBlock{first}, [][32]byte{root}, probe, cfg)
			require.Equal(t, 1, httpResult.fetched)
			require.NoError(t, ValidateDownloadedGloasEnvelope(cfg, first, probe[root]))
			httpCalls.Store(0)
			downloader := NewForwardBeaconDownloader(t.Context(), client, cfg)
			downloader.SetHighestProcessedSlot(9)
			downloader.SetCurrentSlotSampler(func() uint64 { return 12 })
			if httpAvailable {
				downloader.SetHTTPFallbackURL(server.URL)
			}
			downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
				return []*cltypes.SignedBeaconBlock{first, second}, "block-peer", nil
			}
			var rejected int
			downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
				if len(blocks) == 0 {
					return highest, nil
				}
				if err := ValidateDownloadedGloasEnvelope(cfg, first, envelopes[root]); err != nil {
					rejected++
					return highest, fmt.Errorf("%w: %w", ErrUnattributableProcess, err)
				}
				return first.Block.Slot, nil
			})
			for range 2 {
				downloader.RequestMore(t.Context())
			}
			t.Logf("P2P envelope calls=%d invalid callbacks=%d HTTP calls=%d highest=%d", sentinel.calls.Load(), rejected, httpCalls.Load(), downloader.GetHighestProcessedSlot())
			require.Equal(t, int32(2), sentinel.calls.Load())
			require.Zero(t, rejected)
			if httpAvailable {
				require.Equal(t, int32(2), httpCalls.Load())
				require.Equal(t, uint64(10), downloader.GetHighestProcessedSlot())
			} else {
				require.Zero(t, httpCalls.Load())
				require.Equal(t, uint64(9), downloader.GetHighestProcessedSlot())
			}
		})
	}
}
