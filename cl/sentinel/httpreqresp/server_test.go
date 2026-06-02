// Copyright 2024 The Erigon Authors
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

package httpreqresp

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/sentinel/communication"
)

func TestMaxResponseBodySize(t *testing.T) {
	// Keep both lists exhaustive over communication/topics.go: a multi-chunk protocol
	// missing from multiChunk (or unrecognised by maxResponseBodySize) is silently
	// capped at the single-object ceiling and its responses truncate.
	singleObject := []string{
		communication.PingProtocolV1,
		communication.GoodbyeProtocolV1,
		communication.MetadataProtocolV1,
		communication.MetadataProtocolV2,
		communication.MetadataProtocolV3,
		communication.StatusProtocolV1,
		communication.StatusProtocolV2,
		communication.LightClientOptimisticUpdateProtocolV1,
		communication.LightClientFinalityUpdateProtocolV1,
		communication.LightClientBootstrapProtocolV1,
	}
	multiChunk := []string{
		communication.BeaconBlocksByRangeProtocolV1,
		communication.BeaconBlocksByRangeProtocolV2,
		communication.BeaconBlocksByRootProtocolV1,
		communication.BeaconBlocksByRootProtocolV2,
		communication.BeaconBlocksByHeadProtocolV1,
		communication.BlobSidecarByRootProtocolV1,
		communication.BlobSidecarByRangeProtocolV1,
		communication.DataColumnSidecarsByRootProtocolV1,
		communication.DataColumnSidecarsByRangeProtocolV1,
		communication.ExecutionPayloadEnvelopesByRangeProtocolV1,
		communication.ExecutionPayloadEnvelopesByRootProtocolV1,
		communication.LightClientUpdatesByRangeProtocolV1,
	}
	ceiling := int64(maxResponseChunks) * int64(clparams.MaxChunkSize)
	const budget = 5 * 1024 * 1024
	for _, topic := range singleObject {
		// Single-object protocols ignore the caller's byte budget.
		require.EqualValuesf(t, maxSingleObjectResponse, maxResponseBodySize(topic, 0),
			"single-object protocol %s must get the tight ceiling", topic)
		require.EqualValuesf(t, maxSingleObjectResponse, maxResponseBodySize(topic, budget),
			"single-object protocol %s must ignore the byte budget", topic)
	}
	for _, topic := range multiChunk {
		// An unknown (0) or oversized budget falls back to the absolute ceiling.
		require.EqualValuesf(t, ceiling, maxResponseBodySize(topic, 0),
			"multi-chunk protocol %s with no budget must get the ceiling", topic)
		require.EqualValuesf(t, ceiling, maxResponseBodySize(topic, ceiling+1),
			"multi-chunk protocol %s with an oversized budget must clamp to the ceiling", topic)
		// A concrete budget below the ceiling is honored as-is.
		require.EqualValuesf(t, budget, maxResponseBodySize(topic, budget),
			"multi-chunk protocol %s must honor the caller's byte budget", topic)
		require.Lessf(t, maxResponseBodySize(topic, budget), ceiling,
			"multi-chunk protocol %s with a small budget must be tighter than the ceiling", topic)
	}
}

// fetchPeerResponse stands up two libp2p hosts, has the peer answer the given topic
// with a success code byte followed by payloadSize bytes, and returns the response
// body the req/resp handler surfaces to the caller. maxResponseBytes (when > 0) is sent
// as the byte-budget hint that sizes the multi-chunk cap.
func fetchPeerResponse(t *testing.T, topic string, payloadSize int, maxResponseBytes uint64) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	victim, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { victim.Close() })

	peerHost, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { peerHost.Close() })

	peerHost.SetStreamHandler(protocol.ID(topic), func(s network.Stream) {
		defer s.Close()
		_ = s.SetDeadline(time.Now().Add(10 * time.Second))
		// Drain the request, then answer with a success code byte and payloadSize bytes.
		_, _ = io.Copy(io.Discard, s)
		if _, err := s.Write([]byte{0x00}); err != nil {
			return
		}
		buf := make([]byte, 1024*1024)
		for sent := 0; sent < payloadSize; {
			chunk := buf
			if remaining := payloadSize - sent; remaining < len(chunk) {
				chunk = chunk[:remaining]
			}
			n, err := s.Write(chunk)
			if err != nil {
				return
			}
			sent += n
		}
	})

	require.NoError(t, victim.Connect(ctx, peer.AddrInfo{ID: peerHost.ID(), Addrs: peerHost.Addrs()}))

	req, err := http.NewRequestWithContext(ctx, "GET", "http://service.internal/", nil)
	require.NoError(t, err)
	req.Header.Set("REQRESP-PEER-ID", peerHost.ID().String())
	req.Header.Set("REQRESP-TOPIC", topic)
	if maxResponseBytes > 0 {
		req.Header.Set(MaxResponseBytesHeader, strconv.FormatUint(maxResponseBytes, 10))
	}

	resp, err := Do(NewRequestHandler(victim), req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return body
}

// A peer that floods its response stream must not be able to make the handler buffer
// an unbounded amount of memory: a single-object response is capped at maxSingleObjectResponse.
func TestResponseBodyCappedOnFlood(t *testing.T) {
	const floodSize = 40 * 1024 * 1024 // well above the single-object cap
	body := fetchPeerResponse(t, communication.StatusProtocolV1, floodSize, 0)
	require.LessOrEqualf(t, len(body), maxSingleObjectResponse,
		"single-object response must be capped at maxSingleObjectResponse (%d), got %d bytes", maxSingleObjectResponse, len(body))
}

// A legitimate multi-chunk response larger than the single-object cap must pass through
// uncapped: by_range / by_root / by_head topics get the multi-chunk ceiling, so the cap
// added for the flood case must not truncate real block/blob/column responses.
func TestMultiChunkResponseNotCappedAtSingleObject(t *testing.T) {
	const payloadSize = 5 * 1024 * 1024 // above the single-object cap, far below the multi-chunk ceiling
	body := fetchPeerResponse(t, communication.BeaconBlocksByRangeProtocolV2, payloadSize, 0)
	require.EqualValues(t, payloadSize, len(body),
		"a legitimate multi-chunk response must not be truncated by the response cap")
}

// The multi-chunk cap honors the caller's byte budget: a peer that floods beyond it is cut
// off at the budget, not at the loose ceiling, and not at the single-object cap.
func TestMultiChunkResponseCappedByByteBudget(t *testing.T) {
	const budget = 8 * 1024 * 1024
	const floodSize = 40 * 1024 * 1024 // > budget, < the ceiling
	body := fetchPeerResponse(t, communication.BeaconBlocksByRangeProtocolV2, floodSize, budget)
	require.LessOrEqualf(t, len(body), budget,
		"response must be capped at the caller's byte budget (%d), got %d bytes", budget, len(body))
	require.Greaterf(t, len(body), maxSingleObjectResponse,
		"a multi-chunk response must not be clamped to the single-object cap")
}
