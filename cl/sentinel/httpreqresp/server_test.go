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

	"github.com/erigontech/erigon/cl/sentinel/communication"
)

func TestMaxResponseBodySize(t *testing.T) {
	// Drive the cap off communication.AllProtocols, the single source of truth for response-size
	// classification, so a protocol added there is automatically covered here.
	ceiling := maxMultiChunkResponse
	const budget = 5 * 1024 * 1024
	for _, p := range communication.AllProtocols {
		if !p.MultiChunk {
			// Single-object protocols ignore the caller's byte budget.
			require.EqualValuesf(t, maxSingleObjectResponse, maxResponseBodySize(p.ID, 0),
				"single-object protocol %s must get the tight cap", p.ID)
			require.EqualValuesf(t, maxSingleObjectResponse, maxResponseBodySize(p.ID, budget),
				"single-object protocol %s must ignore the byte budget", p.ID)
			continue
		}
		// No budget falls back to the absolute ceiling.
		require.EqualValuesf(t, ceiling, maxResponseBodySize(p.ID, 0),
			"multi-chunk protocol %s with no budget must get the ceiling", p.ID)
		// A budget below the ceiling is honored as-is (an internal, wire-sized value).
		require.EqualValuesf(t, budget, maxResponseBodySize(p.ID, budget),
			"multi-chunk protocol %s must honor the caller's byte budget", p.ID)
		require.Lessf(t, maxResponseBodySize(p.ID, budget), ceiling,
			"multi-chunk protocol %s with a small budget must be tighter than the ceiling", p.ID)
		// A budget above the ceiling is clamped to it, so a miscomputed or overflowed budget
		// can't make the handler buffer more than a spec-maximal response.
		require.EqualValuesf(t, ceiling, maxResponseBodySize(p.ID, ceiling+1),
			"multi-chunk protocol %s must clamp a budget above the ceiling", p.ID)
	}
}

// fetchPeerResponse stands up two libp2p hosts, has the peer answer the given topic
// with a success code byte followed by payloadSize bytes, and returns the status code and
// body the req/resp handler surfaces to the caller. maxResponseBytes (when > 0) is sent
// as the byte-budget hint that sizes the multi-chunk cap.
func fetchPeerResponse(t *testing.T, topic string, payloadSize int, maxResponseBytes uint64) (int, []byte) {
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
	return resp.StatusCode, body
}

// A peer that floods its response stream must not be able to make the handler buffer an
// unbounded amount of memory: a compliant single-object response is returned, but one that
// exceeds the cap is rejected explicitly rather than silently truncated and accepted as 200.
func TestResponseBodyRejectedOnFlood(t *testing.T) {
	status, body := fetchPeerResponse(t, communication.StatusProtocolV1, 1000, 0)
	require.Equal(t, http.StatusOK, status)
	require.Len(t, body, 1000, "a compliant single-object response must pass through")

	const floodSize = 40 * 1024 * 1024 // well above the single-object cap
	status, _ = fetchPeerResponse(t, communication.StatusProtocolV1, floodSize, 0)
	require.Equalf(t, http.StatusRequestEntityTooLarge, status,
		"a single-object flood must be rejected, not truncated and accepted as 200")
}

// A legitimate multi-chunk response larger than the single-object cap must pass through
// uncapped: by_range / by_root / by_head topics get the multi-chunk ceiling, so the cap
// must not truncate real block/blob/column responses.
func TestMultiChunkResponseNotCappedAtSingleObject(t *testing.T) {
	const payloadSize = 5 * 1024 * 1024 // above the single-object cap, far below the multi-chunk ceiling
	status, body := fetchPeerResponse(t, communication.BeaconBlocksByRangeProtocolV2, payloadSize, 0)
	require.Equal(t, http.StatusOK, status)
	require.EqualValues(t, payloadSize, len(body),
		"a legitimate multi-chunk response must not be truncated by the response cap")
}

// The multi-chunk cap honors the caller's byte budget: a response within it passes through, a
// flood beyond it is rejected (not truncated, and not clamped to the single-object cap).
func TestMultiChunkResponseRejectedOverByteBudget(t *testing.T) {
	const budget = 8 * 1024 * 1024

	const okSize = 4 * 1024 * 1024 // within the budget
	status, body := fetchPeerResponse(t, communication.BeaconBlocksByRangeProtocolV2, okSize, budget)
	require.Equal(t, http.StatusOK, status)
	require.EqualValues(t, okSize, len(body), "a response within the budget must pass through in full")

	const floodSize = 40 * 1024 * 1024 // > budget, < the ceiling
	status, _ = fetchPeerResponse(t, communication.BeaconBlocksByRangeProtocolV2, floodSize, budget)
	require.Equalf(t, http.StatusRequestEntityTooLarge, status,
		"a multi-chunk response over the byte budget must be rejected")
}
