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
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/stretchr/testify/require"
)

func TestMaxResponseBodySize(t *testing.T) {
	// Single-object protocols are capped tightly.
	require.EqualValues(t, maxSingleObjectResponse, maxResponseBodySize("/eth2/beacon_chain/req/status/1/ssz_snappy"))
	require.EqualValues(t, maxSingleObjectResponse, maxResponseBodySize("/eth2/beacon_chain/req/ping/1/ssz_snappy"))
	require.EqualValues(t, maxSingleObjectResponse, maxResponseBodySize("/eth2/beacon_chain/req/metadata/2/ssz_snappy"))
	require.EqualValues(t, maxSingleObjectResponse, maxResponseBodySize("/eth2/beacon_chain/req/light_client_bootstrap/1/ssz_snappy"))
	// by_range / by_root protocols may return many chunks, so they get a larger ceiling.
	require.Greater(t, maxResponseBodySize("/eth2/beacon_chain/req/beacon_blocks_by_range/2/ssz_snappy"), int64(maxSingleObjectResponse))
	require.Greater(t, maxResponseBodySize("/eth2/beacon_chain/req/beacon_blocks_by_root/2/ssz_snappy"), int64(maxSingleObjectResponse))
	require.Greater(t, maxResponseBodySize("/eth2/beacon_chain/req/blob_sidecars_by_range/1/ssz_snappy"), int64(maxSingleObjectResponse))
}

// A peer that floods its response stream must not be able to make the handler buffer
// an unbounded amount of memory: a single-chunk response is capped at maxChunkSize.
func TestResponseBodyCappedOnFlood(t *testing.T) {
	ctx := context.Background()

	victim, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { victim.Close() })

	attacker, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { attacker.Close() })

	const statusTopic = "/eth2/beacon_chain/req/status/1/ssz_snappy"
	const floodSize = 40 * 1024 * 1024 // well above the single-chunk cap

	attacker.SetStreamHandler(protocol.ID(statusTopic), func(s network.Stream) {
		defer s.Close()
		_ = s.SetDeadline(time.Now().Add(10 * time.Second))
		// Drain the victim's request, then answer with a success code byte and flood.
		_, _ = io.Copy(io.Discard, s)
		if _, err := s.Write([]byte{0x00}); err != nil {
			return
		}
		buf := make([]byte, 1024*1024)
		for sent := 0; sent < floodSize; {
			n, err := s.Write(buf)
			if err != nil {
				return
			}
			sent += n
		}
	})

	require.NoError(t, victim.Connect(ctx, peer.AddrInfo{ID: attacker.ID(), Addrs: attacker.Addrs()}))

	req, err := http.NewRequest("GET", "http://service.internal/", nil)
	require.NoError(t, err)
	req.Header.Set("REQRESP-PEER-ID", attacker.ID().String())
	req.Header.Set("REQRESP-TOPIC", statusTopic)

	resp, err := Do(NewRequestHandler(victim), req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.LessOrEqualf(t, len(body), maxSingleObjectResponse,
		"single-object response must be capped at maxSingleObjectResponse (%d), got %d bytes", maxSingleObjectResponse, len(body))
}
