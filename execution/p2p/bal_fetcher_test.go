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

package p2p

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

func TestValidateBALResponse(t *testing.T) {
	t.Parallel()
	bal := []byte{0xc2, 0x01, 0x02} // arbitrary non-sentinel payload
	balHash := crypto.Keccak256Hash(bal)
	h0 := common.BytesToHash([]byte{1})
	h1 := common.BytesToHash([]byte{2})

	t.Run("valid populated BAL is returned", func(t *testing.T) {
		reqs := []BALRequest{{Hash: h0, Number: 1, ExpectedHash: balHash}}
		out, bad, err := validateBALResponse(reqs, []rlp.RawValue{bal})
		require.NoError(t, err)
		require.False(t, bad)
		require.Equal(t, bal, out[h0])
	})

	t.Run("0x80 sentinel is a miss not an error", func(t *testing.T) {
		reqs := []BALRequest{{Hash: h0, Number: 1, ExpectedHash: balHash}}
		out, bad, err := validateBALResponse(reqs, []rlp.RawValue{{0x80}})
		require.NoError(t, err)
		require.False(t, bad)
		require.NotContains(t, out, h0)
	})

	t.Run("empty entry is a miss", func(t *testing.T) {
		reqs := []BALRequest{{Hash: h0, Number: 1, ExpectedHash: balHash}}
		out, bad, err := validateBALResponse(reqs, []rlp.RawValue{{}})
		require.NoError(t, err)
		require.False(t, bad)
		require.NotContains(t, out, h0)
	})

	t.Run("0xc0 accepted only for the empty-BAL hash", func(t *testing.T) {
		reqs := []BALRequest{{Hash: h0, Number: 1, ExpectedHash: empty.BlockAccessListHash}}
		out, bad, err := validateBALResponse(reqs, []rlp.RawValue{{0xc0}})
		require.NoError(t, err)
		require.False(t, bad)
		require.Equal(t, []byte{0xc0}, out[h0])
	})

	t.Run("0xc0 for a non-empty-BAL hash penalises the peer", func(t *testing.T) {
		reqs := []BALRequest{{Hash: h0, Number: 1, ExpectedHash: balHash}}
		_, bad, err := validateBALResponse(reqs, []rlp.RawValue{{0xc0}})
		require.Error(t, err)
		require.True(t, bad)
	})

	t.Run("hash mismatch penalises the peer", func(t *testing.T) {
		reqs := []BALRequest{{Hash: h0, Number: 1, ExpectedHash: common.BytesToHash([]byte{0xff})}}
		_, bad, err := validateBALResponse(reqs, []rlp.RawValue{bal})
		require.Error(t, err)
		require.True(t, bad)
	})

	t.Run("over-long response penalises the peer", func(t *testing.T) {
		reqs := []BALRequest{{Hash: h0, Number: 1, ExpectedHash: balHash}}
		_, bad, err := validateBALResponse(reqs, []rlp.RawValue{bal, bal})
		require.Error(t, err)
		require.True(t, bad)
	})

	t.Run("short response leaves trailing requests absent", func(t *testing.T) {
		reqs := []BALRequest{
			{Hash: h0, Number: 1, ExpectedHash: balHash},
			{Hash: h1, Number: 2, ExpectedHash: balHash},
		}
		out, bad, err := validateBALResponse(reqs, []rlp.RawValue{bal}) // only first answered
		require.NoError(t, err)
		require.False(t, bad)
		require.Equal(t, bal, out[h0])
		require.NotContains(t, out, h1)
	})
}

func TestBALRequestsForHeaders(t *testing.T) {
	t.Parallel()
	balHash := common.BytesToHash([]byte{0xaa})
	emptyHash := empty.BlockAccessListHash
	withBAL := &types.Header{Number: *uint256.NewInt(10), BlockAccessListHash: &balHash}
	preFork := &types.Header{Number: *uint256.NewInt(11)}                                   // BlockAccessListHash == nil
	emptyBAL := &types.Header{Number: *uint256.NewInt(12), BlockAccessListHash: &emptyHash} // genuinely empty BAL

	reqs := balRequestsForHeaders([]*types.Header{preFork, withBAL, emptyBAL})
	require.Len(t, reqs, 1) // pre-fork and empty-BAL headers are not requested
	require.Equal(t, withBAL.Hash(), reqs[0].Hash)
	require.Equal(t, balHash, reqs[0].ExpectedHash)
	require.Equal(t, uint64(10), reqs[0].Number)
}

func TestFetchAcrossPeers(t *testing.T) {
	t.Parallel()
	h0 := common.BytesToHash([]byte{1})
	h1 := common.BytesToHash([]byte{2})
	reqs := []BALRequest{{Hash: h0}, {Hash: h1}}
	serveAll := func(rs []BALRequest) map[common.Hash][]byte {
		out := map[common.Hash][]byte{}
		for _, r := range rs {
			out[r.Hash] = []byte{0xaa}
		}
		return out
	}

	t.Run("collects all BALs when one peer serves the batch", func(t *testing.T) {
		serving := PeerIdFromUint64(3)
		fetch := func(_ context.Context, rs []BALRequest, p *PeerId) map[common.Hash][]byte {
			if p.Equal(serving) {
				return serveAll(rs)
			}
			return nil // non-serving peer
		}
		out := fetchAcrossPeers(context.Background(), reqs,
			[]PeerId{*PeerIdFromUint64(1), *PeerIdFromUint64(2), *serving}, 8, fetch)
		require.Len(t, out, 2)
	})

	t.Run("merges partial results across peers", func(t *testing.T) {
		fetch := func(_ context.Context, _ []BALRequest, p *PeerId) map[common.Hash][]byte {
			if p.Equal(PeerIdFromUint64(1)) {
				return map[common.Hash][]byte{h0: {0xaa}}
			}
			return map[common.Hash][]byte{h1: {0xbb}}
		}
		out := fetchAcrossPeers(context.Background(), reqs,
			[]PeerId{*PeerIdFromUint64(1), *PeerIdFromUint64(2)}, 8, fetch)
		require.Len(t, out, 2)
		require.Equal(t, []byte{0xaa}, out[h0])
		require.Equal(t, []byte{0xbb}, out[h1])
	})

	t.Run("no peer serves -> empty result", func(t *testing.T) {
		fetch := func(_ context.Context, _ []BALRequest, _ *PeerId) map[common.Hash][]byte { return nil }
		out := fetchAcrossPeers(context.Background(), reqs,
			[]PeerId{*PeerIdFromUint64(1), *PeerIdFromUint64(2)}, 8, fetch)
		require.Empty(t, out)
	})

	t.Run("honours the parallelism limit", func(t *testing.T) {
		var live, peak atomic.Int32
		fetch := func(_ context.Context, _ []BALRequest, _ *PeerId) map[common.Hash][]byte {
			n := live.Add(1)
			for {
				if p := peak.Load(); n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			live.Add(-1)
			return nil // none serve, so every peer is tried
		}
		fetchAcrossPeers(context.Background(), reqs,
			[]PeerId{*PeerIdFromUint64(1), *PeerIdFromUint64(2), *PeerIdFromUint64(3), *PeerIdFromUint64(4)}, 2, fetch)
		require.LessOrEqual(t, peak.Load(), int32(2)) // limit 2 enforced (would reach 4 unbounded)
	})
}

func TestMissingRequests(t *testing.T) {
	t.Parallel()
	h0 := common.BytesToHash([]byte{1})
	h1 := common.BytesToHash([]byte{2})
	h2 := common.BytesToHash([]byte{3})
	reqs := []BALRequest{{Hash: h0}, {Hash: h1}, {Hash: h2}}

	require.Equal(t, reqs, missingRequests(reqs, nil)) // nothing fetched yet -> all missing

	got := missingRequests(reqs, map[common.Hash][]byte{h1: {0x01}})
	require.Len(t, got, 2)
	require.Equal(t, h0, got[0].Hash)
	require.Equal(t, h2, got[1].Hash)

	require.Empty(t, missingRequests(reqs, map[common.Hash][]byte{h0: {0x01}, h1: {0x01}, h2: {0x01}}))
}

func TestFetchWithFallback(t *testing.T) {
	t.Parallel()
	h0 := common.BytesToHash([]byte{1})
	h1 := common.BytesToHash([]byte{2})
	reqs := []BALRequest{{Hash: h0}, {Hash: h1}}
	main := PeerIdFromUint64(1)
	fallback := []PeerId{*PeerIdFromUint64(2), *PeerIdFromUint64(3)}

	t.Run("main peer serves all -> no fallback", func(t *testing.T) {
		var usedFallback atomic.Bool
		fetch := func(_ context.Context, rs []BALRequest, p *PeerId) map[common.Hash][]byte {
			if !p.Equal(main) {
				usedFallback.Store(true)
			}
			out := map[common.Hash][]byte{}
			for _, r := range rs {
				out[r.Hash] = []byte{0xaa}
			}
			return out
		}
		out := fetchWithFallback(context.Background(), reqs, main, fallback, 8, fetch)
		require.Len(t, out, 2)
		require.False(t, usedFallback.Load()) // main had everything; fallback peers untouched
	})

	t.Run("main peer misses some -> fallback fills the rest", func(t *testing.T) {
		fetch := func(_ context.Context, _ []BALRequest, p *PeerId) map[common.Hash][]byte {
			if p.Equal(main) {
				return map[common.Hash][]byte{h0: {0xaa}} // main only has h0
			}
			return map[common.Hash][]byte{h1: {0xbb}} // fallback has h1
		}
		out := fetchWithFallback(context.Background(), reqs, main, fallback, 8, fetch)
		require.Len(t, out, 2)
		require.Equal(t, []byte{0xaa}, out[h0]) // kept from main
		require.Equal(t, []byte{0xbb}, out[h1]) // recovered from fallback
	})

	t.Run("main peer serves none -> fallback fills all", func(t *testing.T) {
		fetch := func(_ context.Context, rs []BALRequest, p *PeerId) map[common.Hash][]byte {
			if p.Equal(main) {
				return nil
			}
			out := map[common.Hash][]byte{}
			for _, r := range rs {
				out[r.Hash] = []byte{0xbb}
			}
			return out
		}
		out := fetchWithFallback(context.Background(), reqs, main, fallback, 8, fetch)
		require.Len(t, out, 2)
	})

	t.Run("no fallback peers -> only the main peer's BALs", func(t *testing.T) {
		fetch := func(_ context.Context, _ []BALRequest, _ *PeerId) map[common.Hash][]byte {
			return map[common.Hash][]byte{h0: {0xaa}} // main has only h0, h1 stays missing
		}
		out := fetchWithFallback(context.Background(), reqs, main, nil, 8, fetch)
		require.Len(t, out, 1)
	})
}
