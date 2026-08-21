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
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestValidateBALResponse(t *testing.T) {
	t.Parallel()
	wantBAL := types.BlockAccessList{{
		Address: accounts.InternAddress(common.Address{1}),
	}}
	bal, err := types.EncodeBlockAccessListBytes(wantBAL)
	require.NoError(t, err)
	balHash := crypto.Keccak256Hash(bal)
	h0 := common.BytesToHash([]byte{1})
	h1 := common.BytesToHash([]byte{2})
	validGasLimit := uint64(types.BalItemCost)

	t.Run("valid populated BAL is returned", func(t *testing.T) {
		reqs := []BALRequest{{Hash: h0, Number: 1, GasLimit: validGasLimit, ExpectedHash: balHash}}
		out, bad, err := validateBALResponse(reqs, []rlp.RawValue{bal})
		require.NoError(t, err)
		require.False(t, bad)
		require.Equal(t, wantBAL, out[h0].BlockAccessList())
		gotRaw, err := out[h0].Bytes()
		require.NoError(t, err)
		require.Equal(t, bal, gotRaw)
	})

	t.Run("BAL exceeding the block gas bound penalises the peer", func(t *testing.T) {
		reqs := []BALRequest{{Hash: h0, Number: 1, GasLimit: types.BalItemCost - 1, ExpectedHash: balHash}}
		out, bad, err := validateBALResponse(reqs, []rlp.RawValue{bal})
		require.ErrorContains(t, err, "block access list too large")
		require.True(t, bad)
		require.NotContains(t, out, h0)
	})

	t.Run("hash-matching malformed BAL penalises the peer", func(t *testing.T) {
		malformed := []byte{0xc2, 0x01, 0x02}
		reqs := []BALRequest{{Hash: h0, Number: 1, ExpectedHash: crypto.Keccak256Hash(malformed)}}
		out, bad, err := validateBALResponse(reqs, []rlp.RawValue{malformed})
		require.Error(t, err)
		require.True(t, bad)
		require.NotContains(t, out, h0)
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
		require.NotNil(t, out[h0])
		require.Empty(t, out[h0].BlockAccessList())
	})

	t.Run("0xc0 for a non-empty-BAL hash penalises but keeps valid entries", func(t *testing.T) {
		reqs := []BALRequest{
			{Hash: h0, Number: 1, ExpectedHash: empty.BlockAccessListHash},
			{Hash: h1, Number: 2, GasLimit: validGasLimit, ExpectedHash: balHash},
		}
		reqs[0].ExpectedHash = balHash
		out, bad, err := validateBALResponse(reqs, []rlp.RawValue{{0xc0}, bal})
		require.Error(t, err)
		require.True(t, bad)
		require.NotContains(t, out, h0)
		require.Equal(t, wantBAL, out[h1].BlockAccessList())
	})

	t.Run("hash mismatch penalises but keeps valid entries", func(t *testing.T) {
		reqs := []BALRequest{
			{Hash: h0, Number: 1, ExpectedHash: common.BytesToHash([]byte{0xff})},
			{Hash: h1, Number: 2, GasLimit: validGasLimit, ExpectedHash: balHash},
		}
		out, bad, err := validateBALResponse(reqs, []rlp.RawValue{bal, bal})
		require.Error(t, err)
		require.True(t, bad)
		require.NotContains(t, out, h0)
		require.Equal(t, wantBAL, out[h1].BlockAccessList())
	})

	t.Run("over-long response penalises the peer", func(t *testing.T) {
		reqs := []BALRequest{{Hash: h0, Number: 1, ExpectedHash: balHash}}
		_, bad, err := validateBALResponse(reqs, []rlp.RawValue{bal, bal})
		require.Error(t, err)
		require.True(t, bad)
	})

	t.Run("short response leaves trailing requests absent", func(t *testing.T) {
		reqs := []BALRequest{
			{Hash: h0, Number: 1, GasLimit: validGasLimit, ExpectedHash: balHash},
			{Hash: h1, Number: 2, ExpectedHash: balHash},
		}
		out, bad, err := validateBALResponse(reqs, []rlp.RawValue{bal}) // only first answered
		require.NoError(t, err)
		require.False(t, bad)
		require.Equal(t, wantBAL, out[h0].BlockAccessList())
		require.NotContains(t, out, h1)
	})
}

func TestBALRequestsForHeaders(t *testing.T) {
	t.Parallel()
	balHash := common.BytesToHash([]byte{0xaa})
	emptyHash := empty.BlockAccessListHash
	withBAL := &types.Header{Number: *uint256.NewInt(10), GasLimit: 30_000_000, BlockAccessListHash: &balHash}
	preFork := &types.Header{Number: *uint256.NewInt(11)}                                   // BlockAccessListHash == nil
	emptyBAL := &types.Header{Number: *uint256.NewInt(12), BlockAccessListHash: &emptyHash} // genuinely empty BAL

	reqs := balRequestsForHeaders([]*types.Header{preFork, withBAL, emptyBAL})
	require.Len(t, reqs, 1) // pre-fork and empty-BAL headers are not requested
	require.Equal(t, withBAL.Hash(), reqs[0].Hash)
	require.Equal(t, balHash, reqs[0].ExpectedHash)
	require.Equal(t, uint64(10), reqs[0].Number)
	require.Equal(t, withBAL.GasLimit, reqs[0].GasLimit)
}

func TestFetchAcrossPeers(t *testing.T) {
	t.Parallel()
	h0 := common.BytesToHash([]byte{1})
	h1 := common.BytesToHash([]byte{2})
	reqs := []BALRequest{{Hash: h0}, {Hash: h1}}
	balA := types.NewBlockAccessListSidecar(types.BlockAccessList{{Address: accounts.InternAddress(common.Address{0xaa})}})
	balB := types.NewBlockAccessListSidecar(types.BlockAccessList{{Address: accounts.InternAddress(common.Address{0xbb})}})
	serveAll := func(rs []BALRequest) map[common.Hash]*types.BlockAccessListSidecar {
		out := map[common.Hash]*types.BlockAccessListSidecar{}
		for _, r := range rs {
			out[r.Hash] = balA
		}
		return out
	}

	t.Run("collects all BALs when one peer serves the batch", func(t *testing.T) {
		serving := PeerIdFromUint64(3)
		fetch := func(_ context.Context, rs []BALRequest, p *PeerId) map[common.Hash]*types.BlockAccessListSidecar {
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
		fetch := func(_ context.Context, _ []BALRequest, p *PeerId) map[common.Hash]*types.BlockAccessListSidecar {
			if p.Equal(PeerIdFromUint64(1)) {
				return map[common.Hash]*types.BlockAccessListSidecar{h0: balA}
			}
			return map[common.Hash]*types.BlockAccessListSidecar{h1: balB}
		}
		out := fetchAcrossPeers(context.Background(), reqs,
			[]PeerId{*PeerIdFromUint64(1), *PeerIdFromUint64(2)}, 8, fetch)
		require.Len(t, out, 2)
		require.Equal(t, balA, out[h0])
		require.Equal(t, balB, out[h1])
	})

	t.Run("no peer serves -> empty result", func(t *testing.T) {
		fetch := func(_ context.Context, _ []BALRequest, _ *PeerId) map[common.Hash]*types.BlockAccessListSidecar {
			return nil
		}
		out := fetchAcrossPeers(context.Background(), reqs,
			[]PeerId{*PeerIdFromUint64(1), *PeerIdFromUint64(2)}, 8, fetch)
		require.Empty(t, out)
	})
	t.Run("covers the whole batch when peers truncate to a response-size prefix", func(t *testing.T) {
		var hashes []common.Hash
		var prefixReqs []BALRequest
		for i := byte(1); i <= 7; i++ {
			h := common.BytesToHash([]byte{i})
			hashes = append(hashes, h)
			prefixReqs = append(prefixReqs, BALRequest{Hash: h})
		}
		fetch := func(_ context.Context, rs []BALRequest, _ *PeerId) map[common.Hash]*types.BlockAccessListSidecar {
			out := map[common.Hash]*types.BlockAccessListSidecar{}
			for _, r := range rs[:min(2, len(rs))] {
				out[r.Hash] = balA
			}
			return out
		}
		out := fetchAcrossPeers(
			context.Background(),
			prefixReqs,
			[]PeerId{*PeerIdFromUint64(1), *PeerIdFromUint64(2)},
			8,
			fetch,
		)
		require.Len(t, out, 7)
		for _, h := range hashes {
			require.Contains(t, out, h)
		}
	})
	t.Run("straggler held by a single peer is found via broadcast", func(t *testing.T) {
		var many []BALRequest
		for i := byte(1); i <= 40; i++ {
			many = append(many, BALRequest{Hash: common.BytesToHash([]byte{i})})
		}
		rare := many[37].Hash
		holder := PeerIdFromUint64(7)
		fetch := func(_ context.Context, rs []BALRequest, p *PeerId) map[common.Hash]*types.BlockAccessListSidecar {
			out := map[common.Hash]*types.BlockAccessListSidecar{}
			for _, r := range rs {
				if r.Hash == rare {
					if p.Equal(holder) {
						out[r.Hash] = balB
					}
					continue
				}
				out[r.Hash] = balA
			}
			return out
		}
		peers := make([]PeerId, 0, 13)
		for i := uint64(1); i <= 13; i++ {
			peers = append(peers, *PeerIdFromUint64(i))
		}
		out := fetchAcrossPeers(context.Background(), many, peers, 8, fetch)
		require.Len(t, out, 40)
		require.Equal(t, balB, out[rare])
	})
	t.Run("stops when no peer makes progress", func(t *testing.T) {
		var calls atomic.Int32
		fetch := func(_ context.Context, _ []BALRequest, _ *PeerId) map[common.Hash]*types.BlockAccessListSidecar {
			calls.Add(1)
			return map[common.Hash]*types.BlockAccessListSidecar{h0: balA}
		}
		out := fetchAcrossPeers(
			context.Background(),
			reqs,
			[]PeerId{*PeerIdFromUint64(1), *PeerIdFromUint64(2)},
			8,
			fetch,
		)
		require.Len(t, out, 1)
		require.LessOrEqual(t, calls.Load(), int32(4))
	})
	t.Run("honours the parallelism limit", func(t *testing.T) {
		var live, peak atomic.Int32
		fetch := func(_ context.Context, _ []BALRequest, _ *PeerId) map[common.Hash]*types.BlockAccessListSidecar {
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
