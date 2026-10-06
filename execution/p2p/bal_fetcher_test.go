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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

func newTestBALFetcher(t *testing.T, serve func(context.Context, []common.Hash, *PeerId) ([]rlp.RawValue, error)) (BALFetcher, *PeerTracker) {
	t.Helper()
	sentry := direct.NewMockSentryClient(gomock.NewController(t))
	logger := log.New()
	penalizer := NewPeerPenalizer(sentry)
	listener := NewMessageListener(logger, sentry, nil, penalizer)
	tracker := NewPeerTracker(logger, listener)
	sentry.EXPECT().SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *sentryproto.SendMessageByIdRequest, _ ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			if request.Data.Id != sentryproto.MessageId_GET_BLOCK_ACCESS_LISTS_71 {
				return nil, fmt.Errorf("unexpected request message: %v", request.Data.Id)
			}
			var query eth.GetBlockAccessListsPacket66
			if err := rlp.DecodeBytes(request.Data.Data, &query); err != nil {
				return nil, err
			}
			response, err := serve(ctx, query.GetBlockAccessListsPacket, PeerIdFromH512(request.PeerId))
			if err != nil {
				return nil, err
			}
			encoded, err := rlp.EncodeToBytes(eth.BlockAccessListsPacket66{RequestId: query.RequestId, BlockAccessListsPacket: response})
			if err != nil {
				return nil, err
			}
			err = notifyInboundMessageObservers(ctx, logger, penalizer, listener.blockAccessListsObservers, &sentryproto.InboundMessage{
				Id: sentryproto.MessageId_BLOCK_ACCESS_LISTS_71, PeerId: request.PeerId, Data: encoded,
			})
			return &sentryproto.SentPeers{Peers: []*typesproto.H512{request.PeerId}}, err
		}).AnyTimes()
	return NewBALFetcher(logger, listener, NewMessageSender(sentry), penalizer, tracker), tracker
}

func TestBALFetcher_RetriesThrottledSuffix(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bal, err := types.EncodeBlockAccessListBytes(types.BlockAccessList{{Address: common.Address{1}}})
		require.NoError(t, err)
		reqs := make([]BALRequest, 40)
		for i := range reqs {
			reqs[i] = BALRequest{Hash: common.Hash{byte(i + 1)}, Number: uint64(i + 1), GasLimit: types.BalItemCost, ExpectedHash: crypto.Keccak256Hash(bal)}
		}
		limiter := rate.NewLimiter(rate.Every(time.Second), 1)
		var queries [][]common.Hash
		fetcher, tracker := newTestBALFetcher(t, func(_ context.Context, hashes []common.Hash, _ *PeerId) ([]rlp.RawValue, error) {
			queries = append(queries, hashes)
			if !limiter.Allow() {
				return nil, nil
			}
			response := make([]rlp.RawValue, min(len(hashes), eth.MaxBlockAccessListsRegenerate))
			for i := range response {
				response[i] = bal
			}
			return response, nil
		})
		peer := PeerIdFromUint64(1)
		tracker.PeerConnected(peer)
		started := time.Now()
		got := fetcher.Fetch(t.Context(), reqs, peer, nil, 5*time.Second, time.Second)
		require.Len(t, got, 40, "a temporary empty response must not abandon the suffix")
		require.GreaterOrEqual(t, time.Since(started), time.Second)
		require.True(t, tracker.PeerMayHaveBALNum(peer, 40), "throttling must not mark the peer's BALs unavailable")
		for _, query := range queries[1:] {
			require.Equal(t, queries[0][32:], query, "retry only unanswered hashes")
		}
	})
}

func TestBALFetcher_EmptyRepliesStopBeforeBatchDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		fetcher, tracker := newTestBALFetcher(t, func(context.Context, []common.Hash, *PeerId) ([]rlp.RawValue, error) {
			calls++
			return nil, nil
		})
		peer := PeerIdFromUint64(1)
		tracker.PeerConnected(peer)
		reqs := []BALRequest{{Hash: common.Hash{1}, Number: 1, ExpectedHash: empty.BlockAccessListHash}}
		started := time.Now()
		got := fetcher.Fetch(t.Context(), reqs, peer, nil, defaultBbdRequestConfig.balsBatchFetchTimeout, defaultBbdRequestConfig.balsRequestTimeout)
		require.Empty(t, got)
		require.Equal(t, 3, calls, "stop after three consecutive empty replies")
		require.Equal(t, time.Second, time.Since(started), "optional BALs must not hold up blocks until the batch deadline")
		require.True(t, tracker.PeerMayHaveBALNum(peer, 1), "empty replies do not establish that the peer lacks a BAL")
	})
}

func TestBALFetcher_EmptyReplyBudgetResetsOnAnsweredPrefix(t *testing.T) {
	for _, tc := range []struct {
		name  string
		first rlp.RawValue
		bals  int
	}{
		{name: "available prefix", first: rlp.RawValue{0xc0}, bals: 2},
		{name: "unavailable prefix", first: rlp.RawValue{0x80}, bals: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				busy, serving := PeerIdFromUint64(1), PeerIdFromUint64(2)
				var busyCalls, servingCalls int
				fetcher, tracker := newTestBALFetcher(t, func(_ context.Context, hashes []common.Hash, peer *PeerId) ([]rlp.RawValue, error) {
					if peer.Equal(busy) {
						busyCalls++
						return nil, nil
					}
					servingCalls++
					if servingCalls%3 != 0 {
						return nil, nil
					}
					if len(hashes) == 2 {
						return []rlp.RawValue{tc.first}, nil
					}
					return []rlp.RawValue{{0xc0}}, nil
				})
				tracker.PeerConnected(busy)
				tracker.PeerConnected(serving)
				reqs := []BALRequest{
					{Hash: common.Hash{1}, Number: 1, ExpectedHash: empty.BlockAccessListHash},
					{Hash: common.Hash{2}, Number: 2, ExpectedHash: empty.BlockAccessListHash},
				}

				got := fetcher.Fetch(t.Context(), reqs, busy, []PeerId{*serving}, time.Minute, time.Second)
				require.Equal(t, 3, busyCalls, "another peer's progress must not reset an empty peer's budget")
				require.Equal(t, 6, servingCalls, "each answered prefix resets that peer's empty-reply budget")
				require.Len(t, got, tc.bals)
				require.Contains(t, got, reqs[1].Hash, "keep retrying the unanswered suffix on a peer that makes progress")
				require.True(t, tracker.PeerMayHaveBALNum(busy, 1))
			})
		})
	}
}

func TestBALFetcher_ConcurrentBatchesRespectPeerRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limiter := rate.NewLimiter(2, 4)
		var rejected atomic.Int32
		fetcher, tracker := newTestBALFetcher(t, func(context.Context, []common.Hash, *PeerId) ([]rlp.RawValue, error) {
			if !limiter.Allow() {
				rejected.Add(1)
				return nil, fmt.Errorf("peer request rate exceeded")
			}
			return []rlp.RawValue{{0xc0}}, nil
		})
		peer := PeerIdFromUint64(1)
		tracker.PeerConnected(peer)
		results := make([]map[common.Hash]*types.BlockAccessListSidecar, 6)
		var wg sync.WaitGroup
		started := time.Now()
		for i := range results {
			wg.Go(func() {
				reqs := []BALRequest{{Hash: common.Hash{byte(i + 1)}, Number: uint64(i + 1), ExpectedHash: empty.BlockAccessListHash}}
				results[i] = fetcher.Fetch(t.Context(), reqs, peer, nil, 10*time.Second, time.Second)
			})
		}
		wg.Wait()
		require.Zero(t, rejected.Load(), "concurrent fetches must share the peer's request budget")
		for _, got := range results {
			require.Len(t, got, 1)
		}
		require.GreaterOrEqual(t, time.Since(started), time.Second)
	})
}

func TestBALFetcher_RetryPacingIncludesResponseTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fetcher, tracker := newTestBALFetcher(t, func(context.Context, []common.Hash, *PeerId) ([]rlp.RawValue, error) {
			time.Sleep(time.Second)
			return []rlp.RawValue{{0xc0}}, nil
		})
		peer := PeerIdFromUint64(1)
		tracker.PeerConnected(peer)
		reqs := []BALRequest{
			{Hash: common.Hash{1}, Number: 1, ExpectedHash: empty.BlockAccessListHash},
			{Hash: common.Hash{2}, Number: 2, ExpectedHash: empty.BlockAccessListHash},
		}
		started := time.Now()
		got := fetcher.Fetch(t.Context(), reqs, peer, nil, 5*time.Second, 2*time.Second)
		require.Len(t, got, 2)
		require.Equal(t, 2*time.Second, time.Since(started), "time spent receiving a response already spaces requests")
	})
}

func TestBALFetcher_ExplicitUnavailableStopsRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		fetcher, tracker := newTestBALFetcher(t, func(context.Context, []common.Hash, *PeerId) ([]rlp.RawValue, error) {
			calls++
			return []rlp.RawValue{{0x80}}, nil
		})
		peer := PeerIdFromUint64(1)
		tracker.PeerConnected(peer)
		reqs := []BALRequest{{Hash: common.Hash{1}, Number: 1, ExpectedHash: empty.BlockAccessListHash}}
		started := time.Now()
		got := fetcher.Fetch(t.Context(), reqs, peer, nil, 5*time.Second, time.Second)
		require.Empty(t, got)
		require.Equal(t, 1, calls, "explicit unavailability must not be retried as throttling")
		require.Zero(t, time.Since(started))
		require.False(t, tracker.PeerMayHaveBALNum(peer, 1))
	})
}

func TestBALFetcher_RetriesOnlyUnansweredEntries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var queries [][]common.Hash
		fetcher, tracker := newTestBALFetcher(t, func(_ context.Context, hashes []common.Hash, _ *PeerId) ([]rlp.RawValue, error) {
			queries = append(queries, hashes)
			// The peer answers one entry per request, including unavailable entries.
			if hashes[0] == (common.Hash{1}) {
				return []rlp.RawValue{{0x80}}, nil
			}
			return []rlp.RawValue{{0xc0}}, nil
		})
		peer := PeerIdFromUint64(1)
		tracker.PeerConnected(peer)
		reqs := []BALRequest{
			{Hash: common.Hash{1}, Number: 1, ExpectedHash: empty.BlockAccessListHash},
			{Hash: common.Hash{2}, Number: 2, ExpectedHash: empty.BlockAccessListHash},
		}
		got := fetcher.Fetch(t.Context(), reqs, peer, nil, 2*time.Second, time.Second)
		require.Len(t, got, 1)
		require.Contains(t, got, reqs[1].Hash)
		require.Equal(t, [][]common.Hash{{reqs[0].Hash, reqs[1].Hash}, {reqs[1].Hash}}, queries)
	})
}

func TestBALFetcher_RetryStopsOnDeadlineOrCancellation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cancelAfter time.Duration
		elapsed     time.Duration
	}{
		{name: "batch deadline", elapsed: 1250 * time.Millisecond},
		{name: "caller cancellation", cancelAfter: 750 * time.Millisecond, elapsed: 750 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				fetcher, tracker := newTestBALFetcher(t, func(context.Context, []common.Hash, *PeerId) ([]rlp.RawValue, error) {
					calls++
					if calls == 1 {
						return []rlp.RawValue{{0xc0}}, nil
					}
					return nil, nil
				})
				peer := PeerIdFromUint64(1)
				tracker.PeerConnected(peer)
				reqs := []BALRequest{
					{Hash: common.Hash{1}, Number: 1, ExpectedHash: empty.BlockAccessListHash},
					{Hash: common.Hash{2}, Number: 2, ExpectedHash: empty.BlockAccessListHash},
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if tc.cancelAfter > 0 {
					timer := time.AfterFunc(tc.cancelAfter, cancel)
					defer timer.Stop()
				}
				started := time.Now()
				got := fetcher.Fetch(ctx, reqs, peer, nil, 1250*time.Millisecond, time.Second)
				require.Len(t, got, 1, "keep completed BALs when the retry budget ends")
				require.Contains(t, got, reqs[0].Hash)
				require.LessOrEqual(t, time.Since(started), tc.elapsed)
				if tc.cancelAfter > 0 {
					require.ErrorIs(t, ctx.Err(), context.Canceled)
				}
				require.GreaterOrEqual(t, calls, 2)
				require.LessOrEqual(t, calls, 3, "empty replies must not cause a busy retry loop")
				require.True(t, tracker.PeerMayHaveBALNum(peer, 2))
			})
		})
	}
}

func TestBALFetcher_ThrottledShardFallsBack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		busy, serving := PeerIdFromUint64(1), PeerIdFromUint64(2)
		fetcher, tracker := newTestBALFetcher(t, func(_ context.Context, hashes []common.Hash, peer *PeerId) ([]rlp.RawValue, error) {
			if peer.Equal(busy) {
				return nil, nil
			}
			response := make([]rlp.RawValue, len(hashes))
			for i := range response {
				response[i] = rlp.RawValue{0xc0}
			}
			return response, nil
		})
		tracker.PeerConnected(busy)
		tracker.PeerConnected(serving)
		reqs := make([]BALRequest, 40)
		for i := range reqs {
			reqs[i] = BALRequest{Hash: common.Hash{byte(i + 1)}, Number: uint64(i + 1), ExpectedHash: empty.BlockAccessListHash}
		}
		started := time.Now()
		got := fetcher.Fetch(t.Context(), reqs, busy, []PeerId{*serving}, 5*time.Second, time.Second)
		require.Len(t, got, 40)
		require.LessOrEqual(t, time.Since(started), time.Second, "a throttled shard must not prevent fallback to another peer")
	})
}

func TestValidateBALResponse(t *testing.T) {
	t.Parallel()
	wantBAL := types.BlockAccessList{{
		Address: common.Address{1},
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
	balA := types.NewBlockAccessListSidecar(types.BlockAccessList{{Address: common.Address{0xaa}}})
	balB := types.NewBlockAccessListSidecar(types.BlockAccessList{{Address: common.Address{0xbb}}})
	serveAll := func(rs []BALRequest) map[common.Hash]*types.BlockAccessListSidecar {
		out := map[common.Hash]*types.BlockAccessListSidecar{}
		for _, r := range rs {
			out[r.Hash] = balA
		}
		return out
	}

	t.Run("collects all BALs when one peer serves the batch", func(t *testing.T) {
		serving := PeerIdFromUint64(3)
		fetch := func(_ context.Context, rs []BALRequest, p *PeerId) (map[common.Hash]*types.BlockAccessListSidecar, int) {
			if p.Equal(serving) {
				return serveAll(rs), len(rs)
			}
			return nil, len(rs) // non-serving peer
		}
		out := fetchAcrossPeers(context.Background(), reqs,
			[]PeerId{*PeerIdFromUint64(1), *PeerIdFromUint64(2), *serving}, 8, fetch)
		require.Len(t, out, 2)
	})

	t.Run("cancels redundant requests once the batch is complete", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			serving := PeerIdFromUint64(1)
			blocked := PeerIdFromUint64(2)
			started := make(chan struct{})
			stopped := make(chan struct{})
			var queuedCalls atomic.Int32
			fetch := func(ctx context.Context, rs []BALRequest, p *PeerId) (map[common.Hash]*types.BlockAccessListSidecar, int) {
				switch {
				case p.Equal(serving):
					<-started
					return serveAll(rs), len(rs)
				case p.Equal(blocked):
					close(started)
					<-ctx.Done()
					close(stopped)
					return nil, len(rs)
				default:
					queuedCalls.Add(1)
					return nil, len(rs)
				}
			}
			result := make(chan map[common.Hash]*types.BlockAccessListSidecar, 1)
			go func() {
				result <- fetchAcrossPeers(ctx, reqs,
					[]PeerId{*serving, *blocked, *PeerIdFromUint64(3)}, 2, fetch)
			}()
			synctest.Wait()
			select {
			case got := <-result:
				require.Equal(t, serveAll(reqs), got)
			default:
				t.Fatal("complete BAL batch is still waiting for a redundant peer")
			}
			select {
			case <-stopped:
			default:
				t.Fatal("redundant peer request was not stopped")
			}
			require.Zero(t, queuedCalls.Load())
			require.NoError(t, ctx.Err())
		})
	})

	t.Run("merges partial results across peers", func(t *testing.T) {
		fetch := func(_ context.Context, rs []BALRequest, p *PeerId) (map[common.Hash]*types.BlockAccessListSidecar, int) {
			if p.Equal(PeerIdFromUint64(1)) {
				return map[common.Hash]*types.BlockAccessListSidecar{h0: balA}, len(rs)
			}
			return map[common.Hash]*types.BlockAccessListSidecar{h1: balB}, len(rs)
		}
		out := fetchAcrossPeers(context.Background(), reqs,
			[]PeerId{*PeerIdFromUint64(1), *PeerIdFromUint64(2)}, 8, fetch)
		require.Len(t, out, 2)
		require.Equal(t, balA, out[h0])
		require.Equal(t, balB, out[h1])
	})

	t.Run("no peer serves -> empty result", func(t *testing.T) {
		fetch := func(_ context.Context, rs []BALRequest, _ *PeerId) (map[common.Hash]*types.BlockAccessListSidecar, int) {
			return nil, len(rs)
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
		fetch := func(_ context.Context, rs []BALRequest, _ *PeerId) (map[common.Hash]*types.BlockAccessListSidecar, int) {
			out := map[common.Hash]*types.BlockAccessListSidecar{}
			for _, r := range rs[:min(2, len(rs))] {
				out[r.Hash] = balA
			}
			return out, min(2, len(rs))
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
		fetch := func(_ context.Context, rs []BALRequest, p *PeerId) (map[common.Hash]*types.BlockAccessListSidecar, int) {
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
			return out, len(rs)
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
		fetch := func(_ context.Context, rs []BALRequest, _ *PeerId) (map[common.Hash]*types.BlockAccessListSidecar, int) {
			calls.Add(1)
			return map[common.Hash]*types.BlockAccessListSidecar{h0: balA}, len(rs)
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
		fetch := func(_ context.Context, rs []BALRequest, _ *PeerId) (map[common.Hash]*types.BlockAccessListSidecar, int) {
			n := live.Add(1)
			for {
				if p := peak.Load(); n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			live.Add(-1)
			return nil, len(rs) // none serve, so every peer is tried
		}
		fetchAcrossPeers(context.Background(), reqs,
			[]PeerId{*PeerIdFromUint64(1), *PeerIdFromUint64(2), *PeerIdFromUint64(3), *PeerIdFromUint64(4)}, 2, fetch)
		require.LessOrEqual(t, peak.Load(), int32(2)) // limit 2 enforced (would reach 4 unbounded)
	})
}
