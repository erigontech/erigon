package sentry_multi_client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/bal"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

type balGetterFunc func(context.Context, *chain.Config, kv.TemporalTx, common.Hash, uint64) ([]byte, error)

func (f balGetterFunc) GetBlockAccessListBytes(ctx context.Context, cfg *chain.Config, tx kv.TemporalTx, hash common.Hash, number uint64, beforeReplay func() error) ([]byte, error) {
	if beforeReplay != nil {
		if err := beforeReplay(); err != nil {
			return nil, err
		}
	}
	return f(ctx, cfg, tx, hash, number)
}

type cachedBALGetter struct {
	balGetterFunc
	cached map[common.Hash][]byte
}

func (g cachedBALGetter) GetBlockAccessListBytes(ctx context.Context, cfg *chain.Config, tx kv.TemporalTx, hash common.Hash, number uint64, beforeReplay func() error) ([]byte, error) {
	if bal, ok := g.cached[hash]; ok {
		return bal, nil
	}
	return g.balGetterFunc.GetBlockAccessListBytes(ctx, cfg, tx, hash, number, beforeReplay)
}

func (m *balHeaderNumberReader) TxnumReader() rawdbv3.TxNumsReader {
	return rawdbv3.TxNums
}

func newBALTestClient(t *testing.T) *MultiClient {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return rawdb.WriteBlockAccessListBytes(tx, common.Hash{1}, 1, []byte{0xc0})
	}))
	reader := &balHeaderNumberReader{byHash: map[common.Hash]uint64{{1}: 1, {2}: 2, {3}: 3}}
	cs, err := NewMultiClient(dirs, db, chain.AllProtocolChanges, nil, nil, reader, nil, false, log.New())
	require.NoError(t, err)
	return cs
}

func queryBALs(ctx context.Context, cs *MultiClient, query eth.GetBlockAccessListsPacket) ([]rlp.RawValue, error) {
	request := eth.GetBlockAccessListsPacket66{RequestId: 42, GetBlockAccessListsPacket: query}
	encoded, err := rlp.EncodeToBytes(request)
	if err != nil {
		return nil, err
	}
	var response eth.BlockAccessListsPacket66
	sentry := &mockSentryClient{
		sendMessageByIdFunc: func(ctx context.Context, req *sentryproto.SendMessageByIdRequest, _ ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			if err := ctx.Err(); err != nil {
				return nil, fmt.Errorf("reply uses an expired context: %w", err)
			}
			return &sentryproto.SentPeers{}, rlp.DecodeBytes(req.Data.Data, &response)
		},
	}
	if err := cs.getBlockAccessLists71(ctx, &sentryproto.InboundMessage{Data: encoded}, sentry); err != nil {
		return nil, err
	}
	if request.RequestId != response.RequestId {
		return nil, fmt.Errorf("response request ID %d, want %d", response.RequestId, request.RequestId)
	}
	return response.BlockAccessListsPacket, nil
}

func requireBALs(t *testing.T, cs *MultiClient, query eth.GetBlockAccessListsPacket) []rlp.RawValue {
	t.Helper()
	response, err := queryBALs(t.Context(), cs, query)
	require.NoError(t, err)
	return response
}

func TestGetBlockAccessLists71_ReplayDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cs := newBALTestClient(t)
		calls := 0
		cs.balGenerator = balGetterFunc(func(ctx context.Context, _ *chain.Config, _ kv.TemporalTx, _ common.Hash, _ uint64) ([]byte, error) {
			calls++
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Error("BAL regeneration has no request deadline")
				return nil, context.DeadlineExceeded
			}
			require.LessOrEqual(t, time.Until(deadline), time.Second)
			<-ctx.Done()
			return nil, ctx.Err()
		})
		response := requireBALs(t, cs, eth.GetBlockAccessListsPacket{{1}, {2}, {3}})
		require.Equal(t, []rlp.RawValue{{0xc0}}, response)
		require.Equal(t, 1, calls, "a timed-out request must not start another replay")
	})
}

func TestGetBlockAccessLists71_ReplayRateLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cs := newBALTestClient(t)
		calls := 0
		cs.balGenerator = balGetterFunc(func(context.Context, *chain.Config, kv.TemporalTx, common.Hash, uint64) ([]byte, error) {
			calls++
			return []byte{0xc0}, nil
		})

		require.Equal(t, []rlp.RawValue{{0xc0}, {0x80}}, requireBALs(t, cs, eth.GetBlockAccessListsPacket{{1}, {99}}))
		require.Zero(t, calls, "stored and unknown blocks must not consume the replay budget")
		require.Equal(t, []rlp.RawValue{{0xc0}, {0xc0}}, requireBALs(t, cs, eth.GetBlockAccessListsPacket{{2}, {3}}))
		require.Equal(t, 2, calls, "one replay budget covers a batch")

		require.Empty(t, requireBALs(t, cs, eth.GetBlockAccessListsPacket{{2}}))
		require.Equal(t, []rlp.RawValue{{0xc0}}, requireBALs(t, cs, eth.GetBlockAccessListsPacket{{1}, {3}}))
		require.Equal(t, 2, calls, "throttled requests must not replay blocks")

		time.Sleep(time.Second)
		require.Equal(t, []rlp.RawValue{{0xc0}}, requireBALs(t, cs, eth.GetBlockAccessListsPacket{{2}}))
		require.Equal(t, 3, calls, "replay must resume after the budget refills")
	})
}

type balPreflightReader struct {
	dbservices.FullBlockReader
	header *types.Header
}

func (r balPreflightReader) Header(context.Context, kv.Getter, common.Hash, uint64) (*types.Header, error) {
	return r.header, nil
}

func (r balPreflightReader) CanonicalHash(context.Context, kv.Getter, uint64) (common.Hash, bool, error) {
	return common.Hash{99}, true, nil
}

func TestGetBlockAccessLists71_PreflightBypassesReplayBudget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header *types.Header
	}{
		{name: "missing header"},
		{name: "pre-Amsterdam", header: &types.Header{}},
		{name: "non-canonical", header: &types.Header{BlockAccessListHash: &common.Hash{8}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cs := newBALTestClient(t)
				reader := balPreflightReader{FullBlockReader: cs.blockReader, header: tc.header}
				cs.balGenerator = bal.NewRegenerator(reader, nil, log.New())
				query := make(eth.GetBlockAccessListsPacket, eth.MaxBlockAccessListsRegenerate+1)
				unavailable := make([]rlp.RawValue, len(query))
				for i := range query {
					query[i] = common.Hash{2}
					unavailable[i] = rlp.RawValue{0x80}
				}

				require.Equal(t, unavailable, requireBALs(t, cs, query), "rejected preflight must not consume the per-request replay count")
				require.True(t, cs.balReplayLimiter.Allow(), "rejected preflight must leave the replay budget available")
				require.Equal(t, unavailable, requireBALs(t, cs, query), "preflight must still answer while replay is throttled")
			})
		})
	}
}

func TestGetBlockAccessLists71_CompletedReplayAfterDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cs := newBALTestClient(t)
		calls := 0
		cs.balGenerator = balGetterFunc(func(ctx context.Context, _ *chain.Config, _ kv.TemporalTx, _ common.Hash, _ uint64) ([]byte, error) {
			calls++
			<-ctx.Done()
			return []byte{0xc0}, nil
		})

		response := requireBALs(t, cs, eth.GetBlockAccessListsPacket{{1}, {2}, {3}})
		require.Equal(t, []rlp.RawValue{{0xc0}, {0xc0}}, response, "keep a completed BAL even if the deadline expires before it is returned")
		require.Equal(t, 1, calls, "a timed-out request must not start another replay")
	})
}

func TestGetBlockAccessLists71_CacheBypassesReplayBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cs := newBALTestClient(t)
		calls := 0
		cs.balGenerator = cachedBALGetter{
			cached: map[common.Hash][]byte{{2}: {0xc0}},
			balGetterFunc: func(context.Context, *chain.Config, kv.TemporalTx, common.Hash, uint64) ([]byte, error) {
				calls++
				return []byte{0xc0}, nil
			},
		}

		require.Equal(t, []rlp.RawValue{{0xc0}}, requireBALs(t, cs, eth.GetBlockAccessListsPacket{{2}}))
		require.Zero(t, calls, "a cache hit must not start replay")
		require.Equal(t, []rlp.RawValue{{0xc0}}, requireBALs(t, cs, eth.GetBlockAccessListsPacket{{3}}))
		require.Equal(t, 1, calls, "a cache hit must leave the replay budget available")

		require.Equal(t, []rlp.RawValue{{0xc0}}, requireBALs(t, cs, eth.GetBlockAccessListsPacket{{2}, {3}}))
		require.Equal(t, 1, calls, "cached BALs must remain available while replay is throttled")
	})
}

func TestGetBlockAccessLists71_ReplayRemainsExclusiveAfterDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cs := newBALTestClient(t)
		var calls atomic.Int32
		deadlineReached := make(chan struct{})
		release := make(chan struct{})
		unblockReplay := sync.OnceFunc(func() { close(release) })
		defer unblockReplay()
		cs.balGenerator = balGetterFunc(func(ctx context.Context, _ *chain.Config, _ kv.TemporalTx, _ common.Hash, _ uint64) ([]byte, error) {
			if calls.Add(1) == 1 {
				<-ctx.Done()
				close(deadlineReached)
				// Cancellation cannot interrupt every database operation immediately.
				<-release
				return nil, ctx.Err()
			}
			return []byte{0xc0}, nil
		})
		var firstResponse []rlp.RawValue
		firstDone := make(chan error, 1)
		go func() {
			var err error
			firstResponse, err = queryBALs(t.Context(), cs, eth.GetBlockAccessListsPacket{{1}, {2}})
			firstDone <- err
		}()
		select {
		case <-deadlineReached:
		case err := <-firstDone:
			require.NoError(t, err)
			t.Fatal("replay returned before reaching its deadline")
		}
		time.Sleep(time.Second)

		assert.Equal(t, []rlp.RawValue{{0xc0}}, requireBALs(t, cs, eth.GetBlockAccessListsPacket{{1}, {3}}))
		assert.Equal(t, int32(1), calls.Load(), "a refilled rate budget must not allow overlapping replay")
		unblockReplay()
		require.NoError(t, <-firstDone)
		require.Equal(t, []rlp.RawValue{{0xc0}}, firstResponse)

		require.Equal(t, []rlp.RawValue{{0xc0}}, requireBALs(t, cs, eth.GetBlockAccessListsPacket{{3}}))
		require.Equal(t, int32(2), calls.Load(), "the replay slot must be released when work actually finishes")
	})
}

func TestUploadStreams_SeparateBALReplay(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var subscribed []sentryproto.MessageId
	sentry := &mockSentryClient{
		handShakeFunc: func(context.Context, *emptypb.Empty, ...grpc.CallOption) (*sentryproto.HandShakeReply, error) {
			return &sentryproto.HandShakeReply{}, nil
		},
		setStatusFunc: func(context.Context, *sentryproto.StatusData, ...grpc.CallOption) (*sentryproto.SetStatusReply, error) {
			return &sentryproto.SetStatusReply{}, nil
		},
		messagesFunc: func(_ context.Context, req *sentryproto.MessagesRequest, _ ...grpc.CallOption) (sentryproto.Sentry_MessagesClient, error) {
			subscribed = req.Ids
			cancel()
			return nil, context.Canceled
		},
	}
	cs := &MultiClient{
		logger: log.New(),
		statusDataProvider: &mockStatusDataProvider{getStatusDataFunc: func(context.Context) (*sentryproto.StatusData, error) {
			return &sentryproto.StatusData{}, nil
		}},
	}
	cs.RecvUploadMessageLoop(ctx, sentry, nil)
	require.Contains(t, subscribed, sentryproto.MessageId_GET_BLOCK_BODIES_66)
	require.Contains(t, subscribed, sentryproto.MessageId_GET_RECEIPTS_66)
	require.NotContains(t, subscribed, sentryproto.MessageId_GET_BLOCK_ACCESS_LISTS_71, "BAL replay must not block bodies or receipts")

	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	cs.RecvUploadBlockAccessListsMessageLoop(ctx, sentry, nil)
	require.Equal(t, []sentryproto.MessageId{sentryproto.MessageId_GET_BLOCK_ACCESS_LISTS_71}, subscribed)
}
