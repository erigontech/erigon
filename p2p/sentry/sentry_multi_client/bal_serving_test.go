package sentry_multi_client

import (
	"context"
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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

type balGetterFunc func(context.Context, *chain.Config, kv.TemporalTx, common.Hash, uint64) ([]byte, error)

func (f balGetterFunc) GetBlockAccessListBytes(ctx context.Context, cfg *chain.Config, tx kv.TemporalTx, hash common.Hash, number uint64) ([]byte, error) {
	return f(ctx, cfg, tx, hash, number)
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

func queryBALs(t *testing.T, cs *MultiClient, query eth.GetBlockAccessListsPacket) []rlp.RawValue {
	t.Helper()
	request := eth.GetBlockAccessListsPacket66{RequestId: 42, GetBlockAccessListsPacket: query}
	encoded, err := rlp.EncodeToBytes(request)
	require.NoError(t, err)
	var response eth.BlockAccessListsPacket66
	sentry := &mockSentryClient{
		sendMessageByIdFunc: func(ctx context.Context, req *sentryproto.SendMessageByIdRequest, _ ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			require.NoError(t, ctx.Err(), "the reply must not use the expired replay context")
			require.NoError(t, rlp.DecodeBytes(req.Data.Data, &response))
			return &sentryproto.SentPeers{}, nil
		},
	}
	require.NoError(t, cs.getBlockAccessLists71(t.Context(), &sentryproto.InboundMessage{Data: encoded}, sentry))
	require.Equal(t, request.RequestId, response.RequestId)
	return response.BlockAccessListsPacket
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
		response := queryBALs(t, cs, eth.GetBlockAccessListsPacket{{1}, {2}, {3}})
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

		require.Equal(t, []rlp.RawValue{{0xc0}, {0x80}}, queryBALs(t, cs, eth.GetBlockAccessListsPacket{{1}, {99}}))
		require.Zero(t, calls, "stored and unknown blocks must not consume the replay budget")
		require.Equal(t, []rlp.RawValue{{0xc0}, {0xc0}}, queryBALs(t, cs, eth.GetBlockAccessListsPacket{{2}, {3}}))
		require.Equal(t, 2, calls, "one replay budget covers a batch")

		require.Empty(t, queryBALs(t, cs, eth.GetBlockAccessListsPacket{{2}}))
		require.Equal(t, []rlp.RawValue{{0xc0}}, queryBALs(t, cs, eth.GetBlockAccessListsPacket{{1}, {3}}))
		require.Equal(t, 2, calls, "throttled requests must not replay blocks")

		time.Sleep(time.Second)
		require.Equal(t, []rlp.RawValue{{0xc0}}, queryBALs(t, cs, eth.GetBlockAccessListsPacket{{2}}))
		require.Equal(t, 3, calls, "replay must resume after the budget refills")
	})
}

func TestGetBlockAccessLists71_ReplayRemainsExclusiveAfterDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cs := newBALTestClient(t)
		var calls atomic.Int32
		deadlineReached := make(chan struct{})
		release := make(chan struct{})
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
		firstResponse := make(chan []rlp.RawValue, 1)
		go func() {
			firstResponse <- queryBALs(t, cs, eth.GetBlockAccessListsPacket{{1}, {2}})
		}()
		<-deadlineReached
		time.Sleep(time.Second)

		assert.Equal(t, []rlp.RawValue{{0xc0}}, queryBALs(t, cs, eth.GetBlockAccessListsPacket{{1}, {3}}))
		assert.Equal(t, int32(1), calls.Load(), "a refilled rate budget must not allow overlapping replay")
		close(release)
		require.Equal(t, []rlp.RawValue{{0xc0}}, <-firstResponse)

		require.Equal(t, []rlp.RawValue{{0xc0}}, queryBALs(t, cs, eth.GetBlockAccessListsPacket{{3}}))
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
