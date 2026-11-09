package sentry_multi_client

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
)

type uploadBlockReader struct {
	balHeaderNumberReader
	body func(context.Context, uint64) (rlp.RawValue, error)
}

func (r uploadBlockReader) BlockWithSenders(_ context.Context, _ kv.Getter, _ common.Hash, number uint64) (*types.Block, []common.Address, error) {
	header := &types.Header{}
	header.Number.SetUint64(number)
	return types.NewBlockWithHeader(header, nil), nil, nil
}

func (r uploadBlockReader) BodyRlp(ctx context.Context, _ kv.Getter, _ common.Hash, number uint64) (rlp.RawValue, error) {
	if r.body != nil {
		return r.body(ctx, number)
	}
	return rlp.EncodeToBytes(&types.RawBody{})
}

type uploadReceiptsGetter struct {
	getReceipts func(context.Context, *types.Block) (types.Receipts, error)
	cached      map[common.Hash]types.Receipts
}

func (g uploadReceiptsGetter) GetCachedReceipts(_ context.Context, hash common.Hash) (types.Receipts, bool) {
	receipts, ok := g.cached[hash]
	return receipts, ok
}

func (g uploadReceiptsGetter) GetReceipts(ctx context.Context, _ *chain.Config, _ kv.TemporalTx, block *types.Block, _ eth.ReceiptsOpts) (types.Receipts, error) {
	return g.getReceipts(ctx, block)
}

func newUploadTestClient(t *testing.T) *MultiClient {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	reader := &uploadBlockReader{balHeaderNumberReader: balHeaderNumberReader{byHash: map[common.Hash]uint64{{1}: 1, {2}: 2}}}
	cs, err := NewMultiClient(dirs, db, chain.AllProtocolChanges, nil, nil, reader, &mockStatusDataProvider{
		getStatusDataFunc: func(context.Context) (*sentryproto.StatusData, error) {
			return &sentryproto.StatusData{}, nil
		},
	}, false, log.New())
	require.NoError(t, err)
	return cs
}

func startUploadTestStream(t *testing.T, ctx context.Context, cs *MultiClient, wg *sync.WaitGroup, send func(*sentryproto.SendMessageByIdRequest)) *libsentry.SentryStreamS[*sentryproto.InboundMessage] {
	t.Helper()
	s, c := libsentry.NewSentryStream[*sentryproto.InboundMessage](ctx)
	sentry := &mockSentryClient{
		handShakeFunc: func(context.Context, *emptypb.Empty, ...grpc.CallOption) (*sentryproto.HandShakeReply, error) {
			return &sentryproto.HandShakeReply{}, nil
		},
		setStatusFunc: func(context.Context, *sentryproto.StatusData, ...grpc.CallOption) (*sentryproto.SetStatusReply, error) {
			return &sentryproto.SetStatusReply{}, nil
		},
		messagesFunc: func(context.Context, *sentryproto.MessagesRequest, ...grpc.CallOption) (sentryproto.Sentry_MessagesClient, error) {
			return c, nil
		},
		sendMessageByIdFunc: func(_ context.Context, request *sentryproto.SendMessageByIdRequest, _ ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			send(request)
			return &sentryproto.SentPeers{}, nil
		},
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		cs.RecvUploadMessageLoop(ctx, sentry, wg)
	}()
	t.Cleanup(func() { <-done })
	return s
}

func uploadRequest(t *testing.T, id sentryproto.MessageId, peer byte, requestID uint64, hashes ...common.Hash) *sentryproto.InboundMessage {
	t.Helper()
	var packet any = eth.GetReceiptsPacket66{RequestId: requestID, GetReceiptsPacket: hashes}
	if id == sentryproto.MessageId_GET_RECEIPTS_70 {
		packet = eth.GetReceiptsPacket70{RequestId: requestID, GetReceiptsPacket: hashes}
	}
	data, err := rlp.EncodeToBytes(packet)
	require.NoError(t, err)
	return &sentryproto.InboundMessage{Id: id, Data: data, PeerId: gointerfaces.ConvertHashToH512([64]byte{peer})}
}

func requireUploadResponse(t *testing.T, response *sentryproto.SendMessageByIdRequest, requestID uint64) {
	t.Helper()
	if response.Data.Id == sentryproto.MessageId_RECEIPTS_70 {
		var packet eth.ReceiptsRLPPacket70
		require.NoError(t, rlp.DecodeBytes(response.Data.Data, &packet))
		require.Equal(t, requestID, packet.RequestId)
		require.False(t, packet.LastBlockIncomplete)
		require.Len(t, packet.ReceiptsRLPPacket, 1)
	} else {
		var packet eth.ReceiptsRLPPacket66
		require.NoError(t, rlp.DecodeBytes(response.Data.Data, &packet))
		require.Equal(t, requestID, packet.RequestId)
		require.Len(t, packet.ReceiptsRLPPacket, 1)
	}
}

type uploadCountingDB struct {
	kv.TemporalRoDB
	reads atomic.Int32
}

func (db *uploadCountingDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	db.reads.Add(1)
	return db.TemporalRoDB.BeginTemporalRo(ctx)
}

func TestUploadRequests_ReplaysLeaveWorkersForBodiesAndCacheHits(t *testing.T) {
	for _, id := range []sentryproto.MessageId{sentryproto.MessageId_GET_RECEIPTS_66, sentryproto.MessageId_GET_RECEIPTS_69, sentryproto.MessageId_GET_RECEIPTS_70} {
		t.Run(id.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				cs := newUploadTestClient(t)
				db := &uploadCountingDB{TemporalRoDB: cs.db}
				cs.db = db
				release := make(chan struct{})
				var calls atomic.Int32
				cs.ethApiWrapper = uploadReceiptsGetter{
					cached: map[common.Hash]types.Receipts{{2}: {{CumulativeGasUsed: 21_000}}},
					getReceipts: func(ctx context.Context, _ *types.Block) (types.Receipts, error) {
						calls.Add(1)
						select {
						case <-release:
							return types.Receipts{{CumulativeGasUsed: 21_000}}, nil
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					},
				}
				var handled sync.WaitGroup
				responses := make(chan *sentryproto.SendMessageByIdRequest, 6)
				send := func(response *sentryproto.SendMessageByIdRequest) { responses <- response }
				first := startUploadTestStream(t, ctx, cs, &handled, send)
				second := startUploadTestStream(t, ctx, cs, &handled, send)
				handled.Add(1)
				require.NoError(t, first.Send(uploadRequest(t, id, 1, 1, common.Hash{1})))
				synctest.Wait()
				for peer := byte(2); peer <= 4; peer++ {
					handled.Add(1)
					require.NoError(t, second.Send(uploadRequest(t, id, peer, uint64(peer), common.Hash{1})))
				}
				synctest.Wait()
				require.Equal(t, int32(1), calls.Load(), "all sentries must share one uncached receipt slot")
				require.Equal(t, int32(1), db.reads.Load(), "waiting replays must not open read transactions")

				handled.Add(2)
				require.NoError(t, second.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, 5, 5, common.Hash{2})))
				require.NoError(t, second.Send(uploadRequest(t, id, 6, 6, common.Hash{2})))
				synctest.Wait()
				require.Len(t, responses, 2, "waiting replays must leave workers available for bodies and cache hits")
				for range 2 {
					response := <-responses
					if response.Data.Id == sentryproto.MessageId_BLOCK_BODIES_66 {
						requireUploadResponse(t, response, 5)
					} else {
						requireUploadResponse(t, response, 6)
					}
				}
				close(release)
				handled.Wait()
				require.Equal(t, int32(4), calls.Load())
				require.Len(t, responses, 4, "waiting replays must eventually receive replies")
			})
		})
	}
}

func TestUploadRequests_SlowPeerDoesNotBlockOtherPeers(t *testing.T) {
	for _, id := range []sentryproto.MessageId{sentryproto.MessageId_GET_RECEIPTS_66, sentryproto.MessageId_GET_RECEIPTS_69, sentryproto.MessageId_GET_RECEIPTS_70} {
		t.Run(id.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				cs := newUploadTestClient(t)
				release := make(chan struct{})
				var slowCalls atomic.Int32
				cs.ethApiWrapper = uploadReceiptsGetter{cached: map[common.Hash]types.Receipts{{2}: {{CumulativeGasUsed: 21_000}}}, getReceipts: func(ctx context.Context, block *types.Block) (types.Receipts, error) {
					if block.NumberU64() == 1 {
						slowCalls.Add(1)
						select {
						case <-release:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					return types.Receipts{{CumulativeGasUsed: 21_000}}, nil
				}}
				responses := make(chan *sentryproto.SendMessageByIdRequest, 8)
				var handled sync.WaitGroup
				stream := startUploadTestStream(t, ctx, cs, &handled, func(response *sentryproto.SendMessageByIdRequest) { responses <- response })
				for range 8 {
					handled.Add(1)
					require.NoError(t, stream.Send(uploadRequest(t, id, 1, 1, common.Hash{1})))
				}
				synctest.Wait()

				handled.Add(2)
				require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, 2, 2, common.Hash{2})))
				require.NoError(t, stream.Send(uploadRequest(t, id, 3, 3, common.Hash{2})))
				synctest.Wait()
				require.Equal(t, int32(1), slowCalls.Load(), "one peer must not occupy all upload slots")
				require.Len(t, responses, 2, "other peers must receive bodies and cached receipts while the first peer is still busy")
				for range 2 {
					response := <-responses
					if response.Data.Id == sentryproto.MessageId_BLOCK_BODIES_66 {
						requireUploadResponse(t, response, 2)
					} else {
						requireUploadResponse(t, response, 3)
					}
				}

				close(release)
				handled.Wait()
				require.Len(t, responses, 5, "serve the active request and four queued requests from the busy peer")
				for range 5 {
					requireUploadResponse(t, <-responses, 1)
				}
			})
		})
	}
}

func TestUploadRequests_LimitsSharedAcrossSentries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		cs := newUploadTestClient(t)
		release := make(chan struct{})
		var calls atomic.Int32
		cs.blockReader.(*uploadBlockReader).body = func(_ context.Context, _ uint64) (rlp.RawValue, error) {
			calls.Add(1)
			select {
			case <-release:
				return rlp.EncodeToBytes(&types.RawBody{})
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		var handled sync.WaitGroup
		responses := make(chan *sentryproto.SendMessageByIdRequest, 10)
		send := func(response *sentryproto.SendMessageByIdRequest) { responses <- response }
		first := startUploadTestStream(t, ctx, cs, &handled, send)
		second := startUploadTestStream(t, ctx, cs, &handled, send)

		handled.Add(2)
		require.NoError(t, first.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, 1, 1, common.Hash{1})))
		synctest.Wait()
		require.NoError(t, second.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, 1, 2, common.Hash{1})))
		synctest.Wait()
		require.Equal(t, int32(1), calls.Load(), "the same peer on another sentry must not get a second slot")

		for peer := byte(2); peer <= 8; peer++ {
			handled.Add(1)
			stream := first
			if peer%2 == 0 {
				stream = second
			}
			require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, peer, uint64(peer), common.Hash{1})))
		}
		synctest.Wait()
		require.Equal(t, int32(4), calls.Load(), "all sentries must share the four upload slots")

		close(release)
		handled.Wait()
		require.Len(t, responses, 9, "queued requests must drain after the four active handlers finish")
		handled.Add(1)
		require.NoError(t, first.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, 1, 10, common.Hash{1})))
		handled.Wait()
		require.Equal(t, int32(10), calls.Load(), "completed work must release its peer and global slot")
	})
}

func TestUploadRequests_CancellationKeepsSlotUntilHandlerReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		firstCtx, cancelFirst := context.WithCancel(ctx)
		defer cancelFirst()
		cs := newUploadTestClient(t)
		release := make(chan struct{})
		defer close(release)
		var calls atomic.Int32
		cs.ethApiWrapper = uploadReceiptsGetter{cached: map[common.Hash]types.Receipts{{2}: {{CumulativeGasUsed: 21_000}}}, getReceipts: func(ctx context.Context, _ *types.Block) (types.Receipts, error) {
			if calls.Add(1) == 1 {
				<-ctx.Done()
				<-release
				return nil, ctx.Err()
			}
			return types.Receipts{{CumulativeGasUsed: 21_000}}, nil
		}}
		responses := make(chan *sentryproto.SendMessageByIdRequest, 3)
		send := func(response *sentryproto.SendMessageByIdRequest) { responses <- response }
		first := startUploadTestStream(t, firstCtx, cs, nil, send)
		second := startUploadTestStream(t, ctx, cs, nil, send)
		require.NoError(t, first.Send(uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 1, 1, common.Hash{1})))
		synctest.Wait()
		cancelFirst()
		synctest.Wait()

		require.NoError(t, second.Send(uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 1, 2, common.Hash{1})))
		require.NoError(t, second.Send(uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 2, 5, common.Hash{1})))
		require.NoError(t, second.Send(uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 3, 3, common.Hash{2})))
		synctest.Wait()
		require.Equal(t, int32(1), calls.Load(), "cancellation must keep the replay slot and leave cache hits free to run")
		require.Len(t, responses, 1)
		requireUploadResponse(t, <-responses, 3)

		release <- struct{}{}
		synctest.Wait()
		require.Len(t, responses, 2)
		requireUploadResponse(t, <-responses, 5)
		requireUploadResponse(t, <-responses, 2)
		require.NoError(t, second.Send(uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 1, 4, common.Hash{1})))
		synctest.Wait()
		require.Len(t, responses, 1)
		requireUploadResponse(t, <-responses, 4)
	})
}

func TestUploadRequests_PipelinedRequestsBothAnswered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		cs := newUploadTestClient(t)
		release := make(chan struct{})
		cs.ethApiWrapper = uploadReceiptsGetter{getReceipts: func(ctx context.Context, _ *types.Block) (types.Receipts, error) {
			select {
			case <-release:
				return types.Receipts{{CumulativeGasUsed: 21_000}}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}
		var handled sync.WaitGroup
		responses := make(chan *sentryproto.SendMessageByIdRequest, 2)
		stream := startUploadTestStream(t, ctx, cs, &handled, func(response *sentryproto.SendMessageByIdRequest) { responses <- response })
		handled.Add(2)
		require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 1, 100, common.Hash{1})))
		synctest.Wait()
		require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, 1, 101, common.Hash{2})))
		synctest.Wait()
		close(release)
		handled.Wait()
		require.Len(t, responses, 2, "one peer's concurrent bodies and receipts requests must both get a reply")
		requireUploadResponse(t, <-responses, 100)
		requireUploadResponse(t, <-responses, 101)
	})
}

func TestUploadRequests_SmallBurstAllAnswered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		cs := newUploadTestClient(t)
		release := make(chan struct{})
		cs.ethApiWrapper = uploadReceiptsGetter{getReceipts: func(ctx context.Context, _ *types.Block) (types.Receipts, error) {
			select {
			case <-release:
				return types.Receipts{{CumulativeGasUsed: 21_000}}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}
		var handled sync.WaitGroup
		responses := make(chan *sentryproto.SendMessageByIdRequest, 5)
		stream := startUploadTestStream(t, ctx, cs, &handled, func(response *sentryproto.SendMessageByIdRequest) { responses <- response })
		for peer := byte(1); peer <= 5; peer++ {
			handled.Add(1)
			require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, peer, uint64(peer), common.Hash{1})))
		}
		synctest.Wait()
		close(release)
		handled.Wait()
		require.Len(t, responses, 5, "a small burst must drain after active work finishes")
	})
}

func TestUploadRequests_WaitingPeersTakeTurns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		cs := newUploadTestClient(t)
		releases := make([]chan struct{}, 6)
		for peer := byte(1); peer <= 5; peer++ {
			cs.blockReader.(*uploadBlockReader).byHash[common.Hash{peer}] = uint64(peer)
			releases[peer] = make(chan struct{})
		}
		started := make(chan uint64, 6)
		cs.blockReader.(*uploadBlockReader).body = func(_ context.Context, number uint64) (rlp.RawValue, error) {
			started <- number
			select {
			case <-releases[number]:
				return rlp.EncodeToBytes(&types.RawBody{})
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		var handled sync.WaitGroup
		responses := make(chan *sentryproto.SendMessageByIdRequest, 6)
		stream := startUploadTestStream(t, ctx, cs, &handled, func(response *sentryproto.SendMessageByIdRequest) { responses <- response })
		for peer := byte(1); peer <= 4; peer++ {
			handled.Add(1)
			require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, peer, uint64(peer), common.Hash{peer})))
		}
		synctest.Wait()
		require.Len(t, started, 4)
		for range 4 {
			<-started
		}
		handled.Add(2)
		require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, 1, 6, common.Hash{1})))
		synctest.Wait()
		require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, 5, 5, common.Hash{5})))
		synctest.Wait()

		close(releases[1])
		synctest.Wait()
		require.Len(t, started, 1)
		require.Equal(t, uint64(5), <-started, "a waiting peer must run before a busy peer's next request")
		close(releases[5])
		synctest.Wait()
		require.Len(t, started, 1)
		require.Equal(t, uint64(1), <-started)
		for peer := 2; peer <= 4; peer++ {
			close(releases[peer])
		}
		handled.Wait()
		require.Len(t, responses, 6)
	})
}

func TestUploadRequests_CancellationRemovesWaitingWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		waitingCtx, cancelWaiting := context.WithCancel(ctx)
		defer cancelWaiting()
		cs := newUploadTestClient(t)
		release := make(chan struct{})
		cs.ethApiWrapper = uploadReceiptsGetter{getReceipts: func(ctx context.Context, _ *types.Block) (types.Receipts, error) {
			select {
			case <-release:
				return types.Receipts{{CumulativeGasUsed: 21_000}}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}
		var active, waiting sync.WaitGroup
		responses := make(chan *sentryproto.SendMessageByIdRequest, 13)
		send := func(response *sentryproto.SendMessageByIdRequest) { responses <- response }
		first := startUploadTestStream(t, ctx, cs, &active, send)
		second := startUploadTestStream(t, waitingCtx, cs, &waiting, send)
		for peer := byte(1); peer <= 4; peer++ {
			active.Add(1)
			require.NoError(t, first.Send(uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, peer, uint64(peer), common.Hash{1})))
		}
		synctest.Wait()
		for peer := byte(1); peer <= 8; peer++ {
			waiting.Add(1)
			require.NoError(t, second.Send(uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, peer, uint64(peer)+4, common.Hash{1})))
		}
		waitingDone := make(chan struct{})
		go func() {
			waiting.Wait()
			close(waitingDone)
		}()
		synctest.Wait()
		select {
		case <-waitingDone:
			t.Fatal("uncached requests must wait while the replay slot is busy")
		default:
		}
		active.Add(1)
		require.NoError(t, first.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, 5, 13, common.Hash{2})))
		synctest.Wait()
		require.Empty(t, responses)
		cancelWaiting()
		synctest.Wait()
		select {
		case <-waitingDone:
		default:
			t.Fatal("cancelled requests must finish without waiting for unrelated active work")
		}
		require.Len(t, responses, 1, "removing a waiting replay must let the peer's next request run")
		requireUploadResponse(t, <-responses, 13)
		close(release)
		active.Wait()
		require.Len(t, responses, 4, "cancelled requests must not be served")
	})
}

func TestUploadRequests_QueueLimits(t *testing.T) {
	for _, tc := range []struct {
		name   string
		replay bool
		bytes  bool
	}{
		{name: "count"},
		{name: "bytes", bytes: true},
		{name: "replay_count", replay: true},
		{name: "replay_bytes", replay: true, bytes: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				cs := newUploadTestClient(t)
				cs.blockReader.(*uploadBlockReader).body = func(_ context.Context, _ uint64) (rlp.RawValue, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				active := byte(4)
				id := sentryproto.MessageId_GET_BLOCK_BODIES_66
				if tc.replay {
					active = 1
					id = sentryproto.MessageId_GET_RECEIPTS_69
					cs.ethApiWrapper = uploadReceiptsGetter{getReceipts: func(ctx context.Context, _ *types.Block) (types.Receipts, error) {
						<-ctx.Done()
						return nil, ctx.Err()
					}}
				}
				stream := startUploadTestStream(t, ctx, cs, nil, func(*sentryproto.SendMessageByIdRequest) {})
				for peer := byte(1); peer <= active; peer++ {
					require.NoError(t, stream.Send(uploadRequest(t, id, peer, uint64(peer), common.Hash{1})))
				}
				synctest.Wait()

				waitingCtx, cancelWaiting := context.WithCancel(ctx)
				defer cancelWaiting()
				defer cs.cancelUploads(waitingCtx)
				message := uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 100, 1, common.Hash{1})
				capacity := libsentry.MessagesQueueSize
				if tc.bytes {
					hashes := make([]common.Hash, 128*1024)
					for i := range hashes {
						hashes[i] = common.Hash{1}
					}
					message = uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 100, 1, hashes...)
					capacity = libsentry.MessagesQueueByteLimit / proto.Size(message)
				}
				var completed atomic.Int32
				done := func() { completed.Add(1) }
				for i := 0; i <= capacity; i++ {
					var peerID [64]byte
					binary.LittleEndian.PutUint64(peerID[:], uint64(i+100))
					request := &sentryproto.InboundMessage{Id: message.Id, Data: message.Data, PeerId: gointerfaces.ConvertHashToH512(peerID)}
					cs.enqueueUpload(uploadTask{ctx: waitingCtx, message: request, done: done})
					synctest.Wait()
				}
				require.Equal(t, int32(1), completed.Load(), "only the request over the limit must be dropped")
				cs.enqueueUpload(uploadTask{ctx: waitingCtx, message: message, done: done})
				synctest.Wait()
				require.Equal(t, int32(2), completed.Load(), "requests behind a waiting replay must also obey the queue limit")
				cancelWaiting()
				cs.cancelUploads(waitingCtx)
				require.Equal(t, int32(capacity+2), completed.Load())

				nextCtx, cancelNext := context.WithCancel(ctx)
				defer cancelNext()
				defer cs.cancelUploads(nextCtx)
				cs.enqueueUpload(uploadTask{ctx: nextCtx, message: message, done: done})
				synctest.Wait()
				require.Equal(t, int32(capacity+2), completed.Load(), "cancellation must free the queue budget for new work")
				cancelNext()
				cs.enqueueUpload(uploadTask{ctx: nextCtx, message: message, done: done})
				require.Equal(t, int32(capacity+3), completed.Load(), "cancelled work must not enter the queue")
			})
		})
	}
}
