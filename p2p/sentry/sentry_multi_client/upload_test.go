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
	"github.com/erigontech/erigon/diagnostics/metrics"
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

func waitForUploadReceipts(release <-chan struct{}) func(context.Context, *types.Block) (types.Receipts, error) {
	return func(ctx context.Context, _ *types.Block) (types.Receipts, error) {
		select {
		case <-release:
			return types.Receipts{{CumulativeGasUsed: 21_000}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
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

func TestUploadRequests_ReceiptsUseFreeReplaySlot(t *testing.T) {
	for _, id := range []sentryproto.MessageId{sentryproto.MessageId_GET_RECEIPTS_66, sentryproto.MessageId_GET_RECEIPTS_69, sentryproto.MessageId_GET_RECEIPTS_70} {
		t.Run(id.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				cs := newUploadTestClient(t)
				cs.blockReader.(*uploadBlockReader).body = func(context.Context, uint64) (rlp.RawValue, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				var cacheLookups, uncachedCalls atomic.Int32
				cs.ethApiWrapper = &mockReceiptsGenerator{
					ReceiptsGetter: uploadReceiptsGetter{getReceipts: func(context.Context, *types.Block) (types.Receipts, error) {
						uncachedCalls.Add(1)
						return types.Receipts{{CumulativeGasUsed: 21_000}}, nil
					}},
					getCachedReceiptsFunc: func(_ context.Context, hash common.Hash) (types.Receipts, bool) {
						cacheLookups.Add(1)
						if hash == (common.Hash{1}) {
							return types.Receipts{{CumulativeGasUsed: 21_000}}, true
						}
						return nil, false
					},
				}
				responses := make(chan *sentryproto.SendMessageByIdRequest, 2)
				stream := startUploadTestStream(t, ctx, cs, nil, func(response *sentryproto.SendMessageByIdRequest) { responses <- response })
				require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, 1, 1, common.Hash{1})))
				synctest.Wait()
				require.NoError(t, stream.Send(uploadRequest(t, id, 2, 2, common.Hash{1}, common.Hash{2})))
				synctest.Wait()
				require.Len(t, responses, 1, "bodies must leave the replay slot available for receipts")
				require.Equal(t, int32(1), uncachedCalls.Load())
				require.Equal(t, int32(2), cacheLookups.Load(), "a free replay slot must avoid repeating the cache pass")
			})
		})
	}
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
				responses := make(chan *sentryproto.SendMessageByIdRequest, 6)
				send := func(response *sentryproto.SendMessageByIdRequest) { responses <- response }
				first := startUploadTestStream(t, ctx, cs, nil, send)
				second := startUploadTestStream(t, ctx, cs, nil, send)
				require.NoError(t, first.Send(uploadRequest(t, id, 1, 1, common.Hash{1})))
				synctest.Wait()
				for peer := byte(2); peer <= 4; peer++ {
					require.NoError(t, second.Send(uploadRequest(t, id, peer, uint64(peer), common.Hash{1})))
				}
				synctest.Wait()
				require.Equal(t, int32(1), calls.Load(), "all sentries must share one uncached receipt slot")
				require.Equal(t, int32(1), db.reads.Load(), "waiting replays must not open read transactions")

				require.NoError(t, second.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, 2, 5, common.Hash{2})))
				require.NoError(t, second.Send(uploadRequest(t, id, 3, 6, common.Hash{2})))
				synctest.Wait()
				require.Len(t, responses, 2, "waiting replays must let the same peers receive bodies and cache hits")
				for range 2 {
					response := <-responses
					if response.Data.Id == sentryproto.MessageId_BLOCK_BODIES_66 {
						requireUploadResponse(t, response, 5)
					} else {
						requireUploadResponse(t, response, 6)
					}
				}
				close(release)
				synctest.Wait()
				require.Equal(t, int32(4), calls.Load())
				require.Len(t, responses, 4, "waiting replays must eventually receive replies")
			})
		})
	}
}

func TestUploadRequests_WaitingReplaysKeepOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		cs := newUploadTestClient(t)
		release := make(chan struct{})
		cs.ethApiWrapper = uploadReceiptsGetter{
			cached:      map[common.Hash]types.Receipts{{2}: {{CumulativeGasUsed: 21_000}}},
			getReceipts: waitForUploadReceipts(release),
		}
		responses := make(chan *sentryproto.SendMessageByIdRequest, 4)
		stream := startUploadTestStream(t, ctx, cs, nil, func(response *sentryproto.SendMessageByIdRequest) { responses <- response })
		require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 1, 1, common.Hash{1})))
		synctest.Wait()
		for requestID := uint64(2); requestID <= 3; requestID++ {
			require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 2, requestID, common.Hash{1})))
			synctest.Wait()
		}
		require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 2, 4, common.Hash{2})))
		synctest.Wait()
		require.Len(t, responses, 1, "cache hits must run past all waiting replays from the same peer")
		requireUploadResponse(t, <-responses, 4)
		close(release)
		synctest.Wait()
		require.Len(t, responses, 3)
		for requestID := uint64(1); requestID <= 3; requestID++ {
			requireUploadResponse(t, <-responses, requestID)
		}
	})
}

func TestUploadRequests_ReplayOrderWhenSlotFreesDuringProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		cs := newUploadTestClient(t)
		release := make(chan struct{})
		probe := make(chan struct{})
		cs.ethApiWrapper = &mockReceiptsGenerator{
			ReceiptsGetter: uploadReceiptsGetter{getReceipts: waitForUploadReceipts(release)},
			getCachedReceiptsFunc: func(ctx context.Context, hash common.Hash) (types.Receipts, bool) {
				if hash == (common.Hash{2}) {
					select {
					case <-probe:
					case <-ctx.Done():
					}
				}
				return nil, false
			},
		}
		responses := make(chan *sentryproto.SendMessageByIdRequest, 3)
		stream := startUploadTestStream(t, ctx, cs, nil, func(response *sentryproto.SendMessageByIdRequest) { responses <- response })
		require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 1, 1, common.Hash{1})))
		synctest.Wait()
		require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 2, 2, common.Hash{2})))
		synctest.Wait()
		require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 2, 3, common.Hash{1})))
		synctest.Wait()
		close(release)
		synctest.Wait()
		require.Len(t, responses, 1)
		close(probe)
		synctest.Wait()
		require.Len(t, responses, 3)
		for requestID := uint64(1); requestID <= 3; requestID++ {
			requireUploadResponse(t, <-responses, requestID)
		}
	})
}

func TestUploadRequests_RetryQueueLimits(t *testing.T) {
	for _, tc := range []struct {
		name          string
		earlierReplay bool
		readyPeer     bool
		wantDrops     uint64
		wantIDs       []uint64
	}{
		{name: "immediate", wantIDs: []uint64{1, 2, 3, 4}},
		{name: "earlier_replay", earlierReplay: true, wantDrops: 1, wantIDs: []uint64{1, 5, 3, 4}},
		{name: "ready_peer", readyPeer: true, wantDrops: 1, wantIDs: []uint64{1, 3, 4, 5, 6, 7, 8}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				cs := newUploadTestClient(t)
				dropped := metrics.GetOrCreateCounter(`p2p_sentry_upload_dropped_requests_total{reason="peer_bytes"}`)
				before := dropped.GetValueUint64()
				replayRelease := make(chan struct{})
				probeRelease := make(chan struct{})
				bodyRelease := make(chan struct{})
				cs.ethApiWrapper = &mockReceiptsGenerator{
					ReceiptsGetter: uploadReceiptsGetter{getReceipts: waitForUploadReceipts(replayRelease)},
					getCachedReceiptsFunc: func(ctx context.Context, hash common.Hash) (types.Receipts, bool) {
						if hash == (common.Hash{2}) {
							select {
							case <-probeRelease:
							case <-ctx.Done():
							}
						}
						return nil, false
					},
				}
				cs.blockReader.(*uploadBlockReader).body = func(_ context.Context, number uint64) (rlp.RawValue, error) {
					if number == 1 {
						select {
						case <-bodyRelease:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					return rlp.EncodeToBytes(&types.RawBody{})
				}
				responses := make(chan *sentryproto.SendMessageByIdRequest, 8)
				stream := startUploadTestStream(t, ctx, cs, nil, func(response *sentryproto.SendMessageByIdRequest) { responses <- response })
				require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 1, 1, common.Hash{1})))
				synctest.Wait()
				if tc.earlierReplay {
					require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 2, 5, common.Hash{1})))
					synctest.Wait()
				}

				hashes := make([]common.Hash, 200_000)
				for i := range hashes {
					hashes[i] = common.Hash{2}
				}
				probe := uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 2, 2, hashes...)
				require.LessOrEqual(t, len(probe.Data), eth.ProtocolMaxMsgSize)
				require.NoError(t, stream.Send(probe))
				synctest.Wait()
				for id := uint64(3); id <= 4; id++ {
					require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, 2, id, hashes...)))
				}
				synctest.Wait()

				close(replayRelease)
				synctest.Wait()
				func() {
					cs.uploads.mu.Lock()
					defer cs.uploads.mu.Unlock()
					require.Equal(t, 1, cs.uploads.active)
					require.False(t, cs.uploads.uncachedActive)
					require.Empty(t, cs.uploads.ready)
					require.Greater(t, cs.uploads.bytes+proto.Size(probe), maxUploadQueuedBytesPerPeer)
				}()
				if tc.readyPeer {
					for peer := byte(3); peer <= 6; peer++ {
						require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, peer, uint64(peer)+2, common.Hash{1})))
					}
					synctest.Wait()
				}
				close(probeRelease)
				synctest.Wait()
				require.Equal(t, before+tc.wantDrops, dropped.GetValueUint64(), "only retries that must wait need waiting-queue space")
				close(bodyRelease)
				synctest.Wait()
				var ids []uint64
				for len(responses) > 0 {
					var response eth.ReceiptsRLPPacket66
					require.NoError(t, rlp.DecodeBytes((<-responses).Data.Data, &response))
					ids = append(ids, response.RequestId)
				}
				if tc.readyPeer {
					require.ElementsMatch(t, tc.wantIDs, ids)
				} else {
					require.Equal(t, tc.wantIDs, ids)
				}
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
				dropped := metrics.GetOrCreateCounter(`p2p_sentry_upload_dropped_requests_total{reason="peer_count"}`)
				before := dropped.GetValueUint64()
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
				require.Equal(t, before+3, dropped.GetValueUint64())
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
		responses := make(chan *sentryproto.SendMessageByIdRequest, 10)
		send := func(response *sentryproto.SendMessageByIdRequest) { responses <- response }
		first := startUploadTestStream(t, ctx, cs, nil, send)
		second := startUploadTestStream(t, ctx, cs, nil, send)

		require.NoError(t, first.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, 1, 1, common.Hash{1})))
		synctest.Wait()
		require.NoError(t, second.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, 1, 2, common.Hash{1})))
		synctest.Wait()
		require.Equal(t, int32(1), calls.Load(), "the same peer on another sentry must not get a second slot")

		for peer := byte(2); peer <= 8; peer++ {
			stream := first
			if peer%2 == 0 {
				stream = second
			}
			require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, peer, uint64(peer), common.Hash{1})))
		}
		synctest.Wait()
		require.Equal(t, int32(4), calls.Load(), "all sentries must share the four upload slots")

		close(release)
		synctest.Wait()
		require.Len(t, responses, 9, "queued requests must drain after the four active handlers finish")
		require.NoError(t, first.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, 1, 10, common.Hash{1})))
		synctest.Wait()
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
		cs.ethApiWrapper = uploadReceiptsGetter{getReceipts: waitForUploadReceipts(release)}
		responses := make(chan *sentryproto.SendMessageByIdRequest, 2)
		stream := startUploadTestStream(t, ctx, cs, nil, func(response *sentryproto.SendMessageByIdRequest) { responses <- response })
		require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 1, 100, common.Hash{1})))
		synctest.Wait()
		require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, 1, 101, common.Hash{2})))
		synctest.Wait()
		close(release)
		synctest.Wait()
		require.Len(t, responses, 2, "one peer's concurrent bodies and receipts requests must both get a reply")
		requireUploadResponse(t, <-responses, 100)
		requireUploadResponse(t, <-responses, 101)
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
		responses := make(chan *sentryproto.SendMessageByIdRequest, 6)
		stream := startUploadTestStream(t, ctx, cs, nil, func(response *sentryproto.SendMessageByIdRequest) { responses <- response })
		for peer := byte(1); peer <= 4; peer++ {
			require.NoError(t, stream.Send(uploadRequest(t, sentryproto.MessageId_GET_BLOCK_BODIES_66, peer, uint64(peer), common.Hash{peer})))
		}
		synctest.Wait()
		require.Len(t, started, 4)
		for range 4 {
			<-started
		}
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
		synctest.Wait()
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
		cs.ethApiWrapper = uploadReceiptsGetter{getReceipts: waitForUploadReceipts(release)}
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
		require.Len(t, responses, 1, "a waiting replay must let the peer's next body request run")
		requireUploadResponse(t, <-responses, 13)
		cancelWaiting()
		synctest.Wait()
		select {
		case <-waitingDone:
		default:
			t.Fatal("cancelled requests must finish without waiting for unrelated active work")
		}
		require.Empty(t, responses)
		close(release)
		active.Wait()
		require.Len(t, responses, 4, "cancelled requests must not be served")
		cs.uploads.mu.Lock()
		defer cs.uploads.mu.Unlock()
		require.Empty(t, cs.uploads.peers)
		require.Empty(t, cs.uploads.ready)
	})
}

func TestUploadRequests_PeerByteLimit(t *testing.T) {
	for _, replay := range []bool{false, true} {
		name := "busy_workers"
		if replay {
			name = "busy_replay_slot"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				cs := newUploadTestClient(t)
				dropped := metrics.GetOrCreateCounter(`p2p_sentry_upload_dropped_requests_total{reason="peer_bytes"}`)
				before := dropped.GetValueUint64()
				cs.ethApiWrapper = uploadReceiptsGetter{getReceipts: waitForUploadReceipts(make(chan struct{}))}
				cs.blockReader.(*uploadBlockReader).body = func(context.Context, uint64) (rlp.RawValue, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				active := byte(4)
				id := sentryproto.MessageId_GET_BLOCK_BODIES_66
				if replay {
					active = 1
					id = sentryproto.MessageId_GET_RECEIPTS_69
				}
				stream := startUploadTestStream(t, ctx, cs, nil, func(*sentryproto.SendMessageByIdRequest) {})
				for peer := byte(1); peer <= active; peer++ {
					require.NoError(t, stream.Send(uploadRequest(t, id, peer, uint64(peer), common.Hash{1})))
				}
				synctest.Wait()

				waitingCtx, cancelWaiting := context.WithCancel(ctx)
				defer cancelWaiting()
				defer cs.cancelUploads(waitingCtx)
				hashes := make([]common.Hash, 200_000)
				for i := range hashes {
					hashes[i] = common.Hash{1}
				}
				message := uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 10, 1, hashes...)
				require.LessOrEqual(t, len(message.Data), eth.ProtocolMaxMsgSize)
				var completed atomic.Int32
				done := func() { completed.Add(1) }
				for i, peer := range []byte{10, 11} {
					for range 5 {
						request := &sentryproto.InboundMessage{Id: message.Id, Data: message.Data, PeerId: gointerfaces.ConvertHashToH512([64]byte{peer})}
						cs.enqueueUpload(uploadTask{ctx: waitingCtx, message: request, done: done})
						synctest.Wait()
					}
					require.Equal(t, int32(3*(i+1)), completed.Load(), "each peer may queue two 6.6 MB requests within its 16 MiB budget")
				}
				cs.enqueueUpload(uploadTask{ctx: waitingCtx, message: uploadRequest(t, sentryproto.MessageId_GET_RECEIPTS_69, 12, 1, common.Hash{1}), done: done})
				synctest.Wait()
				require.Equal(t, int32(6), completed.Load(), "other peers must retain room in the waiting queue")
				cancelWaiting()
				cs.cancelUploads(waitingCtx)
				require.Equal(t, int32(11), completed.Load())

				nextCtx, cancelNext := context.WithCancel(ctx)
				defer cancelNext()
				defer cs.cancelUploads(nextCtx)
				cs.enqueueUpload(uploadTask{ctx: nextCtx, message: message, done: done})
				synctest.Wait()
				require.Equal(t, int32(11), completed.Load(), "cancellation must free the peer's byte budget")
				require.Equal(t, before+6, dropped.GetValueUint64(), "only requests over the peer's byte limit count as dropped")
			})
		})
	}
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
				reason := "queue_count"
				if tc.bytes {
					reason = "queue_bytes"
				}
				dropped := metrics.GetOrCreateCounter(`p2p_sentry_upload_dropped_requests_total{reason="` + reason + `"}`)
				before := dropped.GetValueUint64()
				active := byte(4)
				id := sentryproto.MessageId_GET_BLOCK_BODIES_66
				if tc.replay {
					active = 1
					id = sentryproto.MessageId_GET_RECEIPTS_69
					cs.ethApiWrapper = uploadReceiptsGetter{
						cached: map[common.Hash]types.Receipts{{2}: {{CumulativeGasUsed: 21_000}}},
						getReceipts: func(ctx context.Context, _ *types.Block) (types.Receipts, error) {
							<-ctx.Done()
							return nil, ctx.Err()
						},
					}
				} else {
					cs.blockReader.(*uploadBlockReader).body = func(context.Context, uint64) (rlp.RawValue, error) {
						<-ctx.Done()
						return nil, ctx.Err()
					}
				}
				responses := make(chan *sentryproto.SendMessageByIdRequest, maxConcurrentUploadRequests+2)
				stream := startUploadTestStream(t, ctx, cs, nil, func(response *sentryproto.SendMessageByIdRequest) { responses <- response })
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
				activePeerRequest := &sentryproto.InboundMessage{Id: message.Id, Data: message.Data, PeerId: gointerfaces.ConvertHashToH512([64]byte{1})}
				cs.enqueueUpload(uploadTask{ctx: waitingCtx, message: activePeerRequest, done: done})
				synctest.Wait()
				require.Equal(t, int32(3), completed.Load(), "requests behind an active peer must also obey the queue limit")
				if tc.replay {
					for i, id := range []sentryproto.MessageId{sentryproto.MessageId_GET_BLOCK_BODIES_66, sentryproto.MessageId_GET_RECEIPTS_69} {
						requestID := uint64(i + 2)
						require.NoError(t, stream.Send(uploadRequest(t, id, 100, requestID, common.Hash{2})))
						synctest.Wait()
						require.Len(t, responses, 1, "a full waiting queue must allow work that can start immediately")
						requireUploadResponse(t, <-responses, requestID)
					}
				}
				cancelWaiting()
				cs.cancelUploads(waitingCtx)
				require.Equal(t, int32(capacity+3), completed.Load())

				nextCtx, cancelNext := context.WithCancel(ctx)
				defer cancelNext()
				defer cs.cancelUploads(nextCtx)
				cs.enqueueUpload(uploadTask{ctx: nextCtx, message: message, done: done})
				synctest.Wait()
				require.Equal(t, int32(capacity+3), completed.Load(), "cancellation must free the queue budget for new work")
				cancelNext()
				cs.enqueueUpload(uploadTask{ctx: nextCtx, message: message, done: done})
				require.Equal(t, int32(capacity+4), completed.Load(), "cancelled work must not enter the queue")
				require.Equal(t, before+3, dropped.GetValueUint64(), "only requests over the global queue limit count as dropped")
			})
		})
	}
}
