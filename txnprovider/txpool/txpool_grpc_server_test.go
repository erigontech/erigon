// Copyright 2025 The Erigon Authors
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

package txpool

import (
	"bytes"
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	accounts3 "github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

type addMockTxPool struct {
	knownByCall      []bool
	idHashKnownCalls int
	addReasons       []txpoolcfg.DiscardReason
	addLocalSlotsLen int
}

func (m *addMockTxPool) ValidateSerializedTxn(serializedTxn []byte) error { return nil }
func (m *addMockTxPool) PeekBest(ctx context.Context, n int, txns *TxnsRlp, onTopOf uint64) (bool, error) {
	return false, nil
}
func (m *addMockTxPool) GetRlp(tx kv.Tx, hash []byte) ([]byte, error) { return nil, nil }
func (m *addMockTxPool) AddLocalTxns(ctx context.Context, newTxns TxnSlots) ([]txpoolcfg.DiscardReason, error) {
	m.addLocalSlotsLen = len(newTxns.Txns)
	return m.addReasons, nil
}
func (m *addMockTxPool) deprecatedForEach(f func(rlp []byte, sender common.Address, t SubPoolType), tx kv.Tx) {
}
func (m *addMockTxPool) CountContent() (int, int, int) { return 0, 0, 0 }
func (m *addMockTxPool) IdHashKnown(tx kv.Tx, hash []byte) (bool, error) {
	i := m.idHashKnownCalls
	m.idHashKnownCalls++
	if i < len(m.knownByCall) {
		return m.knownByCall[i], nil
	}
	return false, nil
}
func (m *addMockTxPool) NonceFromAddress(addr [20]byte) (nonce uint64, inPool bool)       { return 0, false }
func (m *addMockTxPool) GetBlobs(blobhashes []common.Hash) (blobBundles []PoolBlobBundle) { return nil }

func TestGrpcServerAddDiscardReasonIndexAlignment(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	chainID := *uint256.NewInt(1)

	mockPool := &addMockTxPool{
		knownByCall: []bool{true, false}, // first tx treated as already-known, second goes to AddLocalTxns
		addReasons:  []txpoolcfg.DiscardReason{txpoolcfg.TipAboveFeeCap},
	}

	s := NewGrpcServer(ctx, mockPool, mdbxtest.NewTestPoolDB(t), nil, chainID, log.New())
	validRlp := hexutil.MustDecodeHex(TxnParseMainnetTests[0].PayloadStr)

	reply, err := s.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{validRlp, validRlp}})
	if err != nil {
		t.Fatalf("Add returned error: %v", err)
	}

	if got := mockPool.addLocalSlotsLen; got != 1 {
		t.Fatalf("expected 1 slot sent to AddLocalTxns, got %d", got)
	}

	if len(reply.Imported) != 2 || len(reply.Errors) != 2 {
		t.Fatalf("unexpected reply lengths: imported=%d errors=%d", len(reply.Imported), len(reply.Errors))
	}

	if reply.Imported[0] != txpoolproto.ImportResult_ALREADY_EXISTS || reply.Errors[0] != txpoolcfg.AlreadyKnown.String() {
		t.Fatalf("unexpected first tx result: imported=%v error=%q", reply.Imported[0], reply.Errors[0])
	}
	if reply.Imported[1] != txpoolproto.ImportResult_INVALID || reply.Errors[1] != "max priority fee per gas higher than max fee per gas" {
		t.Fatalf("unexpected second tx result: imported=%v error=%q", reply.Imported[1], reply.Errors[1])
	}
}

// TestQueryAllWithoutPanicUnknown tries to reproduce https://github.com/erigontech/erigon/issues/18076 relying on
// the TOCTOU between the deprecatedForEach locking window and the conversion of currentSubPool in GrpcServer.All().
// It runs 3 concurrent loops: one repeatedly calling GrpcServer.All(), the others repeatedly triggering public
// operations that reset currentSubPool to zero (mined removal and replacement), aiming to hit the race window.
// If the panic("unknown") is triggered in the observation window, the test fails.
func TestQueryAllWithoutPanicUnknown(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	const ObservationWindow = 10 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), ObservationWindow)
	defer cancel()

	// Prepare tx pool and core+pool DBs
	newTxns := make(chan Announcements, 1)
	chainDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	poolDB := mdbxtest.NewTestPoolDB(t)
	cfg := txpoolcfg.DefaultConfig
	cache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, newTxns, poolDB, chainDB, cfg, cache, chain.AllProtocolChanges, nil, nil, func() {}, nil, nil, log.New())
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}

	// Seed minimal chain state so the pool accepts local txns
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	h256 := gointerfaces.ConvertHashToH256(common.Hash{})
	change := &remoteproto.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1_000_000,
		ChangeBatch:         []*remoteproto.StateChange{{BlockHeight: 0, BlockHash: h256}},
	}
	var addr common.Address
	addr[0] = 0xAB
	acc := accounts3.Account{Nonce: 0, Balance: *uint256.NewInt(10 * common.Ether)}
	accBlob := accounts3.SerialiseV3(&acc)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remoteproto.AccountChange{
		Action:  remoteproto.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    accBlob,
	})

	// Apply state change
	if err := pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}); err != nil {
		t.Fatalf("OnNewBlock: %v", err)
	}

	// Prepare two alternating local transactions with the same nonce to exercise replacement
	mkSlot := func(id byte, tip uint64) *TxnSlot {
		to := common.Address{1}
		s := &TxnSlot{
			Txn: &types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{GasLimit: 21000, To: &to},
				TipCap:   *uint256.NewInt(tip),
				FeeCap:   *uint256.NewInt(tip),
			},
			Nonce: 0,
			Rlp:   []byte{id}, // ensure All() doesn't need DB to fetch
		}
		s.IDHash[0] = id
		return s
	}
	slotA := mkSlot(0xA1, 300000)
	slotB := mkSlot(0xB2, 400000) // higher to ensure replacement

	// Add initial txn (A)
	var slots TxnSlots
	slots.Append(slotA, addr[:], true)
	discards, err := pool.AddLocalTxns(ctx, slots)
	if err != nil {
		t.Fatalf("AddLocalTxns(A): %v", err)
	}
	if len(discards) != 1 || discards[0] != txpoolcfg.Success {
		t.Fatalf("unexpected add result A: %+v", discards)
	}

	// Build gRPC server for TxPool
	chainID := *uint256.NewInt(1)
	s := NewGrpcServer(ctx, pool, poolDB, nil, chainID, log.New())

	var panicObserved atomic.Bool
	panicCh := make(chan struct{}, 1)

	var allTasks sync.WaitGroup

	// Reader task: repeatedly call GrpcServer.All() and catch the panic("unknown")
	allTasks.Go(func() {
		for !panicObserved.Load() {
			func() {
				defer func() {
					if r := recover(); r != nil {
						if r == "unknown" {
							panicObserved.Store(true)
							select {
							case panicCh <- struct{}{}:
							default:
							}
						}
					}
				}()
				_, _ = s.All(ctx, &txpoolproto.AllRequest{})
			}()

			// Either exit if the observation window is done or sleep a bit
			select {
			case <-ctx.Done():
				return
			default:
				time.Sleep(50 * time.Microsecond)
			}
		}
	})

	// Mutator task: alternate between replacement and mined-removal cycles
	allTasks.Go(func() {
		for !panicObserved.Load() {
			// Replacement path: add B to replace A (or vice versa)
			var r TxnSlots
			r.Append(slotB, addr[:], true)
			_, _ = pool.AddLocalTxns(ctx, r)

			// Now mined-removal path for whichever is present (use B here)
			var mined TxnSlots
			mined.Append(slotB, addr[:], true)
			_ = pool.OnNewBlock(ctx, &remoteproto.StateChangeBatch{ // keep the same base fee
				StateVersionId:      stateVersionID,
				PendingBlockBaseFee: pendingBaseFee,
				BlockGasLimit:       1_000_000,
				ChangeBatch:         []*remoteproto.StateChange{{BlockHeight: 0, BlockHash: h256}},
			}, TxnSlots{}, TxnSlots{}, mined)

			// Re-add A again to keep cycling
			var r2 TxnSlots
			r2.Append(slotA, addr[:], true)
			_, _ = pool.AddLocalTxns(ctx, r2)

			// Either exit if the observation window is done or sleep a bit
			select {
			case <-ctx.Done():
				return
			default:
				time.Sleep(50 * time.Microsecond)
			}
		}
	})

	// BaseFee churn task: alternates base fee above/below thresholds to force demotions/promotions across sub-pools
	// while sender mapping remains.
	allTasks.Go(func() {
		flip := false
		for !panicObserved.Load() {
			var bf uint64
			if flip {
				bf = pendingBaseFee * 20 // very high to push below fee cap
			} else {
				bf = pendingBaseFee / 20 // very low to allow promotions
			}
			flip = !flip
			_ = pool.OnNewBlock(ctx, &remoteproto.StateChangeBatch{
				StateVersionId:      stateVersionID,
				PendingBlockBaseFee: bf,
				BlockGasLimit:       1_000_000,
				ChangeBatch:         []*remoteproto.StateChange{{BlockHeight: 0, BlockHash: h256}},
			}, TxnSlots{}, TxnSlots{}, TxnSlots{})

			// Either exit if the observation window is done or sleep a bit
			select {
			case <-ctx.Done():
				return
			default:
				time.Sleep(75 * time.Microsecond)
			}
		}
	})

	// Wait for all tasks to finish
	allTasks.Wait()

	select {
	case <-panicCh:
		t.Fatalf("panic(\"unknown\") triggered")
	case <-ctx.Done():
		// Success
	}
}

// stalledOnAddStream is an OnAdd subscriber that never finishes a Send, the way
// a connected client that stops reading exhausts its HTTP/2 flow control.
type stalledOnAddStream struct {
	grpc.ServerStream
	ctx      context.Context
	entered  chan struct{}
	enterOne sync.Once
}

func newStalledOnAddStream(ctx context.Context) *stalledOnAddStream {
	return &stalledOnAddStream{ctx: ctx, entered: make(chan struct{})}
}

func (s *stalledOnAddStream) Context() context.Context { return s.ctx }

func (s *stalledOnAddStream) Send(*txpoolproto.OnAddReply) error {
	s.enterOne.Do(func() { close(s.entered) })
	<-s.ctx.Done()
	return s.ctx.Err()
}

// awaitSubscribed broadcasts until wait returns, since a subscriber only
// registers once its handler runs on the server.
func awaitSubscribed(t *testing.T, streams *NewSlotsStreams, wait func()) {
	t.Helper()
	probing := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-probing:
				return
			case <-ticker.C:
				streams.Broadcast(&txpoolproto.OnAddReply{}, log.New())
			}
		}
	}()
	wait()
	close(probing)
	<-done
}

// TestP2PPropagationContinuesWhileOnAddSubscriberStalls drives the real
// newPendingTxns path in TxPool.Run and asserts that a gRPC OnAdd subscriber
// stuck in Send does not keep the batch from reaching the p2p sender.
func TestP2PPropagationContinuesWhileOnAddSubscriberStalls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.New()
	ctrl := gomock.NewController(t)

	sentToPeers := make(chan *sentryproto.SendMessageToRandomPeersRequest, 16)
	sentryClient := sentryproto.NewMockSentryClient(ctrl)
	sentryClient.EXPECT().
		SendMessageToRandomPeers(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *sentryproto.SendMessageToRandomPeersRequest, _ ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			select {
			case sentToPeers <- req:
			default:
			}
			return &sentryproto.SentPeers{}, nil
		}).AnyTimes()
	sentryClient.EXPECT().
		Peers(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&sentryproto.PeersReply{}, nil).AnyTimes()

	stateChanges := remoteproto.NewMockKVClient(ctrl)
	stateChanges.EXPECT().
		StateChanges(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, status.Error(codes.Unavailable, "no core node in this test")).AnyTimes()

	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	poolDB := mdbxtest.NewTestPoolDB(t)
	newSlotsStreams := &NewSlotsStreams{}
	pool, err := New(
		ctx,
		make(chan Announcements, 16),
		poolDB,
		coreDB,
		txpoolcfg.DefaultConfig,
		kvcache.New(kvcache.DefaultCoherentConfig),
		chain.AllProtocolChanges,
		nil, // sentry clients for the fetcher: this test only drives the sender
		stateChanges,
		func() {},
		newSlotsStreams,
		nil,
		logger,
		WithFeeCalculator(nil),
	)
	require.NoError(t, err)
	pool.p2pSender = NewSend(ctx, []sentryproto.SentryClient{sentryClient}, logger)

	sender := common.Address{1}
	account := accounts3.Account{Balance: *uint256.NewInt(common.Ether)}
	change := &remoteproto.StateChangeBatch{
		PendingBlockBaseFee: 1,
		BlockGasLimit:       1_000_000,
		ChangeBatch: []*remoteproto.StateChange{{
			BlockHash: gointerfaces.ConvertHashToH256(common.Hash{}),
			Changes: []*remoteproto.AccountChange{{
				Action:  remoteproto.Action_UPSERT,
				Address: gointerfaces.ConvertAddressToH160(sender),
				Data:    accounts3.SerialiseV3(&account),
			}},
		}},
	}
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		pool.Run(ctx) //nolint:errcheck // returns the cancellation error on teardown
	}()

	grpcServer := NewGrpcServer(ctx, pool, poolDB, newSlotsStreams, *chain.AllProtocolChanges.ChainID, logger)
	stalled := newStalledOnAddStream(ctx)
	go grpcServer.OnAdd(&txpoolproto.OnAddRequest{}, stalled) //nolint:errcheck // ends with ctx
	awaitSubscribed(t, newSlotsStreams, func() { <-stalled.entered })

	slot := newTestTxnSlot(0, 0, 300_000, 300_000, 100_000)
	slot.IDHash[0] = 1
	slot.Rlp = []byte{0xc0}
	slot.Size = uint32(len(slot.Rlp))
	var slots TxnSlots
	slots.Append(slot, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, slots)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

	select {
	case req := <-sentToPeers:
		require.Equal(t, sentryproto.MessageId_TRANSACTIONS_66, req.Data.Id)
	case <-time.After(20 * time.Second):
		t.Fatal("p2p propagation never reached the sentry while an OnAdd subscriber was stalled")
	}

	cancel()
	<-runDone
}

// TestOnAddSubscriberThatStopsReadingOverTCP exercises OnAdd over a real
// gRPC connection, where a client that stops calling Recv drains its HTTP/2
// flow-control window and wedges the server-side Send.
func TestOnAddSubscriberThatStopsReadingOverTCP(t *testing.T) {
	ctx := t.Context()
	logger := log.New()

	newSlotsStreams := &NewSlotsStreams{}
	grpcServer := NewGrpcServer(ctx, nil, nil, newSlotsStreams, *uint256.NewInt(1), logger)
	srv := grpc.NewServer()
	txpoolproto.RegisterTxpoolServer(srv, grpcServer)
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go srv.Serve(listener) //nolint:errcheck // stopped below
	t.Cleanup(srv.Stop)

	// Separate connections, so that connection-level flow control on the
	// stalled client cannot account for anything the healthy one sees.
	subscribe := func() txpoolproto.Txpool_OnAddClient {
		conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		stream, err := txpoolproto.NewTxpoolClient(conn).OnAdd(ctx, &txpoolproto.OnAddRequest{})
		require.NoError(t, err)
		return stream
	}

	awaitRegistered := func(stream txpoolproto.Txpool_OnAddClient) {
		t.Helper()
		awaitSubscribed(t, newSlotsStreams, func() {
			_, err := stream.Recv()
			require.NoError(t, err)
		})
	}

	stalled := subscribe()
	awaitRegistered(stalled)
	healthy := subscribe()
	awaitRegistered(healthy)

	// stalled is never read again from here on. healthy keeps reading, so its
	// own queue stays empty and only the stalled one can hold the broadcaster up.
	received := make(chan *txpoolproto.OnAddReply, 1024)
	go func() {
		defer close(received)
		for {
			reply, err := healthy.Recv()
			if err != nil {
				return
			}
			received <- reply
		}
	}()

	const (
		messages    = 40
		payloadSize = 256 * 1024
	)
	marker := []byte("last")
	for i := range messages {
		payload := make([]byte, payloadSize)
		if i == messages-1 {
			copy(payload, marker)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			newSlotsStreams.Broadcast(&txpoolproto.OnAddReply{RplTxs: [][]byte{payload}}, logger)
		}()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatalf("Broadcast %d blocked on the subscriber that stopped reading", i)
		}
	}

	timeout := time.After(30 * time.Second)
	for {
		select {
		case reply, ok := <-received:
			require.True(t, ok, "healthy subscriber's stream ended before the marker message")
			if len(reply.RplTxs) == 1 && bytes.Equal(reply.RplTxs[0][:len(marker)], marker) {
				return
			}
		case <-timeout:
			t.Fatal("healthy subscriber never received the marker message")
		}
	}
}

// A handler wedged in Send is released when the server tears its transport
// down, so a subscriber that stopped reading cannot outlive the server.
func TestStalledOnAddSubscriberIsReleasedWhenTheServerStops(t *testing.T) {
	ctx := t.Context()
	logger := log.New()

	handlerDone := make(chan struct{})
	newSlotsStreams := &NewSlotsStreams{}
	srv := grpc.NewServer(grpc.StreamInterceptor(
		func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			defer close(handlerDone)
			return handler(srv, ss)
		}))
	txpoolproto.RegisterTxpoolServer(srv, NewGrpcServer(ctx, nil, nil, newSlotsStreams, *uint256.NewInt(1), logger))
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go srv.Serve(listener) //nolint:errcheck // stopped below

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	stalled, err := txpoolproto.NewTxpoolClient(conn).OnAdd(ctx, &txpoolproto.OnAddRequest{})
	require.NoError(t, err)

	awaitSubscribed(t, newSlotsStreams, func() {
		_, err := stalled.Recv()
		require.NoError(t, err)
	})

	// stalled is never read again: wedge its handler inside Send.
	for range 40 {
		newSlotsStreams.Broadcast(&txpoolproto.OnAddReply{RplTxs: [][]byte{make([]byte, 256*1024)}}, logger)
	}

	srv.Stop()
	select {
	case <-handlerDone:
	case <-time.After(30 * time.Second):
		t.Fatal("the wedged OnAdd handler was not released when the server stopped")
	}
}
