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

package txpool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
)

func TestRemoteFramePeerQueueLimit(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	for peer := byte(1); peer <= 2; peer++ {
		var txns TxnSlots
		for nonce := range uint64(5) {
			txns.Append(framePoolSlotWithNonce(t, sender, uint64(peer)*10+nonce, 1, 2), sender[:], false)
		}
		pool.AddRemoteTxns(ctx, txns, gointerfaces.ConvertHashToH512([64]byte{peer}), nil)
	}
	require.Len(t, pool.unprocessedRemoteTxns.Txns, 8, "each peer may retain four frames, including before dispatch")
	ordinary := newTestTxnSlot(0, 0, 1, 2, 21_000)
	ordinary.IDHash[0] = 2
	var txns TxnSlots
	txns.Append(ordinary, sender[:], false)
	pool.AddRemoteTxns(ctx, txns, gointerfaces.ConvertHashToH512([64]byte{1}), nil)
	require.Contains(t, pool.unprocessedRemoteByHash, string(ordinary.IDHash[:]), "the frame limit must not reject ordinary transactions")
}

func TestRemoteFrameQueueLimit(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	pool.started.Store(true)
	for i := range 65 {
		var txns TxnSlots
		txns.Append(framePoolSlotWithNonce(t, sender, uint64(i), 1, 2), sender[:], false)
		pool.AddRemoteTxns(ctx, txns, gointerfaces.ConvertHashToH512([64]byte{byte(i)}), nil)
	}
	require.Len(t, pool.unprocessedRemoteTxns.Txns, 64, "the bound must apply before dispatch as well as during simulation")
	require.NoError(t, pool.processRemoteTxns(ctx))
	require.Len(t, pool.remoteFrames.ready, 64)
	require.Len(t, pool.remoteFrames.pending, 64)
}

func TestRemoteFramePeerRateLimit(t *testing.T) {
	_, pool, sender := keyedFramePoolEnv(t)
	q := &pool.remoteFrames
	source := remoteSource{peerID: gointerfaces.ConvertHashToH512([64]byte{1})}
	now := time.Now()
	for i := range 4 {
		txn := framePoolSlotWithNonce(t, sender, uint64(i), 1, 2)
		require.True(t, q.reserve(txn, source, now))
		q.release(txn, source)
	}
	next := framePoolSlotWithNonce(t, sender, 4, 1, 2)
	require.False(t, q.reserve(next, source, now), "fresh hashes must not bypass the peer's simulation budget")
	require.True(t, q.reserve(next, source, now.Add(time.Second/16)))
}

func TestRemoteFrameAdmissionReleasesQueue(t *testing.T) {
	for _, tc := range []struct {
		name  string
		valid bool
		err   error
	}{
		{name: "accepted", valid: true},
		{name: "state-dependent rejection"},
		{name: "backend unavailable", err: errors.New("backend unavailable")},
		{name: "timeout", err: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			pool.started.Store(true)
			backend := pool.ethBackend.(*frameValidationBackend)
			backend.valid, backend.err = tc.valid, tc.err
			sentry := sentryproto.NewMockSentryClient(gomock.NewController(t))
			peer := gointerfaces.ConvertHashToH512([64]byte{1})
			frame := framePoolSlot(t, sender, 1, 2)
			var txns TxnSlots
			txns.Append(frame, sender[:], false)
			pool.AddRemoteTxns(ctx, txns, peer, sentry)
			_, _, hashes := pool.AppendAllAnnouncements(nil, nil, nil)
			require.Empty(t, hashes, "frames must pass simulation before they are announced")
			require.NoError(t, pool.processRemoteTxns(ctx))
			backend.onValidate = func() {
				known, err := pool.IdHashKnown(nil, frame.IDHash[:])
				require.NoError(t, err)
				require.True(t, known, "in-flight frames remain known after leaving the input queue")
				_, _, hashes := pool.AppendAllAnnouncements(nil, nil, nil)
				require.Empty(t, hashes)
				pool.AddRemoteTxns(ctx, txns, peer, sentry)
				require.Empty(t, pool.unprocessedRemoteTxns.Txns, "a duplicate must not consume another queue slot")
			}
			pool.processRemoteFrame(ctx, <-pool.remoteFrames.ready)
			require.Empty(t, pool.remoteFrames.pending)
			require.Empty(t, pool.remoteFrames.peerCounts)
			require.Len(t, backend.calls, 1)
			_, admitted := pool.byHash[string(frame.IDHash[:])]
			require.Equal(t, tc.valid, admitted)
			_, _, hashes = pool.AppendAllAnnouncements(nil, nil, nil)
			if tc.valid {
				require.Equal(t, frame.IDHash[:], hashes)
			} else {
				require.Empty(t, hashes)
			}
			if tc.err != nil {
				known, err := pool.IdHashKnown(nil, frame.IDHash[:])
				require.NoError(t, err)
				require.False(t, known, "backend failures must permit retry")
			}
		})
	}
}

func TestRemoteFramePanicReleasesQueue(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	pool.started.Store(true)
	pool.ethBackend.(*frameValidationBackend).onValidate = func() { panic("validation panic") }
	var txns TxnSlots
	txns.Append(framePoolSlot(t, sender, 1, 2), sender[:], false)
	pool.AddRemoteTxns(ctx, txns, nil, nil)
	require.NoError(t, pool.processRemoteTxns(ctx))
	require.NotPanics(t, func() { pool.processRemoteFrame(ctx, <-pool.remoteFrames.ready) })
	require.Empty(t, pool.remoteFrames.pending)
	require.Empty(t, pool.remoteFrames.peerCounts)
}

func TestRemoteFrameSimulationDoesNotBlockOrdinaryPropagation(t *testing.T) {
	parent, pool, sender := keyedFramePoolEnv(t)
	ctx, cancel := context.WithCancel(parent)
	pool.started.Store(true)
	pool.cfg.ProcessRemoteTxnsEvery = time.Millisecond
	pool.cfg.SyncToNewPeersEvery = time.Hour
	pool.p2pFetcher.ctx = ctx
	ctrl := gomock.NewController(t)
	stateChanges := remoteproto.NewMockKVClient(ctrl)
	stateChanges.EXPECT().StateChanges(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, status.Error(codes.Unavailable, "no core node in this test")).AnyTimes()
	pool.p2pFetcher.stateChangesClient = stateChanges
	propagated := make(chan struct{}, 2)
	sentry := sentryproto.NewMockSentryClient(ctrl)
	sentry.EXPECT().Peers(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&sentryproto.PeersReply{}, nil).AnyTimes()
	sentry.EXPECT().SendMessageToRandomPeers(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *sentryproto.SendMessageToRandomPeersRequest, _ ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			if req.Data.Id == sentryproto.MessageId_TRANSACTIONS_66 {
				propagated <- struct{}{}
			}
			return &sentryproto.SentPeers{}, nil
		}).AnyTimes()
	pool.p2pSender = NewSend(ctx, []sentryproto.SentryClient{sentry}, pool.logger)
	entered := make(chan struct{})
	pool.ethBackend = &frameValidationFuncBackend{validate: func(ctx context.Context, _ *remoteproto.FrameValidationRequest) (*remoteproto.FrameValidationReply, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
	ordinary := newTestTxnSlot(0, 0, 1, 2, 21_000)
	ordinary.IDHash[0], ordinary.Rlp, ordinary.Size = 2, []byte{0xc0}, 1
	var txns TxnSlots
	txns.Append(frame, sender[:], false)
	txns.Append(framePoolSlotWithNonce(t, sender, 1, 1, 2, uint256.Int{1}), sender[:], false)
	txns.Append(ordinary, sender[:], false)
	pool.AddRemoteTxns(ctx, txns, nil, nil)
	done := make(chan error, 1)
	go func() { done <- pool.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
			require.NotContains(t, pool.remoteFrames.pending, string(frame.IDHash[:]), "Run must wait for in-flight admission to finish")
		case <-time.After(10 * time.Second):
			t.Error("pool did not stop after cancellation")
		}
	})
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("frame simulation did not start")
	}
	pool.lock.Lock()
	_, admitted := pool.byHash[string(ordinary.IDHash[:])]
	pool.lock.Unlock()
	require.True(t, admitted, "ordinary transactions in the batch must precede frame simulation")
	for nonce := range uint64(2) {
		if nonce > 0 {
			later := newTestTxnSlot(nonce, 0, 1, 2, 21_000)
			later.IDHash[0], later.Rlp, later.Size = 3, []byte{0xc0}, 1
			var arrivals TxnSlots
			arrivals.Append(later, sender[:], false)
			pool.AddRemoteTxns(ctx, arrivals, nil, nil)
		}
		select {
		case <-propagated:
		case <-time.After(2 * time.Second):
			t.Fatal("ordinary transaction propagation is waiting for frame simulation")
		}
	}
}
