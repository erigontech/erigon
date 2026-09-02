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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	accounts3 "github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// stalledOnAddStream is an OnAdd subscriber that never finishes a Send, the way
// a connected client that stops reading exhausts its HTTP/2 flow control.
type stalledOnAddStream struct {
	grpc.ServerStream
	ctx      context.Context
	entered  chan struct{}
	enterOne sync.Once
}

func (s *stalledOnAddStream) Context() context.Context { return s.ctx }

func (s *stalledOnAddStream) Send(*txpoolproto.OnAddReply) error {
	s.enterOne.Do(func() { close(s.entered) })
	<-s.ctx.Done()
	return s.ctx.Err()
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
	stalled := &stalledOnAddStream{ctx: ctx, entered: make(chan struct{})}
	go grpcServer.OnAdd(&txpoolproto.OnAddRequest{}, stalled) //nolint:errcheck // ends with ctx

	// Probe until the subscriber is registered and wedged inside Send.
	probing := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-probing:
				return
			case <-ticker.C:
				newSlotsStreams.Broadcast(&txpoolproto.OnAddReply{}, logger)
			}
		}
	}()
	<-stalled.entered
	close(probing)

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
