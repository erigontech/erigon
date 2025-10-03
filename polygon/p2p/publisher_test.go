// Copyright 2024 The Erigon Authors
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

package polygonp2p

import (
	"context"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common/event"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

func TestPublisher(t *testing.T) {
	newPublisherTest(t).run(func(ctx context.Context, t *testing.T, pt publisherTest) {
		pt.peerEvent(&sentryproto.PeerEvent{
			PeerId:  p2p.PeerIdFromUint64(1).H512(),
			EventId: sentryproto.PeerEvent_Connect,
		})
		pt.peerEvent(&sentryproto.PeerEvent{
			PeerId:  p2p.PeerIdFromUint64(2).H512(),
			EventId: sentryproto.PeerEvent_Connect,
		})
		pt.peerEvent(&sentryproto.PeerEvent{
			PeerId:  p2p.PeerIdFromUint64(3).H512(),
			EventId: sentryproto.PeerEvent_Connect,
		})
		pt.peerEvent(&sentryproto.PeerEvent{
			PeerId:  p2p.PeerIdFromUint64(4).H512(),
			EventId: sentryproto.PeerEvent_Connect,
		})
		pt.peerEvent(&sentryproto.PeerEvent{
			PeerId:  p2p.PeerIdFromUint64(5).H512(),
			EventId: sentryproto.PeerEvent_Connect,
		})
		pt.peerEvent(&sentryproto.PeerEvent{
			PeerId:  p2p.PeerIdFromUint64(6).H512(),
			EventId: sentryproto.PeerEvent_Connect,
		})
		pt.peerEvent(&sentryproto.PeerEvent{
			PeerId:  p2p.PeerIdFromUint64(7).H512(),
			EventId: sentryproto.PeerEvent_Connect,
		})
		pt.peerEvent(&sentryproto.PeerEvent{
			PeerId:  p2p.PeerIdFromUint64(8).H512(),
			EventId: sentryproto.PeerEvent_Connect,
		})

		// we hear about block1 from peers 1,2,3,4
		header1 := &types.Header{Number: big.NewInt(1)}
		block1 := types.NewBlockWithHeader(header1)
		td1 := big.NewInt(5)
		waitPeersMayMissHash := func(peersCount int) func() bool {
			return func() bool { return len(pt.peerTracker.ListPeersMayMissBlockHash(header1.Hash())) == peersCount }
		}
		require.Eventually(t, waitPeersMayMissHash(8), time.Second, 5*time.Millisecond)
		pt.newBlockEvent(&p2p.DecodedInboundMessage[*eth.NewBlockPacket]{
			PeerId: p2p.PeerIdFromUint64(1),
			Decoded: &eth.NewBlockPacket{
				Block: block1,
				TD:    td1,
			},
		})
		require.Eventually(t, waitPeersMayMissHash(7), time.Second, 5*time.Millisecond)
		pt.newBlockEvent(&p2p.DecodedInboundMessage[*eth.NewBlockPacket]{
			PeerId: p2p.PeerIdFromUint64(2),
			Decoded: &eth.NewBlockPacket{
				Block: block1,
				TD:    td1,
			},
		})
		require.Eventually(t, waitPeersMayMissHash(6), time.Second, 5*time.Millisecond)
		pt.newBlockHashesEvent(&p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]{
			PeerId: p2p.PeerIdFromUint64(3),
			Decoded: &eth.NewBlockHashesPacket{
				{
					Hash:   header1.Hash(),
					Number: header1.Number.Uint64(),
				},
			},
		})
		require.Eventually(t, waitPeersMayMissHash(5), time.Second, 5*time.Millisecond)
		pt.newBlockHashesEvent(&p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]{
			PeerId: p2p.PeerIdFromUint64(4),
			Decoded: &eth.NewBlockHashesPacket{
				{
					Hash:   header1.Hash(),
					Number: header1.Number.Uint64(),
				},
			},
		})
		require.Eventually(t, waitPeersMayMissHash(4), time.Second, 5*time.Millisecond)

		p := pt.publisher
		p.PublishNewBlock(block1, big.NewInt(55))
		waitSends := func(sendsCount int) func() bool {
			return func() bool { return len(pt.capturedSends()) == sendsCount }
		}
		// NewBlock announces should be send to only sqrt(peers) that do not know about this block hash
		// according to our knowledge: sqrt(4)=2 -> peers 5,6
		knownSends := map[p2p.PeerId]struct{}{}
		knownSends[*p2p.PeerIdFromUint64(1)] = struct{}{}
		knownSends[*p2p.PeerIdFromUint64(2)] = struct{}{}
		knownSends[*p2p.PeerIdFromUint64(3)] = struct{}{}
		knownSends[*p2p.PeerIdFromUint64(4)] = struct{}{}
		require.Eventually(t, waitSends(2), time.Second, 5*time.Millisecond)
		capturedSend1PeerId := *p2p.PeerIdFromH512(pt.capturedSends()[0].PeerId)
		_, known := knownSends[capturedSend1PeerId]
		require.False(t, known)
		knownSends[capturedSend1PeerId] = struct{}{}
		capturedSend2PeerId := *p2p.PeerIdFromH512(pt.capturedSends()[1].PeerId)
		_, known = knownSends[capturedSend2PeerId]
		require.False(t, known)
		knownSends[capturedSend2PeerId] = struct{}{}

		p.PublishNewBlockHashes(block1)
		// NewBlockHashes should be sent to all remaining peers that do not already know this block hash
		// according to our knowledge: peers 7,8
		require.Eventually(t, waitSends(4), time.Second, 5*time.Millisecond)
		capturedSend3PeerId := *p2p.PeerIdFromH512(pt.capturedSends()[2].PeerId)
		_, known = knownSends[capturedSend3PeerId]
		require.False(t, known)
		knownSends[capturedSend3PeerId] = struct{}{}
		capturedSend4PeerId := *p2p.PeerIdFromH512(pt.capturedSends()[3].PeerId)
		_, known = knownSends[capturedSend4PeerId]
		require.False(t, known)
		knownSends[capturedSend4PeerId] = struct{}{}
		require.Len(t, knownSends, 8)
		allPeerIds := maps.Keys(knownSends)
		require.ElementsMatch(t, allPeerIds, []p2p.PeerId{
			*p2p.PeerIdFromUint64(1),
			*p2p.PeerIdFromUint64(2),
			*p2p.PeerIdFromUint64(3),
			*p2p.PeerIdFromUint64(4),
			*p2p.PeerIdFromUint64(5),
			*p2p.PeerIdFromUint64(6),
			*p2p.PeerIdFromUint64(7),
			*p2p.PeerIdFromUint64(8),
		})

		// all 8 peers must now know about the hash according to our knowledge
		require.Eventually(t, waitPeersMayMissHash(0), time.Second, 5*time.Millisecond)
	})
}

func newPublisherTest(t *testing.T) publisherTest {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logger := testlog.Logger(t, log.LvlCrit)
	ctrl := gomock.NewController(t)
	peerEventRegistrar := p2p.NewMockpeerEventRegistrar(ctrl)
	peerTracker := p2p.NewPeerTracker(logger, peerEventRegistrar, p2p.WithPreservingPeerShuffle)
	sentryClient := direct.NewMockSentryClient(ctrl)
	messageSender := p2p.NewMessageSender(sentryClient)
	publisher := NewPublisher(logger, messageSender, peerTracker)
	capturedSends := make([]*sentryproto.SendMessageByIdRequest, 0, 1024)
	test := publisherTest{
		ctx:                  ctx,
		ctxCancel:            cancel,
		t:                    t,
		peerTracker:          peerTracker,
		peerEventRegistrar:   peerEventRegistrar,
		publisher:            publisher,
		peerEventStream:      make(chan *sentryproto.PeerEvent),
		newBlockHashesStream: make(chan *p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]),
		newBlockStream:       make(chan *p2p.DecodedInboundMessage[*eth.NewBlockPacket]),
		sentryClient:         sentryClient,
		capturedSendsPtr:     &capturedSends,
		capturedSendsMu:      &sync.Mutex{},
	}

	test.mockPeerEvents(test.peerEventStream)
	test.mockNewBlockHashesEvents(test.newBlockHashesStream)
	test.mockNewBlockEvents(test.newBlockStream)
	test.captureSends(test.capturedSendsPtr)
	return test
}

type publisherTest struct {
	ctx                  context.Context
	ctxCancel            context.CancelFunc
	t                    *testing.T
	peerTracker          *p2p.PeerTracker
	peerEventRegistrar   *p2p.MockpeerEventRegistrar
	peerEventStream      chan *sentryproto.PeerEvent
	newBlockHashesStream chan *p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]
	newBlockStream       chan *p2p.DecodedInboundMessage[*eth.NewBlockPacket]
	sentryClient         *direct.MockSentryClient
	capturedSendsPtr     *[]*sentryproto.SendMessageByIdRequest
	capturedSendsMu      *sync.Mutex
	publisher            *Publisher
}

func (pt publisherTest) mockPeerEvents(events <-chan *sentryproto.PeerEvent) {
	pt.peerEventRegistrar.EXPECT().
		RegisterPeerEventObserver(gomock.Any(), gomock.Any()).
		DoAndReturn(func(observer event.Observer[*sentryproto.PeerEvent], opts ...p2p.RegisterOpt) p2p.UnregisterFunc {
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case event := <-events:
						observer(event)
					}
				}
			}()

			return p2p.UnregisterFunc(cancel)
		}).
		Times(1)
}

func (pt publisherTest) peerEvent(e *sentryproto.PeerEvent) {
	send(pt.ctx, pt.t, pt.peerEventStream, e)
}

func (pt publisherTest) mockNewBlockHashesEvents(events <-chan *p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]) {
	pt.peerEventRegistrar.EXPECT().
		RegisterNewBlockHashesObserver(gomock.Any()).
		DoAndReturn(
			func(observer event.Observer[*p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]]) p2p.UnregisterFunc {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					for {
						select {
						case <-ctx.Done():
							return
						case event := <-events:
							observer(event)
						}
					}
				}()

				return p2p.UnregisterFunc(cancel)
			},
		).
		Times(1)
}

func (pt publisherTest) newBlockHashesEvent(e *p2p.DecodedInboundMessage[*eth.NewBlockHashesPacket]) {
	send(pt.ctx, pt.t, pt.newBlockHashesStream, e)
}

func (pt publisherTest) mockNewBlockEvents(events <-chan *p2p.DecodedInboundMessage[*eth.NewBlockPacket]) {
	pt.peerEventRegistrar.EXPECT().
		RegisterNewBlockObserver(gomock.Any()).
		DoAndReturn(
			func(observer event.Observer[*p2p.DecodedInboundMessage[*eth.NewBlockPacket]]) p2p.UnregisterFunc {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					for {
						select {
						case <-ctx.Done():
							return
						case event := <-events:
							observer(event)
						}
					}
				}()

				return p2p.UnregisterFunc(cancel)
			},
		).
		Times(1)
}

func (pt publisherTest) newBlockEvent(e *p2p.DecodedInboundMessage[*eth.NewBlockPacket]) {
	send(pt.ctx, pt.t, pt.newBlockStream, e)
}

func (pt publisherTest) captureSends(sends *[]*sentryproto.SendMessageByIdRequest) {
	pt.sentryClient.EXPECT().
		SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context,
			r *sentryproto.SendMessageByIdRequest,
			_ ...grpc.CallOption,
		) (*sentryproto.SentPeers, error) {
			pt.capturedSendsMu.Lock()
			defer pt.capturedSendsMu.Unlock()
			*sends = append(*sends, r)
			return &sentryproto.SentPeers{Peers: []*typesproto.H512{r.PeerId}}, nil
		}).
		AnyTimes()
}

// sortedCapturedSends assumes peer ids are created using PeerIdFromUint64 and sorts all captured sends by PeerId
func (pt publisherTest) capturedSends() []*sentryproto.SendMessageByIdRequest {
	pt.capturedSendsMu.Lock()
	defer pt.capturedSendsMu.Unlock()
	return *pt.capturedSendsPtr
}

func (pt publisherTest) run(f func(ctx context.Context, t *testing.T, pt publisherTest)) {
	var done atomic.Bool
	pt.t.Run("start", func(t *testing.T) {
		go func() {
			defer done.Store(true)
			eg, ctx := errgroup.WithContext(pt.ctx)
			eg.Go(func() error { return pt.peerTracker.Run(ctx) })
			eg.Go(func() error { return pt.publisher.Run(ctx) })
			err := eg.Wait()
			require.ErrorIs(t, err, context.Canceled)
		}()
	})

	pt.t.Run("test", func(t *testing.T) {
		f(pt.ctx, t, pt)
	})

	pt.t.Run("stop", func(t *testing.T) {
		pt.ctxCancel()
		require.Eventually(t, done.Load, time.Second, 5*time.Millisecond)
	})
}

func send[T any](ctx context.Context, t *testing.T, ch chan T, e T) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		require.FailNow(t, "send timed out")
	case ch <- e: // no-op
	}
}
