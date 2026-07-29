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

package libsentry

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

var _ sentryproto.SentryClient = (*sentryMultiplexer)(nil)

type client struct {
	sync.RWMutex
	sentryproto.SentryClient
	protocol sentryproto.Protocol
}

type sentryMultiplexer struct {
	clients []*client
}

func NewSentryMultiplexer(clients []sentryproto.SentryClient) *sentryMultiplexer {
	mux := &sentryMultiplexer{}
	mux.clients = make([]*client, len(clients))
	for i, c := range clients {
		mux.clients[i] = &client{sync.RWMutex{}, c, noProtocol}
	}
	return mux
}

// fanOut calls call concurrently on every client whose negotiated protocol is
// at least minProtocol (noProtocol admits clients that have not handshaken
// yet) and feeds each reply to collect (may be nil) under a shared mutex. A
// non-nil error from call or collect cancels the remaining calls.
func fanOut[R any](ctx context.Context, clients []*client, minProtocol sentryproto.Protocol, call func(context.Context, *client) (R, error), collect func(clientIndex int, reply R) error) error {
	g, gctx := errgroup.WithContext(ctx)
	var mu sync.Mutex

	for i, client := range clients {

		if minProtocol > noProtocol {
			client.RLock()
			protocol := client.protocol
			client.RUnlock()

			if protocol < minProtocol {
				continue
			}
		}

		g.Go(func() error {
			reply, err := call(gctx, client)

			if err != nil {
				return err
			}

			if collect == nil {
				return nil
			}

			mu.Lock()
			defer mu.Unlock()

			return collect(i, reply)
		})
	}

	return g.Wait()
}

func fanOutSuccess[R interface{ GetSuccess() bool }](ctx context.Context, clients []*client, call func(context.Context, *client) (R, error)) (bool, error) {
	var success bool

	err := fanOut(ctx, clients, noProtocol, call, func(_ int, reply R) error {
		success = success || reply.GetSuccess()
		return nil
	})

	return success, err
}

func streamFanIn[T protoreflect.ProtoMessage, S interface{ Recv() (T, error) }](ctx context.Context, clients []*client, open func(context.Context, *client) (S, error)) *SentryStreamC[T] {
	g, gctx := errgroup.WithContext(ctx)

	ch := make(chan StreamReply[T], MessagesQueueSize)
	streamServer := &SentryStreamS[T]{Ch: ch, Ctx: ctx}

	go func() {
		defer close(ch)

		for _, client := range clients {

			g.Go(func() error {
				stream, err := open(gctx, client)

				if err != nil {
					streamServer.Err(err)
					return err
				}

				for {
					message, err := stream.Recv()

					if err != nil {
						if errors.Is(err, io.EOF) {
							return nil
						}

						streamServer.Err(err)

						select {
						case <-gctx.Done():
							return gctx.Err()
						default:
						}

						return fmt.Errorf("recv: %w", err)
					}

					streamServer.Send(message)
				}
			})
		}

		g.Wait()
	}()

	return &SentryStreamC[T]{Ch: ch, Ctx: ctx}
}

func (m *sentryMultiplexer) SetStatus(ctx context.Context, in *sentryproto.StatusData, opts ...grpc.CallOption) (*sentryproto.SetStatusReply, error) {
	err := fanOut(ctx, m.clients, 0, func(gctx context.Context, client *client) (*sentryproto.SetStatusReply, error) {
		return client.SetStatus(gctx, in, opts...)
	}, nil)

	if err != nil {
		return nil, err
	}

	return &sentryproto.SetStatusReply{}, nil
}

func (m *sentryMultiplexer) PenalizePeer(ctx context.Context, in *sentryproto.PenalizePeerRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, fanOut(ctx, m.clients, noProtocol, func(gctx context.Context, client *client) (*emptypb.Empty, error) {
		return client.PenalizePeer(gctx, in, opts...)
	}, nil)
}

func (m *sentryMultiplexer) SetPeerLatestBlock(ctx context.Context, in *sentryproto.SetPeerLatestBlockRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, fanOut(ctx, m.clients, noProtocol, func(gctx context.Context, client *client) (*emptypb.Empty, error) {
		return client.SetPeerLatestBlock(gctx, in, opts...)
	}, nil)
}

func (m *sentryMultiplexer) SetPeerMinimumBlock(ctx context.Context, in *sentryproto.SetPeerMinimumBlockRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, fanOut(ctx, m.clients, noProtocol, func(gctx context.Context, client *client) (*emptypb.Empty, error) {
		return client.SetPeerMinimumBlock(gctx, in, opts...)
	}, nil)
}

func (m *sentryMultiplexer) SetPeerBlockRange(ctx context.Context, in *sentryproto.SetPeerBlockRangeRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, fanOut(ctx, m.clients, noProtocol, func(gctx context.Context, client *client) (*emptypb.Empty, error) {
		return client.SetPeerBlockRange(gctx, in, opts...)
	}, nil)
}

// HandShake is not performed on the multi-client level
func (m *sentryMultiplexer) HandShake(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*sentryproto.HandShakeReply, error) {
	var protocol sentryproto.Protocol

	err := fanOut(ctx, m.clients, noProtocol, func(gctx context.Context, client *client) (sentryproto.Protocol, error) {
		client.RLock()
		clientProtocol := client.protocol
		client.RUnlock()

		if clientProtocol >= 0 {
			return clientProtocol, nil
		}

		reply, err := client.HandShake(gctx, &emptypb.Empty{}, grpc.WaitForReady(true))

		if err != nil {
			return noProtocol, err
		}

		client.Lock()
		client.protocol = reply.Protocol
		client.Unlock()

		return reply.Protocol, nil
	}, func(_ int, clientProtocol sentryproto.Protocol) error {
		if clientProtocol > protocol {
			protocol = clientProtocol
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &sentryproto.HandShakeReply{Protocol: protocol}, nil
}

func (m *sentryMultiplexer) SendMessageByMinBlock(ctx context.Context, in *sentryproto.SendMessageByMinBlockRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
	var allSentPeers []*typesproto.H512

	// run this in series as we need to keep track of peers - note
	// that there is a possibility that we will generate duplicate
	// sends via cliants with duplicate peers - that would require
	// a refactor of the entry code
	for _, client := range m.clients {
		cin := &sentryproto.SendMessageByMinBlockRequest{
			Data:     in.Data,
			MinBlock: in.MinBlock,
			MaxPeers: in.MaxPeers - uint64(len(allSentPeers)),
		}

		sentPeers, err := client.SendMessageByMinBlock(ctx, cin, opts...)

		if err != nil {
			return nil, err
		}

		allSentPeers = append(allSentPeers, sentPeers.GetPeers()...)

		if len(allSentPeers) >= int(in.MaxPeers) {
			break
		}
	}

	return &sentryproto.SentPeers{Peers: allSentPeers}, nil
}

func AsPeerIdString(peerId *typesproto.H512) string {
	peerHash := gointerfaces.ConvertH512ToHash(peerId)
	return hex.EncodeToString(peerHash[:])
}

func (m *sentryMultiplexer) SendMessageById(ctx context.Context, in *sentryproto.SendMessageByIdRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
	if in.Data == nil {
		return nil, fmt.Errorf("no data")
	}

	if in.PeerId == nil {
		return nil, fmt.Errorf("no peer")
	}

	minProtocol := MinProtocol(in.Data.Id)

	if minProtocol < 0 {
		return nil, fmt.Errorf("unknown protocol for: %s", in.Data.Id.String())
	}

	peerReplies, err := m.peersByClient(ctx, minProtocol, opts...)

	if err != nil {
		return nil, err
	}

	clientIndex := -1
	peerId := AsPeerIdString(in.PeerId)

CLIENTS:
	for i, reply := range peerReplies {
		for _, peer := range reply.GetPeers() {
			if peer.Id == peerId {
				clientIndex = i
				break CLIENTS
			}
		}
	}

	if clientIndex < 0 {
		return nil, fmt.Errorf("peer not found: %s", peerId)
	}

	g, gctx := errgroup.WithContext(ctx)

	var allSentPeers []*typesproto.H512

	g.Go(func() error {
		sentPeers, err := m.clients[clientIndex].SendMessageById(gctx, in, opts...)

		if err != nil {
			return err
		}

		allSentPeers = sentPeers.GetPeers()

		return nil
	})

	if err = g.Wait(); err != nil {
		return nil, err
	}

	return &sentryproto.SentPeers{Peers: allSentPeers}, nil
}

func (m *sentryMultiplexer) SendMessageToRandomPeers(ctx context.Context, in *sentryproto.SendMessageToRandomPeersRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
	if in.Data == nil {
		return nil, fmt.Errorf("no data")
	}

	minProtocol := MinProtocol(in.Data.Id)

	if minProtocol < 0 {
		return nil, fmt.Errorf("unknown protocol for: %s", in.Data.Id.String())
	}

	peerReplies, err := m.peersByClient(ctx, minProtocol, opts...)

	if err != nil {
		return nil, err
	}

	type peer struct {
		clientIndex int
		peerId      *typesproto.H512
	}

	seen := map[string]struct{}{}
	var peers []*peer

	for i, reply := range peerReplies {
		for _, p := range reply.GetPeers() {
			if _, ok := seen[p.Id]; !ok {
				peers = append(peers, &peer{
					clientIndex: i,
					peerId:      gointerfaces.ConvertHashToH512([64]byte(common.Hex2Bytes(p.Id))),
				})
				seen[p.Id] = struct{}{}
			}
		}
	}

	g, gctx := errgroup.WithContext(ctx)

	var allSentPeers []*typesproto.H512
	var allSentMutex sync.RWMutex

	rand.Shuffle(len(peers), func(i int, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})

	if in.MaxPeers > 0 {
		if in.MaxPeers < uint64(len(peers)) {
			peers = peers[0:in.MaxPeers]
		}
	}

	for _, peer := range peers {

		g.Go(func() error {
			sentPeers, err := m.clients[peer.clientIndex].SendMessageById(gctx, &sentryproto.SendMessageByIdRequest{
				PeerId: peer.peerId,
				Data:   in.Data,
			}, opts...)

			if err != nil {
				return err
			}

			allSentMutex.Lock()
			defer allSentMutex.Unlock()

			allSentPeers = append(allSentPeers, sentPeers.GetPeers()...)

			return nil
		})
	}

	err = g.Wait()

	if err != nil {
		return nil, err
	}

	return &sentryproto.SentPeers{Peers: allSentPeers}, nil
}

func (m *sentryMultiplexer) SendMessageToAll(ctx context.Context, in *sentryproto.OutboundMessageData, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
	minProtocol := MinProtocol(in.Id)

	if minProtocol < 0 {
		return nil, fmt.Errorf("unknown protocol for: %s", in.Id.String())
	}

	peerReplies, err := m.peersByClient(ctx, minProtocol, opts...)

	if err != nil {
		return nil, err
	}

	type peer struct {
		clientIndex int
		peerId      *typesproto.H512
	}

	peers := map[string]peer{}

	for i, reply := range peerReplies {
		for _, p := range reply.GetPeers() {
			if _, ok := peers[p.Id]; !ok {
				peers[p.Id] = peer{
					clientIndex: i,
					peerId:      gointerfaces.ConvertHashToH512([64]byte(common.Hex2Bytes(p.Id))),
				}
			}
		}
	}

	g, gctx := errgroup.WithContext(ctx)

	var allSentPeers []*typesproto.H512
	var allSentMutex sync.RWMutex

	for _, peer := range peers {

		g.Go(func() error {
			sentPeers, err := m.clients[peer.clientIndex].SendMessageById(gctx, &sentryproto.SendMessageByIdRequest{
				PeerId: peer.peerId,
				Data: &sentryproto.OutboundMessageData{
					Id:   in.Id,
					Data: in.Data,
				}}, opts...)

			if err != nil {
				return err
			}

			allSentMutex.Lock()
			defer allSentMutex.Unlock()

			allSentPeers = append(allSentPeers, sentPeers.GetPeers()...)

			return nil
		})
	}

	err = g.Wait()

	if err != nil {
		return nil, err
	}

	return &sentryproto.SentPeers{Peers: allSentPeers}, nil
}

func (m *sentryMultiplexer) Messages(ctx context.Context, in *sentryproto.MessagesRequest, opts ...grpc.CallOption) (sentryproto.Sentry_MessagesClient, error) {
	return streamFanIn(ctx, m.clients,
		func(gctx context.Context, client *client) (sentryproto.Sentry_MessagesClient, error) {
			return client.Messages(gctx, in, opts...)
		}), nil
}

func (m *sentryMultiplexer) peersByClient(ctx context.Context, minProtocol sentryproto.Protocol, opts ...grpc.CallOption) ([]*sentryproto.PeersReply, error) {
	allReplies := make([]*sentryproto.PeersReply, len(m.clients))

	err := fanOut(ctx, m.clients, minProtocol, func(gctx context.Context, client *client) (*sentryproto.PeersReply, error) {
		return client.Peers(gctx, &emptypb.Empty{}, opts...)
	}, func(clientIndex int, reply *sentryproto.PeersReply) error {
		allReplies[clientIndex] = reply
		return nil
	})

	if err != nil {
		return nil, err
	}

	return allReplies, nil
}

func (m *sentryMultiplexer) Peers(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*sentryproto.PeersReply, error) {
	var allPeers []*typesproto.PeerInfo

	allReplies, err := m.peersByClient(ctx, noProtocol, opts...)

	if err != nil {
		return nil, err
	}

	for _, sentPeers := range allReplies {
		allPeers = append(allPeers, sentPeers.GetPeers()...)
	}

	return &sentryproto.PeersReply{Peers: allPeers}, nil
}

func (m *sentryMultiplexer) PeerCount(ctx context.Context, in *sentryproto.PeerCountRequest, opts ...grpc.CallOption) (*sentryproto.PeerCountReply, error) {
	var allCount uint64

	err := fanOut(ctx, m.clients, noProtocol, func(gctx context.Context, client *client) (*sentryproto.PeerCountReply, error) {
		return client.PeerCount(gctx, in, opts...)
	}, func(_ int, reply *sentryproto.PeerCountReply) error {
		allCount += reply.GetCount()
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &sentryproto.PeerCountReply{Count: allCount}, nil
}

var errFound = fmt.Errorf("found peer")

func (m *sentryMultiplexer) PeerById(ctx context.Context, in *sentryproto.PeerByIdRequest, opts ...grpc.CallOption) (*sentryproto.PeerByIdReply, error) {
	var peer *typesproto.PeerInfo

	err := fanOut(ctx, m.clients, noProtocol, func(gctx context.Context, client *client) (*sentryproto.PeerByIdReply, error) {
		return client.PeerById(gctx, in, opts...)
	}, func(_ int, reply *sentryproto.PeerByIdReply) error {
		if peer == nil && reply.GetPeer() != nil {
			peer = reply.GetPeer()
			// return a success error here to have the
			// group stop other concurrent requests
			return errFound
		}
		return nil
	})

	if err != nil && !errors.Is(err, errFound) {
		return nil, err
	}

	return &sentryproto.PeerByIdReply{Peer: peer}, nil
}

func (m *sentryMultiplexer) PeerEvents(ctx context.Context, in *sentryproto.PeerEventsRequest, opts ...grpc.CallOption) (sentryproto.Sentry_PeerEventsClient, error) {
	return streamFanIn(ctx, m.clients,
		func(gctx context.Context, client *client) (sentryproto.Sentry_PeerEventsClient, error) {
			return client.PeerEvents(gctx, in, opts...)
		}), nil
}

func (m *sentryMultiplexer) AddPeer(ctx context.Context, in *sentryproto.AddPeerRequest, opts ...grpc.CallOption) (*sentryproto.AddPeerReply, error) {
	success, err := fanOutSuccess(ctx, m.clients, func(gctx context.Context, client *client) (*sentryproto.AddPeerReply, error) {
		return client.AddPeer(gctx, in, opts...)
	})

	if err != nil {
		return nil, err
	}

	return &sentryproto.AddPeerReply{Success: success}, nil
}

func (m *sentryMultiplexer) RemovePeer(ctx context.Context, in *sentryproto.RemovePeerRequest, opts ...grpc.CallOption) (*sentryproto.RemovePeerReply, error) {
	success, err := fanOutSuccess(ctx, m.clients, func(gctx context.Context, client *client) (*sentryproto.RemovePeerReply, error) {
		return client.RemovePeer(gctx, in, opts...)
	})

	if err != nil {
		return nil, err
	}

	return &sentryproto.RemovePeerReply{Success: success}, nil
}

func (m *sentryMultiplexer) AddTrustedPeer(ctx context.Context, in *sentryproto.AddPeerRequest, opts ...grpc.CallOption) (*sentryproto.AddPeerReply, error) {
	success, err := fanOutSuccess(ctx, m.clients, func(gctx context.Context, client *client) (*sentryproto.AddPeerReply, error) {
		return client.AddTrustedPeer(gctx, in, opts...)
	})

	if err != nil {
		return nil, err
	}

	return &sentryproto.AddPeerReply{Success: success}, nil
}

func (m *sentryMultiplexer) RemoveTrustedPeer(ctx context.Context, in *sentryproto.RemovePeerRequest, opts ...grpc.CallOption) (*sentryproto.RemovePeerReply, error) {
	success, err := fanOutSuccess(ctx, m.clients, func(gctx context.Context, client *client) (*sentryproto.RemovePeerReply, error) {
		return client.RemoveTrustedPeer(gctx, in, opts...)
	})

	if err != nil {
		return nil, err
	}

	return &sentryproto.RemovePeerReply{Success: success}, nil
}

func (m *sentryMultiplexer) NodeInfo(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*typesproto.NodeInfoReply, error) {
	return nil, status.Errorf(codes.Unimplemented, `method "NodeInfo" not implemented: use "NodeInfos" instead`)
}

func (m *sentryMultiplexer) NodeInfos(ctx context.Context, opts ...grpc.CallOption) ([]*typesproto.NodeInfoReply, error) {
	var allInfos []*typesproto.NodeInfoReply

	err := fanOut(ctx, m.clients, noProtocol, func(gctx context.Context, client *client) (*typesproto.NodeInfoReply, error) {
		return client.NodeInfo(gctx, &emptypb.Empty{}, opts...)
	}, func(_ int, info *typesproto.NodeInfoReply) error {
		allInfos = append(allInfos, info)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return allInfos, nil
}
