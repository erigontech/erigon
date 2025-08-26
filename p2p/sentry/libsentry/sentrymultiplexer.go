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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/emptypb"
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
		mux.clients[i] = &client{sync.RWMutex{}, c, -1}
	}
	return mux
}

func (m *sentryMultiplexer) SetStatus(ctx context.Context, in *sentryproto.StatusData, opts ...grpc.CallOption) (*sentryproto.SetStatusReply, error) {
	g, gctx := errgroup.WithContext(ctx)

	for _, client := range m.clients {
		client := client

		client.RLock()
		protocol := client.protocol
		client.RUnlock()

		if protocol >= 0 {
			g.Go(func() error {
				_, err := client.SetStatus(gctx, in, opts...)
				return err
			})
		}
	}

	err := g.Wait()

	if err != nil {
		return nil, err
	}

	return &sentryproto.SetStatusReply{}, nil
}

func (m *sentryMultiplexer) PenalizePeer(ctx context.Context, in *sentryproto.PenalizePeerRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	g, gctx := errgroup.WithContext(ctx)

	for _, client := range m.clients {
		client := client

		g.Go(func() error {
			_, err := client.PenalizePeer(gctx, in, opts...)
			return err
		})
	}

	return &emptypb.Empty{}, g.Wait()
}

func (m *sentryMultiplexer) PeerMinBlock(ctx context.Context, in *sentryproto.PeerMinBlockRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	g, gctx := errgroup.WithContext(ctx)

	for _, client := range m.clients {
		client := client

		g.Go(func() error {
			_, err := client.PeerMinBlock(gctx, in, opts...)
			return err
		})
	}

	return &emptypb.Empty{}, g.Wait()
}

// Handshake is not performed on the multi-client level
func (m *sentryMultiplexer) HandShake(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*sentryproto.HandShakeReply, error) {
	g, gctx := errgroup.WithContext(ctx)

	var protocol sentryproto.Protocol
	var mu sync.Mutex

	for _, client := range m.clients {
		client := client

		client.RLock()
		protocol := client.protocol
		client.RUnlock()

		if protocol < 0 {
			g.Go(func() error {
				reply, err := client.HandShake(gctx, &emptypb.Empty{}, grpc.WaitForReady(true))

				if err != nil {
					return err
				}

				mu.Lock()
				defer mu.Unlock()

				if reply.Protocol > protocol {
					protocol = reply.Protocol
				}

				client.Lock()
				client.protocol = protocol
				client.Unlock()

				return nil
			})
		}
	}

	err := g.Wait()

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
		peer := peer

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
		peer := peer

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

type StreamReply[T protoreflect.ProtoMessage] struct {
	R   T
	Err error
}

// SentryMessagesStreamS implements proto_sentry.Sentry_ReceiveMessagesServer
type SentryStreamS[T protoreflect.ProtoMessage] struct {
	Ch  chan StreamReply[T]
	Ctx context.Context
	grpc.ServerStream
}

func (s *SentryStreamS[T]) Send(m T) error {
	s.Ch <- StreamReply[T]{R: m}
	return nil
}

func (s *SentryStreamS[T]) Context() context.Context { return s.Ctx }

func (s *SentryStreamS[T]) Err(err error) {
	if err == nil {
		return
	}
	s.Ch <- StreamReply[T]{Err: err}
}

func (s *SentryStreamS[T]) Close() {
	if s.Ch != nil {
		ch := s.Ch
		s.Ch = nil
		close(ch)
	}
}

type SentryStreamC[T protoreflect.ProtoMessage] struct {
	Ch  chan StreamReply[T]
	Ctx context.Context
	grpc.ClientStream
}

func (c *SentryStreamC[T]) Recv() (T, error) {
	m, ok := <-c.Ch
	if !ok {
		var t T
		return t, io.EOF
	}
	return m.R, m.Err
}

func (c *SentryStreamC[T]) Context() context.Context { return c.Ctx }

func (c *SentryStreamC[T]) RecvMsg(anyMessage interface{}) error {
	m, err := c.Recv()
	if err != nil {
		return err
	}
	outMessage := anyMessage.(T)
	proto.Merge(outMessage, m)
	return nil
}

func (m *sentryMultiplexer) Messages(ctx context.Context, in *sentryproto.MessagesRequest, opts ...grpc.CallOption) (sentryproto.Sentry_MessagesClient, error) {
	g, gctx := errgroup.WithContext(ctx)

	ch := make(chan StreamReply[*sentryproto.InboundMessage], 16384)
	streamServer := &SentryStreamS[*sentryproto.InboundMessage]{Ch: ch, Ctx: ctx}

	go func() {
		defer close(ch)

		for _, client := range m.clients {
			client := client

			g.Go(func() error {
				messages, err := client.Messages(gctx, in, opts...)

				if err != nil {
					streamServer.Err(err)
					return err
				}

				for {
					inboundMessage, err := messages.Recv()

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

					streamServer.Send(inboundMessage)
				}
			})
		}

		g.Wait()
	}()

	return &SentryStreamC[*sentryproto.InboundMessage]{Ch: ch, Ctx: ctx}, nil
}

func (m *sentryMultiplexer) peersByClient(ctx context.Context, minProtocol sentryproto.Protocol, opts ...grpc.CallOption) ([]*sentryproto.PeersReply, error) {
	g, gctx := errgroup.WithContext(ctx)

	var allReplies []*sentryproto.PeersReply = make([]*sentryproto.PeersReply, len(m.clients))
	var allMutex sync.RWMutex

	for i, client := range m.clients {
		i := i
		client := client

		client.RLock()
		protocol := client.protocol
		client.RUnlock()

		if protocol < minProtocol {
			continue
		}

		g.Go(func() error {
			sentPeers, err := client.Peers(gctx, &emptypb.Empty{}, opts...)

			if err != nil {
				return err
			}

			allMutex.Lock()
			defer allMutex.Unlock()

			allReplies[i] = sentPeers

			return nil
		})
	}

	err := g.Wait()

	if err != nil {
		return nil, err
	}

	return allReplies, nil
}

func (m *sentryMultiplexer) Peers(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*sentryproto.PeersReply, error) {
	var allPeers []*typesproto.PeerInfo

	allReplies, err := m.peersByClient(ctx, -1, opts...)

	if err != nil {
		return nil, err
	}

	for _, sentPeers := range allReplies {
		allPeers = append(allPeers, sentPeers.GetPeers()...)
	}

	return &sentryproto.PeersReply{Peers: allPeers}, nil
}

func (m *sentryMultiplexer) PeerCount(ctx context.Context, in *sentryproto.PeerCountRequest, opts ...grpc.CallOption) (*sentryproto.PeerCountReply, error) {
	g, gctx := errgroup.WithContext(ctx)

	var allCount uint64
	var allMutex sync.RWMutex

	for _, client := range m.clients {
		client := client

		g.Go(func() error {
			peerCount, err := client.PeerCount(gctx, in, opts...)

			if err != nil {
				return err
			}

			allMutex.Lock()
			defer allMutex.Unlock()

			allCount += peerCount.GetCount()

			return nil
		})
	}

	err := g.Wait()

	if err != nil {
		return nil, err
	}

	return &sentryproto.PeerCountReply{Count: allCount}, nil
}

var errFound = fmt.Errorf("found peer")

func (m *sentryMultiplexer) PeerById(ctx context.Context, in *sentryproto.PeerByIdRequest, opts ...grpc.CallOption) (*sentryproto.PeerByIdReply, error) {
	g, gctx := errgroup.WithContext(ctx)

	var peer *typesproto.PeerInfo
	var peerMutex sync.RWMutex

	for _, client := range m.clients {
		client := client

		g.Go(func() error {
			reply, err := client.PeerById(gctx, in, opts...)

			if err != nil {
				return err
			}

			peerMutex.Lock()
			defer peerMutex.Unlock()

			if peer == nil && reply.GetPeer() != nil {
				peer = reply.GetPeer()
				// return a success error here to have the
				// group stop other concurrent requests
				return errFound
			}

			return nil
		})
	}

	err := g.Wait()

	if err != nil && !errors.Is(errFound, err) {
		return nil, err
	}

	return &sentryproto.PeerByIdReply{Peer: peer}, nil
}

func (m *sentryMultiplexer) PeerEvents(ctx context.Context, in *sentryproto.PeerEventsRequest, opts ...grpc.CallOption) (sentryproto.Sentry_PeerEventsClient, error) {
	g, gctx := errgroup.WithContext(ctx)

	ch := make(chan StreamReply[*sentryproto.PeerEvent], 16384)
	streamServer := &SentryStreamS[*sentryproto.PeerEvent]{Ch: ch, Ctx: ctx}

	go func() {
		defer close(ch)

		for _, client := range m.clients {
			client := client

			g.Go(func() error {
				messages, err := client.PeerEvents(gctx, in, opts...)

				if err != nil {
					streamServer.Err(err)
					return err
				}

				for {
					inboundMessage, err := messages.Recv()

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

					streamServer.Send(inboundMessage)
				}
			})
		}

		g.Wait()
	}()

	return &SentryStreamC[*sentryproto.PeerEvent]{Ch: ch, Ctx: ctx}, nil
}

func (m *sentryMultiplexer) AddPeer(ctx context.Context, in *sentryproto.AddPeerRequest, opts ...grpc.CallOption) (*sentryproto.AddPeerReply, error) {
	g, gctx := errgroup.WithContext(ctx)

	var success bool
	var successMutex sync.RWMutex

	for _, client := range m.clients {
		client := client

		g.Go(func() error {
			result, err := client.AddPeer(gctx, in, opts...)

			if err != nil {
				return err
			}

			successMutex.Lock()
			defer successMutex.Unlock()

			// if any client returns success return success
			if !success && result.GetSuccess() {
				success = true
			}

			return nil
		})
	}

	err := g.Wait()

	if err != nil {
		return nil, err
	}

	return &sentryproto.AddPeerReply{Success: success}, nil
}

func (m *sentryMultiplexer) RemovePeer(ctx context.Context, in *sentryproto.RemovePeerRequest, opts ...grpc.CallOption) (*sentryproto.RemovePeerReply, error) {
	g, gctx := errgroup.WithContext(ctx)

	var success bool
	var successMutex sync.RWMutex

	for _, client := range m.clients {
		client := client

		g.Go(func() error {
			result, err := client.RemovePeer(gctx, in, opts...)

			if err != nil {
				return err
			}

			successMutex.Lock()
			defer successMutex.Unlock()

			// if any client returns success return success
			if !success && result.GetSuccess() {
				success = true
			}

			return nil
		})
	}

	err := g.Wait()

	if err != nil {
		return nil, err
	}

	return &sentryproto.RemovePeerReply{Success: success}, nil
}

func (m *sentryMultiplexer) NodeInfo(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*typesproto.NodeInfoReply, error) {
	return nil, status.Errorf(codes.Unimplemented, `method "NodeInfo" not implemented: use "NodeInfos" instead`)
}

func (m *sentryMultiplexer) NodeInfos(ctx context.Context, opts ...grpc.CallOption) ([]*typesproto.NodeInfoReply, error) {
	g, gctx := errgroup.WithContext(ctx)

	var allInfos []*typesproto.NodeInfoReply
	var allMutex sync.RWMutex

	for _, client := range m.clients {
		client := client

		g.Go(func() error {
			info, err := client.NodeInfo(gctx, &emptypb.Empty{}, opts...)

			if err != nil {
				return err
			}

			allMutex.Lock()
			defer allMutex.Unlock()

			allInfos = append(allInfos, info)

			return nil
		})
	}

	err := g.Wait()

	if err != nil {
		return nil, err
	}

	return allInfos, nil
}
