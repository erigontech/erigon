package sentry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

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

type sentryMultiplexer struct {
	clients []sentryproto.SentryClient
}

func NewSentryMultiplexer(clients []sentryproto.SentryClient) *sentryMultiplexer {
	return &sentryMultiplexer{clients}
}

func (m *sentryMultiplexer) SetStatus(ctx context.Context, in *sentryproto.StatusData, opts ...grpc.CallOption) (*sentryproto.SetStatusReply, error) {
	g, gctx := errgroup.WithContext(ctx)

	for _, client := range m.clients {
		client := client

		g.Go(func() error {
			_, err := client.SetStatus(gctx, in, opts...)
			return err
		})
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

	return nil, g.Wait()
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

	return nil, g.Wait()
}

// Handshake is not performed on the multi-client level
func (m *sentryMultiplexer) HandShake(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*sentryproto.HandShakeReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SetStatus not implemented")
}

func (m *sentryMultiplexer) SendMessageByMinBlock(ctx context.Context, in *sentryproto.SendMessageByMinBlockRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
	g, gctx := errgroup.WithContext(ctx)

	var allSentPeers []*typesproto.H512
	var allSentMutex sync.RWMutex

	for _, client := range m.clients {
		client := client

		g.Go(func() error {
			sentPeers, err := client.SendMessageByMinBlock(gctx, in, opts...)

			if err != nil {
				return err
			}

			allSentMutex.Lock()
			defer allSentMutex.Unlock()

			allSentPeers = append(allSentPeers, sentPeers.GetPeers()...)

			return nil
		})
	}

	err := g.Wait()

	if err != nil {
		return nil, err
	}

	return &sentryproto.SentPeers{Peers: allSentPeers}, nil
}

func (m *sentryMultiplexer) SendMessageById(ctx context.Context, in *sentryproto.SendMessageByIdRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
	g, gctx := errgroup.WithContext(ctx)

	var allSentPeers []*typesproto.H512
	var allSentMutex sync.RWMutex

	for _, client := range m.clients {
		client := client

		g.Go(func() error {
			sentPeers, err := client.SendMessageById(gctx, in, opts...)

			if err != nil {
				return err
			}

			allSentMutex.Lock()
			defer allSentMutex.Unlock()

			allSentPeers = append(allSentPeers, sentPeers.GetPeers()...)

			return nil
		})
	}

	err := g.Wait()

	if err != nil {
		return nil, err
	}

	return &sentryproto.SentPeers{Peers: allSentPeers}, nil
}

func (m *sentryMultiplexer) SendMessageToRandomPeers(ctx context.Context, in *sentryproto.SendMessageToRandomPeersRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
	g, gctx := errgroup.WithContext(ctx)

	var allSentPeers []*typesproto.H512
	var allSentMutex sync.RWMutex

	for _, client := range m.clients {
		client := client

		g.Go(func() error {
			sentPeers, err := client.SendMessageToRandomPeers(gctx, in, opts...)

			if err != nil {
				return err
			}

			allSentMutex.Lock()
			defer allSentMutex.Unlock()

			allSentPeers = append(allSentPeers, sentPeers.GetPeers()...)

			return nil
		})
	}

	err := g.Wait()

	if err != nil {
		return nil, err
	}

	return &sentryproto.SentPeers{Peers: allSentPeers}, nil
}

func (m *sentryMultiplexer) SendMessageToAll(ctx context.Context, in *sentryproto.OutboundMessageData, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
	g, gctx := errgroup.WithContext(ctx)

	var allSentPeers []*typesproto.H512
	var allSentMutex sync.RWMutex

	for _, client := range m.clients {
		client := client

		g.Go(func() error {
			sentPeers, err := client.SendMessageToAll(gctx, in, opts...)

			if err != nil {
				return err
			}

			allSentMutex.Lock()
			defer allSentMutex.Unlock()

			allSentPeers = append(allSentPeers, sentPeers.GetPeers()...)

			return nil
		})
	}

	err := g.Wait()

	if err != nil {
		return nil, err
	}

	return &sentryproto.SentPeers{Peers: allSentPeers}, nil
}

type reply[T protoreflect.ProtoMessage] struct {
	r   T
	err error
}

// SentryMessagesStreamS implements proto_sentry.Sentry_ReceiveMessagesServer
type SentryStreamS[T protoreflect.ProtoMessage] struct {
	ch  chan reply[T]
	ctx context.Context
	grpc.ServerStream
}

func (s *SentryStreamS[T]) Send(m T) error {
	s.ch <- reply[T]{r: m}
	return nil
}

func (s *SentryStreamS[T]) Context() context.Context { return s.ctx }

func (s *SentryStreamS[T]) Err(err error) {
	if err == nil {
		return
	}
	s.ch <- reply[T]{err: err}
}

type SentryStreamC[T protoreflect.ProtoMessage] struct {
	ch  chan reply[T]
	ctx context.Context
	grpc.ClientStream
}

func (c *SentryStreamC[T]) Recv() (T, error) {
	m, ok := <-c.ch
	if !ok {
		var t T
		return t, io.EOF
	}
	return m.r, m.err
}

func (c *SentryStreamC[T]) Context() context.Context { return c.ctx }

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

	ch := make(chan reply[*sentryproto.InboundMessage], 16384)
	streamServer := &SentryStreamS[*sentryproto.InboundMessage]{ch: ch, ctx: ctx}

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

	return &SentryStreamC[*sentryproto.InboundMessage]{ch: ch, ctx: ctx}, nil
}

func (m *sentryMultiplexer) Peers(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*sentryproto.PeersReply, error) {
	g, gctx := errgroup.WithContext(ctx)

	var allPeers []*typesproto.PeerInfo
	var allMutex sync.RWMutex

	for _, client := range m.clients {
		client := client

		g.Go(func() error {
			sentPeers, err := client.Peers(gctx, in, opts...)

			if err != nil {
				return err
			}

			allMutex.Lock()
			defer allMutex.Unlock()

			allPeers = append(allPeers, sentPeers.GetPeers()...)

			return nil
		})
	}

	err := g.Wait()

	if err != nil {
		return nil, err
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

	ch := make(chan reply[*sentryproto.PeerEvent], 16384)
	streamServer := &SentryStreamS[*sentryproto.PeerEvent]{ch: ch, ctx: ctx}

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

	return &SentryStreamC[*sentryproto.PeerEvent]{ch: ch, ctx: ctx}, nil
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
