package sentry_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/secp256k1"

	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
)

func newClient(ctrl *gomock.Controller, i int, caps []string) *direct.MockSentryClient {
	client := direct.NewMockSentryClient(ctrl)
	pk, _ := ecdsa.GenerateKey(secp256k1.S256(), rand.Reader)
	node := enode.NewV4(&pk.PublicKey, net.IPv4(127, 0, 0, byte(i)), 30001, 30001)

	if len(caps) == 0 {
		caps = []string{"eth/68"}
	}

	client.EXPECT().NodeInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(&typesproto.NodeInfoReply{
		Id:    node.ID().String(),
		Name:  fmt.Sprintf("client-%d", i),
		Enode: node.URLv4(),
		Enr:   node.String(),
		Ports: &typesproto.NodeInfoPorts{
			Discovery: uint32(30000),
			Listener:  uint32(30001),
		},
		ListenerAddr: fmt.Sprintf("127.0.0.%d", i),
	}, nil).AnyTimes()

	client.EXPECT().HandShake(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sentryproto.HandShakeReply{Protocol: sentryproto.Protocol_ETH67}, nil).AnyTimes()

	client.EXPECT().Peers(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*sentryproto.PeersReply, error) {
			id := [64]byte{byte(i)}
			return &sentryproto.PeersReply{
				Peers: []*typesproto.PeerInfo{
					{
						Id:   hex.EncodeToString(id[:]),
						Caps: caps,
					},
				},
			}, nil
		}).AnyTimes()

	return client
}

func TestCreateMultiplexer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var clients []sentryproto.SentryClient

	for i := 0; i < 10; i++ {
		clients = append(clients, newClient(ctrl, i, nil))
	}

	mux := libsentry.NewSentryMultiplexer(clients)
	require.NotNil(t, mux)

	hs, err := mux.HandShake(context.Background(), &emptypb.Empty{})
	require.NotNil(t, hs)
	require.NoError(t, err)

	info, err := mux.NodeInfo(context.Background(), &emptypb.Empty{})
	require.Nil(t, info)
	require.Error(t, err)

	infos, err := mux.NodeInfos(context.Background())
	require.NoError(t, err)
	require.Len(t, infos, 10)
}

func TestStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var clients []sentryproto.SentryClient

	var statusCount int
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		client := newClient(ctrl, i, nil)
		client.EXPECT().SetStatus(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, sd *sentryproto.StatusData, co ...grpc.CallOption) (*sentryproto.SetStatusReply, error) {
				mu.Lock()
				defer mu.Unlock()
				statusCount++
				return &sentryproto.SetStatusReply{}, nil
			})
		client.EXPECT().PenalizePeer(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, sd *sentryproto.PenalizePeerRequest, co ...grpc.CallOption) (*emptypb.Empty, error) {
				mu.Lock()
				defer mu.Unlock()
				statusCount++
				return &emptypb.Empty{}, nil
			})
		client.EXPECT().PeerMinBlock(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, sd *sentryproto.PeerMinBlockRequest, co ...grpc.CallOption) (*emptypb.Empty, error) {
				mu.Lock()
				defer mu.Unlock()
				statusCount++
				return &emptypb.Empty{}, nil
			})

		clients = append(clients, client)
	}

	mux := libsentry.NewSentryMultiplexer(clients)
	require.NotNil(t, mux)

	hs, err := mux.HandShake(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, hs)

	reply, err := mux.SetStatus(context.Background(), &sentryproto.StatusData{})
	require.NoError(t, err)
	require.NotNil(t, reply)
	require.Equal(t, 10, statusCount)

	statusCount = 0

	empty, err := mux.PenalizePeer(context.Background(), &sentryproto.PenalizePeerRequest{})
	require.NoError(t, err)
	require.NotNil(t, empty)
	require.Equal(t, 10, statusCount)

	statusCount = 0

	empty, err = mux.PeerMinBlock(context.Background(), &sentryproto.PeerMinBlockRequest{})
	require.NoError(t, err)
	require.NotNil(t, empty)
	require.Equal(t, 10, statusCount)
}

func TestSend(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var clients []sentryproto.SentryClient

	var statusCount int
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		client := newClient(ctrl, i, nil)
		client.EXPECT().SendMessageByMinBlock(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, in *sentryproto.SendMessageByMinBlockRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
				mu.Lock()
				defer mu.Unlock()
				statusCount++
				return &sentryproto.SentPeers{}, nil
			}).AnyTimes()
		client.EXPECT().SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, in *sentryproto.SendMessageByIdRequest, opts ...grpc.CallOption) (*sentryproto.SentPeers, error) {
				mu.Lock()
				defer mu.Unlock()
				statusCount++
				return &sentryproto.SentPeers{}, nil
			}).AnyTimes()

		clients = append(clients, client)
	}

	mux := libsentry.NewSentryMultiplexer(clients)
	require.NotNil(t, mux)

	_, err := mux.HandShake(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)

	sendReply, err := mux.SendMessageByMinBlock(context.Background(), &sentryproto.SendMessageByMinBlockRequest{})
	require.NoError(t, err)
	require.NotNil(t, sendReply)
	require.Equal(t, 1, statusCount)

	statusCount = 0

	for i := byte(0); i < 10; i++ {
		sendReply, err = mux.SendMessageById(context.Background(), &sentryproto.SendMessageByIdRequest{
			Data: &sentryproto.OutboundMessageData{
				Id: sentryproto.MessageId_BLOCK_BODIES_66,
			},
			PeerId: gointerfaces.ConvertHashToH512([64]byte{i}),
		})
		require.NoError(t, err)
		require.NotNil(t, sendReply)
		require.Equal(t, 1, statusCount)

		statusCount = 0
	}

	sendReply, err = mux.SendMessageToRandomPeers(context.Background(), &sentryproto.SendMessageToRandomPeersRequest{
		Data: &sentryproto.OutboundMessageData{
			Id: sentryproto.MessageId_BLOCK_BODIES_66,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, sendReply)
	require.Equal(t, 10, statusCount)

	statusCount = 0

	sendReply, err = mux.SendMessageToAll(context.Background(), &sentryproto.OutboundMessageData{
		Id: sentryproto.MessageId_BLOCK_BODIES_66,
	})
	require.NoError(t, err)
	require.NotNil(t, sendReply)
	require.Equal(t, 10, statusCount)
}

func TestMessages(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var clients []sentryproto.SentryClient

	for i := 0; i < 10; i++ {
		client := newClient(ctrl, i, nil)
		client.EXPECT().Messages(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, in *sentryproto.MessagesRequest, opts ...grpc.CallOption) (sentryproto.Sentry_MessagesClient, error) {
				ch := make(chan libsentry.StreamReply[*sentryproto.InboundMessage], 16384)
				streamServer := &libsentry.SentryStreamS[*sentryproto.InboundMessage]{Ch: ch, Ctx: ctx}

				go func() {
					for i := 0; i < 5; i++ {
						streamServer.Send(&sentryproto.InboundMessage{})
					}

					streamServer.Close()
				}()

				return &libsentry.SentryStreamC[*sentryproto.InboundMessage]{Ch: ch, Ctx: ctx}, nil
			})

		clients = append(clients, client)
	}

	mux := libsentry.NewSentryMultiplexer(clients)
	require.NotNil(t, mux)

	client, err := mux.Messages(context.Background(), &sentryproto.MessagesRequest{})
	require.NoError(t, err)
	require.NotNil(t, client)

	var messageCount int

	for {
		message, err := client.Recv()

		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}

		messageCount++
		require.NotNil(t, message)
	}

	require.Equal(t, 50, messageCount)
}

func TestPeers(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var clients []sentryproto.SentryClient

	var statusCount int
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		client := newClient(ctrl, i, nil)
		client.EXPECT().AddPeer(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, in *sentryproto.AddPeerRequest, opts ...grpc.CallOption) (*sentryproto.AddPeerReply, error) {
				mu.Lock()
				defer mu.Unlock()
				statusCount++
				return &sentryproto.AddPeerReply{}, nil
			})
		client.EXPECT().PeerEvents(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, in *sentryproto.PeerEventsRequest, opts ...grpc.CallOption) (sentryproto.Sentry_PeerEventsClient, error) {
				ch := make(chan libsentry.StreamReply[*sentryproto.PeerEvent], 16384)
				streamServer := &libsentry.SentryStreamS[*sentryproto.PeerEvent]{Ch: ch, Ctx: ctx}

				go func() {
					for i := 0; i < 5; i++ {
						streamServer.Send(&sentryproto.PeerEvent{})
					}

					streamServer.Close()
				}()

				return &libsentry.SentryStreamC[*sentryproto.PeerEvent]{Ch: ch, Ctx: ctx}, nil
			})
		client.EXPECT().PeerById(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, in *sentryproto.PeerByIdRequest, opts ...grpc.CallOption) (*sentryproto.PeerByIdReply, error) {
				mu.Lock()
				defer mu.Unlock()
				statusCount++
				return &sentryproto.PeerByIdReply{}, nil
			})
		client.EXPECT().PeerCount(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, in *sentryproto.PeerCountRequest, opts ...grpc.CallOption) (*sentryproto.PeerCountReply, error) {
				mu.Lock()
				defer mu.Unlock()
				statusCount++
				return &sentryproto.PeerCountReply{}, nil
			})

		clients = append(clients, client)
	}

	mux := libsentry.NewSentryMultiplexer(clients)
	require.NotNil(t, mux)

	_, err := mux.HandShake(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)

	addPeerReply, err := mux.AddPeer(context.Background(), &sentryproto.AddPeerRequest{})
	require.NoError(t, err)
	require.NotNil(t, addPeerReply)
	require.Equal(t, 10, statusCount)

	client, err := mux.PeerEvents(context.Background(), &sentryproto.PeerEventsRequest{})
	require.NoError(t, err)
	require.NotNil(t, client)

	var eventCount int

	for {
		message, err := client.Recv()

		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}

		eventCount++
		require.NotNil(t, message)
	}

	require.Equal(t, 50, eventCount)

	statusCount = 0

	peerIdReply, err := mux.PeerById(context.Background(), &sentryproto.PeerByIdRequest{})
	require.NoError(t, err)
	require.NotNil(t, peerIdReply)
	require.Equal(t, 10, statusCount)

	statusCount = 0

	peerCountReply, err := mux.PeerCount(context.Background(), &sentryproto.PeerCountRequest{})
	require.NoError(t, err)
	require.NotNil(t, peerCountReply)
	require.Equal(t, 10, statusCount)

	peersReply, err := mux.Peers(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, peersReply)
	require.Len(t, peersReply.GetPeers(), 10)
}
