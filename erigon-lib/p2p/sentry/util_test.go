package sentry_test

import (
	"context"
	"testing"

	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	sentry "github.com/erigontech/erigon-lib/p2p/sentry"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func newClient(ctrl *gomock.Controller, peerId *typesproto.H512, caps []string) *direct.MockSentryClient {
	client := direct.NewMockSentryClient(ctrl)
	client.EXPECT().PeerById(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&sentryproto.PeerByIdReply{
			Peer: &typesproto.PeerInfo{
				Id:   peerId.String(),
				Caps: caps,
			},
		}, nil).AnyTimes()

	client.EXPECT().Peers(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&sentryproto.PeersReply{
			Peers: []*typesproto.PeerInfo{{
				Id:   peerId.String(),
				Caps: caps,
			}},
		}, nil).AnyTimes()

	return client
}

type sentryClient struct {
	sentryproto.SentryClient
	mock *direct.MockSentryClient
}

func (c *sentryClient) Peers(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*sentryproto.PeersReply, error) {
	return c.mock.Peers(ctx, in, opts...)
}

func TestProtocols(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	direct := newClient(ctrl, gointerfaces.ConvertHashToH512([64]byte{0}), []string{"eth/67"})
	direct.EXPECT().Protocol().Return(67)

	p := sentry.Protocols(direct)

	require.Len(t, p, 1)
	require.Equal(t, byte(67), p[0])

	base := &sentryClient{
		mock: newClient(ctrl, gointerfaces.ConvertHashToH512([64]byte{1}), []string{"eth/68"}),
	}

	p = sentry.Protocols(base)

	require.Len(t, p, 1)
	require.Equal(t, byte(68), p[0])

	mux := sentry.NewSentryMultiplexer([]sentryproto.SentryClient{direct, base})
	require.NotNil(t, mux)

	p = sentry.Protocols(mux)

	require.Len(t, p, 2)
	require.Contains(t, p, byte(67))
	require.Contains(t, p, byte(68))
}

func TestProtocolsByPeerId(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	peerId := gointerfaces.ConvertHashToH512([64]byte{})

	direct := newClient(ctrl, peerId, []string{"eth/67"})

	p := sentry.PeerProtocols(direct, peerId)

	require.Len(t, p, 1)
	require.Equal(t, byte(67), p[0])

	mux := sentry.NewSentryMultiplexer([]sentryproto.SentryClient{direct})
	require.NotNil(t, mux)

	p = sentry.PeerProtocols(mux, peerId)

	require.Len(t, p, 1)
	require.Equal(t, byte(67), p[0])
}
