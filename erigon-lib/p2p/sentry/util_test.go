package sentry_test

import (
	"context"
	"testing"

	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	sentry "github.com/erigontech/erigon-lib/p2p/sentry"
	"github.com/erigontech/erigon/p2p"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

var eth67 = p2p.Protocol{
	Name:           "eth",
	Version:        67,
	Length:         17,
	DialCandidates: nil,
	Run: func(peer *p2p.Peer, rw p2p.MsgReadWriter) *p2p.PeerError {
		return nil
	},
	NodeInfo: func() interface{} {
		return nil
	},
	PeerInfo: func(peerID [64]byte) interface{} {
		return nil
	},
}

var eth68 = p2p.Protocol{
	Name:           "eth",
	Version:        68,
	Length:         17,
	DialCandidates: nil,
	/*Run: func(peer *p2p.Peer, rw p2p.MsgReadWriter) *p2p.PeerError {
		return nil
	},
	NodeInfo: func() interface{} {
		return nil
	},
	PeerInfo: func(peerID [64]byte) interface{} {
		return nil
	},*/
}

type sentryClient struct {
	sentryproto.SentryClient
	mock *direct.MockSentryClient
}

func (c *sentryClient) NodeInfo(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*typesproto.NodeInfoReply, error) {
	return c.mock.NodeInfo(ctx, in, opts...)
}

func TestProtocols(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	direct := newClient(ctrl, 0, []p2p.Protocol{eth67})
	direct.EXPECT().Protocol().Return(67)

	p := sentry.Protocols(direct)

	require.Len(t, p, 1)
	require.Equal(t, byte(67), p[0])

	base := &sentryClient{
		mock: newClient(ctrl, 1, []p2p.Protocol{eth68}),
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

	direct := newClient(ctrl, 0, []p2p.Protocol{eth67})
	direct.EXPECT().PeerById(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&sentryproto.PeerByIdReply{
			Peer: &typesproto.PeerInfo{
				Id:   peerId.String(),
				Caps: []string{"eth/67"},
			},
		}, nil).AnyTimes()

	p := sentry.PeerProtocols(direct, peerId)

	require.Len(t, p, 1)
	require.Equal(t, byte(67), p[0])

	mux := sentry.NewSentryMultiplexer([]sentryproto.SentryClient{direct})
	require.NotNil(t, mux)

	p = sentry.PeerProtocols(mux, peerId)

	require.Len(t, p, 1)
	require.Equal(t, byte(67), p[0])
}
