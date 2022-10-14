package ssz_snappy

import (
	"context"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication/p2p"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p/core/network"
	basichost "github.com/libp2p/go-libp2p/p2p/host/basic"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"
	"github.com/test-go/testify/require"
)

func TestMetadataPacketStream(t *testing.T) {
	ctx := context.Background()

	h1, err := basichost.NewHost(swarmt.GenSwarm(t), nil)
	require.NoError(t, err)
	defer h1.Close()

	h2, err := basichost.NewHost(swarmt.GenSwarm(t), nil)
	require.NoError(t, err)
	defer h2.Close()

	packet := &p2p.Goodbye{
		Reason: 2,
	}

	doneCh := make(chan struct{})
	h2.SetStreamHandler(protocol.TestingID, func(stream network.Stream) {
		p := &p2p.Goodbye{}
		codecA := NewStreamCodec(stream)
		_, err := codecA.Decode(p)
		require.NoError(t, err)
		require.Equal(t, *p, *packet)
		doneCh <- struct{}{}
	})

	h2pi := h2.Peerstore().PeerInfo(h2.ID())
	require.NoError(t, h1.Connect(ctx, h2pi))

	s, err := h1.NewStream(ctx, h2pi.ID, protocol.TestingID)
	require.NoError(t, err)

	codec := NewStreamCodec(s)
	_, err = codec.WritePacket(packet)
	require.NoError(t, err)
	require.NoError(t, codec.CloseWriter())
	timeout := time.NewTimer(2 * time.Second)
	select {
	case <-doneCh:
	case <-timeout.C:
		t.Fail()
	}
}
