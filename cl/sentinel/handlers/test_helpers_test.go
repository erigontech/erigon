package handlers

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common/ssz"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/stretchr/testify/require"
)

type envelopeHandlerStream struct {
	network.Stream
	request  *bytes.Reader
	response bytes.Buffer
	conn     network.Conn
	protocol protocol.ID
	expired  bool
}

func (s *envelopeHandlerStream) Read(p []byte) (int, error) { return s.request.Read(p) }
func (s *envelopeHandlerStream) Write(p []byte) (int, error) {
	if s.expired {
		return 0, os.ErrDeadlineExceeded
	}
	return s.response.Write(p)
}
func (s *envelopeHandlerStream) Conn() network.Conn    { return s.conn }
func (s *envelopeHandlerStream) Protocol() protocol.ID { return s.protocol }

func newHandlerTestStream(t *testing.T, proto string, request ssz.EncodableSSZ) *envelopeHandlerStream {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&buf, request))
	return &envelopeHandlerStream{
		request:  bytes.NewReader(buf.Bytes()),
		conn:     &remotePeerConn{peerID: peer.ID("request-peer")},
		protocol: protocol.ID(proto),
	}
}

func admitHandlerTestRequest(t *testing.T, c *ConsensusHandlers, stream network.Stream) *tokenBucket {
	t.Helper()
	key := peerProtocolKey{peerID: stream.Conn().RemotePeer().String(), protocol: string(stream.Protocol())}
	require.True(t, c.rateLimiter.allowRequest(key.peerID, key.protocol, 1))
	value, ok := c.rateLimiter.buckets.Load(key)
	require.True(t, ok)
	bucket := value.(*tokenBucket)
	// Keep refill outside the test interval so exact charges do not depend on timing.
	bucket.lastRefill = time.Now().Add(time.Hour)
	return bucket
}

func getEthClock(t *testing.T) eth_clock.EthereumClock {
	s, err := initial_state.GetGenesisState(t.Context(), chainspec.MainnetChainID)
	require.NoError(t, err)
	return eth_clock.NewEthereumClock(s.GenesisTime(), s.GenesisValidatorsRoot(), s.BeaconConfig())
}
