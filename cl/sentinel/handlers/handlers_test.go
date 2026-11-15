// Copyright 2026 The Erigon Authors
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

package handlers

import (
	"errors"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/sentinel/communication"
)

func TestConsumeRateLimitRejectsNegativeCost(t *testing.T) {
	c := &ConsensusHandlers{rateLimiter: newPeerRateLimiter()}
	stream := newHandlerTestStream(t, communication.PingProtocolV1, &cltypes.Ping{})
	require.True(t, c.consumeRateLimit(stream, 0))
	require.False(t, c.consumeRateLimit(stream, -1))
	code, err := stream.response.ReadByte()
	require.NoError(t, err)
	require.Equal(t, byte(InvalidRequestPrefix), code)
}

type remotePeerConn struct {
	network.Conn
	peerID peer.ID
}

func (c *remotePeerConn) RemotePeer() peer.ID { return c.peerID }

type deadlineFailingStream struct {
	network.Stream
	conn    network.Conn
	err     error
	reset   bool
	closed  bool
	written int
}

func (s *deadlineFailingStream) Conn() network.Conn          { return s.conn }
func (s *deadlineFailingStream) SetDeadline(time.Time) error { return s.err }
func (s *deadlineFailingStream) Write(p []byte) (int, error) { s.written += len(p); return len(p), nil }
func (s *deadlineFailingStream) Reset() error                { s.reset = true; return nil }
func (s *deadlineFailingStream) Close() error                { s.closed = true; return nil }

func TestStreamHandlerStopsWhenDeadlineCannotBeSet(t *testing.T) {
	h, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, h.Close()) })

	peerID := peer.ID("deadline-failure-peer")
	c := &ConsensusHandlers{host: h, rateLimiter: newPeerRateLimiter()}
	c.rateLimiter.concurrency[peerID.String()] = maxConcurrentRequestsPerPeer
	stream := &deadlineFailingStream{
		conn: &remotePeerConn{peerID: peerID},
		err:  errors.New("deadline unavailable"),
	}
	handlerCalled := false
	handler := c.wrapStreamHandler(communication.PingProtocolV1, func(network.Stream) error {
		handlerCalled = true
		return nil
	})

	handler(stream)

	require.False(t, handlerCalled)
	require.True(t, stream.reset)
	require.True(t, stream.closed)
	require.Zero(t, stream.written, "the concurrency limiter must not be reached")
	require.Equal(t, maxConcurrentRequestsPerPeer, c.rateLimiter.concurrency[peerID.String()], "no slot may be acquired or released")
}

type deadlineRecordingStream struct {
	deadlineFailingStream
	deadlineAfterBytes []int
}

func (s *deadlineRecordingStream) SetDeadline(time.Time) error {
	s.deadlineAfterBytes = append(s.deadlineAfterBytes, s.written)
	return nil
}

func TestStreamHandlerSetsDeadlineBeforeRejectingAdmission(t *testing.T) {
	peerID := peer.ID("rejected-peer")
	for _, tc := range []struct {
		name   string
		reject func(c *ConsensusHandlers)
	}{
		{"concurrency limit", func(c *ConsensusHandlers) {
			c.rateLimiter.concurrency[peerID.String()] = maxConcurrentRequestsPerPeer
		}},
		{"rate limit", func(c *ConsensusHandlers) {
			key := peerProtocolKey{peerID: peerID.String(), protocol: communication.PingProtocolV1}
			c.rateLimiter.punished.Store(key, time.Now().Add(time.Hour))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &ConsensusHandlers{rateLimiter: newPeerRateLimiter()}
			tc.reject(c)
			stream := &deadlineRecordingStream{
				deadlineFailingStream: deadlineFailingStream{conn: &remotePeerConn{peerID: peerID}},
			}
			handlerCalled := false

			c.wrapStreamHandler(communication.PingProtocolV1, func(network.Stream) error {
				handlerCalled = true
				return nil
			})(stream)

			require.False(t, handlerCalled)
			require.Equal(t, []int{0}, stream.deadlineAfterBytes, "SetDeadline must run once, before the error response is written")
			require.NotZero(t, stream.written)
		})
	}
}
