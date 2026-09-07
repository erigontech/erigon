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
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/sentinel/communication"
)

type remotePeerConn struct {
	network.Conn
	peerID peer.ID
}

func (c *remotePeerConn) RemotePeer() peer.ID { return c.peerID }

type deadlineFailingStream struct {
	network.Stream
	conn   network.Conn
	err    error
	reset  bool
	closed bool
}

func (s *deadlineFailingStream) Conn() network.Conn          { return s.conn }
func (s *deadlineFailingStream) SetDeadline(time.Time) error { return s.err }
func (s *deadlineFailingStream) Reset() error                { s.reset = true; return nil }
func (s *deadlineFailingStream) Close() error                { s.closed = true; return nil }

func TestStreamHandlerStopsWhenDeadlineCannotBeSet(t *testing.T) {
	h, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, h.Close()) })

	peerID := peer.ID("deadline-failure-peer")
	c := &ConsensusHandlers{host: h, rateLimiter: newPeerRateLimiter()}
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
	counter, ok := c.rateLimiter.concurrency.Load(peerID.String())
	require.True(t, ok)
	require.Zero(t, counter.(*atomic.Int32).Load())
}
