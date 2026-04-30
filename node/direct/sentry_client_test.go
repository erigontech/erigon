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

package direct

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
)

// Flooding Send() with no consumer must not grow the buffer beyond capacity
// (the pre-fix 16384 chan would hold ~160 GB of peer-controlled payload at
// the eth 10 MB limit). Eviction must drop oldest entries, preserving the
// freshest.
func TestSentryMessagesStreamS_SendEvictsWhenConsumerSlow(t *testing.T) {
	ch := make(chan *inboundMessageReply, libsentry.MessagesQueueSize)
	s := &SentryMessagesStreamS{ch: ch, ctx: t.Context()}

	const flood = libsentry.MessagesQueueSize * 10
	for i := range flood {
		data := make([]byte, 4)
		binary.LittleEndian.PutUint32(data, uint32(i))
		require.NoError(t, s.Send(&sentryproto.InboundMessage{Data: data}))
	}

	require.LessOrEqual(t, len(ch), cap(ch), "buffer must not exceed capacity")
	require.Less(t, len(ch), flood, "eviction must have dropped entries")

	var last uint32
	for len(ch) > 0 {
		r := <-ch
		require.NotNil(t, r)
		require.NotNil(t, r.r)
		last = binary.LittleEndian.Uint32(r.r.Data)
	}
	assert.Equal(t, uint32(flood-1), last,
		"eviction drops from the front; the most recent Send must remain queued")
}

func TestSentryPeersStreamS_SendEvictsWhenConsumerSlow(t *testing.T) {
	ch := make(chan *peersReply, libsentry.MessagesQueueSize)
	s := &SentryPeersStreamS{ch: ch, ctx: t.Context()}

	const flood = libsentry.MessagesQueueSize * 10
	for range flood {
		require.NoError(t, s.Send(&sentryproto.PeerEvent{}))
	}

	require.LessOrEqual(t, len(ch), cap(ch))
	require.Less(t, len(ch), flood)
}
