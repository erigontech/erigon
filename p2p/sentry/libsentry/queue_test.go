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

package libsentry_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
)

func TestSentryQueue_NoEvictionBelowThreshold(t *testing.T) {
	s, c := libsentry.NewSentryStream[*sentryproto.InboundMessage](t.Context())
	for range 512 {
		require.NoError(t, s.Send(&sentryproto.InboundMessage{}))
	}
	s.Close()
	assert.Len(t, drainMessages(t, c), 512)
}

func TestSentryQueue_DropsQuarterFromOldest(t *testing.T) {
	s, c := libsentry.NewSentryStream[*sentryproto.InboundMessage](t.Context())
	for i := range 600 {
		require.NoError(t, s.Send(&sentryproto.InboundMessage{Id: sentryproto.MessageId(i)}))
	}
	s.Close()
	messages := drainMessages(t, c)
	require.Len(t, messages, 344)
	assert.Equal(t, sentryproto.MessageId(256), messages[0].Id)
	assert.Equal(t, sentryproto.MessageId(599), messages[len(messages)-1].Id)
}

func TestSentryQueue_EmptyClose(t *testing.T) {
	s, c := libsentry.NewSentryStream[*sentryproto.InboundMessage](t.Context())
	s.Close()
	s.Close()
	require.Empty(t, drainMessages(t, c))
}
