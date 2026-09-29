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

package libsentry

import (
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
)

func TestMessageQueuePopEmptyPanics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, _ := NewSentryStream[*sentryproto.InboundMessage](t.Context())
		s.queue.mu.Lock()
		defer s.queue.mu.Unlock()
		require.PanicsWithValue(t, "sentry queue: pop from empty queue", func() { s.queue.pop() })
	})
}

func TestMessageQueuePushFullPanics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, _ := NewSentryStream[*sentryproto.InboundMessage](t.Context())
		s.queue.mu.Lock()
		for range cap(s.queue.items) {
			s.queue.items <- streamReply[*sentryproto.InboundMessage]{}
		}
		s.queue.mu.Unlock()
		require.PanicsWithValue(t, "sentry queue: push to full queue", func() {
			_ = s.Send(&sentryproto.InboundMessage{})
		})
	})
}
