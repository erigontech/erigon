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
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
)

// Flooding Send with no consumer must not block or grow the queue without bound.
// Small payloads exercise repeated eviction by message count without reaching
// the byte limit; the newest message must survive.
func TestSentryStreamS_SendEvictsWhenConsumerSlow(t *testing.T) {
	s, c := newTestSentryStream(t)

	const flood = libsentry.MessagesQueueSize * 10
	for i := range flood {
		data := make([]byte, 4)
		binary.LittleEndian.PutUint32(data, uint32(i))
		require.NoError(t, s.Send(&sentryproto.InboundMessage{Data: data}))
	}

	s.Close()
	messages := drainMessages(t, c)
	require.LessOrEqual(t, len(messages), libsentry.MessagesQueueSize)
	require.Less(t, len(messages), flood, "eviction must have dropped entries")

	var last uint32
	for _, message := range messages {
		require.NotNil(t, message)
		last = binary.LittleEndian.Uint32(message.Data)
	}
	assert.Equal(t, uint32(flood-1), last,
		"eviction drops from the front; the most recent Send must remain queued")
}

func TestSentryStreamS_BoundsQueuedPayloadBytes(t *testing.T) {
	s, c := newTestSentryStream(t)
	dropped := metrics.GetOrCreateCounter(`p2p_sentry_queue_dropped_messages_total{limit="bytes"}`)
	before := dropped.GetValueUint64()
	data := make([]byte, 10*1024*1024)
	for i := range 20 {
		require.NoError(t, s.Send(&sentryproto.InboundMessage{Id: sentryproto.MessageId(i), Data: data}))
	}

	s.Close()
	messages := drainMessages(t, c)
	require.Len(t, messages, 6)
	require.Equal(t, sentryproto.MessageId(14), messages[0].Id)
	require.Equal(t, sentryproto.MessageId(19), messages[len(messages)-1].Id)
	var queuedBytes int
	for _, message := range messages {
		queuedBytes += len(message.Data)
	}
	require.LessOrEqual(t, queuedBytes, 64*1024*1024)
	require.Equal(t, before+14, dropped.GetValueUint64(), "only evicted messages count as dropped")
}

func TestSentryStreamS_ReceiveReleasesByteBudget(t *testing.T) {
	s, c := newTestSentryStream(t)
	data := make([]byte, 10*1024*1024)
	for i := range 6 {
		require.NoError(t, s.Send(&sentryproto.InboundMessage{Id: sentryproto.MessageId(i), Data: data}))
	}
	for range 5 {
		_, err := c.Recv()
		require.NoError(t, err)
	}
	for i := range 5 {
		require.NoError(t, s.Send(&sentryproto.InboundMessage{Id: sentryproto.MessageId(6 + i), Data: data}))
	}
	s.Close()
	messages := drainMessages(t, c)
	require.Len(t, messages, 6)
	require.Equal(t, sentryproto.MessageId(5), messages[0].Id)
}

func TestSentryStream_ConcurrentSendAndReceive(t *testing.T) {
	s, c := newTestSentryStream(t)
	var producers sync.WaitGroup
	for range 4 {
		producers.Go(func() {
			for range 1000 {
				assert.NoError(t, s.Send(&sentryproto.InboundMessage{Data: []byte{1}}))
			}
		})
	}
	go func() {
		producers.Wait()
		assert.NoError(t, s.Send(&sentryproto.InboundMessage{Data: []byte{2}}))
		s.Close()
	}()
	messages := drainMessages(t, c)
	require.NotEmpty(t, messages)
	require.Equal(t, []byte{2}, messages[len(messages)-1].Data)
}

func TestSentryStream_ReceiveCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	_, c := libsentry.NewSentryStream[*sentryproto.InboundMessage](ctx)
	cancel()
	_, err := c.Recv()
	require.ErrorIs(t, err, context.Canceled)
}

func TestSentryStream_SendCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	s, _ := libsentry.NewSentryStream[*sentryproto.InboundMessage](ctx)
	cancel()
	err := s.Send(&sentryproto.InboundMessage{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestSentryStream_SendAfterClose(t *testing.T) {
	s, c := newTestSentryStream(t)
	s.Close()
	err := s.Send(&sentryproto.InboundMessage{})
	require.ErrorIs(t, err, io.EOF)
	require.Empty(t, drainMessages(t, c))
}

func TestSentryStream_RejectsOversizeMessage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, c := newTestSentryStream(t)
		err := s.Send(&sentryproto.InboundMessage{Data: make([]byte, libsentry.MessagesQueueByteLimit+1)})
		require.ErrorContains(t, err, "exceeds queue byte limit")
		require.NoError(t, s.Send(&sentryproto.InboundMessage{Data: []byte{1}}))
		s.Close()
		messages := drainMessages(t, c)
		require.Len(t, messages, 1)
		require.Equal(t, []byte{1}, messages[0].Data)
	})
}

func TestSentryStream_ErrorAfterMessages(t *testing.T) {
	s, c := newTestSentryStream(t)
	// A terminal error must not consume a queue slot and trigger eviction.
	for range libsentry.MessagesQueueSize / 2 {
		require.NoError(t, s.Send(&sentryproto.InboundMessage{Data: []byte{1}}))
	}
	s.Err(io.ErrUnexpectedEOF)
	s.Close()
	for range libsentry.MessagesQueueSize / 2 {
		message, err := c.Recv()
		require.NoError(t, err)
		require.Equal(t, []byte{1}, message.Data)
	}
	_, err := c.Recv()
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	_, err = c.Recv()
	require.ErrorIs(t, err, io.EOF)
}

func TestSentryStream_ErrorClosesStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, c := newTestSentryStream(t)
		errs := make(chan error, 2)
		for range 2 {
			go func() {
				_, err := c.Recv()
				errs <- err
			}()
		}
		synctest.Wait()
		s.Err(io.ErrUnexpectedEOF)
		require.ElementsMatch(t, []error{io.ErrUnexpectedEOF, io.EOF}, []error{<-errs, <-errs})
		require.ErrorIs(t, s.Send(&sentryproto.InboundMessage{}), io.EOF)
	})
}

func TestSentryStream_NilErrorKeepsStreamOpen(t *testing.T) {
	s, c := newTestSentryStream(t)
	s.Err(nil)
	require.NoError(t, s.Send(&sentryproto.InboundMessage{}))
	s.Close()
	require.Len(t, drainMessages(t, c), 1)
}

func TestSentryStream_FirstCloseWins(t *testing.T) {
	for _, terminalErr := range []error{io.ErrUnexpectedEOF, io.EOF} {
		t.Run(terminalErr.Error(), func(t *testing.T) {
			s, c := newTestSentryStream(t)
			if errors.Is(terminalErr, io.EOF) {
				s.Close()
			} else {
				s.Err(terminalErr)
			}
			s.Err(io.ErrClosedPipe)
			var producers sync.WaitGroup
			for range 4 {
				producers.Go(func() { s.Err(io.ErrClosedPipe) })
				producers.Go(s.Close)
			}
			producers.Wait()
			_, err := c.Recv()
			require.ErrorIs(t, err, terminalErr)
			_, err = c.Recv()
			require.ErrorIs(t, err, io.EOF)
		})
	}
}

func newTestSentryStream(t *testing.T) (*libsentry.SentryStreamS[*sentryproto.InboundMessage], *libsentry.SentryStreamC[*sentryproto.InboundMessage]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return libsentry.NewSentryStream[*sentryproto.InboundMessage](ctx)
}

func drainMessages(t *testing.T, c *libsentry.SentryStreamC[*sentryproto.InboundMessage]) []*sentryproto.InboundMessage {
	t.Helper()
	var messages []*sentryproto.InboundMessage
	for {
		message, err := c.Recv()
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			return messages
		}
		messages = append(messages, message)
	}
}
