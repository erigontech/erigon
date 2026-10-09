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
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
)

func TestPumpStreamLoopDoesNotReadAhead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		s, c := NewSentryStream[*sentryproto.InboundMessage](ctx)
		data := make([]byte, 10*1024*1024)
		for range 2 {
			require.NoError(t, s.Send(&sentryproto.InboundMessage{Data: data}))
		}

		stream := &countingClientStream{ClientStream: c}
		done := make(chan error, 1)
		go func() {
			done <- pumpStreamLoop(ctx, nil, "test",
				func(context.Context, sentryproto.SentryClient) (grpc.ClientStream, error) { return stream, nil },
				func() *sentryproto.InboundMessage { return new(sentryproto.InboundMessage) },
				func(ctx context.Context, _ *sentryproto.InboundMessage, _ sentryproto.SentryClient) error {
					<-ctx.Done()
					return nil
				}, nil, log.New())
		}()

		synctest.Wait()
		require.Equal(t, 1, stream.received, "a blocked handler must leave later payloads in the bounded stream queue")
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
	})
}

type countingClientStream struct {
	grpc.ClientStream
	received int
}

func (s *countingClientStream) RecvMsg(message any) error {
	err := s.ClientStream.RecvMsg(message)
	if err == nil {
		s.received++
	}
	return err
}
