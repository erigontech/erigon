// Copyright 2024 The Erigon Authors
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

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// NewSentryStream returns the sending and receiving ends of a shared bounded queue.
func NewSentryStream[T protoreflect.ProtoMessage](ctx context.Context) (*SentryStreamS[T], *SentryStreamC[T]) {
	queue := &messageQueue[T]{
		items: make([]queuedMessage[T], 0, MessagesQueueSize),
		ready: make(chan struct{}, 1),
	}
	return &SentryStreamS[T]{queue: queue, Ctx: ctx}, &SentryStreamC[T]{queue: queue, Ctx: ctx}
}

type SentryStreamS[T protoreflect.ProtoMessage] struct {
	queue *messageQueue[T]
	Ctx   context.Context
	grpc.ServerStream
}

// Send queues m without copying and evicts old messages instead of waiting for
// a slow receiver. After a successful send, the caller must not modify m or its
// payload, since queued messages and receivers may share the same data.
func (s *SentryStreamS[T]) Send(m T) error {
	if err := s.Ctx.Err(); err != nil {
		return err
	}
	return s.queue.push(m)
}

func (s *SentryStreamS[T]) Context() context.Context { return s.Ctx }

// Err closes the stream with err unless it is nil or the stream is already closed.
// Receivers drain queued messages, then receive the error once, followed by EOF,
// unless their context is canceled.
func (s *SentryStreamS[T]) Err(err error) {
	if err == nil {
		return
	}
	s.queue.close(err)
}

// Close rejects new sends. Receivers can drain queued messages before EOF
// unless their context is canceled.
func (s *SentryStreamS[T]) Close() {
	s.queue.close(nil)
}

type SentryStreamC[T protoreflect.ProtoMessage] struct {
	queue *messageQueue[T]
	Ctx   context.Context
	grpc.ClientStream
}

func (c *SentryStreamC[T]) Recv() (T, error) {
	return c.queue.recv(c.Ctx)
}

func (c *SentryStreamC[T]) Context() context.Context { return c.Ctx }

func (c *SentryStreamC[T]) RecvMsg(anyMessage any) error {
	m, err := c.Recv()
	if err != nil {
		return err
	}
	outMessage := anyMessage.(T)
	proto.Merge(outMessage, m)
	return nil
}
