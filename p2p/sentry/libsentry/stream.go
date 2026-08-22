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
	"io"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type StreamReply[T protoreflect.ProtoMessage] struct {
	R   T
	Err error
}

type SentryStreamS[T protoreflect.ProtoMessage] struct {
	Ch  chan StreamReply[T]
	Ctx context.Context
	grpc.ServerStream
}

func (s *SentryStreamS[T]) Send(m T) error {
	s.Ch <- StreamReply[T]{R: m}
	EvictOldestIfHalfFull(s.Ch)
	return nil
}

func (s *SentryStreamS[T]) Context() context.Context { return s.Ctx }

func (s *SentryStreamS[T]) Err(err error) {
	if err == nil {
		return
	}
	s.Ch <- StreamReply[T]{Err: err}
}

func (s *SentryStreamS[T]) Close() {
	if s.Ch != nil {
		ch := s.Ch
		s.Ch = nil
		close(ch)
	}
}

type SentryStreamC[T protoreflect.ProtoMessage] struct {
	Ch  chan StreamReply[T]
	Ctx context.Context
	grpc.ClientStream
}

func (c *SentryStreamC[T]) Recv() (T, error) {
	m, ok := <-c.Ch
	if !ok {
		var t T
		return t, io.EOF
	}
	return m.R, m.Err
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
