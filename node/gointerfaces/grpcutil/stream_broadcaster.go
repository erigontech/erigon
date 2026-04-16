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

package grpcutil

import (
	"sync"

	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common/log/v3"
)

// StreamBroadcaster manages a set of gRPC server-streaming subscribers and
// broadcasts messages to all of them. It is safe to use as a non-pointer value.
//
// T is the response message type (e.g. OnAddReply, OnMinedBlockReply).
type StreamBroadcaster[T any] struct {
	chans map[uint]grpc.ServerStreamingServer[T]
	mu    sync.Mutex
	id    uint
}

// Add registers a new stream subscriber and returns a function that removes it.
func (s *StreamBroadcaster[T]) Add(stream grpc.ServerStreamingServer[T]) (remove func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.chans == nil {
		s.chans = make(map[uint]grpc.ServerStreamingServer[T])
	}
	s.id++
	id := s.id
	s.chans[id] = stream
	return func() { s.remove(id) }
}

// Broadcast sends reply to every registered stream. Streams whose context is
// done are automatically removed.
func (s *StreamBroadcaster[T]) Broadcast(reply *T, logger log.Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, stream := range s.chans {
		err := stream.Send(reply)
		if err != nil {
			logger.Debug("failed send to stream", "err", err)
			select {
			case <-stream.Context().Done():
				delete(s.chans, id)
			default:
			}
		}
	}
}

func (s *StreamBroadcaster[T]) remove(id uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.chans[id]
	if !ok { // double-unsubscribe support
		return
	}
	delete(s.chans, id)
}
