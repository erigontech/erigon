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
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/common/log/v3"
)

// subscriberQueueLen is a jitter buffer for a short read stall, not a
// durability buffer.
const subscriberQueueLen = 64

// errSubscriberTooSlow ends a subscription that fell behind. IsRetryLater
// recognises the code, so clients back off and resubscribe.
var errSubscriberTooSlow = status.Error(codes.ResourceExhausted, "stream subscriber fell behind")

// StreamBroadcaster fans a message out to a set of gRPC server-streaming
// subscribers. It is safe to use as a non-pointer value.
//
// Delivery is best-effort: a subscriber that falls subscriberQueueLen messages
// behind is dropped and has to resubscribe.
//
// Broadcast takes ownership of the message it is given: every subscriber is
// handed the same pointer and it may still be queued after Broadcast returns,
// so callers must neither mutate nor reuse it.
//
// T is the response message type (e.g. OnAddReply, OnMinedBlockReply).
type StreamBroadcaster[T any] struct {
	subs map[uint]chan *T
	mu   sync.Mutex
	id   uint
}

// Subscribe registers stream and forwards broadcasts to it until ctx or the
// stream ends, a send fails, or the subscriber falls behind.
//
// It must be called on the gRPC handler goroutine, and the handler must return
// when it returns: that goroutine performs every Send, which is what keeps the
// stream from being written concurrently or after the handler has finished.
func (s *StreamBroadcaster[T]) Subscribe(ctx context.Context, stream grpc.ServerStreamingServer[T]) error {
	streamCtx := stream.Context()
	queue, id := s.add()
	defer s.remove(id)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-streamCtx.Done():
			return streamCtx.Err()
		case reply, ok := <-queue:
			if !ok {
				return errSubscriberTooSlow
			}
			if err := stream.Send(reply); err != nil {
				return err
			}
		}
	}
}

// Broadcast queues reply for every registered subscriber. The lock is held only
// across non-blocking queue writes, so an unresponsive subscriber can stall
// neither the caller nor the other subscribers.
//
// A subscriber whose queue is full is dropped, and its queued messages are
// released rather than delivered: a wedged Send would otherwise keep all of them
// reachable. Its Subscribe returns whatever ended that Send.
func (s *StreamBroadcaster[T]) Broadcast(reply *T, logger log.Logger) {
	var dropped int
	s.mu.Lock()
	for id, queue := range s.subs {
		select {
		case queue <- reply:
		default:
			delete(s.subs, id)
			discard(queue)
			close(queue)
			dropped++
		}
	}
	s.mu.Unlock()

	if dropped > 0 {
		logger.Warn("[grpc] dropped stream subscribers that stopped reading",
			"count", dropped, "stream", fmt.Sprintf("%T", reply))
	}
}

func discard[T any](queue chan *T) {
	for {
		select {
		case <-queue:
		default:
			return
		}
	}
}

func (s *StreamBroadcaster[T]) add() (queue chan *T, id uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subs == nil {
		s.subs = make(map[uint]chan *T)
	}
	s.id++
	queue = make(chan *T, subscriberQueueLen)
	s.subs[s.id] = queue
	return queue, s.id
}

// ids are never reused, so a subscriber tearing down late cannot unregister a
// later one that has since taken its place.
func (s *StreamBroadcaster[T]) remove(id uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subs, id)
}
