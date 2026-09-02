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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common/log/v3"
)

const testTimeout = 5 * time.Second

type testMsg struct{ n int }

// fakeStream stands in for a gRPC server stream. Send blocks until a token is
// read from gate, or until the stream context ends - which is what a real
// SendMsg does when it runs out of flow control.
type fakeStream struct {
	grpc.ServerStream
	ctx     context.Context
	gate    chan struct{}
	sent    chan *testMsg
	sendErr error
}

func newFakeStream(ctx context.Context) *fakeStream {
	return &fakeStream{ctx: ctx, sent: make(chan *testMsg, 1024)}
}

func (f *fakeStream) Context() context.Context { return f.ctx }

func (f *fakeStream) Send(m *testMsg) error {
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-f.ctx.Done():
			return f.ctx.Err()
		}
	}
	if f.sendErr != nil {
		return f.sendErr
	}
	f.sent <- m
	return nil
}

func subCount[T any](s *StreamBroadcaster[T]) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subs)
}

// subscribe starts Subscribe on its own goroutine, waits until the stream is
// registered, and returns a channel carrying Subscribe's result.
func subscribe(t *testing.T, b *StreamBroadcaster[testMsg], ctx context.Context, stream *fakeStream) chan error {
	t.Helper()
	before := subCount(b)
	errs := make(chan error, 1)
	go func() { errs <- b.Subscribe(ctx, stream) }()
	require.Eventually(t, func() bool { return subCount(b) > before }, testTimeout, time.Millisecond)
	return errs
}

func recvMsg(t *testing.T, stream *fakeStream) *testMsg {
	t.Helper()
	select {
	case m := <-stream.sent:
		return m
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for a message")
		return nil
	}
}

// broadcastNow fails instead of hanging if Broadcast waits on a subscriber.
func broadcastNow(t *testing.T, b *StreamBroadcaster[testMsg], m *testMsg) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.Broadcast(m, log.Root())
	}()
	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("Broadcast blocked on a subscriber")
	}
}

func TestBroadcastReachesHealthySubscriberWhileAnotherStalls(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var b StreamBroadcaster[testMsg]

	stalled := newFakeStream(ctx)
	stalled.gate = make(chan struct{}) // never fed: Send blocks
	subscribe(t, &b, ctx, stalled)

	healthy := newFakeStream(ctx)
	subscribe(t, &b, ctx, healthy)

	broadcastNow(t, &b, &testMsg{n: 1})
	require.Equal(t, 1, recvMsg(t, healthy).n)
}

func TestLaterBroadcastsProceedWhileSubscriberStalls(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var b StreamBroadcaster[testMsg]

	stalled := newFakeStream(ctx)
	stalled.gate = make(chan struct{})
	subscribe(t, &b, ctx, stalled)

	healthy := newFakeStream(ctx)
	subscribe(t, &b, ctx, healthy)

	for i := 1; i <= 10; i++ {
		broadcastNow(t, &b, &testMsg{n: i})
	}
	for i := 1; i <= 10; i++ {
		require.Equal(t, i, recvMsg(t, healthy).n)
	}
}

func TestSubscribeIsNotBlockedByStalledSubscriber(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var b StreamBroadcaster[testMsg]

	stalled := newFakeStream(ctx)
	stalled.gate = make(chan struct{})
	subscribe(t, &b, ctx, stalled)
	broadcastNow(t, &b, &testMsg{n: 1}) // stalled is now inside Send

	late := newFakeStream(ctx)
	subscribe(t, &b, ctx, late) // subscribe() fails the test if registration hangs

	broadcastNow(t, &b, &testMsg{n: 2})
	require.Equal(t, 2, recvMsg(t, late).n)
}

func TestStalledSubscriberIsReleasedOnStreamCancel(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var b StreamBroadcaster[testMsg]

	healthy := newFakeStream(ctx)
	subscribe(t, &b, ctx, healthy)

	stalledCtx, cancel := context.WithCancel(ctx)
	stalled := newFakeStream(stalledCtx)
	stalled.gate = make(chan struct{})
	errs := subscribe(t, &b, ctx, stalled)
	broadcastNow(t, &b, &testMsg{n: 1})
	require.Equal(t, 2, subCount(&b))

	cancel()
	select {
	case err := <-errs:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(testTimeout):
		t.Fatal("Subscribe did not return after the stream was cancelled")
	}
	require.Eventually(t, func() bool { return subCount(&b) == 1 }, testTimeout, time.Millisecond)

	broadcastNow(t, &b, &testMsg{n: 2})
	require.Equal(t, 1, recvMsg(t, healthy).n)
	require.Equal(t, 2, recvMsg(t, healthy).n)
}

func TestSubscriberThatFallsBehindIsDropped(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var b StreamBroadcaster[testMsg]

	stalled := newFakeStream(ctx)
	stalled.gate = make(chan struct{}, subscriberQueueLen*2)
	errs := subscribe(t, &b, ctx, stalled)

	for i := range subscriberQueueLen + 2 {
		broadcastNow(t, &b, &testMsg{n: i})
	}

	// Let it drain: it must deliver what was buffered before reporting the drop.
	for range subscriberQueueLen * 2 {
		stalled.gate <- struct{}{}
	}
	select {
	case err := <-errs:
		require.ErrorIs(t, err, ErrSubscriberTooSlow)
	case <-time.After(testTimeout):
		t.Fatal("Subscribe did not return after the subscriber fell behind")
	}
	require.Equal(t, 0, subCount(&b))
	require.NotEmpty(t, stalled.sent)
}

func TestSendErrorEndsSubscription(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var b StreamBroadcaster[testMsg]

	boom := errors.New("boom")
	broken := newFakeStream(ctx)
	broken.sendErr = boom
	errs := subscribe(t, &b, ctx, broken)

	broadcastNow(t, &b, &testMsg{n: 1})
	select {
	case err := <-errs:
		require.ErrorIs(t, err, boom)
	case <-time.After(testTimeout):
		t.Fatal("Subscribe did not return after Send failed")
	}
	require.Eventually(t, func() bool { return subCount(&b) == 0 }, testTimeout, time.Millisecond)
}

func TestConcurrentBroadcastsKeepEverySubscriberInTheSameOrder(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var b StreamBroadcaster[testMsg]

	const (
		subscribers = 4
		senders     = 4
		perSender   = subscriberQueueLen / senders
	)
	streams := make([]*fakeStream, subscribers)
	for i := range streams {
		streams[i] = newFakeStream(ctx)
		subscribe(t, &b, ctx, streams[i])
	}

	var wg sync.WaitGroup
	for s := range senders {
		wg.Go(func() {
			for i := range perSender {
				b.Broadcast(&testMsg{n: s*perSender + i}, log.Root())
			}
		})
	}
	wg.Wait()

	want := make([]int, 0, senders*perSender)
	for range senders * perSender {
		want = append(want, recvMsg(t, streams[0]).n)
	}
	require.Len(t, want, senders*perSender)
	for _, stream := range streams[1:] {
		got := make([]int, 0, len(want))
		for range want {
			got = append(got, recvMsg(t, stream).n)
		}
		require.Equal(t, want, got)
	}
}
