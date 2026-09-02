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

// A subscriber that overflows while wedged in Send runs its deferred remove
// only once that Send returns, by which time a later subscriber holds a fresh
// id. Its teardown must not take that one with it.
func TestLateTeardownDoesNotUnregisterALaterSubscriber(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var b StreamBroadcaster[testMsg]

	dropped := newFakeStream(ctx)
	dropped.gate = make(chan struct{})
	errs := subscribe(t, &b, ctx, dropped)
	for i := range subscriberQueueLen + 2 {
		broadcastNow(t, &b, &testMsg{n: i})
	}
	require.Equal(t, 0, subCount(&b), "the queue did not fill, so nothing was dropped")

	// dropped is unregistered but still inside Send when the next one arrives.
	fresh := newFakeStream(ctx)
	subscribe(t, &b, ctx, fresh)
	require.Equal(t, 1, subCount(&b))

	close(dropped.gate)
	select {
	case err := <-errs:
		require.ErrorIs(t, err, ErrSubscriberTooSlow)
	case <-time.After(testTimeout):
		t.Fatal("Subscribe did not return after the subscriber fell behind")
	}

	require.Equal(t, 1, subCount(&b))
	broadcastNow(t, &b, &testMsg{n: 99})
	require.Equal(t, 99, recvMsg(t, fresh).n)
}

func TestManyStalledSubscribersDoNotAffectAHealthyOne(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var b StreamBroadcaster[testMsg]

	const stalledCount = 8
	stalled := make([]*fakeStream, stalledCount)
	for i := range stalled {
		stalled[i] = newFakeStream(ctx)
		stalled[i].gate = make(chan struct{})
		subscribe(t, &b, ctx, stalled[i])
	}
	healthy := newFakeStream(ctx)
	subscribe(t, &b, ctx, healthy)

	// Drain as we broadcast: the healthy subscriber must never have slack to
	// lose, or it overflows too and the test is measuring the scheduler.
	const messages = subscriberQueueLen + 2
	for i := range messages {
		broadcastNow(t, &b, &testMsg{n: i})
		require.Equal(t, i, recvMsg(t, healthy).n)
	}
	require.Equal(t, 1, subCount(&b), "every stalled subscriber should have been dropped")
}

// A subscriber that fills its queue is unregistered at once, but its Subscribe
// only returns when the Send it is wedged in does - and whatever ended that
// Send decides the error. Its queued messages are released, not delivered.
func TestSubscriberThatFallsBehindIsDropped(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	for _, tc := range []struct {
		name    string
		sendErr error
		cancel  bool
		want    error
	}{
		{name: "send released", want: ErrSubscriberTooSlow},
		{name: "stream cancelled", cancel: true, want: context.Canceled},
		{name: "send failed", sendErr: boom, want: boom},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			var b StreamBroadcaster[testMsg]

			streamCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			stalled := newFakeStream(streamCtx)
			stalled.gate = make(chan struct{}) // wedges the first Send
			stalled.sendErr = tc.sendErr
			errs := subscribe(t, &b, ctx, stalled)

			for i := range subscriberQueueLen + 2 {
				broadcastNow(t, &b, &testMsg{n: i})
			}
			require.Equal(t, 0, subCount(&b), "the queue did not fill, so nothing was dropped")

			if tc.cancel {
				cancel()
			} else {
				close(stalled.gate)
			}
			select {
			case err := <-errs:
				require.ErrorIs(t, err, tc.want)
			case <-time.After(testTimeout):
				t.Fatal("Subscribe did not return after the subscriber fell behind")
			}
			require.LessOrEqual(t, len(stalled.sent), 1, "queued messages must be released, not delivered")
		})
	}
}
