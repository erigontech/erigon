// Copyright 2016 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package event

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestFeedPanics(t *testing.T) {
	{
		var f Feed
		f.Send(2)
		want := feedTypeError{op: "Send", got: reflect.TypeFor[uint64](), want: reflect.TypeFor[int]()}
		if err := checkPanic(want, func() { f.Send(uint64(2)) }); err != nil {
			t.Error(err)
		}
	}
	{
		var f Feed
		ch := make(chan int)
		f.Subscribe(ch)
		want := feedTypeError{op: "Send", got: reflect.TypeFor[uint64](), want: reflect.TypeFor[int]()}
		if err := checkPanic(want, func() { f.Send(uint64(2)) }); err != nil {
			t.Error(err)
		}
	}
	{
		var f Feed
		f.Send(2)
		want := feedTypeError{op: "Subscribe", got: reflect.TypeFor[chan uint64](), want: reflect.TypeFor[chan<- int]()}
		if err := checkPanic(want, func() { f.Subscribe(make(chan uint64)) }); err != nil {
			t.Error(err)
		}
	}
	{
		var f Feed
		if err := checkPanic(errBadChannel, func() { f.Subscribe(make(<-chan int)) }); err != nil {
			t.Error(err)
		}
	}
	{
		var f Feed
		if err := checkPanic(errBadChannel, func() { f.Subscribe(0) }); err != nil {
			t.Error(err)
		}
	}
	{
		var f Feed
		f.TrySend(2)
		want := feedTypeError{op: "TrySend", got: reflect.TypeFor[uint64](), want: reflect.TypeFor[int]()}
		if err := checkPanic(want, func() { f.TrySend(uint64(2)) }); err != nil {
			t.Error(err)
		}
	}
}

func checkPanic(want error, fn func()) (err error) {
	defer func() {
		panic := recover()
		if panic == nil {
			err = errors.New("didn't panic")
		} else if !reflect.DeepEqual(panic, want) {
			err = fmt.Errorf("panicked with wrong error: got %v, want %w", panic, want)
		}
	}()
	fn()
	return nil
}

func TestFeed(t *testing.T) {
	var feed Feed
	var done, subscribed sync.WaitGroup
	subscriber := func(i int) {
		defer done.Done()

		subchan := make(chan int)
		sub := feed.Subscribe(subchan)
		timeout := time.NewTimer(2 * time.Second)
		defer timeout.Stop()
		subscribed.Done()

		select {
		case v := <-subchan:
			if v != 1 {
				t.Errorf("%d: received value %d, want 1", i, v)
			}
		case <-timeout.C:
			t.Errorf("%d: receive timeout", i)
		}

		sub.Unsubscribe()
		select {
		case _, ok := <-sub.Err():
			if ok {
				t.Errorf("%d: error channel not closed after unsubscribe", i)
			}
		case <-timeout.C:
			t.Errorf("%d: unsubscribe timeout", i)
		}
	}

	const n = 1000
	done.Add(n)
	subscribed.Add(n)
	for i := range n {
		go subscriber(i)
	}
	subscribed.Wait()
	if nsent := feed.Send(1); nsent != n {
		t.Errorf("first send delivered %d times, want %d", nsent, n)
	}
	if nsent := feed.Send(2); nsent != 0 {
		t.Errorf("second send delivered %d times, want 0", nsent)
	}
	done.Wait()
}

func TestFeedTrySendSkipsSlowSubscribers(t *testing.T) {
	var feed Feed
	slow := make(chan int)
	slowSub := feed.Subscribe(slow)
	defer slowSub.Unsubscribe()
	ready := make(chan int, 1)
	readySub := feed.Subscribe(ready)
	defer readySub.Unsubscribe()

	if delivered := feed.TrySend(1); delivered != 1 {
		t.Fatalf("TrySend delivered %d times, want 1", delivered)
	}
	if value := <-ready; value != 1 {
		t.Fatalf("received %d, want 1", value)
	}
}

func TestFeedTrySendDoesNotWaitForBlockedSend(t *testing.T) {
	var feed Feed
	slow := make(chan int)
	slowSub := feed.Subscribe(slow)
	defer slowSub.Unsubscribe()
	ready := make(chan int)
	readySub := feed.Subscribe(ready)
	defer readySub.Unsubscribe()

	sendDone := make(chan struct{})
	go func() {
		feed.Send(1)
		close(sendDone)
	}()
	if value := <-ready; value != 1 {
		t.Fatalf("received %d, want 1", value)
	}

	completed := make(chan int, 1)
	go func() { completed <- feed.TrySend(2) }()
	select {
	case delivered := <-completed:
		if delivered != 0 {
			t.Fatalf("TrySend delivered %d times, want 0", delivered)
		}
	case <-time.After(time.Second):
		t.Fatal("TrySend waited for a blocked Send")
	}

	slowSub.Unsubscribe()
	select {
	case <-sendDone:
	case <-time.After(time.Second):
		t.Fatal("Send remained blocked after unsubscribe")
	}
}

func TestFeedTrySendDoesNotWaitForSubscriptionLock(t *testing.T) {
	var feed Feed
	sub := feed.Subscribe(make(chan int, 1))
	defer sub.Unsubscribe()
	feed.mu.Lock()
	defer feed.mu.Unlock()

	completed := make(chan int, 1)
	go func() { completed <- feed.TrySend(1) }()
	select {
	case delivered := <-completed:
		if delivered != 0 {
			t.Fatalf("TrySend delivered %d times, want 0", delivered)
		}
	case <-time.After(time.Second):
		t.Fatal("TrySend waited for the subscription lock")
	}
}

func TestFeedSubscribeSameChannel(t *testing.T) {
	var (
		feed Feed
		done sync.WaitGroup
		ch   = make(chan int)
		sub1 = feed.Subscribe(ch)
		sub2 = feed.Subscribe(ch)
		_    = feed.Subscribe(ch)
	)
	expectSends := func(value, n int) {
		if nsent := feed.Send(value); nsent != n {
			t.Errorf("send delivered %d times, want %d", nsent, n)
		}
		done.Done()
	}
	expectRecv := func(wantValue, n int) {
		for range n {
			if v := <-ch; v != wantValue {
				t.Errorf("received %d, want %d", v, wantValue)
			}
		}
	}

	done.Add(1)
	go expectSends(1, 3)
	expectRecv(1, 3)
	done.Wait()

	sub1.Unsubscribe()

	done.Add(1)
	go expectSends(2, 2)
	expectRecv(2, 2)
	done.Wait()

	sub2.Unsubscribe()

	done.Add(1)
	go expectSends(3, 1)
	expectRecv(3, 1)
	done.Wait()
}

func TestFeedSubscribeBlockedPost(t *testing.T) {
	var (
		feed   Feed
		nsends = 2000
		ch1    = make(chan int)
		ch2    = make(chan int)
		wg     sync.WaitGroup
	)
	defer wg.Wait()

	feed.Subscribe(ch1)
	for range nsends {
		wg.Go(func() {
			feed.Send(99)
		})
	}

	sub2 := feed.Subscribe(ch2)
	defer sub2.Unsubscribe()

	// We're done when ch1 has received N times.
	// The number of receives on ch2 depends on scheduling.
	for i := 0; i < nsends; {
		select {
		case <-ch1:
			i++
		case <-ch2:
		}
	}
}

func TestFeedUnsubscribeBlockedPost(t *testing.T) {
	var (
		feed   Feed
		nsends = 200
		chans  = make([]chan int, 2000)
		subs   = make([]Subscription, len(chans))
		bchan  = make(chan int)
		bsub   = feed.Subscribe(bchan)
		wg     sync.WaitGroup
	)
	for i := range chans {
		chans[i] = make(chan int, nsends)
	}

	// Queue up some Sends. None of these can make progress while bchan isn't read.
	for range nsends {
		wg.Go(func() {
			feed.Send(99)
		})
	}
	// Subscribe the other channels.
	for i, ch := range chans {
		subs[i] = feed.Subscribe(ch)
	}
	// Unsubscribe them again.
	for _, sub := range subs {
		sub.Unsubscribe()
	}
	// Unblock the Sends.
	bsub.Unsubscribe()
	wg.Wait()
}

// Checks that unsubscribing a channel during Send works even if that
// channel has already been sent on.
func TestFeedUnsubscribeSentChan(t *testing.T) {
	var (
		feed Feed
		ch1  = make(chan int)
		ch2  = make(chan int)
		sub1 = feed.Subscribe(ch1)
		sub2 = feed.Subscribe(ch2)
		wg   sync.WaitGroup
	)
	defer sub2.Unsubscribe()

	wg.Go(func() {
		feed.Send(0)
	})

	// Wait for the value on ch1.
	<-ch1
	// Unsubscribe ch1, removing it from the send cases.
	sub1.Unsubscribe()

	// Receive ch2, finishing Send.
	<-ch2
	wg.Wait()

	// Send again. This should send to ch2 only, so the wait group will unblock
	// as soon as a value is received on ch2.
	wg.Go(func() {
		feed.Send(0)
	})
	<-ch2
	wg.Wait()
}

func TestFeedUnsubscribeFromInbox(t *testing.T) {
	var (
		feed Feed
		ch1  = make(chan int)
		ch2  = make(chan int)
		sub1 = feed.Subscribe(ch1)
		sub2 = feed.Subscribe(ch1)
		sub3 = feed.Subscribe(ch2)
	)
	if len(feed.inbox) != 3 {
		t.Errorf("inbox length != 3 after subscribe")
	}
	if len(feed.sendCases) != 1 {
		t.Errorf("sendCases is non-empty after unsubscribe")
	}

	sub1.Unsubscribe()
	sub2.Unsubscribe()
	sub3.Unsubscribe()
	if len(feed.inbox) != 0 {
		t.Errorf("inbox is non-empty after unsubscribe")
	}
	if len(feed.sendCases) != 1 {
		t.Errorf("sendCases is non-empty after unsubscribe")
	}
}
