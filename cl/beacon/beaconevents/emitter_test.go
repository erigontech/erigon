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

package beaconevents

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/erigontech/erigon/common"
	ethevent "github.com/erigontech/erigon/common/event"
	"github.com/stretchr/testify/require"
)

func TestEmitterSet(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		emitter := NewEventEmitter()
		done := make(chan struct{})
		go func() {
			ch := make(chan *EventStream, 10)
			t.Logf("Subscribing to emitter")
			sub := emitter.Operation().Subscribe(ch)
			defer sub.Unsubscribe()
			t.Logf("Subscribed to emitter")
			event := <-ch
			t.Logf("Received event: %v", event)
			close(done)
		}()
		synctest.Wait()
		emitter.Operation().SendAttestation(&AttestationData{})
		<-done
	})
}

func TestPayloadAttributesSubscriberTracking(t *testing.T) {
	emitter := NewEventEmitter()
	require.False(t, emitter.HasPayloadAttributesSubscribers())

	releaseFirst := emitter.TrackPayloadAttributesSubscriber()
	releaseSecond := emitter.TrackPayloadAttributesSubscriber()
	require.True(t, emitter.HasPayloadAttributesSubscribers())

	releaseFirst()
	require.True(t, emitter.HasPayloadAttributesSubscribers())

	releaseSecond()
	require.False(t, emitter.HasPayloadAttributesSubscribers())
}

func TestNotifyDataColumnStoredDoesNotBlockWhenFull(t *testing.T) {
	emitter := NewEventEmitter()
	for range dataColumnStoredBufferSize {
		emitter.NotifyDataColumnStored(common.Hash{1})
	}

	done := make(chan struct{})
	go func() {
		emitter.NotifyDataColumnStored(common.Hash{2})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("notification blocked on a full buffer")
	}
}

func TestNotifyDataColumnStoredIsNilSafe(t *testing.T) {
	var emitter *EventEmitter
	require.Nil(t, emitter.DataColumnStored())
	emitter.NotifyDataColumnStored(common.Hash{1})
}

func distinctRoots(n int) []common.Hash {
	roots := make([]common.Hash, n)
	for i := range roots {
		roots[i] = common.BytesToHash([]byte{byte(i + 1)})
	}
	return roots
}

func TestNotifyDataColumnStoredAlsoWakesPendingEnvelopeRetry(t *testing.T) {
	emitter := NewEventEmitter()
	roots := distinctRoots(pendingEnvelopeRetryRootsCap)
	for _, root := range roots {
		emitter.NotifyDataColumnStored(root)
	}

	for _, want := range roots {
		select {
		case got := <-emitter.DataColumnStored():
			require.Equal(t, want, got)
		default:
			t.Fatal("the block service lost a notification")
		}
	}
	require.Len(t, emitter.PendingEnvelopeRetryWake(), 1, "a burst leaves one wake")
	taken := emitter.TakePendingEnvelopeRetryRoots()
	require.Len(t, taken, len(roots))
	for _, root := range roots {
		require.Contains(t, taken, root)
	}
	require.Empty(t, emitter.TakePendingEnvelopeRetryRoots(), "taking the roots empties the set")
}

func TestWakePendingEnvelopeRetryKeepsAtMostCapRoots(t *testing.T) {
	emitter := NewEventEmitter()
	roots := distinctRoots(pendingEnvelopeRetryRootsCap + 1)
	for _, root := range roots {
		emitter.WakePendingEnvelopeRetry(root)
	}

	require.Len(t, emitter.PendingEnvelopeRetryWake(), 1)
	require.Len(t, emitter.TakePendingEnvelopeRetryRoots(), pendingEnvelopeRetryRootsCap, "overflow evicts a root")

	emitter.WakePendingEnvelopeRetry(roots[pendingEnvelopeRetryRootsCap])
	require.Contains(t, emitter.TakePendingEnvelopeRetryRoots(), roots[pendingEnvelopeRetryRootsCap], "a taken set accepts roots again")
}

func TestWakePendingEnvelopeRetryEvictsTheOldestReportWhenFull(t *testing.T) {
	emitter := NewEventEmitter()
	roots := distinctRoots(pendingEnvelopeRetryRootsCap + 1)
	for _, root := range roots {
		emitter.WakePendingEnvelopeRetry(root)
	}

	taken := emitter.TakePendingEnvelopeRetryRoots()
	require.ElementsMatch(t, roots[1:], taken, "the fresh root is kept and the oldest one is evicted")
}

func TestWakePendingEnvelopeRetryEvictsNothingForAMarkedRootWhenFull(t *testing.T) {
	emitter := NewEventEmitter()
	roots := distinctRoots(pendingEnvelopeRetryRootsCap)
	for _, root := range roots {
		emitter.WakePendingEnvelopeRetry(root)
	}
	emitter.WakePendingEnvelopeRetry(roots[len(roots)-1])

	require.ElementsMatch(t, roots, emitter.TakePendingEnvelopeRetryRoots())
}

func TestWakePendingEnvelopeRetryRefreshesAMarkedRootWhenFull(t *testing.T) {
	emitter := NewEventEmitter()
	roots := distinctRoots(pendingEnvelopeRetryRootsCap + 1)
	for _, root := range roots[:pendingEnvelopeRetryRootsCap] {
		emitter.WakePendingEnvelopeRetry(root)
	}
	emitter.WakePendingEnvelopeRetry(roots[0])
	emitter.WakePendingEnvelopeRetry(roots[pendingEnvelopeRetryRootsCap])

	taken := emitter.TakePendingEnvelopeRetryRoots()
	require.Contains(t, taken, roots[0], "the report made the root the newest")
	require.NotContains(t, taken, roots[1], "the oldest report is the one evicted")
	require.Len(t, taken, pendingEnvelopeRetryRootsCap)
}

func TestWakePendingEnvelopeRetryMarksTheRootBeforeSignalling(t *testing.T) {
	emitter := NewEventEmitter()
	root := common.Hash{1}
	emitter.envelopeRetryMu.Lock()
	unlock := sync.OnceFunc(emitter.envelopeRetryMu.Unlock)
	defer unlock()
	done := make(chan struct{})
	go func() {
		emitter.WakePendingEnvelopeRetry(root)
		close(done)
	}()

	require.Never(t, func() bool { return len(emitter.PendingEnvelopeRetryWake()) > 0 }, 50*time.Millisecond, time.Millisecond,
		"a waiter that takes the signal must find the root marked")
	unlock()
	<-done
	require.Len(t, emitter.PendingEnvelopeRetryWake(), 1)
	require.Equal(t, []common.Hash{root}, emitter.TakePendingEnvelopeRetryRoots())
}

func notifyColumnStoredWithinASecond(t *testing.T, emitter *EventEmitter, root common.Hash) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		emitter.NotifyDataColumnStored(root)
		close(done)
	}()
	t.Cleanup(func() { <-done })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("a column store waited for the check")
	}
}

func TestPrunePendingEnvelopeRetryRootsChecksWithoutBlockingColumnStores(t *testing.T) {
	emitter := NewEventEmitter()
	emitter.WakePendingEnvelopeRetry(common.Hash{1})

	emitter.PrunePendingEnvelopeRetryRoots(func(common.Hash) bool {
		notifyColumnStoredWithinASecond(t, emitter, common.Hash{2})
		return true
	})
}

func TestPrunePendingEnvelopeRetryRootsKeepsARootReportedDuringTheCheck(t *testing.T) {
	emitter := NewEventEmitter()
	root := common.Hash{1}
	emitter.WakePendingEnvelopeRetry(root)

	passed := emitter.PrunePendingEnvelopeRetryRoots(func(common.Hash) bool {
		notifyColumnStoredWithinASecond(t, emitter, root)
		return false
	})

	require.False(t, passed, "the new report is checked by the pass that it wakes")
	require.Contains(t, emitter.TakePendingEnvelopeRetryRoots(), root)
}

func TestPendingEnvelopeRetryWakeIsNilSafe(t *testing.T) {
	var emitter *EventEmitter
	require.Nil(t, emitter.PendingEnvelopeRetryWake())
	require.Nil(t, emitter.TakePendingEnvelopeRetryRoots())
	emitter.WakePendingEnvelopeRetry(common.Hash{1})
}

// head and head_v2 report a dropped event only on their own feeds, to subscribers of that topic. The state feed, which
// carries every topic, drops them silently: its subscribers may not have asked for them.
func TestHeadEventsReportAFullSubscriberOnlyOnTheirOwnFeed(t *testing.T) {
	for name, tc := range map[string]struct {
		subscribe func(*stateFeed, chan *EventStream) ethevent.Subscription
		send      func(*stateFeed) int
	}{
		"head": {
			subscribe: (*stateFeed).SubscribeHead,
			send:      func(f *stateFeed) int { return f.TrySendHead(&HeadData{}) },
		},
		"head_v2": {
			subscribe: (*stateFeed).SubscribeHeadV2,
			send:      func(f *stateFeed) int { return f.SendHeadV2(&HeadV2Data{}) },
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newStateFeed()
			topic := make(chan *EventStream)
			topicSub := tc.subscribe(f, topic)
			defer topicSub.Unsubscribe()
			all := make(chan *EventStream)
			allSub := f.Subscribe(all)
			defer allSub.Unsubscribe()

			require.Zero(t, tc.send(f))
			select {
			case err := <-topicSub.Err():
				require.ErrorIs(t, err, ethevent.ErrSubscriberLagged)
			default:
				t.Fatal("head event dropped without telling its subscriber")
			}
			select {
			case err := <-allSub.Err():
				t.Fatalf("state feed subscriber told about a head event it may not want: %v", err)
			default:
			}
		})
	}
}
