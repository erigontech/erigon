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

package jsonrpc

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/erigontech/erigon/common"
)

func mkPush(n uint64) witnessPush {
	return witnessPush{
		num:  n,
		hash: common.Hash{byte(n)},
		json: json.RawMessage(fmt.Sprintf(`{"n":%d}`, n)),
	}
}

func TestWitnessFeedPublishReachesSubscribers(t *testing.T) {
	f := newWitnessFeed()
	_, ch1 := f.subscribe()
	_, ch2 := f.subscribe()

	p := mkPush(1)
	f.publish(p)

	for i, ch := range []<-chan witnessPush{ch1, ch2} {
		select {
		case got := <-ch:
			if got.num != p.num || got.hash != p.hash {
				t.Fatalf("subscriber %d got (%d,%s) want (%d,%s)", i, got.num, got.hash, p.num, p.hash)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d did not receive publish", i)
		}
	}
}

func TestWitnessFeedUnsubscribeStopsDeliveryIdempotent(t *testing.T) {
	f := newWitnessFeed()
	id, ch := f.subscribe()

	f.unsubscribe(id)
	f.unsubscribe(id) // idempotent: a second unsubscribe must not panic

	f.publish(mkPush(1))

	select {
	case p := <-ch:
		t.Fatalf("received %d after unsubscribe", p.num)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWitnessFeedPublishNoSubscribers(t *testing.T) {
	f := newWitnessFeed()
	f.publish(mkPush(1)) // must not block or panic
}

func TestWitnessFeedOverflowDropsOldest(t *testing.T) {
	f := newWitnessFeed()
	_, ch := f.subscribe()

	for i := uint64(1); i <= witnessFeedBuffer+2; i++ {
		f.publish(mkPush(i))
	}

	var got []uint64
	for drained := false; !drained; {
		select {
		case p := <-ch:
			got = append(got, p.num)
		default:
			drained = true
		}
	}

	want := []uint64{3, 4, 5, 6}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("overflow kept %v want newest %v", got, want)
	}

	f.mu.Lock()
	dropped := f.dropped
	f.mu.Unlock()
	if dropped != 2 {
		t.Fatalf("recorded %d drops want 2", dropped)
	}
}

func TestWitnessFeedConcurrent(t *testing.T) {
	f := newWitnessFeed()
	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				id, ch := f.subscribe()
				for j := 0; j < 8; j++ {
					select {
					case <-ch:
					case <-time.After(time.Millisecond):
					}
				}
				f.unsubscribe(id)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(0); ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			f.publish(mkPush(i))
		}
	}()

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()

	f.mu.Lock()
	remaining := len(f.subs)
	f.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("subscribers leaked: %d still registered after all workers stopped", remaining)
	}
}
