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

package cache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erigontech/erigon/db/kv"
)

// BenchmarkStateCachePublicationUnderLoad measures what a commit costs the
// readers running beside it. b.N counts publications; the reported
// reads/s and fill-reject ratio come from reader goroutines that run for the
// whole timed region, so a publication that stalls readers shows up as reads/s
// collapsing rather than as ns/op moving.
//
// version=current models a reader bound to the state the cache just published.
// version=stale repeatedly constructs views for a transaction opened before
// the last commit. Production getters retain this rejection; constructing each
// view here deliberately measures the worst-case binding contention.
func BenchmarkStateCachePublicationUnderLoad(b *testing.B) {
	const keySpace = 4096

	mkKey := func(i int) []byte {
		return []byte{byte(i), byte(i >> 8), 0x5A}
	}

	for _, batch := range []int{1, 1000, 20000} {
		for _, readers := range []int{0, 8, 32} {
			for _, mix := range []string{"current", "stale", "half"} {
				if readers == 0 && mix != "current" {
					continue // reader mix is meaningless with no readers
				}
				b.Run(fmt.Sprintf("batch=%d/readers=%d/version=%s", batch, readers, mix), func(b *testing.B) {
					c := NewStateCache(64<<20, 64<<20, 16<<20, 8<<20)
					defer c.Close()
					ap := c.Applier()

					var version atomic.Uint64
					version.Store(1)
					ap.Initialize(1)

					// Seed so readers mostly hit.
					seed := make([]StateUpdate, keySpace)
					for i := range seed {
						seed[i] = StateUpdate{Domain: kv.AccountsDomain, Key: mkKey(i),
							Value: []byte{byte(i), 0xEE}, TxNum: uint64(i)}
					}
					ap.Publish(1, 2, seed)
					version.Store(2)

					updates := make([]StateUpdate, batch)
					for i := range updates {
						updates[i] = StateUpdate{Domain: kv.AccountsDomain, Key: mkKey(i % keySpace),
							Value: []byte{byte(i), 0xFF}, TxNum: uint64(i)}
					}

					var reads, fillsOffered, fillsLanded atomic.Uint64
					stop := make(chan struct{})
					var wg sync.WaitGroup

					for r := range readers {
						wg.Add(1)
						go func(r int) {
							defer wg.Done()
							useStale := mix == "stale" || (mix == "half" && r%2 == 0)
							n := uint64(r * 7919)
							for {
								select {
								case <-stop:
									return
								default:
								}
								for range 64 {
									n = n*1103515245 + 12345
									idx := int(n>>16) % keySpace
									key := mkKey(idx)

									sv := version.Load()
									if useStale {
										sv = 1 // the version the cache has moved past
									}
									v := c.View(FrontierWithStateVersion(
										FrontierFunc(func(kv.Domain) (uint64, bool) { return uint64(keySpace), true }), sv))

									if _, ok := v.Get(kv.AccountsDomain, key); !ok {
										fillsOffered.Add(1)
										v.Fill(kv.AccountsDomain, key, []byte{byte(idx), 0xEE}, uint64(idx))
										if _, ok := c.View(nil).Get(kv.AccountsDomain, key); ok {
											fillsLanded.Add(1)
										}
									}
									reads.Add(1)
								}
							}
						}(r)
					}

					b.ResetTimer()
					start := time.Now()
					for i := 0; b.Loop(); i++ {
						src := version.Load()
						ap.Publish(src, src+1, updates)
						version.Store(src + 1)
					}
					elapsed := time.Since(start)
					b.StopTimer()

					close(stop)
					wg.Wait()

					if readers > 0 {
						b.ReportMetric(float64(reads.Load())/elapsed.Seconds()/1e6, "Mreads/s")
						if off := fillsOffered.Load(); off > 0 {
							b.ReportMetric(float64(fillsLanded.Load())/float64(off)*100, "%fills-landed")
						}
					}
					b.ReportMetric(float64(batch), "updates/publish")
				})
			}
		}
	}
}

// BenchmarkPublishVsViewBindLock isolates what admissionMu costs a publication.
// Readers do identical work; only the bind differs. View(nil) returns without
// touching admissionMu, so the delta is the read-lock's contribution to both
// the publisher's cost and reader throughput.
func BenchmarkPublishVsViewBindLock(b *testing.B) {
	const keySpace = 4096
	mkKey := func(i int) []byte { return []byte{byte(i), byte(i >> 8), 0x5A} }

	for _, bind := range []string{"frontier-RLock", "nil-nolock"} {
		b.Run(bind, func(b *testing.B) {
			c := NewStateCache(64<<20, 64<<20, 16<<20, 8<<20)
			defer c.Close()
			ap := c.Applier()
			ap.Initialize(1)

			seed := make([]StateUpdate, keySpace)
			for i := range seed {
				seed[i] = StateUpdate{Domain: kv.AccountsDomain, Key: mkKey(i), Value: []byte{byte(i), 0xEE}, TxNum: uint64(i)}
			}
			ap.Publish(1, 2, seed)
			var version atomic.Uint64
			version.Store(2)

			updates := make([]StateUpdate, 20000)
			for i := range updates {
				updates[i] = StateUpdate{Domain: kv.AccountsDomain, Key: mkKey(i % keySpace), Value: []byte{byte(i), 0xFF}, TxNum: uint64(i)}
			}

			var reads atomic.Uint64
			stop := make(chan struct{})
			var wg sync.WaitGroup
			for r := range 32 {
				wg.Add(1)
				go func(r int) {
					defer wg.Done()
					n := uint64(r * 7919)
					for {
						select {
						case <-stop:
							return
						default:
						}
						for range 64 {
							n = n*1103515245 + 12345
							key := mkKey(int(n>>16) % keySpace)
							var v ReadView
							if bind == "frontier-RLock" {
								v = c.View(FrontierWithStateVersion(
									FrontierFunc(func(kv.Domain) (uint64, bool) { return keySpace, true }), version.Load()))
							} else {
								v = c.View(nil)
							}
							v.Get(kv.AccountsDomain, key)
							reads.Add(1)
						}
					}
				}(r)
			}

			b.ResetTimer()
			start := time.Now()
			for b.Loop() {
				src := version.Load()
				ap.Publish(src, src+1, updates)
				version.Store(src + 1)
			}
			el := time.Since(start)
			b.StopTimer()
			close(stop)
			wg.Wait()
			b.ReportMetric(float64(reads.Load())/el.Seconds()/1e6, "Mreads/s")
		})
	}
}
