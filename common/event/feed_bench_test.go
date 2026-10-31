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
	"sync"
	"testing"
)

func BenchmarkFeedSend1000(b *testing.B) {
	var (
		done  sync.WaitGroup
		feed  Feed
		nsubs = 1000
	)
	subscriber := func(ch <-chan int) {
		for i := 0; i < b.N; i++ {
			<-ch
		}
		done.Done()
	}
	done.Add(nsubs)
	for range nsubs {
		ch := make(chan int, 200)
		feed.Subscribe(ch)
		go subscriber(ch)
	}

	// The actual benchmark.
	for i := 0; b.Loop(); i++ {
		if feed.Send(i) != nsubs {
			panic("wrong number of sends")
		}
	}

	b.StopTimer()
	done.Wait()
}
