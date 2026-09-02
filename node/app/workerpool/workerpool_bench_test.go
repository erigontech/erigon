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

package workerpool

import (
	"sync"
	"testing"
)

func BenchmarkEnqueue(b *testing.B) {
	wp := New(1)
	defer wp.Stop()
	releaseChan := make(chan struct{})

	b.ResetTimer()

	// Start workers, and have them all wait on a channel before completing.
	for i := 0; i < b.N; i++ {
		wp.Submit(func() { <-releaseChan })
	}
	close(releaseChan)
}

func BenchmarkEnqueue2(b *testing.B) {
	wp := New(2)
	defer wp.Stop()

	b.ResetTimer()

	// Start workers, and have them all wait on a channel before completing.
	for i := 0; i < b.N; i++ {
		releaseChan := make(chan struct{})
		for range 64 {
			wp.Submit(func() { <-releaseChan })
		}
		close(releaseChan)
	}
}

func BenchmarkExecute1Worker(b *testing.B) {
	benchmarkExecWorkers(1, b)
}

func BenchmarkExecute2Worker(b *testing.B) {
	benchmarkExecWorkers(2, b)
}

func BenchmarkExecute4Workers(b *testing.B) {
	benchmarkExecWorkers(4, b)
}

func BenchmarkExecute16Workers(b *testing.B) {
	benchmarkExecWorkers(16, b)
}

func BenchmarkExecute64Workers(b *testing.B) {
	benchmarkExecWorkers(64, b)
}

func BenchmarkExecute1024Workers(b *testing.B) {
	benchmarkExecWorkers(1024, b)
}

func benchmarkExecWorkers(n int, b *testing.B) {
	wp := New(n)
	defer wp.Stop()
	var allDone sync.WaitGroup
	allDone.Add(b.N * n)

	b.ResetTimer()

	// Start workers, and have them all wait on a channel before completing.
	for i := 0; i < b.N; i++ {
		for range n {
			wp.Submit(func() {
				//time.Sleep(100 * time.Microsecond)
				allDone.Done()
			})
		}
	}
	allDone.Wait()
}
