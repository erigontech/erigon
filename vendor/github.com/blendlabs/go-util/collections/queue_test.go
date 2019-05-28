package collections

import (
	"testing"
	"time"
)

const (
	// DefaultSampleSize is the default number of steps to run per test.
	DefaultSampleSize = 10000

	// DefaultStasisSize is the stasis size for the fixed length test.
	DefaultStasisSize = 512
)

type QueueFactory func(capacity int) Queue

func doQueueBenchmark(queueFactory QueueFactory, sampleSize int, b *testing.B) {
	for iteration := 0; iteration < b.N; iteration++ {
		q := queueFactory(sampleSize)
		for x := 0; x < sampleSize; x++ {
			q.Enqueue(time.Now().UTC())
		}
		for x := 0; x < sampleSize; x++ {
			q.Dequeue()
		}
	}
}

func doFixedQueueBenchmark(queueFactory QueueFactory, sampleSize, stasisSize int, b *testing.B) {
	for iteration := 0; iteration < b.N; iteration++ {
		q := queueFactory(stasisSize)
		for x := 0; x < sampleSize; x++ {
			q.Enqueue(time.Now().UTC())
			if q.Len() < stasisSize {
				continue
			}
			q.Dequeue()
		}
	}
}

func makeLinkedList(capacity int) Queue {
	return NewLinkedList()
}

func makeChannelQueue(capacity int) Queue {
	return NewChannelQueue(capacity)
}

func makeRingBuffer(capacity int) Queue {
	return NewRingBufferWithCapacity(capacity)
}

func makeSyncedRingBuffer(capacity int) Queue {
	rb := NewSynchronizedRingBufferWithCapacity(capacity)
	return rb
}

func BenchmarkLinkedList(b *testing.B) {
	doQueueBenchmark(makeLinkedList, DefaultSampleSize, b)
}

func BenchmarkChannelQueue(b *testing.B) {
	doQueueBenchmark(makeChannelQueue, DefaultSampleSize, b)
}

func BenchmarkRingBuffer(b *testing.B) {
	doQueueBenchmark(makeRingBuffer, DefaultSampleSize, b)
}

func BenchmarkRingBufferSynced(b *testing.B) {
	doQueueBenchmark(makeSyncedRingBuffer, DefaultSampleSize, b)
}

func BenchmarkFixedLinkedList(b *testing.B) {
	doFixedQueueBenchmark(makeLinkedList, DefaultSampleSize, DefaultStasisSize, b)
}

func BenchmarkFixedChannelQueue(b *testing.B) {
	doFixedQueueBenchmark(makeChannelQueue, DefaultSampleSize, DefaultStasisSize, b)
}

func BenchmarkFixedRingBuffer(b *testing.B) {
	doFixedQueueBenchmark(makeRingBuffer, DefaultSampleSize, DefaultStasisSize, b)
}
