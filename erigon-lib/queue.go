package erigon_lib

import (
	"fmt"
	"time"
)

// Queue represents a FIFO queue with a limited capacity.
type Queue struct {
	Items   []time.Duration
	maxSize int
	head    int
	tail    int

	max time.Duration
	min time.Duration
}

// NewQueue creates a new Queue with the specified capacity.
func NewQueue(capacity int) *Queue {
	return &Queue{
		Items:   make([]time.Duration, capacity),
		maxSize: capacity,
		head:    0,
		tail:    0,
		max:     0,
		min:     0,
	}
}

// IsEmpty checks if the queue is empty.
func (q *Queue) IsEmpty() bool {
	return q.head == q.tail
}

// IsFull checks if the queue is full.
func (q *Queue) IsFull() bool {
	return (q.tail+1)%q.maxSize == q.head
}

// Enqueue adds an element to the back of the queue.
func (q *Queue) Enqueue(values []time.Duration) error {
	for _, value := range values {
		if q.IsFull() {
			q.Dequeue()
		}

		q.Items[q.tail] = value
		q.tail = (q.tail + 1) % q.maxSize

		if q.max == 0 || q.max < value {
			q.max = value
		}
		if q.min == 0 || q.min > value {
			q.min = value
		}
	}

	return nil
}

func (q *Queue) Stats() (time.Duration, time.Duration, time.Duration) {
	if q.IsEmpty() {
		return q.max, q.min, 0
	}

	var sum time.Duration
	count := 0

	for i := q.head; i != q.tail; i = (i + 1) % q.maxSize {
		sum += q.Items[i]
		count++
	}

	average := sum / time.Duration(count)

	return q.max, q.min, average
}

// Dequeue removes and returns the element from the front of the queue.
func (q *Queue) Dequeue() (time.Duration, error) {
	if q.IsEmpty() {
		var empty time.Duration
		return empty, fmt.Errorf("queue is empty")
	}

	value := q.Items[q.head]
	q.head = (q.head + 1) % q.maxSize
	return value, nil
}
