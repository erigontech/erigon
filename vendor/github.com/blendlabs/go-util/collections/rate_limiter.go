package collections

import (
	"time"
)

// NewRateLimiter returns a new RateLimiter instance.
func NewRateLimiter(numberOfActions int, quantum time.Duration) *RateLimiter {
	return &RateLimiter{
		NumberOfActions: numberOfActions,
		Quantum:         quantum,
		Limits:          map[string]Queue{},
	}
}

// RateLimiter is a simple implementation of a rate checker.
type RateLimiter struct {
	NumberOfActions int
	Quantum         time.Duration
	Limits          map[string]Queue
}

// Check returns true if it has been called NumberOfActions times or more in Quantum or smaller duration.
func (rl *RateLimiter) Check(id string) bool {
	queue, hasQueue := rl.Limits[id]
	if !hasQueue {
		queue = NewRingBufferWithCapacity(rl.NumberOfActions)
		rl.Limits[id] = queue
	}

	currentTime := time.Now().UTC()
	queue.Enqueue(currentTime)
	if queue.Len() < rl.NumberOfActions {
		return false
	}

	oldest := queue.Dequeue().(time.Time)
	return currentTime.Sub(oldest) < rl.Quantum
}
