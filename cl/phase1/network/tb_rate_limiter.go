package network

import (
	"sync"
	"time"
)

type timeBasedRateLimiter struct {
	duration    time.Duration
	start       time.Time
	maxRequests int
	counter     int

	mu sync.Mutex
}

func newTimeBasedRateLimiter(duration time.Duration, maxRequests int) *timeBasedRateLimiter {
	return &timeBasedRateLimiter{
		duration:    duration,
		maxRequests: maxRequests,
	}
}

// tryAcquire tries to acquire a token from the rate limiter. It returns true if a token was acquired, false otherwise.
func (r *timeBasedRateLimiter) tryAcquire() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if time.Since(r.start) > r.duration {
		r.start = time.Now()
		r.counter = 0
	}

	if r.counter < r.maxRequests {
		r.counter++
		return true
	}

	return false
}
