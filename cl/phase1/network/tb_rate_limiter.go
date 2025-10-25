package network

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
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

type tokenBucketLimiter struct {
	limiter    *rate.Limiter
	lastAccess time.Time
}

type tokenBucketRateLimiter struct {
	limiter map[string]tokenBucketLimiter
	mu      sync.Mutex
	rate    rate.Limit
	burst   int
}

func newTokenBucketRateLimiter(ratePerSecond float64, burst int) *tokenBucketRateLimiter {
	tb := &tokenBucketRateLimiter{
		limiter: make(map[string]tokenBucketLimiter),
		mu:      sync.Mutex{},
		rate:    rate.Limit(ratePerSecond),
		burst:   burst,
	}
	go tb.cleanup()
	return tb
}

func (r *tokenBucketRateLimiter) acquire(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	limiter, ok := r.limiter[key]
	if !ok {
		limiter.limiter = rate.NewLimiter(r.rate, r.burst)
		r.limiter[key] = limiter
	}

	limiter.lastAccess = time.Now()
	return limiter.limiter.Allow()
}

func (r *tokenBucketRateLimiter) cleanup() {
	for {
		time.Sleep(10 * time.Second)
		r.mu.Lock()
		for key, limiter := range r.limiter {
			if time.Since(limiter.lastAccess) > 3*time.Minute {
				delete(r.limiter, key)
			}
		}
		r.mu.Unlock()
	}
}
