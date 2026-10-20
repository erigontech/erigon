package registry

import (
	"maps"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type timeBasedRateLimiter struct {
	duration    time.Duration
	start       time.Time
	maxRequests func() int
	limit       int
	counter     int

	mu sync.Mutex
}

func newTimeBasedRateLimiter(duration time.Duration, maxRequests func() int) *timeBasedRateLimiter {
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
		r.limit = r.maxRequests()
		r.counter = 0
	}

	if r.counter < r.limit {
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
	limiter   map[string]*tokenBucketLimiter
	mu        sync.Mutex
	rate      rate.Limit
	burst     int
	lastSweep time.Time
}

func newTokenBucketRateLimiter(ratePerSecond float64, burst int) *tokenBucketRateLimiter {
	return &tokenBucketRateLimiter{
		limiter: make(map[string]*tokenBucketLimiter),
		rate:    rate.Limit(ratePerSecond),
		burst:   burst,
	}
}

func (r *tokenBucketRateLimiter) acquire(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	if now.Sub(r.lastSweep) > 10*time.Second {
		r.lastSweep = now
		maps.DeleteFunc(r.limiter, func(_ string, l *tokenBucketLimiter) bool {
			return now.Sub(l.lastAccess) > 3*time.Minute
		})
	}

	limiter, ok := r.limiter[key]
	if !ok {
		limiter = &tokenBucketLimiter{limiter: rate.NewLimiter(r.rate, r.burst)}
		r.limiter[key] = limiter
	}

	limiter.lastAccess = now
	return limiter.limiter.Allow()
}
