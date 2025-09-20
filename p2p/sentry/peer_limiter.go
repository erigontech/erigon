package sentry

import "sync/atomic"

// PeerSlotLimiter tracks the number of active peers that count towards the shared
// MaxPeers limit across multiple protocol handlers.
type PeerSlotLimiter struct {
	max  int32
	used atomic.Int32
}

// NewPeerSlotLimiter constructs a new limiter. A non-positive max disables the limiter.
func NewPeerSlotLimiter(max int) *PeerSlotLimiter {
	if max <= 0 {
		return nil
	}
	return &PeerSlotLimiter{max: int32(max)}
}

// TryAcquire attempts to reserve a slot for a non-trusted peer. Trusted peers are
// always allowed and do not consume slots.
func (l *PeerSlotLimiter) TryAcquire(isTrusted bool) bool {
	if l == nil || isTrusted {
		return true
	}
	for {
		current := l.used.Load()
		if current >= l.max {
			return false
		}
		if l.used.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

// Release frees a previously acquired slot.
func (l *PeerSlotLimiter) Release() {
	if l == nil {
		return
	}
	for {
		current := l.used.Load()
		if current == 0 {
			return
		}
		if l.used.CompareAndSwap(current, current-1) {
			return
		}
	}
}

// InUse returns the number of currently reserved slots, primarily for testing.
func (l *PeerSlotLimiter) InUse() int {
	if l == nil {
		return 0
	}
	return int(l.used.Load())
}
