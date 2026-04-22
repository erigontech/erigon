package epbs

import (
	"fmt"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/cltypes"
)

// PreferencesWatcher watches for incoming proposer preferences via the
// EpbsPool.OnPreferencesReceived callback and provides a blocking wait
// interface for the builder loop.
//
// Preferences may arrive before the builder loop calls WaitForPreferences
// (proposers broadcast preferences for upcoming slots). The watcher stores
// all received preferences keyed by slot so that early arrivals are not lost.
type PreferencesWatcher struct {
	mu          sync.Mutex
	ch          chan struct{} // signalled when prefs for the watched slot arrive
	waitingSlot uint64        // the slot WaitForPreferences is currently blocking on
	waiting     bool          // true while WaitForPreferences is blocked
	prefsBySlot map[uint64]*cltypes.SignedProposerPreferences
}

// NewPreferencesWatcher creates a PreferencesWatcher.
func NewPreferencesWatcher() *PreferencesWatcher {
	return &PreferencesWatcher{
		ch:          make(chan struct{}, 1),
		prefsBySlot: make(map[uint64]*cltypes.SignedProposerPreferences),
	}
}

// OnPreferencesReceived is the callback to register on EpbsPool.OnPreferencesReceived.
// It must not block — it stores the preferences and signals the watcher if
// the preferences match the slot the builder loop is currently waiting for.
func (w *PreferencesWatcher) OnPreferencesReceived(slot uint64, prefs *cltypes.SignedProposerPreferences) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.prefsBySlot[slot] = prefs

	// Signal only if the builder loop is waiting for this exact slot.
	if w.waiting && slot == w.waitingSlot {
		select {
		case w.ch <- struct{}{}:
		default:
		}
	}
}

// WaitForPreferences blocks until preferences arrive for the given slot
// or the timeout expires. If preferences for the slot arrived before this
// call, they are returned immediately.
func (w *PreferencesWatcher) WaitForPreferences(slot uint64, timeout time.Duration) (*cltypes.SignedProposerPreferences, error) {
	w.mu.Lock()

	// Fast path: preferences already arrived before we started waiting.
	if prefs, ok := w.prefsBySlot[slot]; ok {
		delete(w.prefsBySlot, slot)
		w.mu.Unlock()
		return prefs, nil
	}

	// Register the slot we are waiting on and drain any stale signal.
	w.waitingSlot = slot
	w.waiting = true
	select {
	case <-w.ch:
	default:
	}
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		w.waiting = false
		// Prune old entries — keep only future slots.
		for s := range w.prefsBySlot {
			if s < slot {
				delete(w.prefsBySlot, s)
			}
		}
		w.mu.Unlock()
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-w.ch:
		w.mu.Lock()
		prefs := w.prefsBySlot[slot]
		delete(w.prefsBySlot, slot)
		w.mu.Unlock()
		if prefs == nil {
			return nil, fmt.Errorf("epbs/preferences: signal received but no preferences for slot %d", slot)
		}
		return prefs, nil
	case <-timer.C:
		return nil, fmt.Errorf("epbs/preferences: timeout waiting for preferences for slot %d", slot)
	}
}
