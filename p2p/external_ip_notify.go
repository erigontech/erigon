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

package p2p

import "time"

// defaultEventDebounce coalesces bursts of OS network-change events (an
// interface flap emits many) into a single re-resolve once the link settles.
const defaultEventDebounce = 3 * time.Second

// Per-OS notifiers share these messages so logs read the same everywhere; the
// "err" field carries the platform-specific cause.
const (
	notifierUnavailableMsg = "network-change notifier unavailable, using periodic external IP refresh only"
	notifierReadFailedMsg  = "network-change notifier read failed, disabling event-driven external IP refresh"
)

// netChangeNotifier watches OS network-configuration changes. Events delivers a
// coalesced signal to reconsider the external IP; it never carries an address,
// so this path cannot be used to move the advertised endpoint. Platforms with
// no native implementation return a notifier whose channel never fires, leaving
// periodic polling as the sole refresh path.
type netChangeNotifier interface {
	Events() <-chan struct{}
	Close() error
}

// noopNotifier never fires. It backs platforms without a native event source
// and is the runtime fallback when a native notifier fails to initialize.
type noopNotifier struct{}

func (noopNotifier) Events() <-chan struct{} { return nil }
func (noopNotifier) Close() error            { return nil }

// runWithNotifier re-resolves immediately, then on each interval tick and on
// each debounced network-change event, until stop closes. An event only
// triggers a re-resolution; the committed address is always the resolver's
// validated answer, never anything carried by the event.
func (t *externalIPTracker) runWithNotifier(stop <-chan struct{}, notifier netChangeNotifier, interval, debounce time.Duration) {
	t.refresh()

	tick := time.NewTimer(interval)
	defer tick.Stop()

	settle := time.NewTimer(debounce)
	stopAndDrain(settle)
	defer settle.Stop()

	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			t.refresh()
			tick.Reset(interval)
		case <-notifier.Events():
			stopAndDrain(settle)
			settle.Reset(debounce)
		case <-settle.C:
			t.refresh()
		}
	}
}

// stopAndDrain stops t and discards any pending tick, leaving it safe to Reset.
func stopAndDrain(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}
