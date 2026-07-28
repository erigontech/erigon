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

import (
	"net"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/nat"
)

// externalIPRefreshInterval is how often the public IP is re-resolved for NAT
// mechanisms whose external address can change while the node runs (STUN).
const externalIPRefreshInterval = 10 * time.Minute

// usableExternalIP reports whether ip can be advertised as the node's public
// address. It rejects the non-routable values a misbehaving resolver might hand
// back so they never reach the local node record.
func usableExternalIP(ip net.IP) bool {
	return ip.IsGlobalUnicast() && !ip.IsPrivate()
}

// externalIPTracker re-resolves the external IP through a NAT mechanism and
// applies it via set only when it changes to a usable value.
type externalIPTracker struct {
	nat     nat.Interface
	set     func(net.IP)
	logger  log.Logger
	last    net.IP
	failing bool
}

// refresh resolves the external IP once and applies it on change. It reports
// whether the address was updated.
func (t *externalIPTracker) refresh() bool {
	ip, err := t.nat.ExternalIP()
	if err != nil {
		t.logFailure("NAT ExternalIP resolution has failed, try to pass a different --nat option", "err", err)
		return false
	}
	if !usableExternalIP(ip) {
		t.logFailure("NAT returned an unusable external IP, ignoring", "ip", ip)
		return false
	}
	t.failing = false
	if ip.Equal(t.last) {
		return false
	}
	t.last = ip
	t.set(ip)
	t.logger.Info("NAT ExternalIP resolved", "ip", ip)
	return true
}

// logFailure reports a resolution failure loudly the first time, then quietly
// while it persists, so a flapping link or a down STUN server cannot spam logs.
func (t *externalIPTracker) logFailure(msg string, ctx ...any) {
	if t.failing {
		t.logger.Debug(msg, ctx...)
		return
	}
	t.failing = true
	t.logger.Warn(msg, ctx...)
}

// run resolves immediately, then re-resolves on every interval and on every OS
// network-change event until stop closes.
func (t *externalIPTracker) run(stop <-chan struct{}, interval time.Duration) {
	notifier := newNetChangeNotifier(t.logger)
	defer notifier.Close()
	t.runWithNotifier(stop, notifier, interval, defaultEventDebounce)
}
