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
	if ip == nil {
		return false
	}
	return !ip.IsUnspecified() &&
		!ip.IsLoopback() &&
		!ip.IsPrivate() &&
		!ip.IsLinkLocalUnicast() &&
		!ip.IsLinkLocalMulticast() &&
		!ip.IsMulticast()
}

// externalIPTracker re-resolves the external IP through a NAT mechanism and
// applies it via set only when it changes to a usable value.
type externalIPTracker struct {
	nat    nat.Interface
	set    func(net.IP)
	logger log.Logger
	last   net.IP
}

// refresh resolves the external IP once and applies it on change. It reports
// whether the address was updated.
func (t *externalIPTracker) refresh() bool {
	ip, err := t.nat.ExternalIP()
	if err != nil {
		t.logger.Warn("NAT ExternalIP resolution has failed, try to pass a different --nat option", "err", err)
		return false
	}
	if !usableExternalIP(ip) {
		t.logger.Warn("NAT returned an unusable external IP, ignoring", "ip", ip)
		return false
	}
	if ip.Equal(t.last) {
		return false
	}
	t.last = ip
	t.set(ip)
	t.logger.Info("NAT ExternalIP resolved", "ip", ip)
	return true
}

// run resolves immediately, then re-resolves every interval until stop closes.
func (t *externalIPTracker) run(stop <-chan struct{}, interval time.Duration) {
	t.refresh()
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-stop:
			return
		case <-timer.C:
			t.refresh()
			timer.Reset(interval)
		}
	}
}
