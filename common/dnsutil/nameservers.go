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

// Package dnsutil provides a lightweight DNS nameserver configuration
// reader that refreshes lazily when the underlying system configuration
// changes, keeping physical disk / registry I/O to a minimum.
package dnsutil

import (
	"net"
	"sync"

	"github.com/miekg/dns"
)

// FallbackNameservers are used when the system configuration is unavailable
// (e.g. on a fresh Windows install before any adapter is configured) or
// cannot be read.
var FallbackNameservers = []string{"8.8.8.8:53", "1.1.1.1:53", "9.9.9.9:53"}

// NameserverConfig caches the DNS nameserver list for use with raw DNS
// clients (miekg/dns) and refreshes it lazily when the underlying OS
// configuration changes.
//
// Platform behaviour:
//
//   - Linux / macOS: a stat(2) call is made before every DNS exchange to
//     check whether /etc/resolv.conf has been modified.  stat(2) is always
//     served from the kernel inode cache — no physical disk read unless the
//     file metadata itself changed, which on a server node essentially never
//     happens.  The actual file is re-read only when its mtime advances.
//
//   - Windows: nameservers are read from the Windows registry.  Registry
//     reads are served from the kernel's memory (no physical disk I/O).
//     The config is re-read at most once every 60 s so that network changes
//     (VPN connect / disconnect) are picked up quickly with negligible cost.
type NameserverConfig struct {
	mu      sync.Mutex
	servers []string
	state   nsState // platform-specific staleness state
}

// NewNameserverConfig reads the current system nameserver configuration and
// returns a NameserverConfig that will track future changes.
func NewNameserverConfig() *NameserverConfig {
	servers, state := loadNameservers()
	return &NameserverConfig{servers: servers, state: state}
}

// Get returns the current nameserver list, refreshing it transparently if
// the underlying configuration has changed since the last call.
func (nc *NameserverConfig) Get() []string {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	if nc.hasChanged() {
		if updated, state := loadNameservers(); len(updated) > 0 {
			nc.servers = updated
			nc.state = state
		}
	}
	return nc.servers
}

// formatServers converts a parsed dns.ClientConfig into "host:port" strings.
func formatServers(cfg *dns.ClientConfig) []string {
	servers := make([]string, 0, len(cfg.Servers))
	for _, s := range cfg.Servers {
		servers = append(servers, net.JoinHostPort(s, cfg.Port))
	}
	return servers
}
