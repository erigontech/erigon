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

//go:build windows

package dnsutil

import (
	"net"
	"strings"
	"time"

	"golang.org/x/sys/windows/registry"
)

const windowsCheckInterval = 60 * time.Second

// nsState holds the timestamp of the last registry read so that the config
// is refreshed at most once every windowsCheckInterval.  Registry reads are
// served from the kernel memory cache — no physical disk I/O — but we still
// throttle them to avoid any overhead on hot paths.
type nsState struct {
	checkedAt time.Time
}

// hasChanged reports whether enough time has passed since the last nameserver
// read to warrant a fresh registry query.
func (nc *NameserverConfig) hasChanged() bool {
	return time.Since(nc.state.checkedAt) > windowsCheckInterval
}

// loadNameservers reads nameserver addresses from the Windows registry.
//
// Search order (highest priority first):
//  1. Per-adapter NameServer / DhcpNameServer values under
//     HKLM\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters\Interfaces\*
//  2. Global NameServer / DhcpNameServer values under
//     HKLM\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters
//
// Falls back to FallbackNameservers when the registry is inaccessible or
// returns no usable addresses.
func loadNameservers() ([]string, nsState) {
	state := nsState{checkedAt: time.Now()}

	var servers []string

	// Per-adapter keys (higher priority — covers VPN adapters, etc.)
	const interfacesKey = `SYSTEM\CurrentControlSet\Services\Tcpip\Parameters\Interfaces`
	if k, err := registry.OpenKey(registry.LOCAL_MACHINE, interfacesKey, registry.ENUMERATE_SUB_KEYS); err == nil {
		defer k.Close()
		if subkeys, err := k.ReadSubKeyNames(-1); err == nil {
			for _, sub := range subkeys {
				subPath := interfacesKey + `\` + sub
				if sk, err := registry.OpenKey(registry.LOCAL_MACHINE, subPath, registry.QUERY_VALUE); err == nil {
					servers = append(servers, readRegNameservers(sk)...)
					sk.Close()
				}
			}
		}
	}

	// Global fallback key.
	const globalKey = `SYSTEM\CurrentControlSet\Services\Tcpip\Parameters`
	if k, err := registry.OpenKey(registry.LOCAL_MACHINE, globalKey, registry.QUERY_VALUE); err == nil {
		defer k.Close()
		servers = append(servers, readRegNameservers(k)...)
	}

	// De-duplicate while preserving order.
	servers = dedup(servers)

	if len(servers) == 0 {
		return FallbackNameservers, state
	}
	return servers, state
}

// readRegNameservers extracts nameserver IP addresses from a registry key by
// reading the NameServer value (statically configured) and DhcpNameServer
// value (DHCP-assigned).  Each value may contain multiple space- or
// comma-separated IP addresses.
func readRegNameservers(k registry.Key) []string {
	var out []string
	for _, valueName := range []string{"NameServer", "DhcpNameServer"} {
		val, _, err := k.GetStringValue(valueName)
		if err != nil || val == "" {
			continue
		}
		// Windows stores multiple nameservers separated by spaces or commas.
		for _, raw := range strings.FieldsFunc(val, func(r rune) bool { return r == ' ' || r == ',' }) {
			raw = strings.TrimSpace(raw)
			if raw == "" {
				continue
			}
			// Addresses are plain IPs without a port; add the standard DNS port.
			if net.ParseIP(raw) != nil {
				out = append(out, net.JoinHostPort(raw, "53"))
			}
		}
	}
	return out
}

// dedup removes duplicate entries from s while preserving their first-seen order.
func dedup(s []string) []string {
	seen := make(map[string]struct{}, len(s))
	out := s[:0]
	for _, v := range s {
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			out = append(out, v)
		}
	}
	return out
}
