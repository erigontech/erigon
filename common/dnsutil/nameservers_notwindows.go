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

//go:build !windows

package dnsutil

import (
	"os"
	"time"

	"github.com/miekg/dns"
)

const resolvConf = "/etc/resolv.conf"

// nsState holds the mtime of /etc/resolv.conf at the time the nameservers
// were last loaded.
type nsState struct {
	mtime time.Time
}

// hasChanged reports whether /etc/resolv.conf has been modified since the
// last load.  It calls stat(2) which is always served from the kernel inode
// cache — there is no physical disk read unless the file metadata changed.
func (nc *NameserverConfig) hasChanged() bool {
	fi, err := os.Stat(resolvConf)
	return err == nil && fi.ModTime().After(nc.state.mtime)
}

// loadNameservers reads /etc/resolv.conf and returns the nameserver list
// together with the file's current mtime.  Falls back to public resolvers
// when the file is absent or empty.
func loadNameservers() ([]string, nsState) {
	fi, err := os.Stat(resolvConf)
	if err != nil {
		return FallbackNameservers, nsState{}
	}
	cfg, err := dns.ClientConfigFromFile(resolvConf)
	if err != nil || len(cfg.Servers) == 0 {
		return FallbackNameservers, nsState{mtime: fi.ModTime()}
	}
	return formatServers(cfg), nsState{mtime: fi.ModTime()}
}
