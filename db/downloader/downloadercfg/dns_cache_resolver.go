// Copyright 2024 The Erigon Authors
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

package downloadercfg

import (
	"context"
	"net"
	"time"

	"github.com/rs/dnscache"
)

// DnsCacheResolver resolves DNS requests for an HTTP client using an in-memory cache.
type DnsCacheResolver struct {
	RefreshTimeout time.Duration

	resolver dnscache.Resolver
}

func (r *DnsCacheResolver) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	ips, err := r.resolver.LookupHost(ctx, host)
	if err != nil {
		return nil, err
	}
	var conn net.Conn
	for _, ip := range ips {
		var dialer net.Dialer
		conn, err = dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
		if err == nil {
			break
		}
	}
	return conn, err
}

func (r *DnsCacheResolver) Run(ctx context.Context) {
	ticker := time.NewTicker(r.RefreshTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.resolver.Refresh(true)
		}
	}
}
