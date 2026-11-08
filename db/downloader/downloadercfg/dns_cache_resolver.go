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
	"sync"
	"time"

	"github.com/miekg/dns"

	"github.com/erigontech/erigon/common/dnsutil"
)

// TTLDNSDialer is a DialContext-compatible resolver that caches A/AAAA record
// lookups for the duration of their actual DNS TTL.  Entries are refreshed
// lazily on demand — no background goroutine is needed.
type TTLDNSDialer struct {
	mu    sync.RWMutex
	ns    *dnsutil.NameserverConfig
	cache map[string]ttlEntry
}

type ttlEntry struct {
	addrs  []string
	expiry time.Time
}

// NewTTLDNSDialer creates a dialer that uses the system nameservers, refreshing
// them lazily when the underlying OS configuration changes.
func NewTTLDNSDialer() *TTLDNSDialer {
	return &TTLDNSDialer{
		ns:    dnsutil.NewNameserverConfig(),
		cache: make(map[string]ttlEntry),
	}
}

// DialContext resolves host to an IP address (honouring the DNS TTL cache) and
// then dials the resulting address.  It tries each resolved IP in turn and
// returns the first successful connection.
func (d *TTLDNSDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	addrs, err := d.lookupCached(ctx, host)
	if err != nil {
		return nil, err
	}
	var dialErr error
	for _, ip := range addrs {
		var dialer net.Dialer
		conn, connErr := dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
		if connErr == nil {
			return conn, nil
		}
		dialErr = connErr
	}
	return nil, dialErr
}

func (d *TTLDNSDialer) lookupCached(ctx context.Context, host string) ([]string, error) {
	// Literal IP addresses are returned directly without caching.
	if net.ParseIP(host) != nil {
		return []string{host}, nil
	}

	// Fast path: valid (non-expired) cache hit.
	d.mu.RLock()
	if e, ok := d.cache[host]; ok && time.Now().Before(e.expiry) {
		addrs := e.addrs
		d.mu.RUnlock()
		return addrs, nil
	}
	d.mu.RUnlock()

	// Slow path: DNS lookup.
	addrs, ttl, err := d.resolve(ctx, host)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	d.cache[host] = ttlEntry{addrs: addrs, expiry: time.Now().Add(ttl)}
	d.mu.Unlock()

	return addrs, nil
}

// resolve queries A records first; if that yields no addresses it falls back
// to AAAA.  Returns all found addresses and the minimum TTL across all records.
func (d *TTLDNSDialer) resolve(ctx context.Context, host string) ([]string, time.Duration, error) {
	addrs, ttl, err := d.queryRR(ctx, host, dns.TypeA)
	if err == nil && len(addrs) > 0 {
		return addrs, ttl, nil
	}
	return d.queryRR(ctx, host, dns.TypeAAAA)
}

const ttlDialMaxTimeout = 5 * time.Second

func (d *TTLDNSDialer) queryRR(ctx context.Context, host string, qtype uint16) ([]string, time.Duration, error) {
	if !dns.IsFqdn(host) {
		host += "."
	}
	msg := new(dns.Msg)
	msg.SetQuestion(host, qtype)
	msg.RecursionDesired = true

	timeout := ttlDialMaxTimeout
	if dl, ok := ctx.Deadline(); ok {
		if rem := time.Until(dl); rem < timeout {
			timeout = rem
		}
	}

	client := &dns.Client{Timeout: timeout}

	var (
		resp *dns.Msg
		err  error
	)
	for _, ns := range d.ns.Get() {
		resp, _, err = client.ExchangeContext(ctx, msg, ns)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, 0, err
	}

	switch resp.Rcode {
	case dns.RcodeNameError:
		return nil, 0, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	case dns.RcodeSuccess:
		// handled below
	default:
		return nil, 0, &net.DNSError{Err: dns.RcodeToString[resp.Rcode], Name: host}
	}

	// TCP fallback when the UDP response was truncated.
	if resp.Truncated {
		tcpClient := &dns.Client{Net: "tcp", Timeout: timeout}
		for _, ns := range d.ns.Get() {
			resp, _, err = tcpClient.ExchangeContext(ctx, msg, ns)
			if err == nil {
				break
			}
		}
		if err != nil {
			return nil, 0, err
		}
	}

	maxInitialTTL := time.Duration(^uint32(0)) * time.Second
	minTTL := maxInitialTTL

	var addrs []string
	for _, rr := range resp.Answer {
		if ttl := time.Duration(rr.Header().Ttl) * time.Second; ttl < minTTL {
			minTTL = ttl
		}
		switch r := rr.(type) {
		case *dns.A:
			addrs = append(addrs, r.A.String())
		case *dns.AAAA:
			addrs = append(addrs, r.AAAA.String())
		}
	}
	if len(addrs) == 0 {
		// NOERROR but no address records — caller will try the other record type.
		return nil, 0, nil
	}
	if minTTL == maxInitialTTL {
		minTTL = 5 * time.Minute // safety fallback if no TTL was present
	}
	return addrs, minTTL, nil
}
