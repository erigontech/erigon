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

package dnsdisc

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/miekg/dns"

	"github.com/erigontech/erigon/common/dnsutil"
)

// systemTTLResolver performs DNS TXT lookups via the system nameservers using
// raw DNS messages so that the actual record TTL can be extracted and honored
// by the caller's cache.
type systemTTLResolver struct {
	ns *dnsutil.NameserverConfig
}

func newSystemTTLResolver() *systemTTLResolver {
	return &systemTTLResolver{ns: dnsutil.NewNameserverConfig()}
}

// LookupTXT queries TXT records for the given domain and returns the records
// together with the minimum TTL found in the DNS answer section.  The TTL
// allows callers to honour the authoritative caching lifetime instead of using
// a hard-coded interval.
func (r *systemTTLResolver) LookupTXT(ctx context.Context, domain string) ([]string, time.Duration, error) {
	if !dns.IsFqdn(domain) {
		domain += "."
	}

	msg := new(dns.Msg)
	msg.SetQuestion(domain, dns.TypeTXT)
	msg.RecursionDesired = true

	udp := &dns.Client{Timeout: resolveTimeout(ctx)}

	var (
		resp *dns.Msg
		err  error
	)
	for _, ns := range r.ns.Get() {
		resp, _, err = udp.ExchangeContext(ctx, msg, ns)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, 0, err
	}

	switch resp.Rcode {
	case dns.RcodeNameError:
		return nil, 0, &net.DNSError{
			Err:        "no such host",
			Name:       domain,
			IsNotFound: true,
		}
	case dns.RcodeSuccess:
		// handled below
	default:
		return nil, 0, &net.DNSError{
			Err:  dns.RcodeToString[resp.Rcode],
			Name: domain,
		}
	}

	// TCP fallback when the UDP response was truncated.
	if resp.Truncated {
		tcp := &dns.Client{Net: "tcp", Timeout: resolveTimeout(ctx)}
		for _, ns := range r.ns.Get() {
			resp, _, err = tcp.ExchangeContext(ctx, msg, ns)
			if err == nil {
				break
			}
		}
		if err != nil {
			return nil, 0, err
		}
	}

	var (
		txts   []string
		minTTL = time.Duration(^uint32(0)) * time.Second // initialise to max uint32 seconds
	)
	for _, rr := range resp.Answer {
		txt, ok := rr.(*dns.TXT)
		if !ok {
			continue
		}
		// A single TXT RR can contain multiple character-strings (each ≤255 B).
		// Join them the same way net.Resolver.LookupTXT does, otherwise long
		// ENR entries get split and the Keccak hash check fails.
		txts = append(txts, strings.Join(txt.Txt, ""))
		if ttl := time.Duration(rr.Header().Ttl) * time.Second; ttl < minTTL {
			minTTL = ttl
		}
	}
	if len(txts) == 0 {
		// NOERROR but no TXT records — treat as "not found" so callers can
		// handle it uniformly.
		return nil, 0, &net.DNSError{
			Err:        "no TXT records",
			Name:       domain,
			IsNotFound: true,
		}
	}
	return txts, minTTL, nil
}

// resolveTimeout returns a deadline-aware timeout for an individual DNS
// exchange, capped at 5 s so we never block for too long on a single query.
func resolveTimeout(ctx context.Context) time.Duration {
	const maxTimeout = 5 * time.Second
	if dl, ok := ctx.Deadline(); ok {
		if rem := time.Until(dl); rem < maxTimeout {
			return rem
		}
	}
	return maxTimeout
}
