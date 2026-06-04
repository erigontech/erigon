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

package changeset

import (
	"fmt"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

// ReadTier identifies which layer of the state read cascade served a
// SharedDomains.GetLatest lookup.
type ReadTier uint8

const (
	TierStateCache ReadTier = iota // executor StateCache (execution/cache)
	TierMem                        // SharedDomains mem batch (incl. parent chain)
	TierDb                         // MDBX latest-state tables
	TierFileCache                  // per-tx DomainGetFromFileCache LRU
	TierFile                       // .kv snapshot files
	tierCount
)

var tierNames = [tierCount]string{"state_cache", "mem", "db", "filecache", "file"}

var readTierCounters = func() (c [kv.DomainLen][tierCount]metrics.Counter) {
	for d := kv.Domain(0); d < kv.DomainLen; d++ {
		for t := ReadTier(0); t < tierCount; t++ {
			c[d][t] = metrics.GetOrCreateCounter(fmt.Sprintf(`domain_read_tier_total{domain="%s",tier="%s"}`, d.String(), tierNames[t]))
		}
	}
	return c
}()

// IncReadTier records a GetLatest read served by the given tier.
func IncReadTier(domain kv.Domain, tier ReadTier) {
	if domain < kv.DomainLen {
		readTierCounters[domain][tier].Inc()
	}
}
