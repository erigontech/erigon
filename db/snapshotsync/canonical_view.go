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

package snapshotsync

import (
	"math"
	"sync"
	"time"

	"github.com/erigontech/erigon/db/snapcfg"
)

// DefaultCanonicalGCWindow is how long an entry may go unadvertised by
// every trust-verified publisher before the canonical view drops it —
// the safety margin for a long-superseded merge form genuinely leaving
// the swarm.
const DefaultCanonicalGCWindow = 24 * time.Hour

type canonicalEntry struct {
	name string
	hash string
}

// CanonicalView is the consumer-computed Layer 1 canonical set
// (docs/plans/20260520-chaintoml-ucan-flow-spec.md). It is anchored at
// an immutable pinned genesis (v0) and extended by quorum promotion:
// an entry (name, hash) becomes canonical once observed in >= Q
// distinct trust-verified publisher advertisements, where
// Q = max(QFloor, ceil(F*N)) and N is the number of distinct verified
// UCAN issuers observed so far.
//
// The Sybil boundary is the UCAN issuer pubkey, not the peer-id or ENR
// — minting ENRs is free, minting issuers that chain to the trust root
// is not. Pre-merge and merged forms of a range are different (name,
// hash) pairs that accumulate quorum independently and both stay
// canonical, so a merge transition needs no special handling.
//
// The view only grows by promotion; it never demotes. The single
// exception is GC: an entry no verified publisher has advertised for
// the GC window is dropped so a superseded merge form is not retained
// forever. Genesis entries are never GC'd.
//
// All methods are safe for concurrent use.
type CanonicalView struct {
	mu       sync.Mutex
	genesis  snapcfg.PreverifiedItems
	q        snapcfg.QuorumConfig
	gcWindow time.Duration

	issuers  map[string]struct{}                     // distinct verified issuers — N
	observed map[canonicalEntry]map[string]time.Time // entry → issuer → last-seen
	promoted map[canonicalEntry]struct{}             // entries past quorum (monotonic)
	version  int
}

// NewCanonicalView returns a view anchored at genesis with the given
// quorum policy and the default GC window.
func NewCanonicalView(genesis snapcfg.PreverifiedItems, q snapcfg.QuorumConfig) *CanonicalView {
	return &CanonicalView{
		genesis:  genesis,
		q:        q,
		gcWindow: DefaultCanonicalGCWindow,
		issuers:  map[string]struct{}{},
		observed: map[canonicalEntry]map[string]time.Time{},
		promoted: map[canonicalEntry]struct{}{},
	}
}

// SetGCWindow overrides the default GC window. Intended for tests.
func (v *CanonicalView) SetGCWindow(d time.Duration) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.gcWindow = d
}

// quorum computes Q = max(QFloor, ceil(F*N)) under the held lock.
func (v *CanonicalView) quorum() int {
	q := int(math.Ceil(v.q.F * float64(len(v.issuers))))
	if q < v.q.QFloor {
		return v.q.QFloor
	}
	return q
}

// Observe records that the trust-verified publisher identified by
// issuer (its UCAN issuer pubkey) advertised items at time now. It
// returns true if the observation promoted at least one new entry —
// i.e. advanced the canonical version.
func (v *CanonicalView) Observe(issuer []byte, items snapcfg.PreverifiedItems, now time.Time) bool {
	if len(issuer) == 0 || len(items) == 0 {
		return false
	}
	issuerKey := string(issuer)

	v.mu.Lock()
	defer v.mu.Unlock()

	v.issuers[issuerKey] = struct{}{}
	for _, it := range items {
		k := canonicalEntry{it.Name, it.Hash}
		set := v.observed[k]
		if set == nil {
			set = map[string]time.Time{}
			v.observed[k] = set
		}
		set[issuerKey] = now
	}

	q := v.quorum()
	promotedNew := false
	for k, set := range v.observed {
		if _, done := v.promoted[k]; done {
			continue
		}
		if len(set) >= q {
			v.promoted[k] = struct{}{}
			promotedNew = true
		}
	}
	if promotedNew {
		v.version++
	}
	return promotedNew
}

// GC drops every non-genesis entry whose newest observation is older
// than the GC window, returning the number dropped. A dropped entry
// loses its promotion too — the only way the view ever shrinks.
func (v *CanonicalView) GC(now time.Time) int {
	v.mu.Lock()
	defer v.mu.Unlock()

	cutoff := now.Add(-v.gcWindow)
	dropped := 0
	for k, set := range v.observed {
		newest := time.Time{}
		for _, ts := range set {
			if ts.After(newest) {
				newest = ts
			}
		}
		if newest.Before(cutoff) {
			delete(v.observed, k)
			delete(v.promoted, k)
			dropped++
		}
	}
	return dropped
}

// Canonical returns the current canonical set: genesis ∪ promoted
// entries, sorted by name. A name may appear more than once with
// distinct hashes — that is a merge transition, and both forms are
// canonical.
func (v *CanonicalView) Canonical() snapcfg.PreverifiedItems {
	v.mu.Lock()
	defer v.mu.Unlock()

	seen := make(map[canonicalEntry]struct{}, len(v.genesis)+len(v.promoted))
	out := make(snapcfg.PreverifiedItems, 0, len(v.genesis)+len(v.promoted))
	for _, it := range v.genesis {
		k := canonicalEntry{it.Name, it.Hash}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, it)
	}
	for k := range v.promoted {
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, snapcfg.PreverifiedItem{Name: k.name, Hash: k.hash})
	}
	out.Sort()
	return out
}

// Version returns the canonical version counter — genesis is v0, and
// the counter advances once per Observe call that promotes new entries.
func (v *CanonicalView) Version() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.version
}
