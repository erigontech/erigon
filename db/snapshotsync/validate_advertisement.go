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
	"github.com/erigontech/erigon/db/snapcfg"
)

// ValidateAdvertisement filters a peer's advertisement against the
// currently-accepted set of canonical chain.tomls. An entry survives
// iff its (name, hash) pair appears in at least one canonical version.
// Non-matching entries are silently dropped — the function returns
// only the validated subset.
//
// Multi-canonical input from day one per
// docs/plans/20260515-three-layer-snapshot-distribution.md. Today
// canonicals is size 1 (single upstream registry); during merge
// transitions (Round B) it can be size ≥2 — pre-merge and post-merge
// forms simultaneously valid until the old form is retired. The
// signature accepts a slice so adding multi-canonical support later
// is a configuration change at the call site, not a code refactor.
//
// This filter is APPLIED AFTER the signature check on the peer's
// advertisement. Order matters:
//
//  1. Signature gate: chain.<peer_enr>.sig must verify against the
//     peer's ENR public key over chain.<peer_enr>.toml bytes. Failure
//     ⇒ drop the WHOLE file (peer is misbehaving or MITM tampering).
//
//  2. ValidateAdvertisement: per-entry subset filter against canonical
//     set. Failure ⇒ drop only the non-matching entries (peer might
//     still serve valid entries; the bad ones are stale or wrong).
//
// The two checks have different failure modes by design: tampering is
// wholesale (the document itself is suspect), staleness is per-entry
// (some entries good, some out of date).
//
// When canonicals is empty, returns nil — there's no basis on which to
// accept anything. (This is the "no canonical loaded yet" defensive
// case; production should never call with empty canonicals.)
func ValidateAdvertisement(adv snapcfg.PreverifiedItems, canonicals []snapcfg.PreverifiedItems) snapcfg.PreverifiedItems {
	if len(canonicals) == 0 || len(adv) == 0 {
		return nil
	}

	// Build a single name → set-of-hashes lookup across all
	// canonicals. An entry survives if its (name, hash) is found in
	// any canonical version's entry for that name.
	//
	// We use string-keyed maps because preverified.Item.Hash is a hex
	// string (not bytes). Sharing the keyspace across all canonicals
	// is correct because Layer 2's contract says "same name ⇒ same
	// hash within a single canonical version"; across versions
	// (during merge) the SAME name typically does NOT appear — the
	// pre-merge and post-merge forms use different file names. The
	// hashes never collide because they're content-derived; we just
	// need to know "is this (name, hash) in ANY currently-accepted
	// version".
	canon := make(map[string]map[string]struct{})
	for _, c := range canonicals {
		for _, p := range c {
			hashes, ok := canon[p.Name]
			if !ok {
				hashes = make(map[string]struct{})
				canon[p.Name] = hashes
			}
			hashes[p.Hash] = struct{}{}
		}
	}

	out := make(snapcfg.PreverifiedItems, 0, len(adv))
	for _, p := range adv {
		hashes, ok := canon[p.Name]
		if !ok {
			continue
		}
		if _, ok := hashes[p.Hash]; !ok {
			continue
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
