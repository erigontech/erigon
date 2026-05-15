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
	"errors"
	"fmt"
	"strings"

	"github.com/erigontech/erigon/db/snapcfg"
)

// AdvertisementMismatch describes a single entry in an outgoing
// advertisement whose hash diverges from the canonical hash for the
// same name. Used by CheckOwnAdvertisement to surface every divergent
// entry, not just the first — the operator needs the full picture
// to diagnose whether the retire bug is global or scoped.
type AdvertisementMismatch struct {
	Name          string
	OwnHash       string
	CanonicalHash string // first canonical version's hash for this name; others are listed in Error()'s message if relevant
}

// AdvertisementSelfCheckError is the error returned by
// CheckOwnAdvertisement when this node's outgoing advertisement has
// entries that diverge from canonical. Reading the embedded
// Mismatches gives the full list.
//
// Per the three-layer model
// (docs/plans/20260515-three-layer-snapshot-distribution.md, "fail
// loud" requirement): this error MUST propagate as a regular error
// return (not a panic, not a log-and-continue) so the caller can
// trigger a clean process shutdown. Continuing to publish after this
// error would either keep advertising bad info-hashes (poisoning
// the swarm) or keep retrying the same broken retire — neither is
// acceptable. Operators need the process to halt so they can
// investigate.
type AdvertisementSelfCheckError struct {
	Mismatches []AdvertisementMismatch
}

func (e *AdvertisementSelfCheckError) Error() string {
	if len(e.Mismatches) == 0 {
		return "advertisement self-check: unknown failure"
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "advertisement self-check: %d entries diverge from canonical (retire bug; halt and investigate):",
		len(e.Mismatches))
	limit := len(e.Mismatches)
	if limit > 8 {
		limit = 8
	}
	for i := 0; i < limit; i++ {
		m := e.Mismatches[i]
		fmt.Fprintf(&sb, "\n  %s: own=%s canonical=%s", m.Name, m.OwnHash, m.CanonicalHash)
	}
	if len(e.Mismatches) > limit {
		fmt.Fprintf(&sb, "\n  ... %d more (see Mismatches slice)", len(e.Mismatches)-limit)
	}
	return sb.String()
}

// CheckOwnAdvertisement asserts that every entry in this node's
// outgoing advertisement whose NAME appears in any canonical version
// has a HASH matching at least one canonical version's hash for that
// name. Entries whose names are NOT in any canonical pass through
// silently — these are new files retired by this publisher, awaiting
// promotion to canonical by the swarm-agreement layer.
//
// Returns *AdvertisementSelfCheckError on divergence (fail loud:
// "this node is producing files whose bytes don't match the swarm's
// agreed-on hashes — retire is broken or the build is wrong").
// Returns nil when adv is consistent with canonicals OR adds new
// entries cleanly.
//
// This is the producer-side counterpart of ValidateAdvertisement:
// ValidateAdvertisement (consumer) silently drops mismatched entries;
// CheckOwnAdvertisement (producer) fails loudly on mismatched entries.
// The asymmetry is deliberate: a consumer encountering a bad peer
// filters and moves on; a producer encountering a bad own entry has
// no useful next action besides halting and surfacing the bug.
//
// Multi-canonical input from day one — same shape as
// ValidateAdvertisement. During merge transitions, an entry whose
// name appears in EITHER pre-merge OR post-merge canonical must match
// THAT canonical's hash; doesn't have to match both.
//
// When canonicals is empty (e.g., no canonical loaded yet), returns
// nil — there's no basis on which to check. Production callers must
// ensure canonical is loaded before invoking, but the helper itself
// is defensive.
func CheckOwnAdvertisement(adv snapcfg.PreverifiedItems, canonicals []snapcfg.PreverifiedItems) error {
	if len(canonicals) == 0 || len(adv) == 0 {
		return nil
	}

	// Build name → set-of-canonical-hashes lookup across all
	// canonicals. An entry matches if its hash is in the set for
	// its name (or its name is absent → new file, pass through).
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

	// Also track a "first canonical hash per name" for clearer error
	// messages — operators want to see what the expected hash was.
	firstCanonicalHash := make(map[string]string)
	for _, c := range canonicals {
		for _, p := range c {
			if _, seen := firstCanonicalHash[p.Name]; !seen {
				firstCanonicalHash[p.Name] = p.Hash
			}
		}
	}

	var mismatches []AdvertisementMismatch
	for _, p := range adv {
		hashes, nameKnown := canon[p.Name]
		if !nameKnown {
			continue // new file, not yet in canonical — allowed
		}
		if _, hashOK := hashes[p.Hash]; hashOK {
			continue // matches at least one canonical version
		}
		mismatches = append(mismatches, AdvertisementMismatch{
			Name:          p.Name,
			OwnHash:       p.Hash,
			CanonicalHash: firstCanonicalHash[p.Name],
		})
	}
	if len(mismatches) == 0 {
		return nil
	}
	return &AdvertisementSelfCheckError{Mismatches: mismatches}
}

// IsAdvertisementSelfCheckError reports whether err is or wraps an
// AdvertisementSelfCheckError. Callers wanting to handle the
// "halt and shutdown" path specifically use this to discriminate
// from generic errors during the publish path.
func IsAdvertisementSelfCheckError(err error) bool {
	var target *AdvertisementSelfCheckError
	return errors.As(err, &target)
}
