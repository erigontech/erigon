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

// MinorityVerdict is the non-fatal outcome of CheckOwnAdvertisement:
// this node's advertisement disagrees with the canonical set ONLY at
// quorum-promoted entries — never at the pinned genesis. The node is a
// minority publisher whose retired bytes lost the quorum race; it is
// recoverable by adopting the canonical hashes (Phase 7 staged
// adoption). Contrast *AdvertisementSelfCheckError — a genesis-level
// disagreement, which is fatal.
type MinorityVerdict struct {
	// Adopt lists each entry whose own hash must be replaced by the
	// canonical (quorum-promoted) hash.
	Adopt []AdvertisementMismatch
}

// CheckOwnAdvertisement classifies every entry in this node's outgoing
// advertisement whose NAME appears in the canonical set: a HASH that
// matches at least one canonical version passes; a HASH that matches
// none is a mismatch. Entries whose names are NOT in any canonical
// pass through silently — new files this publisher retired, awaiting
// promotion by the swarm-agreement layer.
//
// A mismatch splits two ways
// (docs/plans/20260520-chaintoml-ucan-flow-spec.md, minority
// transition):
//
//   - divergence — the mismatching name is in the pinned genesis v0.
//     Genesis is immutable, so a disagreement means corrupt data or
//     the wrong chain. Returned as *AdvertisementSelfCheckError; the
//     caller must fail loud and halt.
//   - minority — the mismatching name is canonical only via quorum
//     promotion, not genesis. This node's retired bytes lost the
//     quorum race; recoverable by adopting the canonical hashes.
//     Returned as a non-nil *MinorityVerdict with a nil error.
//
// When a single advertisement has both kinds, divergence wins: the
// error is returned and the minority verdict suppressed, because the
// process halts anyway.
//
// This is the producer-side counterpart of ValidateAdvertisement:
// ValidateAdvertisement (consumer) silently drops mismatched entries;
// CheckOwnAdvertisement (producer) halts on a genesis divergence and
// flags a minority for adoption.
//
// Multi-canonical input from day one — during merge transitions, an
// entry whose name appears in EITHER pre-merge OR post-merge canonical
// must match THAT canonical's hash; it need not match both.
//
// When canonicals is empty (no canonical loaded yet) returns
// (nil, nil) — there's no basis on which to check.
func CheckOwnAdvertisement(adv, genesis snapcfg.PreverifiedItems, canonicals []snapcfg.PreverifiedItems) (*MinorityVerdict, error) {
	if len(canonicals) == 0 || len(adv) == 0 {
		return nil, nil
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

	genesisNames := make(map[string]struct{}, len(genesis))
	for _, p := range genesis {
		genesisNames[p.Name] = struct{}{}
	}

	var divergences, minorities []AdvertisementMismatch
	for _, p := range adv {
		hashes, nameKnown := canon[p.Name]
		if !nameKnown {
			continue // new file, not yet in canonical — allowed
		}
		if _, hashOK := hashes[p.Hash]; hashOK {
			continue // matches at least one canonical version
		}
		m := AdvertisementMismatch{
			Name:          p.Name,
			OwnHash:       p.Hash,
			CanonicalHash: firstCanonicalHash[p.Name],
		}
		if _, isGenesis := genesisNames[p.Name]; isGenesis {
			divergences = append(divergences, m)
		} else {
			minorities = append(minorities, m)
		}
	}
	// Divergence wins: the process halts, so a co-occurring minority
	// verdict would never be acted on.
	if len(divergences) > 0 {
		return nil, &AdvertisementSelfCheckError{Mismatches: divergences}
	}
	if len(minorities) > 0 {
		return &MinorityVerdict{Adopt: minorities}, nil
	}
	return nil, nil
}

// IsAdvertisementSelfCheckError reports whether err is or wraps an
// AdvertisementSelfCheckError. Callers wanting to handle the
// "halt and shutdown" path specifically use this to discriminate
// from generic errors during the publish path.
func IsAdvertisementSelfCheckError(err error) bool {
	var target *AdvertisementSelfCheckError
	return errors.As(err, &target)
}
