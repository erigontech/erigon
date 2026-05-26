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

package downloader

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/erigontech/erigon/db/snaptype"
)

// ErrForkManifestContainsForbiddenEntry is returned by
// ValidateForkManifestPostCutOnly when a fork manifest contains an
// entry the fork's authority cannot legitimately speak about. Wrapped
// errors carry the specific name + classification reason.
var ErrForkManifestContainsForbiddenEntry = errors.New("fork manifest contains forbidden entry")

// ValidateForkManifestPostCutOnly is the consumer-side mirror of
// FilterForkManifestPostCutOnly. Returns nil when every entry is
// legitimate for a fork publisher's emission, or an error describing
// the first violation.
//
// Acceptance rules per bucket:
//
//	Blocks / Caplin / Domains files
//	  post-cut classification  → accept
//	  pre-cut classification   → reject (fork can't speak about pre-cut)
//	  straddle classification  → reject (spans the cut; ambiguous)
//	  unparseable filename     → reject (defense-in-depth)
//
//	Meta / Salt files
//	  any name                 → accept
//
// The Meta and Salt buckets carry chain-wide artefacts (erigondb.toml,
// salt-*.txt, etc.) that have no block-range coordinate by design.
// The publisher's per-bucket categorisation is the signal — a file in
// Meta is a meta-file by construction. The strict post-cut rule
// applies only to ranged buckets where the cut-block coordinate is
// meaningful.
//
// cutBlock == 0 returns nil — a non-fork manifest can't violate fork
// rules. Callers should only invoke this on manifests they have
// already established are fork manifests (via the fork authority
// UCAN chain).
//
// Iteration order is deterministic (sorted) so the error message
// names the same offending entry across runs.
func ValidateForkManifestPostCutOnly(manifest *ChainTomlV2, cutBlock uint64, stepToBlock StepToBlock) error {
	if manifest == nil || cutBlock == 0 {
		return nil
	}

	// checkRanged is for buckets where the publisher's categorisation
	// asserts a block-coordinate exists (Blocks, Caplin, Domains).
	checkRanged := func(name string) error {
		entry := classify(name, cutBlock, stepToBlock)
		switch entry.Classification {
		case CopyPostCut:
			return nil
		case CopyUnknown:
			base := filepath.Base(name)
			parsed, _, _ := snaptype.ParseFileName("", base)
			if parsed.TypeString == "" {
				return fmt.Errorf("%w: %q is unparseable", ErrForkManifestContainsForbiddenEntry, name)
			}
			// Non-range files don't belong in a ranged bucket. Reject
			// to keep the categorisation honest.
			return fmt.Errorf("%w: %q has no range but appears in a ranged bucket", ErrForkManifestContainsForbiddenEntry, name)
		case CopyPreCut:
			return fmt.Errorf("%w: %q is pre-cut", ErrForkManifestContainsForbiddenEntry, name)
		case CopyStraddle:
			return fmt.Errorf("%w: %q spans cut block %d", ErrForkManifestContainsForbiddenEntry, name, cutBlock)
		default:
			return fmt.Errorf("%w: %q has unknown classification %d", ErrForkManifestContainsForbiddenEntry, name, entry.Classification)
		}
	}

	for _, b := range sortedBlocks(manifest.Blocks) {
		if err := checkRanged(b.Name); err != nil {
			return err
		}
	}
	for _, f := range manifest.Caplin {
		if err := checkRanged(f.Name); err != nil {
			return err
		}
	}
	for _, domain := range sortedDomainKeys(manifest.Domains) {
		dm := manifest.Domains[domain]
		if dm == nil {
			continue
		}
		for _, f := range dm.Files {
			if err := checkRanged(f.Name); err != nil {
				return err
			}
		}
	}
	// Meta + Salt: chain-wide artefacts; accept by bucket categorisation.
	return nil
}

func sortedBlocks(b []BlockFileEntry) []BlockFileEntry {
	if len(b) == 0 {
		return nil
	}
	out := make([]BlockFileEntry, len(b))
	copy(out, b)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func sortedKeys(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedDomainKeys(m map[string]*DomainManifest) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
