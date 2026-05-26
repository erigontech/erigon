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

// FilterManifestByRetentionFloor mutates a generated *ChainTomlV2 in
// place to remove domain entries whose entire step range falls below
// the publisher's retention floor, and pins each surviving DomainManifest's
// Coverage[0] to the floor.
//
// Minimal-mode publishers don't hold the older state files but their
// inventory still references them (preverified bootstrap, or files
// from a prior archive run). Without this filter the manifest claims
// coverage the publisher cannot serve, and a consumer downloading
// from such a peer fails on the missing range.
//
// The filter is a no-op when floor == 0 (full-history publisher). It
// applies to state-domain files only — block / meta / salt / caplin
// entries are chain-wide and have their own retention semantics that
// don't map onto a single step floor.
//
// Rule per file: drop when Range[1] <= floor (the file ends at or
// before the floor; the publisher cannot serve any of its content).
// A file whose range straddles the floor is kept — the publisher
// can still serve the post-floor portion.
//
// Rule per Coverage: Coverage[0] is set to floor when the filter
// runs, so the advertised window starts where the publisher's
// retention actually begins, not at the earliest remaining file's
// boundary.
func FilterManifestByRetentionFloor(manifest *ChainTomlV2, floor uint64) {
	if manifest == nil || floor == 0 {
		return
	}
	for domain, dm := range manifest.Domains {
		if dm == nil {
			continue
		}
		kept := dm.Files[:0]
		for _, f := range dm.Files {
			if f.Range[1] <= floor {
				continue
			}
			kept = append(kept, f)
		}
		dm.Files = kept
		if len(dm.Files) == 0 {
			delete(manifest.Domains, domain)
			continue
		}
		// Pin Coverage[0] to the floor — the consumer reads this as
		// "this peer serves [floor, Coverage[1]) for this domain".
		dm.Coverage[0] = floor
	}
}
