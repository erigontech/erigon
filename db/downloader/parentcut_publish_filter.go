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

// FilterForkManifestPostCutOnly strips a fork publisher's manifest to
// entries the fork's trust root can authoritatively attest to (post-cut
// only). Pre-cut files belong to the parent's canonicity and get seeded
// via raw BT on the parent's info-hashes, not the fork's chain.v2.
//
// Ranged buckets (Blocks/Caplin/Domains): keep post-cut, drop pre-cut,
// straddle, unparseable, or non-range names. Chain-wide buckets
// (Meta/Salt): keep everything — those have no block coordinate.
// Mirrors ValidateForkManifestPostCutOnly on the consumer side.
//
// stepToBlock is the parent's step→block mapping; empty is the safe
// default (unmapped state files classify as Straddle → drop). cutBlock
// == 0 is a no-op.
func FilterForkManifestPostCutOnly(manifest *ChainTomlV2, cutBlock uint64, stepToBlock StepToBlock) {
	if manifest == nil || cutBlock == 0 {
		return
	}

	// keepRanged is for buckets where the publisher's categorisation
	// asserts a block-coordinate exists (Blocks, Caplin, Domains).
	// Drops pre-cut, straddle, unparseable, and non-range entries.
	keepRanged := func(name string) bool {
		entry := classify(name, cutBlock, stepToBlock)
		return entry.Classification == CopyPostCut
	}

	keptBlocks := manifest.Blocks[:0]
	for _, b := range manifest.Blocks {
		if keepRanged(b.Name) {
			keptBlocks = append(keptBlocks, b)
		}
	}
	manifest.Blocks = keptBlocks

	kept := manifest.Caplin[:0]
	for _, f := range manifest.Caplin {
		if keepRanged(f.Name) {
			kept = append(kept, f)
		}
	}
	manifest.Caplin = kept

	for domain, dm := range manifest.Domains {
		if dm == nil {
			continue
		}
		keptFiles := dm.Files[:0]
		for _, f := range dm.Files {
			if keepRanged(f.Name) {
				keptFiles = append(keptFiles, f)
			}
		}
		dm.Files = keptFiles
		if len(dm.Files) == 0 {
			delete(manifest.Domains, domain)
		}
	}

	// Meta + Salt: chain-wide; the publisher's bucket categorisation
	// is the signal. Touched only if something obviously wrong (nil
	// map) but normally left alone.
}
