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

// FilterForkManifestPostCutOnly mutates a generated *ChainTomlV2 in
// place to remove all entries whose coverage is not strictly post-cut.
// Applied by a fork-chain publisher before writing the manifest so the
// fork's advertisement contains only files the fork's trust root can
// authoritatively speak about.
//
// The trust-root rule (captured 2026-05-24 in conversation with the
// user; see memory/fork-trust-root-model-2026-05-24): the fork's
// chain.v2 publishes files under the fork operator's UCAN attestation,
// which chains to the fork's own trust root. Pre-cut files carry
// parent-canonical hashes — their canonicity is established by the
// parent's manifest (pinned by ParentManifestHash), not by the fork's
// authority. Listing them in the fork's manifest would be impersonating
// parent canonicity. Pre-cut files we hold are still seeded via raw
// BitTorrent peer discovery on the parent's info-hashes — they just
// don't appear in the fork's chain.v2.
//
// Bucket-aware rules:
//
//	Blocks / Caplin / Domains entries (ranged buckets)
//	  post-cut classification     → keep
//	  pre-cut / straddle          → drop
//	  unparseable filename        → drop (defensive — an honest
//	                                publisher's inventory shouldn't
//	                                carry garbage names; a bad entry
//	                                must not silently pass into a
//	                                signed manifest)
//	  non-range parseable name    → drop (chain-wide artefacts have
//	                                their own buckets; a non-range
//	                                file in a ranged bucket is a
//	                                bucket-categorisation bug)
//
//	Meta / Salt entries (chain-wide buckets)
//	  any name                    → keep (the publisher's
//	                                categorisation is the signal;
//	                                erigondb.toml, salt-*.txt, etc.
//	                                have no block coordinate by design)
//
// The bucket asymmetry mirrors the consumer-side validator
// (ValidateForkManifestPostCutOnly): both sides apply strict
// post-cut + parseable rules only where the cut-block coordinate is
// meaningful. The producer's KEEP set ≡ the consumer's ACCEPT set,
// so a manifest that survives the filter always passes the
// validator (round-trip test pins this).
//
// stepToBlock is the parent's step→block mapping. Empty map is the
// safe default: state files with no mapping classify as Straddle and
// are dropped — exactly right for a fork publisher whose first retire
// will produce fresh fork-lineage state files at the next step.
//
// cutBlock == 0 is a no-op: non-fork publishers must not call this,
// and a fork-config with CutBlock=0 is malformed (rejected upstream
// by ValidateForkDatadir + BuildForkBootstrapPlan).
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

	for name := range manifest.Blocks {
		if !keepRanged(name) {
			delete(manifest.Blocks, name)
		}
	}

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
