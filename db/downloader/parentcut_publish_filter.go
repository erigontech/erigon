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
// Classification rules (same as BuildCopyPlan's classifier):
//   - File entirely strictly before cutBlock → drop (PreCut)
//   - File spans cutBlock → drop (Straddle — ValidateForkDatadir
//     guards startup against these)
//   - File entirely strictly after cutBlock → keep (PostCut)
//   - Non-range files (salt-*.txt, etc.) → keep (chain-wide, no lineage)
//   - Unparseable names → drop (defensive; an honest publisher's
//     inventory shouldn't contain unparseable names, but a bad entry
//     should not silently pass into a signed manifest)
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

	keepName := func(name string) bool {
		entry := classify(name, cutBlock, stepToBlock)
		switch entry.Classification {
		case CopyPostCut, CopyUnknown:
			return true
		default:
			return false
		}
	}

	// Block files (flat map name→hash).
	for name := range manifest.Blocks {
		if !keepName(name) {
			delete(manifest.Blocks, name)
		}
	}

	// Meta + Salt: chain-wide, no lineage. Classifier routes them to
	// CopyUnknown (parsed but with no From/To range) so they survive
	// the keepName check above — no special-case needed. State this
	// explicitly so a future refactor that changes the classifier
	// doesn't accidentally drop chain-wide config without noticing.
	for name := range manifest.Meta {
		if !keepName(name) {
			delete(manifest.Meta, name)
		}
	}
	for name := range manifest.Salt {
		if !keepName(name) {
			delete(manifest.Salt, name)
		}
	}

	// Caplin: post-cut filter, same classifier.
	kept := manifest.Caplin[:0]
	for _, f := range manifest.Caplin {
		if keepName(f.Name) {
			kept = append(kept, f)
		}
	}
	manifest.Caplin = kept

	// Domain files: filter the inner Files slice per-domain. A domain
	// whose Files becomes empty is dropped from the Domains map so
	// the manifest doesn't claim coverage it can't back.
	for domain, dm := range manifest.Domains {
		if dm == nil {
			continue
		}
		keptFiles := dm.Files[:0]
		for _, f := range dm.Files {
			if keepName(f.Name) {
				keptFiles = append(keptFiles, f)
			}
		}
		dm.Files = keptFiles
		if len(dm.Files) == 0 {
			delete(manifest.Domains, domain)
		}
	}
}
