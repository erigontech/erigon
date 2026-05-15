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
	"strings"

	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
)

// ManifestTips holds the two derived tips of a snapshot manifest.
//
// Both fields are content-derived from the manifest's item list — there
// are no explicit "tip" fields in the chain.toml schema. See
// docs/plans/20260515-three-layer-snapshot-distribution.md for the
// rationale (derive don't add; single source of truth).
type ManifestTips struct {
	// BlockTip is the highest block number for which the manifest
	// advertises all three of (headers, bodies, transactions). Returned
	// inclusive: blocks 0..BlockTip are present in the advertised set.
	//
	// Computed as min(max headers.To, max bodies.To, max transactions.To)
	// minus 1 to make the bound inclusive. snaptype.ParseFileName
	// converts filename-K-units to block numbers internally (see
	// db/snaptype/files.go:214), so the values we compare are already
	// in block-number units. When any of the three types is absent
	// from the manifest, BlockTip is 0.
	//
	// "min of maxes" because executing block N requires ALL three
	// types present for block N; the tip is the highest block for
	// which the complete triple exists.
	BlockTip uint64

	// StateTipStep is the highest step end across commitment.kv
	// entries — the canonical state-domain anchor in the chain.toml.
	// Returned as a step number in the unit used by the manifest's
	// filenames (e.g., commitment.8704-8960.kv → 8960).
	//
	// Translation step→block requires runtime state (txnum-to-block
	// index via BlockReader or Aggregator) and is intentionally NOT
	// done here. The helper stays pure.
	StateTipStep uint64
}

// DeriveManifestTips computes BlockTip and StateTipStep from a manifest
// (canonical chain.toml or per-node advertisement — same function, same
// derivation, different inputs).
//
// Per the three-layer model
// (docs/plans/20260515-three-layer-snapshot-distribution.md), this
// derivation is pure: no merging of inputs, no swarm-agreement state,
// no per-node inventory. Pass a single manifest, get its tips.
//
// CL data (caplin/ prefix, beaconblocks, blobsidecars) is not relevant
// for either tip and is skipped silently. Pre-state-domain entries
// like salt, erigondb.toml, accessor/, history/, idx/ are also skipped.
func DeriveManifestTips(items snapcfg.PreverifiedItems) ManifestTips {
	var maxHeaders, maxBodies, maxTxs uint64
	var maxCommitStep uint64

	for _, p := range items {
		// Skip CL data — not part of the EL block/state tip
		// computation. Even on consumer-side this category is filtered
		// out earlier in the bootstrap path, but the tip computation
		// must be robust to receiving an unfiltered list.
		if isCLData(p.Name) {
			continue
		}

		// snaptype.ParseFileName handles the full filename grammar
		// and the K-unit-to-block conversion internally. Second return
		// is true when the parsed name is a state-domain file.
		info, isStateFile, ok := snaptype.ParseFileName("", p.Name)
		if !ok {
			continue
		}

		if isStateFile {
			// Only commitment is the canonical state anchor —
			// accounts/storage/code/receipt step-end can lag commitment
			// by one step during retire, so they're not authoritative
			// for state-tip purposes.
			if info.TypeString == "commitment" &&
				strings.HasSuffix(p.Name, ".kv") {
				if info.To > maxCommitStep {
					maxCommitStep = info.To
				}
			}
			continue
		}

		// Block-file branch. We track .seg files only — .idx/.torrent
		// would double-count and the tip is defined by data files.
		if !strings.HasSuffix(p.Name, ".seg") {
			continue
		}
		switch info.TypeString {
		case "headers":
			if info.To > maxHeaders {
				maxHeaders = info.To
			}
		case "bodies":
			if info.To > maxBodies {
				maxBodies = info.To
			}
		case "transactions":
			if info.To > maxTxs {
				maxTxs = info.To
			}
		}
	}

	var blockTip uint64
	if maxHeaders > 0 && maxBodies > 0 && maxTxs > 0 {
		// All three types present. Take the minimum max-To across
		// types so the tip is the highest block for which all three
		// files exist. ParseFileName already converted from filename-
		// K-units to block numbers; subtract 1 to convert exclusive
		// upper bound to inclusive tip.
		minOfMax := maxHeaders
		if maxBodies < minOfMax {
			minOfMax = maxBodies
		}
		if maxTxs < minOfMax {
			minOfMax = maxTxs
		}
		blockTip = minOfMax - 1
	}

	return ManifestTips{
		BlockTip:     blockTip,
		StateTipStep: maxCommitStep,
	}
}
