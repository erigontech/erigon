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
	"sort"
	"strings"

	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
)

// HeldRange is a single advertised range of a particular file kind.
// From is inclusive, To is exclusive — matching snaptype.FileInfo
// semantics. For block files, units are block numbers (the K-units
// in filenames are converted to block numbers by ParseFileName).
// For state-domain files, units are steps (step number directly).
type HeldRange struct {
	From, To uint64
}

// HeldRanges returns the sparse set of ranges a manifest advertises
// for a given file kind. Returned ranges are sorted by From ascending.
//
// IMPORTANT: ranges are NOT merged. A manifest advertising {[A,B),
// [C,D)} stays distinct from a manifest advertising {[A,D)} — even
// if B==C. This is intentional per the three-layer model: per-node
// advertisements are sparse by construction
// (docs/plans/20260515-three-layer-snapshot-distribution.md), and
// the consumer fetch planner needs to see which files exist as
// separate units to choose seeders correctly.
//
// kind is matched against snaptype.FileInfo.TypeString:
//   - "headers", "bodies", "transactions" for block files
//   - "commitment", "accounts", "storage", "code", "receipt" for state
//
// CL data (caplin/ prefix, beaconblocks, blobsidecars) is skipped so
// callers don't accidentally receive CL ranges when asking for an EL
// block-file kind.
//
// Adjacent .seg / .idx / .torrent entries collapse to a single range
// (we return one range per (From, To) tuple regardless of extension).
// This is what callers want: "is the range covered?" not "how many
// torrent files cover it?"
func HeldRanges(items snapcfg.PreverifiedItems, kind string) []HeldRange {
	if kind == "" {
		return nil
	}

	// Deduplicate by (From, To) — multiple file extensions per range
	// collapse to one entry. Map key is encoded as From<<32|To which
	// is safe for any realistic block/step number on a 64-bit system.
	seen := make(map[[2]uint64]struct{})

	for _, p := range items {
		if isCLData(p.Name) {
			continue
		}

		info, isStateFile, ok := snaptype.ParseFileName("", p.Name)
		if !ok {
			continue
		}

		if isStateFile {
			// State files: TypeString is the domain name. Accept
			// only .kv as the primary data file — .kvi, .bt, .vi
			// are accessor/index files that share the same range
			// and would double-count if included.
			if info.TypeString != kind || !strings.HasSuffix(p.Name, ".kv") {
				continue
			}
		} else {
			// Block files: TypeString is the kind (headers / bodies /
			// transactions). Accept only .seg — .idx / .idx.torrent
			// would double-count.
			if info.TypeString != kind || !strings.HasSuffix(p.Name, ".seg") {
				continue
			}
		}

		seen[[2]uint64{info.From, info.To}] = struct{}{}
	}

	if len(seen) == 0 {
		return nil
	}
	out := make([]HeldRange, 0, len(seen))
	for k := range seen {
		out = append(out, HeldRange{From: k[0], To: k[1]})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].From != out[j].From {
			return out[i].From < out[j].From
		}
		return out[i].To < out[j].To
	})
	return out
}
