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

package manifest_exchange

import (
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// v2ToPeerManifest translates a parsed V2 manifest into the flow event
// shape the orchestrator subscribes to. Malformed hex in hash fields and
// unrecognised trust strings are tolerated: the offending entry is dropped,
// not the whole manifest.
func v2ToPeerManifest(peerID string, m *downloader.ChainTomlV2) flow.PeerManifestReceived {
	out := flow.PeerManifestReceived{PeerID: peerID}
	if m == nil {
		return out
	}

	if len(m.Domains) > 0 {
		out.Domains = make(map[snapshot.Domain][]*snapshot.FileEntry, len(m.Domains))
		for domainName, dm := range m.Domains {
			if dm == nil {
				continue
			}
			domain := snapshot.Domain(domainName)
			entries := make([]*snapshot.FileEntry, 0, len(dm.Files))
			for _, f := range dm.Files {
				hash, ok := decodeHash(f.Hash)
				if !ok {
					continue
				}
				trust, err := snapshot.ParseTrustLevel(f.Trust)
				if err != nil {
					// Treat unknown trust as the safest default — peer's
					// advertisement gets the weakest level.
					trust = snapshot.TrustNone
				}
				entries = append(entries, &snapshot.FileEntry{
					Domain:      domain,
					Name:        f.Name,
					FromStep:    f.Range[0],
					ToStep:      f.Range[1],
					TorrentHash: hash,
					Trust:       trust,
				})
			}
			if len(entries) > 0 {
				out.Domains[domain] = entries
			}
		}
	}

	if len(m.Blocks) > 0 {
		out.Blocks = make([]*snapshot.FileEntry, 0, len(m.Blocks))
		for name, hashStr := range m.Blocks {
			hash, ok := decodeHash(hashStr)
			if !ok {
				continue
			}
			// V2 Blocks is a flat map name→hash with no per-entry
			// step range field. Parse the range from the filename so
			// the orchestrator's gap-fill can compare coverage — an
			// entry with zero FromStep/ToStep is treated as trivially
			// covered (IsComplete(0, 0) is true) and would be dropped.
			from, to := parseBlockFileRange(name)
			out.Blocks = append(out.Blocks, &snapshot.FileEntry{
				Name:        name,
				TorrentHash: hash,
				FromStep:    from,
				ToStep:      to,
				// V2 blocks don't carry per-file trust; they're deterministic.
				// The orchestrator promotes to TrustVerified on DownloadComplete.
				Trust: snapshot.TrustNone,
			})
		}
	}

	return out
}

// parseBlockFileRange extracts the block-number range from a V1-era
// block snapshot filename of the form "v1.0-<from>-<to>-<role>.<ext>".
// Returns (0, 0) if the filename doesn't follow the expected shape —
// the orchestrator will then treat the entry as zero-range and
// decline to gap-fill it, which is the correct behaviour for a
// non-canonical name.
func parseBlockFileRange(name string) (uint64, uint64) {
	// Strip the extension and the role segment.
	extIdx := strings.LastIndexByte(name, '.')
	if extIdx < 0 {
		return 0, 0
	}
	base := name[:extIdx]

	// "v1.0-000000-000500-headers" → split on '-'.
	parts := strings.Split(base, "-")
	if len(parts) < 4 {
		return 0, 0
	}
	// The two numeric segments sit at [len-3] and [len-2]; the last
	// segment is the role ("headers", "bodies", etc.).
	from, err := strconv.ParseUint(parts[len(parts)-3], 10, 64)
	if err != nil {
		return 0, 0
	}
	to, err := strconv.ParseUint(parts[len(parts)-2], 10, 64)
	if err != nil {
		return 0, 0
	}
	return from, to
}

// decodeHash parses a 40-char hex torrent hash into a fixed-size array.
// Returns ok=false if the input isn't the right shape.
func decodeHash(s string) ([20]byte, bool) {
	var out [20]byte
	if len(s) != 40 {
		return out, false
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return out, false
	}
	copy(out[:], b)
	return out, true
}
