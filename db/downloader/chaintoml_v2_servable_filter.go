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
	"strings"

	"github.com/anacrolix/torrent/metainfo"
)

// FilterManifestByServable drops every manifest entry whose info-hash
// the torrent client is not currently able to serve — the V2 half of
// the "validate before advertise" rule GenerateChainToml applies to V1
// via its servable set.
//
// The inventory is add-only with respect to file deletion: nothing
// reconciles entries against disk when an unwind, a discard, or an
// adoption cutover unlinks a file or drops its torrent. Without this
// gate those entries keep their cached TorrentHash and every later
// generation advertises a hash no peer can fetch.
//
// A nil servable set disables the gate (callers with no torrent
// client — tests, the free publish helper). An entry whose hash does
// not parse is dropped: it cannot be matched against the set, so it
// cannot be served either.
//
// Returns the number of entries dropped so callers can surface a gate
// that is removing more than expected.
func FilterManifestByServable(manifest *ChainTomlV2, servable map[metainfo.Hash]struct{}) int {
	if manifest == nil || servable == nil {
		return 0
	}
	hexSet := make(map[string]struct{}, len(servable))
	for h := range servable {
		hexSet[h.HexString()] = struct{}{}
	}
	canServe := func(hash string) bool {
		_, ok := hexSet[strings.ToLower(hash)]
		return ok
	}

	dropped := 0
	keptBlocks := manifest.Blocks[:0]
	for _, b := range manifest.Blocks {
		if !canServe(b.Hash) {
			dropped++
			continue
		}
		keptBlocks = append(keptBlocks, b)
	}
	manifest.Blocks = keptBlocks

	for _, m := range []map[string]string{manifest.Meta, manifest.Salt} {
		for name, hash := range m {
			if !canServe(hash) {
				delete(m, name)
				dropped++
			}
		}
	}

	keptCaplin := manifest.Caplin[:0]
	for _, c := range manifest.Caplin {
		if !canServe(c.Hash) {
			dropped++
			continue
		}
		keptCaplin = append(keptCaplin, c)
	}
	manifest.Caplin = keptCaplin

	for domain, dm := range manifest.Domains {
		if dm == nil {
			continue
		}
		kept := dm.Files[:0]
		for _, f := range dm.Files {
			if !canServe(f.Hash) {
				dropped++
				continue
			}
			kept = append(kept, f)
		}
		dm.Files = kept
		if len(dm.Files) == 0 {
			delete(manifest.Domains, domain)
		}
	}
	return dropped
}
