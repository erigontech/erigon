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

	snapshotinv "github.com/erigontech/erigon/node/components/storage/snapshot"
)

// FilterManifestByAtomicSet drops every entry sharing a coordinate with an
// entry the torrent client cannot serve, so a coordinate is advertised whole
// or not at all.
//
// A file rebuilt in place gets a new info-hash and leaves the old one
// unservable. FilterManifestByServable removes that one entry; advertising
// the rest of its coordinate would let a consumer pair a primary with an
// accessor built from a different generation, whose offsets index past the
// end of the file it is paired with. This is the publish-side counterpart of
// the accessor check that gates a file's entry into the node's visible set.
//
// A nil servable set disables the gate, matching FilterManifestByServable.
// Returns the number of entries dropped.
func FilterManifestByAtomicSet(manifest *ChainTomlV2, servable map[metainfo.Hash]struct{}) int {
	if manifest == nil || servable == nil {
		return 0
	}
	hexSet := make(map[string]struct{}, len(servable))
	for h := range servable {
		hexSet[h.HexString()] = struct{}{}
	}

	broken := make(map[snapshotinv.Coordinate]struct{})
	note := func(name, hash string) {
		key, ok := snapshotinv.CoordinateOf(name)
		if !ok {
			return
		}
		if _, servable := hexSet[strings.ToLower(hash)]; !servable {
			broken[key] = struct{}{}
		}
	}
	for _, dm := range manifest.Domains {
		if dm == nil {
			continue
		}
		for _, f := range dm.Files {
			note(f.Name, f.Hash)
		}
	}
	for _, b := range manifest.Blocks {
		note(b.Name, b.Hash)
	}
	if len(broken) == 0 {
		return 0
	}

	isBroken := func(name string) bool {
		key, ok := snapshotinv.CoordinateOf(name)
		if !ok {
			return false
		}
		_, bad := broken[key]
		return bad
	}

	dropped := 0
	for _, dm := range manifest.Domains {
		if dm == nil {
			continue
		}
		kept := dm.Files[:0]
		for _, f := range dm.Files {
			if isBroken(f.Name) {
				dropped++
				continue
			}
			kept = append(kept, f)
		}
		dm.Files = kept
	}
	keptBlocks := manifest.Blocks[:0]
	for _, b := range manifest.Blocks {
		if isBroken(b.Name) {
			dropped++
			continue
		}
		keptBlocks = append(keptBlocks, b)
	}
	manifest.Blocks = keptBlocks

	return dropped
}
