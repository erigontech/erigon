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

package flow

import "github.com/erigontech/erigon/node/components/storage/snapshot"

// staleCoordinates reports the coordinates a peer is advertising at a
// different generation from the one held locally.
//
// A primary and its accessors are built together, so a member whose hash has
// changed leaves the rest of its coordinate a generation behind. Holding a
// file by name says nothing about which generation it came from, which is what
// lets an accessor built from one primary end up paired with another.
//
// Entries that are not held, or that carry no hash, say nothing about
// generations and are left to the ordinary coverage path.
func staleCoordinates(peerEntries []*snapshot.FileEntry, held map[string][20]byte) map[snapshot.Coordinate]struct{} {
	var stale map[snapshot.Coordinate]struct{}
	for _, entry := range peerEntries {
		if entry == nil || entry.TorrentHash == ([20]byte{}) {
			continue
		}
		heldHash, ok := held[entry.Name]
		if !ok || heldHash == entry.TorrentHash {
			continue
		}
		coord, ok := snapshot.CoordinateOf(entry.Name)
		if !ok {
			continue
		}
		if stale == nil {
			stale = make(map[snapshot.Coordinate]struct{})
		}
		stale[coord] = struct{}{}
	}
	return stale
}

// coordinateIsStale reports whether name belongs to one of the stale
// coordinates.
func coordinateIsStale(stale map[snapshot.Coordinate]struct{}, name string) bool {
	if len(stale) == 0 {
		return false
	}
	coord, ok := snapshot.CoordinateOf(name)
	if !ok {
		return false
	}
	_, bad := stale[coord]
	return bad
}
