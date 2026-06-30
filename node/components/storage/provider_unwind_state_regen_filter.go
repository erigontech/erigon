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

package storage

import "github.com/erigontech/erigon/node/components/storage/snapshot"

// localKVRanges narrows an Inventory's per-domain entries to the .kv
// files actually present on disk locally. Inventory carries both
// Local=true entries (this node's own files) and Local=false entries
// (state advertised in peer chain.toml.v2 manifests but not yet
// downloaded). Mode-B regen / removal touches disk via os.Rename /
// dir.RemoveFile, so a non-local entry would resolve to a path that
// doesn't exist — the rename then fails noisily and the file the
// planner intended to act on stays untouched.
//
// Returned slices are parallel: ranges[i] is the step range of
// files[i].
func localKVRanges(entries []*snapshot.FileEntry) ([]*snapshot.FileEntry, []stateFileRange) {
	files := make([]*snapshot.FileEntry, 0, len(entries))
	ranges := make([]stateFileRange, 0, len(entries))
	for _, e := range entries {
		if e == nil || e.Kind != snapshot.KindKV || !e.Local {
			continue
		}
		files = append(files, e)
		ranges = append(ranges, stateFileRange{FromStep: e.FromStep, ToStep: e.ToStep})
	}
	return files, ranges
}
