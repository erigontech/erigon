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
	"sort"

	"github.com/erigontech/erigon/db/snaptype"
)

// blocksFromMap converts a legacy `name → hash` fixture into the typed
// BlockFileEntry slice that ChainTomlV2.Blocks now expects. The range
// is derived from the filename (same path the parser uses for legacy
// `[blocks]` form). Output is sorted by Name for determinism.
func blocksFromMap(m map[string]string) []BlockFileEntry {
	if len(m) == 0 {
		return nil
	}
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]BlockFileEntry, 0, len(names))
	for _, name := range names {
		entry := BlockFileEntry{Name: name, Hash: m[name]}
		if _, from, to, ok := snaptype.ParseRange(name); ok {
			entry.Range = [2]uint64{from, to}
		}
		out = append(out, entry)
	}
	return out
}

// blockNames returns just the names from a Blocks slice, the shape
// older assertions reach for via require.Contains.
func blockNames(b []BlockFileEntry) []string {
	if len(b) == 0 {
		return nil
	}
	out := make([]string, len(b))
	for i, e := range b {
		out[i] = e.Name
	}
	return out
}

// blockHashByName looks up a block's hash by file name (test-only
// convenience for assertions that previously read a map entry).
func blockHashByName(b []BlockFileEntry, name string) string {
	for _, e := range b {
		if e.Name == name {
			return e.Hash
		}
	}
	return ""
}
