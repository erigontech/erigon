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

package snapshot

import (
	"os"
	"path/filepath"
	"slices"
)

// accessorExtsByPrimary maps a primary's extension to the extensions of the
// accessors built from its bytes. A coordinate also groups primaries that are
// independent of each other — a domain's .kv, its history .v and its index .ef
// all carry the same (type, from, to) — so the extension is what separates
// "built from these bytes" from "covers the same range".
var accessorExtsByPrimary = map[string][]string{
	".kv":  {".kvi", ".bt", ".kvei"},
	".v":   {".vi"},
	".ef":  {".efi"},
	".seg": {".idx"},
}

// AccessorsOf returns the candidates that are accessors of primary: same
// coordinate, and an extension built from that primary's kind. Set members
// carry independent version prefixes (v2.2-commitment.330-331.kv pairs with
// v2.1-commitment.330-331.kvi), which is why they are matched on coordinate
// rather than by string surgery on the name.
func AccessorsOf(primary string, candidates []string) []string {
	exts, ok := accessorExtsByPrimary[filepath.Ext(primary)]
	if !ok {
		return nil
	}
	coord, ok := CoordinateOf(primary)
	if !ok {
		return nil
	}
	var out []string
	for _, c := range candidates {
		if !slices.Contains(exts, filepath.Ext(c)) {
			continue
		}
		if cc, ok := CoordinateOf(c); ok && cc == coord {
			out = append(out, c)
		}
	}
	return out
}

// AccessorsOnDisk returns the snap-dir-relative names of the accessors present
// under snapDir that were built from primary. A set member's version prefix is
// independent of the primary's, so members cannot be named outright — the
// directories that can hold them are read and matched on coordinate instead.
func AccessorsOnDisk(snapDir, primary string) []string {
	if _, ok := accessorExtsByPrimary[filepath.Ext(primary)]; !ok {
		return nil
	}
	var candidates []string
	for _, sub := range []string{"", "domain", "history", "idx", "accessor"} {
		entries, err := os.ReadDir(filepath.Join(snapDir, sub))
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			candidates = append(candidates, filepath.ToSlash(filepath.Join(sub, e.Name())))
		}
	}
	return AccessorsOf(primary, candidates)
}
