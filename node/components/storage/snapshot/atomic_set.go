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

	"github.com/erigontech/erigon/db/snaptype"
	// Registers the block snap types, whose declared index names are what
	// accessorTypesOf asks for.
	_ "github.com/erigontech/erigon/db/snaptype2"
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
	types := accessorTypesOf(coord.Type)
	var out []string
	for _, c := range candidates {
		if !slices.Contains(exts, filepath.Ext(c)) {
			continue
		}
		cc, ok := CoordinateOf(c)
		if !ok || cc.From != coord.From || cc.To != coord.To {
			continue
		}
		if slices.Contains(types, cc.Type) {
			out = append(out, c)
		}
	}
	return out
}

// accessorTypesOf names the coordinate types an accessor of this primary can
// carry. A segment's indexes are named after the index rather than the segment,
// so a transactions segment owns both transactions.idx and
// transactions-to-block.idx, and the declaring type is what lists them.
// Everything else's accessors share the primary's own name.
func accessorTypesOf(coordType string) []string {
	t, ok := snaptype.ParseFileType(coordType)
	if !ok {
		return []string{coordType}
	}
	types := []string{coordType}
	for _, idx := range t.Indexes() {
		if !slices.Contains(types, idx.Name) {
			types = append(types, idx.Name)
		}
	}
	return types
}

// HasAccessors reports whether a file of this name is a primary that accessors
// are built from.
func HasAccessors(name string) bool {
	_, ok := accessorExtsByPrimary[filepath.Ext(name)]
	return ok
}

// AccessorsOnDisk returns the snap-dir-relative names of the accessors present
// under snapDir that were built from primary. A set member's version prefix is
// independent of the primary's, so members cannot be named outright — the
// directories that can hold them are read and matched on coordinate instead.
//
// Answering for many primaries at once costs a directory read and a name parse
// per candidate each time; use AccessorIndex for that.
func AccessorsOnDisk(snapDir, primary string) []string {
	if _, ok := accessorExtsByPrimary[filepath.Ext(primary)]; !ok {
		return nil
	}
	return AccessorsOf(primary, snapDirCandidates(snapDir))
}

func snapDirCandidates(snapDir string) []string {
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
	return candidates
}

// AccessorIndex answers AccessorsOnDisk's question for many primaries from one
// reading of the snapshot directories. It is a snapshot of the disk at the
// moment it was built, so callers rebuild it as often as they need the answer
// to be fresh.
type AccessorIndex struct {
	byCoord map[Coordinate][]string
}

func NewAccessorIndex(snapDir string) *AccessorIndex {
	ix := &AccessorIndex{byCoord: map[Coordinate][]string{}}
	for _, name := range snapDirCandidates(snapDir) {
		ext := filepath.Ext(name)
		if !isAccessorExt(ext) {
			continue
		}
		if coord, ok := CoordinateOf(name); ok {
			ix.byCoord[coord] = append(ix.byCoord[coord], ext)
		}
	}
	return ix
}

// Has reports whether an accessor built from primary is on disk.
func (ix *AccessorIndex) Has(primary string) bool {
	exts, ok := accessorExtsByPrimary[filepath.Ext(primary)]
	if !ok {
		return false
	}
	coord, ok := CoordinateOf(primary)
	if !ok {
		return false
	}
	for _, present := range ix.byCoord[coord] {
		if slices.Contains(exts, present) {
			return true
		}
	}
	return false
}

func isAccessorExt(ext string) bool {
	for _, exts := range accessorExtsByPrimary {
		if slices.Contains(exts, ext) {
			return true
		}
	}
	return false
}
