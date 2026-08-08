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

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
)

// block .seg files come in triples: headers / bodies / transactions.
// A missing member means retire aborted mid-build (e.g. transactions
// index refused because bodies had a phantom-tail past head — the
// DumpBlocks ErrRangeAheadOfHead class). Left in place, the surviving
// members confuse Phase 5 disk-clean and can seed a wider merge with
// inconsistent inputs.
var blockTripleKinds = []string{"headers", "bodies", "transactions"}

// sweepIncompleteBlockTriples walks snapDir once at bootstrap. For
// every (version, from, to) range that has at least one block .seg
// member on disk but is missing another, it removes every present
// member of the triple plus its .torrent sidecar and any accessor
// sidecars (.idx / .idx.torrent / .seg.idx). Returns the removed
// primary names.
//
// Safe to call before Inventory is populated — operates on files only.
// Called from Provider bootstrap alongside probeBootstrapDomainFiles.
func sweepIncompleteBlockTriples(snapDir string, logger log.Logger) []string {
	if snapDir == "" {
		return nil
	}
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		return nil
	}

	// Group .seg files by their (version-from-to) prefix — the shared
	// key across a triple. E.g. "v1.1-003230-003240" carries three
	// members named "<prefix>-headers.seg", "<prefix>-bodies.seg",
	// "<prefix>-transactions.seg".
	type triple struct {
		prefix  string
		members map[string]string // kind → full name
	}
	groups := map[string]*triple{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".seg") {
			continue
		}
		prefix, kind, ok := parseBlockSegName(name)
		if !ok {
			continue
		}
		g, seen := groups[prefix]
		if !seen {
			g = &triple{prefix: prefix, members: map[string]string{}}
			groups[prefix] = g
		}
		g.members[kind] = name
	}

	var removed []string
	for _, g := range groups {
		if len(g.members) == len(blockTripleKinds) {
			continue
		}
		for _, name := range g.members {
			removeBlockMember(snapDir, name)
			removed = append(removed, name)
		}
	}
	if len(removed) > 0 && logger != nil {
		logger.Warn("[storage] boot sweep: removed incomplete block .seg triples", "count", len(removed))
	}
	return removed
}

// parseBlockSegName returns the shared prefix and the kind for a
// block .seg name. Recognizes names of the shape:
//
//	<version>-<from-hex>-<to-hex>-<kind>.seg
//
// where kind is one of blockTripleKinds. Returns ok=false for state
// files, caplin snapshots, or unrecognized layouts.
func parseBlockSegName(name string) (prefix, kind string, ok bool) {
	stem := strings.TrimSuffix(name, ".seg")
	for _, k := range blockTripleKinds {
		suffix := "-" + k
		if !strings.HasSuffix(stem, suffix) {
			continue
		}
		return strings.TrimSuffix(stem, suffix), k, true
	}
	return "", "", false
}

// removeBlockMember unlinks a .seg member plus its .torrent sidecar
// and any accessor sidecars (.idx / .idx.torrent) that share its
// stem. Errors are swallowed — this runs during bootstrap where
// individual failures shouldn't block startup.
func removeBlockMember(snapDir, name string) {
	base := filepath.Join(snapDir, name)
	stem := strings.TrimSuffix(base, ".seg")
	for _, path := range []string{
		base,
		base + ".torrent",
		stem + ".idx",
		stem + ".idx.torrent",
		stem + "-to-block.idx",
		stem + "-to-block.idx.torrent",
	} {
		_ = dir.RemoveFile(path)
	}
}
