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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// UnwindTestDeps is the minimum surface Provider.Unwind needs to run
// against a real DB + aggregator + block reader.
//
// Compared to the full Deps the production path passes through
// Initialize, this skips: file-change callback wiring, lifecycle
// driver, bootstrap-from-preverified, validator chain, fork datadir
// guards, OnFilesChange / republish callbacks tied to a downloader.
// Provider.Unwind reads its inputs directly from these fields, so the
// rest of Initialize's setup isn't on the critical path.
//
// Used exclusively by tests that exercise the admin SetHead mode-B
// path end-to-end — see rpc/jsonrpc/debug_api_set_head_e2e_test.go.
type UnwindTestDeps struct {
	ChainDB     kv.TemporalRwDB
	BlockReader *freezeblocks.BlockReader
	Aggregator  StateAggregator
	ChainConfig *chain.Config

	// Inventory may be nil — then snapshot-trim sub-op is a no-op (no
	// file removal). For tests that want to verify file-removal
	// behaviour, pass a populated *snapshot.Inventory.
	Inventory *snapshot.Inventory

	// SnapDir is the directory holding snapshot files. Required when
	// Inventory is non-nil so snapshot-trim can filesystem-delete the
	// trimmed files.
	SnapDir string

	// SnapTmpDir is the scratch directory passed to seg writers during
	// boundary-step file regeneration (production uses config.Dirs.Tmp).
	// Required for mode-B unwinds that hit the regen path; an empty value
	// makes the seg compressor panic on an empty mkdir path.
	SnapTmpDir string

	Logger log.Logger
}

// BuildInventoryFromSnapDirForTest scans snapDir + its standard
// subdirs (domain/, history/, idx/, accessor/, caplin/) for state-
// domain and block snapshot files, and returns a fresh Inventory
// populated with one entry per discovered primary file. Mirrors the
// production lifecycle driver's discoverNewFiles path but as a
// one-shot snapshot for tests.
//
// Used by tests that exercise Provider.Unwind's snapshot-trim sub-op
// — without a populated Inventory, snapshot-trim has nothing to
// iterate and the over-step files stay on disk, leaving the
// commitment anchor verification failing on the over-step file's
// internal blockNum.
//
// Empty snapDir / missing subdirs are silent — the caller's
// fixture may have produced fewer dirs than the standard layout.
func BuildInventoryFromSnapDirForTest(snapDir string) *snapshot.Inventory {
	inv := snapshot.NewInventory()
	if snapDir == "" {
		return inv
	}
	type scanDir struct{ path, prefix string }
	dirs := []scanDir{
		{snapDir, ""},
		{filepath.Join(snapDir, "domain"), "domain"},
		{filepath.Join(snapDir, "history"), "history"},
		{filepath.Join(snapDir, "idx"), "idx"},
		{filepath.Join(snapDir, "accessor"), "accessor"},
		{filepath.Join(snapDir, "caplin"), "caplin"},
	}
	for _, sd := range dirs {
		entries, err := os.ReadDir(sd.path)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			ext := strings.ToLower(filepath.Ext(e.Name()))
			switch ext {
			case ".kv", ".seg", ".v", ".ef":
				// Primary files only. Index/accessor files (.idx,
				// .kvi, .bt, etc.) are tracked indirectly via
				// Dependencies — snapshot-trim removes them
				// alongside the primary.
			default:
				continue
			}
			name := e.Name()
			if sd.prefix != "" {
				name = sd.prefix + "/" + e.Name()
			}
			entry := &snapshot.FileEntry{
				Name:         name,
				Local:        true,
				Advertisable: true,
			}
			snapshot.PopulateFromName(entry)
			_ = inv.AddFile(entry)
		}
	}
	return inv
}

// NewProviderForUnwindTest builds a Provider with exactly the fields
// Provider.Unwind reads. This is a test helper — it bypasses
// Provider.Initialize's heavier setup so tests can exercise the
// admin-unwind path without the full snapshot-flow infrastructure.
//
// The returned Provider:
//
//   - Reports BlockAligned() == true (mode B is engagable).
//   - Has nil downloaderClient + nil republishChainToml (file removal
//     stops at the filesystem; no torrent / manifest republish).
//   - Has the supplied Inventory (may be nil, in which case
//     snapshot-trim returns zero removed files).
//
// Do NOT use this outside tests — Initialize wires production
// invariants (file-change callbacks, lifecycle driver, validator
// chain) that this constructor intentionally omits.
func NewProviderForUnwindTest(deps UnwindTestDeps) *Provider {
	return &Provider{
		ChainDB:                deps.ChainDB,
		BlockReader:            deps.BlockReader,
		Aggregator:             deps.Aggregator,
		ChainConfig:            deps.ChainConfig,
		Inventory:              deps.Inventory,
		blockAlignedBoundaries: true,
		snapDir:                deps.SnapDir,
		snapTmpDir:             deps.SnapTmpDir,
		logger:                 deps.Logger,
	}
}
