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

	Logger log.Logger
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
		logger:                 deps.Logger,
	}
}
