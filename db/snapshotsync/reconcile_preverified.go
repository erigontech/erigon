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

package snapshotsync

import (
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/db/snapcfg"
)

// DownloadRequestLite carries the name + hash of a single preverified
// entry the reconciliation pass found missing on disk. The plain shape
// avoids a circular import with db/services.DownloadRequest while still
// carrying what RequestSnapshotsDownload needs.
type DownloadRequestLite struct {
	Name string
	Hash string
}

// ReconcilePreverifiedAgainstDisk walks the preverified item list and
// reports every entry whose .seg / .idx / sidecar file is missing from
// snapDir. Returned entries are the input for a download retry — the
// caller (typically SyncSnapshots' local-preverified branch) feeds
// them to RequestSnapshotsDownload to fill the gap.
//
// Rationale: preverified.toml is the authoritative list of files the
// node must hold. The legacy "skipping SyncSnapshots, local
// preverified" branch trusted the manifest verbatim and never
// cross-checked against disk; a silently-failed download (one entry
// missing) survived bootstrap and surfaced later as a wedge when the
// EL's chain advanced past the gap. See the live failure on hoodi
// 2026-06-08: preverified.toml advertised
// v1.1-002973-002974-{headers,bodies,transactions}.seg but the .seg
// files were never on disk; DownloadHistoricalBlocks used a
// manifest-derived block_tip that overstated local state and Caplin's
// bulk-download walked past the gap.
//
// The function only checks file existence; it does NOT validate
// content. Hash verification happens at the downloader layer, against
// the preverified hash, when the file lands.
//
// snapDir is the absolute path of the snapshot directory. Entries with
// no corresponding file there are reported as missing.
func ReconcilePreverifiedAgainstDisk(items snapcfg.PreverifiedItems, snapDir string) []DownloadRequestLite {
	if len(items) == 0 {
		return nil
	}
	missing := make([]DownloadRequestLite, 0, 8)
	for _, p := range items {
		path := filepath.Join(snapDir, p.Name)
		if _, err := os.Stat(path); err != nil && os.IsNotExist(err) {
			missing = append(missing, DownloadRequestLite{Name: p.Name, Hash: p.Hash})
		}
	}
	return missing
}
