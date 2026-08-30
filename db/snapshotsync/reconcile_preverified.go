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
	"github.com/erigontech/erigon/db/snaptype"
)

// DownloadRequestLite carries the name + hash of a single preverified
// entry the reconciliation pass found missing on disk. The plain shape
// avoids a circular import with db/services.DownloadRequest while still
// carrying what RequestSnapshotsDownload needs.
type DownloadRequestLite struct {
	Name string
	Hash string
}

// ReconcilePreverifiedAgainstDisk reports preverified entries that are
// neither present on disk nor subsumed by a locally-held wider file —
// post-bootstrap, locally-produced files supersede the preverified
// ranges they cover so reconcile cannot undo a local merge by
// re-pulling the publisher's narrower chunks.
func ReconcilePreverifiedAgainstDisk(items snapcfg.PreverifiedItems, snapDir string) []DownloadRequestLite {
	if len(items) == 0 {
		return nil
	}
	cov := snaptype.BuildLocalCoverageIndex(snapDir)
	missing := make([]DownloadRequestLite, 0, 8)
	for _, p := range items {
		if cov.Covers(p.Name) {
			continue
		}
		path := filepath.Join(snapDir, p.Name)
		if _, err := os.Stat(path); err != nil && os.IsNotExist(err) {
			missing = append(missing, DownloadRequestLite{Name: p.Name, Hash: p.Hash})
		}
	}
	return missing
}

// FilterPreverifiedBySubsumingLocal drops entries whose [From, To)
// range is fully contained by a locally-held wider file of the same
// class. Complements ReconcilePreverifiedAgainstDisk for call sites
// (headerchain OtterSync + Branch B request-building) that need the
// filter applied to the input list rather than the missing-entry
// list. snapDir=="" makes the filter a pass-through.
func FilterPreverifiedBySubsumingLocal(items snapcfg.PreverifiedItems, snapDir string) snapcfg.PreverifiedItems {
	if snapDir == "" || len(items) == 0 {
		return items
	}
	cov := snaptype.BuildLocalCoverageIndex(snapDir)
	out := make(snapcfg.PreverifiedItems, 0, len(items))
	for _, p := range items {
		if cov.Covers(p.Name) {
			continue
		}
		out = append(out, p)
	}
	return out
}
