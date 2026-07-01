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
	"io/fs"
	"os"
	"path/filepath"
	"strings"

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
	cov := buildLocalCoverageIndex(snapDir)
	missing := make([]DownloadRequestLite, 0, 8)
	for _, p := range items {
		if cov.covers(p.Name) {
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
	cov := buildLocalCoverageIndex(snapDir)
	out := make(snapcfg.PreverifiedItems, 0, len(items))
	for _, p := range items {
		if cov.covers(p.Name) {
			continue
		}
		out = append(out, p)
	}
	return out
}

// coverageKey identifies a class of files where a wider [From, To)
// range subsumes narrower ranges of the same class. Includes subdir
// so domain/v2.0-accounts.* does not match history/v2.0-accounts.*.
//
// Version is NOT part of the key: preverified.toml carries entries at
// multiple version generations during a version bump (e.g. v2.0
// narrows plus v2.1 broad for the same block range). A wider file of
// any version already covers the data of narrower files at any
// version — pulling the narrower ones just lands cross-version union-
// cover on disk. Live-caught 2026-07-01 on hoodi commitment domain:
// v2.1-commitment.272-280.kv + v2.0-commitment.{272-276,276-278,278-
// 279}.kv coexisted after the bootstrap+preverified fallback pulled
// both, producing gas-short first post-unwind blocks after mode-B at
// depth 30k+.
type coverageKey struct {
	subdir  string
	typeStr string
	ext     string
}

type stepRange struct{ from, to uint64 }

type localCoverageIndex map[coverageKey][]stepRange

func buildLocalCoverageIndex(snapDir string) localCoverageIndex {
	cov := localCoverageIndex{}
	if snapDir == "" {
		return cov
	}
	_ = filepath.WalkDir(snapDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return nil //nolint:nilerr // WalkDir errors are surfaced via the next iteration; treat unstattable entries as absent.
		}
		name := filepath.Base(p)
		// Sidecars are accounted by their primary file; skip to avoid
		// double-counting (a present .torrent without .kv is not
		// authoritative coverage).
		if strings.HasSuffix(name, ".torrent") {
			return nil
		}
		fi, _, ok := snaptype.ParseFileName("", name)
		if !ok || fi.TypeString == "" || fi.From >= fi.To {
			return nil
		}
		rel, _ := filepath.Rel(snapDir, p)
		subdir := filepath.Dir(rel)
		if subdir == "." {
			subdir = ""
		}
		k := coverageKey{
			subdir:  subdir,
			typeStr: fi.TypeString,
			ext:     fi.Ext,
		}
		cov[k] = append(cov[k], stepRange{from: fi.From, to: fi.To})
		return nil
	})
	return cov
}

// covers reports whether some local file of the same class fully
// contains the preverified entry's [From, To).
func (cov localCoverageIndex) covers(entryName string) bool {
	base := filepath.Base(entryName)
	fi, _, ok := snaptype.ParseFileName("", base)
	if !ok || fi.TypeString == "" || fi.From >= fi.To {
		return false
	}
	subdir := filepath.Dir(entryName)
	if subdir == "." {
		subdir = ""
	}
	k := coverageKey{
		subdir:  subdir,
		typeStr: fi.TypeString,
		ext:     fi.Ext,
	}
	for _, lr := range cov[k] {
		if lr.from <= fi.From && lr.to >= fi.To {
			return true
		}
	}
	return false
}
