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

// ReconcilePreverifiedAgainstDisk walks the preverified item list and
// reports every entry whose .seg / .idx / sidecar file is missing from
// snapDir AND not subsumed by a locally-held wider file. Returned
// entries are the input for a download retry — the caller (typically
// SyncSnapshots' local-preverified branch) feeds them to
// RequestSnapshotsDownload to fill the gap.
//
// Rationale: preverified.toml is authoritative at boot, but ONLY at
// boot. Once the local node has produced files (forward-exec collation
// + merge), those local files supersede the preverified entries they
// cover. Without this distinction, a reconcile after a Mode-B trim
// would re-download every preverified narrow chunk that the local
// merge has since consolidated into a wider chunk — undoing the merge
// and leaving both narrow and wide files on disk, where the aggregator
// visibility set serves reads from either, manifesting as wrong-trie-
// root ~209 blocks past the unwind target.
//
// Coverage check: a preverified entry with (subdir, version, type, ext,
// [From, To)) is considered already covered iff a local file in the
// SAME (subdir, version, type, ext) bucket has [LocalFrom, LocalTo)
// with LocalFrom ≤ From AND LocalTo ≥ To. Identity covers identity, so
// the original "exact file missing" check is preserved.
//
// The function only checks file existence; it does NOT validate
// content. Hash verification happens at the downloader layer, against
// the preverified hash, when the file lands.
//
// snapDir is the absolute path of the snapshot directory. Entries with
// no corresponding file there AND no covering wider local file are
// reported as missing.
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

// coverageKey identifies a class of files where a wider [From, To)
// range subsumes narrower ranges of the same class. Includes subdir
// so domain/v2.0-accounts.* does not match history/v2.0-accounts.*.
type coverageKey struct {
	subdir  string
	version string
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
			version: fi.Version.String(),
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
		version: fi.Version.String(),
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
