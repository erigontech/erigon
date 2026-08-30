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

package snaptype

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// CoverageKey identifies a class of snapshot files where a wider
// [From, To) range subsumes narrower ranges of the same class. Subdir
// is part of the key so `domain/v2.0-accounts.*` does not match
// `history/v2.0-accounts.*`.
//
// Version is deliberately NOT part of the key: preverified.toml can
// carry entries at multiple version generations during a version bump
// (e.g. v2.0 narrows plus v2.1 broad for the same block range). A
// wider file of any version already covers the data of narrower files
// at any version — pulling the narrower ones just lands cross-version
// union-cover on disk. Live-caught 2026-07-01 on hoodi commitment
// domain: v2.1-commitment.272-280.kv coexisting with v2.0 narrows for
// the same range produced gas-short first post-unwind blocks after
// mode-B at depth 30k+.
type CoverageKey struct {
	Subdir  string
	TypeStr string
	Ext     string
}

// StepRange records a single file's [from, to) span. Interpretation
// depends on the file class — block .seg use raw block numbers, state
// .kv/.v/.ef use step numbers or raw txN per v4 shape.
type StepRange struct{ From, To uint64 }

// LocalCoverageIndex maps each file class to the set of on-disk
// [from, to) ranges that class covers. Callers use Covers to check
// whether a candidate external item is subsumed by a local wider file
// of the same class — the shared primitive for the
// "local-overrides-external" invariant that every chain.toml merge
// site must honour so the published manifest doesn't advertise files
// we can't serve.
type LocalCoverageIndex map[CoverageKey][]StepRange

// BuildLocalCoverageIndex scans snapDir recursively and returns an
// index of every on-disk snapshot file classified by (subdir, type,
// ext). Sidecar `.torrent` files are skipped — the primary file
// carries the authoritative range, and a lone sidecar without the
// data file is not coverage.
//
// snapDir == "" returns an empty index.
func BuildLocalCoverageIndex(snapDir string) LocalCoverageIndex {
	cov := LocalCoverageIndex{}
	if snapDir == "" {
		return cov
	}
	_ = filepath.WalkDir(snapDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return nil //nolint:nilerr // WalkDir errors surface via the next iteration; treat unstattable entries as absent.
		}
		name := filepath.Base(p)
		if strings.HasSuffix(name, ".torrent") {
			return nil
		}
		fi, _, ok := ParseFileName("", name)
		if !ok || fi.TypeString == "" || fi.From >= fi.To {
			return nil
		}
		rel, _ := filepath.Rel(snapDir, p)
		subdir := filepath.Dir(rel)
		if subdir == "." {
			subdir = ""
		}
		k := CoverageKey{
			Subdir:  subdir,
			TypeStr: fi.TypeString,
			Ext:     fi.Ext,
		}
		cov[k] = append(cov[k], StepRange{From: fi.From, To: fi.To})
		return nil
	})
	return cov
}

// Covers reports whether some local file of the same class fully
// contains the candidate entry's [From, To). entryName is a path
// relative to the snapDir the index was built from — e.g.
// "domain/v2.0-accounts.0-256.kv" or "v1.1-000000-000500-headers.seg".
func (cov LocalCoverageIndex) Covers(entryName string) bool {
	base := filepath.Base(entryName)
	fi, _, ok := ParseFileName("", base)
	if !ok || fi.TypeString == "" || fi.From >= fi.To {
		return false
	}
	subdir := filepath.Dir(entryName)
	if subdir == "." {
		subdir = ""
	}
	k := CoverageKey{
		Subdir:  subdir,
		TypeStr: fi.TypeString,
		Ext:     fi.Ext,
	}
	for _, lr := range cov[k] {
		if lr.From <= fi.From && lr.To >= fi.To {
			return true
		}
	}
	return false
}

// FileExists returns true when snapDir/entryName exists as a regular
// file. Small helper used by callers that need to distinguish
// "covered by wider local" (Covers) from "on disk at this exact name"
// (FileExists).
func FileExists(snapDir, entryName string) bool {
	if snapDir == "" || entryName == "" {
		return false
	}
	_, err := os.Stat(filepath.Join(snapDir, entryName))
	return err == nil
}
