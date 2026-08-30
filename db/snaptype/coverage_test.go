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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeStub(t *testing.T, dir, name string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("stub"), 0o644))
}

// TestLocalCoverageIndex_WiderSubsumesNarrower pins the primary
// invariant: a 10k-block merged file covers ten 1k-block chunk
// entries of the same class.
func TestLocalCoverageIndex_WiderSubsumesNarrower(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeStub(t, dir, "v1.1-003400-003500-headers.seg")

	cov := BuildLocalCoverageIndex(dir)

	assert.True(t, cov.Covers("v1.1-003400-003410-headers.seg"),
		"1k chunk at [3400, 3410) must be covered by local 10k at [3400, 3500)")
	assert.True(t, cov.Covers("v1.1-003490-003500-headers.seg"),
		"1k chunk at boundary [3490, 3500) must be covered")
	assert.True(t, cov.Covers("v1.1-003400-003500-headers.seg"),
		"exact-range match must be covered (equal ranges qualify)")
}

// TestLocalCoverageIndex_UncoveredRangeReturnsFalse pins the gap-fill
// path: an entry outside local coverage must NOT be reported as
// covered — preverified/peer fills it.
func TestLocalCoverageIndex_UncoveredRangeReturnsFalse(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeStub(t, dir, "v1.1-003400-003500-headers.seg")

	cov := BuildLocalCoverageIndex(dir)

	assert.False(t, cov.Covers("v1.1-003500-003600-headers.seg"),
		"entry past local coverage must not be reported as covered — fills a gap")
	assert.False(t, cov.Covers("v1.1-003300-003400-headers.seg"),
		"entry before local coverage must not be reported as covered")
}

// TestLocalCoverageIndex_DifferentTypeNotCovered pins the class
// partition: local BODIES does not cover HEADERS at the same range.
func TestLocalCoverageIndex_DifferentTypeNotCovered(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeStub(t, dir, "v1.1-003400-003500-bodies.seg")

	cov := BuildLocalCoverageIndex(dir)

	assert.False(t, cov.Covers("v1.1-003400-003500-headers.seg"),
		"local BODIES does not cover HEADERS — same range, different class")
	assert.True(t, cov.Covers("v1.1-003400-003500-bodies.seg"),
		"local BODIES covers same-class BODIES entry")
}

// TestLocalCoverageIndex_CrossVersionSubsumption pins the
// version-agnostic dedup: local v2.1 file covers a v2.0 entry of the
// same class + range. Motivating case: the 2026-07-01 hoodi commitment
// bug where v2.1 broad + v2.0 narrows coexisted on disk after
// bootstrap merge; the primitive must consider them equivalent
// classes so bootstrap picks one, not both.
func TestLocalCoverageIndex_CrossVersionSubsumption(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeStub(t, dir, "domain/v2.1-commitment.272-280.kv")

	cov := BuildLocalCoverageIndex(dir)

	assert.True(t, cov.Covers("domain/v2.0-commitment.272-276.kv"),
		"local v2.1 wider must cover v2.0 narrower at same class + subsumed range")
	assert.True(t, cov.Covers("domain/v2.0-commitment.276-278.kv"), "same")
	assert.True(t, cov.Covers("domain/v2.0-commitment.278-279.kv"), "same")
}

// TestLocalCoverageIndex_TorrentSidecarSkipped pins that .torrent
// files don't count as coverage — a lone sidecar without the primary
// data file is not authoritative.
func TestLocalCoverageIndex_TorrentSidecarSkipped(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeStub(t, dir, "v1.1-003400-003500-headers.seg.torrent")

	cov := BuildLocalCoverageIndex(dir)

	assert.False(t, cov.Covers("v1.1-003400-003410-headers.seg"),
		"lone .torrent sidecar (no data file) must not count as coverage")
}

// TestLocalCoverageIndex_EmptyDirIsEmpty pins the cold-start /
// snapDir=="" behaviour: the index is empty and Covers returns false
// for every input.
func TestLocalCoverageIndex_EmptyDirIsEmpty(t *testing.T) {
	t.Parallel()
	empty := BuildLocalCoverageIndex("")
	assert.False(t, empty.Covers("v1.1-003400-003500-headers.seg"))

	dir := t.TempDir()
	cov := BuildLocalCoverageIndex(dir)
	assert.False(t, cov.Covers("v1.1-003400-003500-headers.seg"))
}

// TestLocalCoverageIndex_SubdirPartition pins that domain/ files
// don't cover history/ files of the same base name. Subdir is part of
// the coverage key.
func TestLocalCoverageIndex_SubdirPartition(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeStub(t, dir, "domain/v2.0-accounts.0-256.kv")

	cov := BuildLocalCoverageIndex(dir)

	assert.True(t, cov.Covers("domain/v2.0-accounts.0-256.kv"),
		"domain/ .kv covers domain/ .kv at same range")
	// history subdir would carry a .v extension anyway; a stub with a
	// .kv extension in history/ would be a synthetic mistake but pin
	// that the subdir alone is a partition key.
	assert.False(t, cov.Covers("history/v2.0-accounts.0-256.v"),
		"different subdir = different class; must not cover")
}
