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
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// renameAccessorsToOld pins the post-regen accessor sidecar dance.
// The straddle-regen path rewrites a .kv in place (same canonical
// name, smaller content truncated past lastTxNum). Without taking
// the stale accessors out of the canonical-name namespace before
// BuildMissedAccessors runs, the rebuild predicate
// (fileItemsWithMissedAccessors at db/state/dirty_files.go:806) sees
// "accessor exists on disk" and skips the rebuild — leaving the new
// .kv served via the OLD accessor's offsets and panicking on read
// with "index out of range" in decompress.go.
//
// The helper renames .bt / .kvi / .kvei sidecars (any version
// prefix) to .old in place, returning the .old paths so the caller
// can unlink them after BuildMissedAccessors lands. Globbing on
// `v*-<domain>.<from>-<to>.<ext>` lets us catch the version-prefix
// mismatch seen on real hoodi:
//
//	domain/v2.0-accounts.272-276.kv   (data)
//	domain/v1.1-accounts.272-276.bt   (accessor, lower version prefix)
//	domain/v1.1-accounts.272-276.kvei (existence, lower prefix)
//
//	domain/v2.0-commitment.272-276.kv  (data, commitment uses kvi)
//	domain/v2.0-commitment.272-276.kvi (accessor, same prefix)

// TestRenameAccessorsToOld_RenamesAllVariantsAcrossVersionPrefixes
// is the headline case: the directory has the real hoodi mix of
// version prefixes (.kv at v2.0, .bt/.kvei at v1.1, .kvi at v2.0).
// Every accessor variant must be renamed regardless of prefix.
func TestRenameAccessorsToOld_RenamesAllVariantsAcrossVersionPrefixes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mk := func(name string) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte("x"), 0644))
		return p
	}
	kvPath := mk("v2.0-accounts.272-276.kv")
	btPath := mk("v1.1-accounts.272-276.bt")
	kveiPath := mk("v1.1-accounts.272-276.kvei")
	kviPath := mk("v2.0-accounts.272-276.kvi")
	// Unrelated files in the same dir — different domain + different
	// step range — must NOT be touched.
	commitmentKv := mk("v2.0-commitment.272-276.kv")
	otherStepBt := mk("v1.1-accounts.264-272.bt")

	p := &Provider{}
	got := p.renameAccessorsToOld(kvPath)
	sort.Strings(got)

	wantOlds := []string{btPath + ".old", kveiPath + ".old", kviPath + ".old"}
	sort.Strings(wantOlds)
	require.Equal(t, wantOlds, got,
		"every accessor variant (.bt/.kvi/.kvei) of accounts.272-276 must be renamed, regardless of version prefix")

	for _, want := range wantOlds {
		_, err := os.Stat(want)
		require.NoError(t, err, "rename target %s must exist", want)
	}
	// Original accessor names must be gone.
	for _, gone := range []string{btPath, kveiPath, kviPath} {
		_, err := os.Stat(gone)
		require.True(t, os.IsNotExist(err), "canonical accessor name %s must no longer exist", gone)
	}
	// Unrelated files must still exist.
	_, err := os.Stat(commitmentKv)
	require.NoError(t, err, "commitment.272-276 must not be touched")
	_, err = os.Stat(otherStepBt)
	require.NoError(t, err, "accounts.264-272 must not be touched")
	// The .kv file itself is renamed elsewhere (by the FinalizeUnwind
	// caller), NOT by this helper.
	_, err = os.Stat(kvPath)
	require.NoError(t, err, "renameAccessorsToOld must NOT touch the .kv primary; that's the caller's job")
}

// TestRenameAccessorsToOld_NoAccessors_NoError pins the early-history
// path: a .kv without any associated accessor files yet (e.g. test
// fixtures) must yield an empty list, no panic.
func TestRenameAccessorsToOld_NoAccessors_NoError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "v2.0-accounts.0-256.kv")
	require.NoError(t, os.WriteFile(p, []byte("x"), 0644))

	prov := &Provider{}
	got := prov.renameAccessorsToOld(p)
	require.Empty(t, got, "no accessor files → empty result")
}

// TestRenameAccessorsToOld_MultipleVersionsRenamed pins the
// version-evolution case: if both .v1.1 and .v2.0 versions of an
// accessor live side by side (during a version migration), BOTH get
// renamed. Otherwise the post-rebuild read could resolve to the
// stale one via version.MatchVersionedFile preference rules.
func TestRenameAccessorsToOld_MultipleVersionsRenamed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	kvPath := filepath.Join(dir, "v2.0-accounts.272-276.kv")
	require.NoError(t, os.WriteFile(kvPath, []byte("x"), 0644))
	v11Bt := filepath.Join(dir, "v1.1-accounts.272-276.bt")
	v20Bt := filepath.Join(dir, "v2.0-accounts.272-276.bt")
	require.NoError(t, os.WriteFile(v11Bt, []byte("x"), 0644))
	require.NoError(t, os.WriteFile(v20Bt, []byte("x"), 0644))

	prov := &Provider{}
	got := prov.renameAccessorsToOld(kvPath)
	sort.Strings(got)

	want := []string{v11Bt + ".old", v20Bt + ".old"}
	sort.Strings(want)
	require.Equal(t, want, got,
		"both version variants of an accessor must be renamed; leaving one behind would let "+
			"version.MatchVersionedFile resolve to the stale accessor and re-introduce the bug")
}

// TestRenameAccessorsToOld_MalformedNameNoOp pins defensive parsing:
// a path that doesn't match the version-prefix-dash convention must
// yield an empty list, no error.
func TestRenameAccessorsToOld_MalformedNameNoOp(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	noDash := filepath.Join(dir, "weirdfile.kv")
	require.NoError(t, os.WriteFile(noDash, []byte("x"), 0644))

	prov := &Provider{}
	got := prov.renameAccessorsToOld(noDash)
	require.Empty(t, got, "no dash in basename → no version prefix → no rename")
}
