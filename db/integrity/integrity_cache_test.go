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

package integrity

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	dir2 "github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/stretchr/testify/require"
)

func TestPrepareIntegrityCacheDoesNotHitForFailedPieces(t *testing.T) {
	dir := t.TempDir()
	validPath := filepath.Join(dir, "valid.seg")
	failedPath := filepath.Join(dir, "failed.seg")
	writeTorrentPair(t, dir, "valid.seg", false)
	writeTorrentPair(t, dir, "failed.seg", true)

	cachePath := filepath.Join(t.TempDir(), "integrity.cache")
	seedIntegrityCache(t, cachePath, "test", validPath, failedPath)

	cache, err := PrepareIntegrityCache(t.Context(), cachePath, dir, false, log.New())
	require.Error(t, err)
	valid, err := fingerprintsOf(validPath)
	require.NoError(t, err)
	failed, err := fingerprintsOf(failedPath)
	require.NoError(t, err)
	require.True(t, cache.has("test", valid))
	require.False(t, cache.has("test", failed), "failed piece verification must not produce a cache hit")
}

func TestPrepareIntegrityCacheSavesOnlyVerifiedFiles(t *testing.T) {
	dir := t.TempDir()
	validPath := filepath.Join(dir, "valid.seg")
	failedPath := filepath.Join(dir, "failed.seg")
	writeTorrentPair(t, dir, "valid.seg", false)
	writeTorrentPair(t, dir, "failed.seg", true)

	cachePath := filepath.Join(t.TempDir(), "integrity.cache")
	cache, err := PrepareIntegrityCache(t.Context(), cachePath, dir, false, log.New())
	require.Error(t, err)
	valid, err := fingerprintsOf(validPath)
	require.NoError(t, err)
	failed, err := fingerprintsOf(failedPath)
	require.NoError(t, err)
	cache.add("test", valid)
	cache.add("test", failed)
	require.NoError(t, cache.Save())

	reloaded, err := LoadIntegrityCache(cachePath)
	require.NoError(t, err)
	_, validCached := reloaded.entries[cacheKey("test", valid)]
	_, failedCached := reloaded.entries[cacheKey("test", failed)]
	require.True(t, validCached)
	require.False(t, failedCached, "failed piece verification must not be saved in the cache")
}

func TestPrepareIntegrityCacheLoadsMissingFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "snapshots")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	cachePath := filepath.Join(t.TempDir(), "integrity.cache")

	cache, err := PrepareIntegrityCache(t.Context(), cachePath, dir, false, log.New())
	require.NoError(t, err)
	require.NotNil(t, cache)
}

func TestPrepareIntegrityCacheMatchesSymlinkedSnapshotPaths(t *testing.T) {
	tmp := t.TempDir()
	target := filepath.Join(tmp, "target")
	writeTorrentPair(t, target, "valid.seg", false)
	link := filepath.Join(tmp, "snapshots")
	require.NoError(t, os.Symlink(target, link))

	cachePath := filepath.Join(t.TempDir(), "integrity.cache")
	cache, err := PrepareIntegrityCache(t.Context(), cachePath, link, true, log.New())
	require.NoError(t, err)
	fps, err := fingerprintsOf(filepath.Join(link, "valid.seg"))
	require.NoError(t, err)
	cache.add("test", fps)
	require.True(t, cache.has("test", fps))
}

func seedIntegrityCache(t *testing.T, path, check string, files ...string) {
	t.Helper()
	cache, err := LoadIntegrityCache(path)
	require.NoError(t, err)
	for _, file := range files {
		fps, err := fingerprintsOf(file)
		require.NoError(t, err)
		cache.entries[cacheKey(check, fps)] = encodeHashes(fps)
	}
	cache.dirty = true
	require.NoError(t, cache.Save())
}

func TestIntegrityCacheFileAliases(t *testing.T) {
	dir := t.TempDir()
	valid := filepath.Join(dir, "valid.seg")
	alias := filepath.Join(dir, "alias.seg")
	writeTorrentPair(t, dir, "valid.seg", false)
	require.NoError(t, os.WriteFile(alias, bytes.Repeat([]byte{0xbb}, 1024), 0o644))
	writeTorrentFile(t, alias)

	cacheFile := filepath.Join(t.TempDir(), "integrity.cache")
	seedIntegrityCache(t, cacheFile, "old", valid, alias)
	require.NoError(t, dir2.RemoveFile(alias))
	require.NoError(t, os.Symlink(valid, alias))

	cache, err := PrepareIntegrityCache(t.Context(), cacheFile, dir, false, log.New())
	require.ErrorContains(t, err, "hash mismatch at piece 0, file: alias.seg")
	good, err := fingerprintsOf(valid)
	require.NoError(t, err)
	failed, err := fingerprintsOf(alias)
	require.NoError(t, err)
	require.True(t, cache.has("old", good))
	if hit := cache.has("old", failed); hit {
		t.Errorf("failed alias cache hit = %t, want false", hit)
	}

	cache.add("new", good)
	cache.add("new", failed)
	require.NoError(t, cache.Save())
	saved, err := LoadIntegrityCache(cacheFile)
	require.NoError(t, err)
	require.Contains(t, saved.entries, cacheKey("new", good))
	if _, present := saved.entries[cacheKey("new", failed)]; present {
		t.Errorf("new failed alias entry on disk = %t, want false", present)
	}
}

func TestIntegrityCacheRejectsLegacyFormat(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "valid.seg")
	writeTorrentPair(t, dir, "valid.seg", false)
	fps, err := fingerprintsOf(dataPath)
	require.NoError(t, err)

	cachePath := filepath.Join(t.TempDir(), "integrity.cache")
	legacy := formatEntry(cacheKey("test", fps), encodeHashes(fps)) + "\n"
	require.NoError(t, os.WriteFile(cachePath, []byte(legacy), 0o644))

	cache, err := LoadIntegrityCache(cachePath)
	require.NoError(t, err)
	cache.markVerified(fps[0].path, fps[0].hash)
	require.False(t, cache.has("test", fps))

	require.NoError(t, cache.Save())
	saved, err := os.ReadFile(cachePath)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(saved), "integrity-cache-v1\n"))
}

func TestIntegrityCachePreservesNewerFormat(t *testing.T) {
	cachePath := filepath.Join(t.TempDir(), "integrity.cache")
	original := "integrity-cache-v2\nfuture-entry\n"
	require.NoError(t, os.WriteFile(cachePath, []byte(original), 0o644))

	cache, err := LoadIntegrityCache(cachePath)
	require.NoError(t, err)
	require.Empty(t, cache.entries)
	cache.add("test", []fileFingerprint{{basename: "test.seg", hash: [20]byte{1}}})
	require.NoError(t, cache.Save())
	saved, err := os.ReadFile(cachePath)
	require.NoError(t, err)
	require.Equal(t, original, string(saved))
}

func TestIntegrityCacheHitsWithRelativeDirUnderSymlinkedCwd(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	snapshots := filepath.Join(target, "snapshots")
	cwdLink := filepath.Join(root, "cwd")
	require.NoError(t, os.MkdirAll(snapshots, 0o755))
	require.NoError(t, os.Symlink(target, cwdLink))
	writeTorrentPair(t, snapshots, "valid.seg", false)

	cachePath := filepath.Join(root, "integrity.cache")
	seedIntegrityCache(t, cachePath, "test", filepath.Join(snapshots, "valid.seg"))
	t.Chdir(cwdLink)
	t.Setenv("PWD", cwdLink)

	cache, err := PrepareIntegrityCache(t.Context(), cachePath, "snapshots", true, log.New())
	require.NoError(t, err)
	fps, err := fingerprintsOf(filepath.Join("snapshots", "valid.seg"))
	require.NoError(t, err)
	require.True(t, cache.has("test", fps))
}

func TestIntegrityCacheKeepsSuppliedVerifiedPath(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.seg")
	replacement := filepath.Join(dir, "replacement.seg")
	require.NoError(t, os.WriteFile(target, bytes.Repeat([]byte{0xaa}, 1024), 0o644))
	require.NoError(t, os.WriteFile(replacement, bytes.Repeat([]byte{0xbb}, 1024), 0o644))
	resolved := cachePath(target)
	cache, err := LoadIntegrityCache(filepath.Join(t.TempDir(), "integrity.cache"))
	require.NoError(t, err)

	require.NoError(t, dir2.RemoveFile(target))
	require.NoError(t, os.Symlink(replacement, target))
	var hash [20]byte
	hash[0] = 1
	cache.markVerified(resolved, hash)
	require.True(t, cache.filesVerified([]fileFingerprint{{path: resolved, hash: hash}}))
}

func TestIntegrityCacheKeepsAllVerifiedHashesForResolvedPath(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data.seg")
	aliasOne := filepath.Join(dir, "one.seg")
	aliasTwo := filepath.Join(dir, "two.seg")
	require.NoError(t, os.WriteFile(dataPath, bytes.Repeat([]byte{0xaa}, 1024), 0o644))
	require.NoError(t, os.Symlink(dataPath, aliasOne))
	require.NoError(t, os.Symlink(dataPath, aliasTwo))
	writeTorrentFile(t, aliasOne)
	writeTorrentFile(t, aliasTwo)

	cachePath := filepath.Join(t.TempDir(), "integrity.cache")
	seedIntegrityCache(t, cachePath, "seed", aliasOne, aliasTwo)
	cache, err := PrepareIntegrityCache(t.Context(), cachePath, dir, true, log.New())
	require.NoError(t, err)
	fpsOne, err := fingerprintsOf(aliasOne)
	require.NoError(t, err)
	fpsTwo, err := fingerprintsOf(aliasTwo)
	require.NoError(t, err)
	require.True(t, cache.has("seed", fpsOne))
	require.True(t, cache.has("seed", fpsTwo))

	cache.add("new", fpsOne)
	cache.add("new", fpsTwo)
	require.NoError(t, cache.Save())
	saved, err := LoadIntegrityCache(cachePath)
	require.NoError(t, err)
	require.Contains(t, saved.entries, cacheKey("new", fpsOne))
	require.Contains(t, saved.entries, cacheKey("new", fpsTwo))
}
