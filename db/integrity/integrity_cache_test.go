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

	cache, err := PrepareIntegrityCache(t.Context(), cachePath, dir, false, false, log.New())
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
	cache, err := PrepareIntegrityCache(t.Context(), cachePath, dir, false, false, log.New())
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

func TestPrepareIntegrityCacheSkipsWhenTorrentPiecesAreSkipped(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "snapshots")
	cachePath := t.TempDir()

	cache, err := PrepareIntegrityCache(t.Context(), cachePath, dir, true, false, log.New())
	require.NoError(t, err)
	require.Nil(t, cache, "skipping TorrentPieces must disable the cache")
}

func TestPrepareIntegrityCacheMatchesSymlinkedSnapshotPaths(t *testing.T) {
	tmp := t.TempDir()
	target := filepath.Join(tmp, "target")
	writeTorrentPair(t, target, "valid.seg", false)
	link := filepath.Join(tmp, "snapshots")
	require.NoError(t, os.Symlink(target, link))

	cachePath := filepath.Join(t.TempDir(), "integrity.cache")
	cache, err := PrepareIntegrityCache(t.Context(), cachePath, link, false, true, log.New())
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

	cache, err := PrepareIntegrityCache(t.Context(), cacheFile, dir, false, false, log.New())
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
