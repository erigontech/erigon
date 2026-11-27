// Copyright 2025 The Erigon Authors
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
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/erigontech/erigon/common/log/v3"
)

type fileFingerprint struct {
	basename string
	hash     [20]byte // SHA1 InfoHash from .torrent file
	path     string
}

type verifiedFile struct {
	path string
	hash [20]byte
}

const integrityCacheVersion = "integrity-cache-v1"

// IntegrityCache records which files have already passed an integrity check so
// that repeated runs can skip them.  has/add must only be called from the main
// goroutine (before spawning workers and after eg.Wait() respectively).
type IntegrityCache struct {
	path        string
	entries     map[string]string // cacheKey (check+basenames) -> tab-separated hashes
	dirty       bool              // true if entries were added or replaced
	verifiedSet map[verifiedFile]struct{}
	readOnly    bool
}

// LoadIntegrityCache reads the cache file at path into an IntegrityCache.
// Returns an empty cache if the file does not exist.
func LoadIntegrityCache(path string) (*IntegrityCache, error) {
	c := &IntegrityCache{
		path:        path,
		entries:     make(map[string]string),
		verifiedSet: make(map[verifiedFile]struct{}),
	}
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return c, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		return c, scanner.Err()
	}
	header := scanner.Text()
	if header != integrityCacheVersion {
		if strings.HasPrefix(header, "integrity-cache-") {
			c.readOnly = true
		} else {
			c.dirty = true
		}
		return c, nil
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		key, hashes := parseEntry(line)
		c.entries[key] = hashes
	}
	return c, scanner.Err()
}

// Save atomically writes all entries to disk.
// No-ops when nothing changed since Load.
func (c *IntegrityCache) Save() error {
	if c.readOnly || !c.dirty {
		return nil
	}
	tmpPath := c.path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	if _, err := fmt.Fprintln(w, integrityCacheVersion); err != nil {
		f.Close()
		return err
	}
	for key, hashes := range c.entries {
		// Reconstruct full entry: interleave basenames from key with hashes
		if _, err := fmt.Fprintln(w, formatEntry(key, hashes)); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, c.path)
}

// has reports whether every file's infohash was verified in this run and matches the cache.
// Safe to call on nil receiver (returns false).
func (c *IntegrityCache) has(check string, files []fileFingerprint) bool {
	if c == nil || !c.filesVerified(files) {
		return false
	}
	key := cacheKey(check, files)
	hashes, ok := c.entries[key]
	if !ok {
		return false
	}
	return hashes == encodeHashes(files)
}

// add records (check, files) after every file's infohash was verified in this run.
// Safe to call on nil receiver (no-op).
func (c *IntegrityCache) add(check string, files []fileFingerprint) {
	if c == nil || !c.filesVerified(files) {
		return
	}
	key := cacheKey(check, files)
	c.entries[key] = encodeHashes(files)
	c.dirty = true
}

// fingerprintsOf loads .torrent files for the given paths and extracts their InfoHashes.
// Returns an error if any .torrent file does not exist (no fallback).
func fingerprintsOf(paths ...string) ([]fileFingerprint, error) {
	fps := make([]fileFingerprint, 0, len(paths))
	for _, path := range paths {
		torrentPath := path + ".torrent"
		mi, err := metainfo.LoadFromFile(torrentPath)
		if err != nil {
			return nil, fmt.Errorf("loading torrent file %s: %w", torrentPath, err)
		}
		fps = append(fps, fileFingerprint{
			basename: filepath.Base(path),
			hash:     mi.HashInfoBytes(),
			path:     cachePath(path),
		})
	}
	return fps, nil
}

func (c *IntegrityCache) markVerified(path string, hash [20]byte) {
	c.verifiedSet[verifiedFile{path: path, hash: hash}] = struct{}{}
}

func (c *IntegrityCache) filesVerified(files []fileFingerprint) bool {
	for _, file := range files {
		if file.path != "" {
			if _, ok := c.verifiedSet[verifiedFile{path: file.path, hash: file.hash}]; !ok {
				return false
			}
		}
	}
	return true
}

func cachePath(path string) string {
	if absolute, err := filepath.Abs(path); err == nil {
		path = absolute
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		path = resolved
	}
	return filepath.Clean(path)
}

func PrepareIntegrityCache(ctx context.Context, path string, snapshotsDir string, failFast bool, logger log.Logger) (*IntegrityCache, error) {
	cache, err := LoadIntegrityCache(path)
	if err != nil {
		return nil, err
	}
	logger.Info("[integrity] verifying torrent piece hashes before integrity checks")
	err = verifyTorrentFiles(ctx, snapshotsDir, failFast, logger, cache.markVerified)
	if err != nil {
		return cache, fmt.Errorf("torrent verification failed: %w", err)
	}
	return cache, nil
}

// cacheKey produces a key from check name and file basenames (without hashes).
// This allows lookup/replacement when file contents change.
func cacheKey(check string, files []fileFingerprint) string {
	var sb strings.Builder
	sb.WriteString(check)
	for _, fp := range files {
		sb.WriteByte('\t')
		sb.WriteString(fp.basename)
	}
	return sb.String()
}

// parseEntry splits a cache file line into key (check+basenames) and value (hashes).
func parseEntry(line string) (key, hashes string) {
	parts := strings.Split(line, "\t")
	if len(parts) == 0 {
		return line, ""
	}
	var keySb, hashesSb strings.Builder
	keySb.WriteString(parts[0]) // check name
	for i, part := range parts[1:] {
		keySb.WriteByte('\t')
		if i > 0 {
			hashesSb.WriteByte('\t')
		}
		// Split "basename:hash"
		if idx := strings.LastIndex(part, ":"); idx > 0 {
			keySb.WriteString(part[:idx])
			hashesSb.WriteString(part[idx+1:])
		} else {
			keySb.WriteString(part)
		}
	}
	return keySb.String(), hashesSb.String()
}

// formatEntry reconstructs a cache file line from key and hashes.
func formatEntry(key, hashes string) string {
	keyParts := strings.Split(key, "\t")
	hashParts := strings.Split(hashes, "\t")
	var sb strings.Builder
	sb.WriteString(keyParts[0]) // check name
	for i, basename := range keyParts[1:] {
		sb.WriteByte('\t')
		sb.WriteString(basename)
		sb.WriteByte(':')
		if i < len(hashParts) {
			sb.WriteString(hashParts[i])
		}
	}
	return sb.String()
}

// encodeHashes produces tab-separated hex hashes from fingerprints.
func encodeHashes(files []fileFingerprint) string {
	var sb strings.Builder
	for i, fp := range files {
		if i > 0 {
			sb.WriteByte('\t')
		}
		sb.WriteString(hex.EncodeToString(fp.hash[:]))
	}
	return sb.String()
}
