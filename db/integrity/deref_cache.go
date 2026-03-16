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
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/anacrolix/torrent/metainfo"
)

type fileFingerprint struct {
	basename string
	hash     [20]byte // SHA1 InfoHash from .torrent file
}

// IntegrityCache records which files have already passed an integrity check so
// that repeated runs can skip them.  has/add must only be called from the main
// goroutine (before spawning workers and after eg.Wait() respectively).
type IntegrityCache struct {
	path    string
	entries map[string]string // cacheKey (check+basenames) -> tab-separated hashes
	dirty   bool              // true if entries were added or replaced
}

// LoadIntegrityCache reads the cache file at path into an IntegrityCache.
// Returns an empty cache if the file does not exist.
func LoadIntegrityCache(path string) (*IntegrityCache, error) {
	c := &IntegrityCache{
		path:    path,
		entries: make(map[string]string),
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
	if !c.dirty {
		return nil
	}
	tmpPath := c.path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
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

// has reports whether (check, files) is already in the cache with matching hashes.
// Safe to call on nil receiver (returns false).
func (c *IntegrityCache) has(check string, files []fileFingerprint) bool {
	if c == nil {
		return false
	}
	key := cacheKey(check, files)
	hashes, ok := c.entries[key]
	if !ok {
		return false
	}
	return hashes == encodeHashes(files)
}

// add records (check, files) as successfully checked, replacing any existing entry
// for the same check+basenames combination.
// Safe to call on nil receiver (no-op).
func (c *IntegrityCache) add(check string, files []fileFingerprint) {
	if c == nil {
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
		})
	}
	return fps, nil
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

// encodeEntry produces the tab-separated cache line written to the cache file:
//
//	CheckName\tbasename1:hash1hex\tbasename2:hash2hex...
//
// Hash is 40 hex characters (20 bytes SHA1 InfoHash).
func encodeEntry(check string, files []fileFingerprint) string {
	return formatEntry(cacheKey(check, files), encodeHashes(files))
}
