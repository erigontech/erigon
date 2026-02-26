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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cespare/xxhash/v2"
)

type fileFingerprint struct {
	basename string
	hash     uint64 // xxhash64 of full file contents
}

// IntegrityCache records which files have already passed an integrity check so
// that repeated runs can skip them.  has/add must only be called from the main
// goroutine (before spawning workers and after eg.Wait() respectively).
type IntegrityCache struct {
	path         string
	checked      map[string]struct{} // encoded line -> present; read-only after Load
	newlyChecked []string            // appended after eg.Wait() in main goroutine
}

// LoadIntegrityCache reads the cache file at path into an IntegrityCache.
// Returns an empty cache if the file does not exist.
func LoadIntegrityCache(path string) (*IntegrityCache, error) {
	c := &IntegrityCache{
		path:    path,
		checked: make(map[string]struct{}),
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
		c.checked[line] = struct{}{}
	}
	return c, scanner.Err()
}

// Save atomically writes all entries (existing + newly added) to disk.
// No-ops when nothing new was added since Load.
func (c *IntegrityCache) Save() error {
	if len(c.newlyChecked) == 0 {
		return nil
	}
	tmpPath := c.path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for entry := range c.checked {
		if _, err := fmt.Fprintln(w, entry); err != nil {
			f.Close()
			return err
		}
	}
	for _, entry := range c.newlyChecked {
		if _, err := fmt.Fprintln(w, entry); err != nil {
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

// has reports whether (check, files) is already in the cache.
// Safe to call on nil receiver (returns false).
func (c *IntegrityCache) has(check string, files []fileFingerprint) bool {
	if c == nil {
		return false
	}
	_, ok := c.checked[encodeEntry(check, files)]
	return ok
}

// add records (check, files) as successfully checked.
// Safe to call on nil receiver (no-op).
func (c *IntegrityCache) add(check string, files []fileFingerprint) {
	if c == nil {
		return
	}
	c.newlyChecked = append(c.newlyChecked, encodeEntry(check, files))
}

// fingerprintOf opens the file at path and computes its xxhash64 fingerprint.
func fingerprintOf(path string) (fileFingerprint, error) {
	f, err := os.Open(path)
	if err != nil {
		return fileFingerprint{}, err
	}
	defer f.Close()
	h := xxhash.New()
	if _, err := io.Copy(h, f); err != nil {
		return fileFingerprint{}, err
	}
	return fileFingerprint{basename: filepath.Base(path), hash: h.Sum64()}, nil
}

// encodeEntry produces the tab-separated cache line used as the map key and
// written to the cache file:
//
//	CheckName\tbasename1:hash1hex\tbasename2:hash2hex...
func encodeEntry(check string, files []fileFingerprint) string {
	var sb strings.Builder
	sb.WriteString(check)
	for _, fp := range files {
		sb.WriteByte('\t')
		sb.WriteString(fp.basename)
		sb.WriteByte(':')
		fmt.Fprintf(&sb, "%016x", fp.hash)
	}
	return sb.String()
}
