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

// eest-fixtures downloads and verifies execution-spec-tests fixture tarballs
// pinned by manifest into a local cache directory. Re-running is a no-op when
// the cached files already match the manifest's sha256 — files are not touched
// (mtime preserved) so Go test caching stays valid across runs.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type entry struct {
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size,omitempty"`
}

func main() {
	manifestPath := flag.String("manifest", "execution/tests/eest-fixtures.json", "path to manifest JSON")
	outDir := flag.String("out", "execution/tests/eest-cache", "output directory for cached tarballs")
	timeout := flag.Duration("timeout", 10*time.Minute, "per-download timeout")
	flag.Parse()

	raw, err := os.ReadFile(*manifestPath)
	if err != nil {
		die("read manifest %q: %v", *manifestPath, err)
	}
	var entries map[string]entry
	if err := json.Unmarshal(raw, &entries); err != nil {
		die("parse manifest %q: %v", *manifestPath, err)
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		die("mkdir %q: %v", *outDir, err)
	}

	names := make([]string, 0, len(entries))
	for n := range entries {
		names = append(names, n)
	}
	sort.Strings(names)

	for _, name := range names {
		e := entries[name]
		path := filepath.Join(*outDir, name)
		if ok, _ := verify(path, e.SHA256); ok {
			fmt.Printf("%s: cached (sha256 %s)\n", name, e.SHA256[:12])
			continue
		}
		fmt.Printf("%s: downloading from %s\n", name, e.URL)
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		err := download(ctx, e.URL, path)
		cancel()
		if err != nil {
			die("%s: download failed: %v", name, err)
		}
		ok, got := verify(path, e.SHA256)
		if !ok {
			die("%s: sha256 mismatch — want %s got %s", name, e.SHA256, got)
		}
		fmt.Printf("%s: ok (%s)\n", name, time.Since(start).Round(time.Second))
	}
}

func verify(path, want string) (bool, string) {
	f, err := os.Open(path)
	if err != nil {
		return false, ""
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return false, ""
	}
	got := hex.EncodeToString(h.Sum(nil))
	return got == want, got
}

func download(ctx context.Context, url, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		os.Remove(tmp) //nolint:gocritic
		return fmt.Errorf("write %q: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp) //nolint:gocritic
		return err
	}
	return os.Rename(tmp, path)
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "eest-fixtures: "+format+"\n", args...)
	os.Exit(1)
}
