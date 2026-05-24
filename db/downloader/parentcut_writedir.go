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

package downloader

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/execution/chain"
)

// WriteForkDatadirOpts configures the file-copy + chain-config write
// performed by WriteForkDatadir. Required fields: ParentSnapDir,
// ForkSnapDir, ForkChainConfig, Plan. ForkChainConfigPath is optional
// (defaults to "chain.json" alongside the snap dir).
type WriteForkDatadirOpts struct {
	// ParentSnapDir is the source snap dir; files in Plan.Copy are read
	// from here. Must exist + be a directory.
	ParentSnapDir string

	// ForkSnapDir is the destination snap dir; files are written here.
	// Created if it doesn't exist. WriteForkDatadir refuses to operate
	// if it exists and is non-empty (call site must clear it
	// explicitly — the safety guard against clobbering an existing
	// datadir).
	ForkSnapDir string

	// ForkChainConfig is the derived chain.Config from
	// DeriveForkChainConfig. Persisted as JSON.
	ForkChainConfig *chain.Config

	// ForkChainConfigPath is where to write the derived chain.Config.
	// Empty defaults to filepath.Join(filepath.Dir(ForkSnapDir),
	// "chain.json") — i.e. the datadir root's chain.json, sibling of
	// the snap dir.
	ForkChainConfigPath string

	// Plan is the file-copy plan from BuildCopyPlan. Only Plan.Copy is
	// physically copied; Straddle / PostCut are operator-visible
	// information only.
	Plan *CopyPlan
}

// WriteForkDatadir performs the actual fork datadir setup:
//   - Validates ForkSnapDir is empty (or doesn't exist)
//   - Creates the ForkSnapDir tree
//   - Copies every file in Plan.Copy from ParentSnapDir to ForkSnapDir,
//     preserving relative paths + file modes
//   - Writes ForkChainConfig as JSON to ForkChainConfigPath
//
// On any error mid-copy, partially-written files remain (the caller
// should clean up the target dir). Returns the count of bytes + files
// copied for the CLI to print a summary.
func WriteForkDatadir(opts WriteForkDatadirOpts) (filesCopied int, bytesCopied int64, err error) {
	if err := validateWriteOpts(opts); err != nil {
		return 0, 0, err
	}

	// Resolve chain-config path default.
	configPath := opts.ForkChainConfigPath
	if configPath == "" {
		configPath = filepath.Join(filepath.Dir(opts.ForkSnapDir), "chain.json")
	}

	if err := refuseIfNotEmpty(opts.ForkSnapDir); err != nil {
		return 0, 0, err
	}
	if err := os.MkdirAll(opts.ForkSnapDir, 0o755); err != nil {
		return 0, 0, fmt.Errorf("mkdir fork snap dir %s: %w", opts.ForkSnapDir, err)
	}

	for _, entry := range opts.Plan.Copy {
		n, err := copyOneFile(opts.ParentSnapDir, opts.ForkSnapDir, entry.RelPath)
		if err != nil {
			return filesCopied, bytesCopied, fmt.Errorf("copy %s: %w", entry.RelPath, err)
		}
		filesCopied++
		bytesCopied += n
	}

	if err := writeChainConfigJSON(configPath, opts.ForkChainConfig); err != nil {
		return filesCopied, bytesCopied, fmt.Errorf("write derived chain.Config: %w", err)
	}

	return filesCopied, bytesCopied, nil
}

// validateWriteOpts ensures the required fields are populated.
func validateWriteOpts(opts WriteForkDatadirOpts) error {
	if opts.ParentSnapDir == "" {
		return fmt.Errorf("write fork datadir: empty ParentSnapDir")
	}
	if opts.ForkSnapDir == "" {
		return fmt.Errorf("write fork datadir: empty ForkSnapDir")
	}
	if opts.ParentSnapDir == opts.ForkSnapDir {
		return fmt.Errorf("write fork datadir: ParentSnapDir and ForkSnapDir are identical (%s) — fork must live in a fresh datadir", opts.ParentSnapDir)
	}
	if opts.ForkChainConfig == nil {
		return fmt.Errorf("write fork datadir: nil ForkChainConfig")
	}
	if opts.Plan == nil {
		return fmt.Errorf("write fork datadir: nil Plan")
	}
	if info, err := os.Stat(opts.ParentSnapDir); err != nil {
		return fmt.Errorf("write fork datadir: ParentSnapDir %s: %w", opts.ParentSnapDir, err)
	} else if !info.IsDir() {
		return fmt.Errorf("write fork datadir: ParentSnapDir %s is not a directory", opts.ParentSnapDir)
	}
	return nil
}

// refuseIfNotEmpty returns an error if path exists and is a non-empty
// directory. Existing + empty is OK (fork-from's --new-datadir may be
// pre-created by the operator). Missing is OK (we create it).
func refuseIfNotEmpty(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat target snap dir %s: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("target snap dir %s exists and is not a directory", path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("read target snap dir %s: %w", path, err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("target snap dir %s is not empty (%d entries) — fork-from refuses to clobber; pass a fresh datadir", path, len(entries))
	}
	return nil
}

// copyOneFile copies srcDir/relPath → dstDir/relPath, preserving the
// source file's permission bits. Creates intermediate directories as
// needed. Returns the number of bytes copied.
func copyOneFile(srcDir, dstDir, relPath string) (int64, error) {
	src := filepath.Join(srcDir, relPath)
	dst := filepath.Join(dstDir, relPath)

	in, err := os.Open(src)
	if err != nil {
		return 0, fmt.Errorf("open source: %w", err)
	}
	defer in.Close()

	stat, err := in.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat source: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return 0, fmt.Errorf("mkdir dst parent: %w", err)
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, stat.Mode().Perm())
	if err != nil {
		return 0, fmt.Errorf("create destination: %w", err)
	}
	defer out.Close()

	n, err := io.Copy(out, in)
	if err != nil {
		return n, fmt.Errorf("io.Copy: %w", err)
	}
	if err := out.Sync(); err != nil {
		return n, fmt.Errorf("fsync destination: %w", err)
	}
	return n, nil
}

// writeChainConfigJSON marshals cfg as indented JSON to path. Parent
// dir is created as needed.
func writeChainConfigJSON(path string, cfg *chain.Config) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir parent: %w", err)
	}
	out, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal chain.Config: %w", err)
	}
	out = append(out, '\n')
	if err := os.WriteFile(path, out, 0o644); err != nil {
		return fmt.Errorf("write chain.json: %w", err)
	}
	return nil
}
