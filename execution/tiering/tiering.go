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

package tiering

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/erigontech/erigon/db/kv"
)

// ColdBoundary returns the first block of the hot window of the last lookback blocks before head.
func ColdBoundary(head, lookback uint64) uint64 {
	if head < lookback {
		return 0
	}
	return head - lookback
}

// KVFiles keeps only the domain data files: a domain's visible files also include its history and index files.
func KVFiles(files kv.VisibleFiles) kv.VisibleFiles {
	var res kv.VisibleFiles
	for _, f := range files {
		if filepath.Ext(f.Fullpath()) == ".kv" {
			res = append(res, f)
		}
	}
	return res
}

// ColdFiles returns the files that hold only txnums below boundaryTxNum.
func ColdFiles(files kv.VisibleFiles, boundaryTxNum uint64) kv.VisibleFiles {
	var cold kv.VisibleFiles
	for _, f := range files {
		if f.EndRootNum() <= boundaryTxNum {
			cold = append(cold, f)
		}
	}
	return cold
}

// File is a visible state file: its txnum range [Start, End), its size and its symlink-resolved path.
type File struct {
	Start, End, Size uint64
	Path             string
}

type Usage struct {
	Files int
	Bytes uint64
	Txs   uint64 // txnums below the cold boundary
}

// Lag splits the files that should be on the cold tier but are not, by reason. Hot counts every file not on the cold
// tier. ColdEnd is the end of the newest cold-resident file; HotStart is the start of the oldest hot file, or ColdEnd
// when every file is cold.
type Lag struct {
	ColdResident, CopyPending, Straddling, Hot Usage
	ColdEnd, HotStart                          uint64
}

// IsCold reports whether a symlink-resolved path lies in the cold dir.
func IsCold(path, coldDir string) bool {
	return coldDir != "" && strings.HasPrefix(path, filepath.Clean(coldDir)+string(filepath.Separator))
}

func ComputeLag(files []File, boundaryTxNum uint64, coldDir string) Lag {
	var lag Lag
	hotStart := uint64(math.MaxUint64)
	for _, f := range files {
		if IsCold(f.Path, coldDir) {
			lag.ColdResident.Files++
			lag.ColdResident.Bytes += f.Size
			lag.ColdEnd = max(lag.ColdEnd, f.End)
			continue
		}
		lag.Hot.Files++
		lag.Hot.Bytes += f.Size
		hotStart = min(hotStart, f.Start)
		var u *Usage
		switch {
		case f.End <= boundaryTxNum:
			u = &lag.CopyPending
			u.Txs += f.End - f.Start
		case f.Start < boundaryTxNum:
			u = &lag.Straddling
			u.Txs += boundaryTxNum - f.Start
		default:
			continue
		}
		u.Files++
		u.Bytes += f.Size
	}
	lag.HotStart = lag.ColdEnd
	if lag.Hot.Files > 0 {
		lag.HotStart = hotStart
	}
	return lag
}

// AccessorBytes sums the sizes of the accessor files in dir that belong to files. Accessors and data files can carry
// different version prefixes, so they are matched by name without the version.
func AccessorBytes(dir string, files []File) (uint64, error) {
	want := make(map[string]struct{}, len(files))
	for _, f := range files {
		want[unversioned(strings.TrimSuffix(filepath.Base(f.Path), ".kv"))] = struct{}{}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, e := range entries {
		ext := filepath.Ext(e.Name())
		if ext != ".kvi" && ext != ".bt" && ext != ".kvei" {
			continue
		}
		if _, ok := want[unversioned(strings.TrimSuffix(e.Name(), ext))]; !ok {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return 0, err
		}
		total += uint64(info.Size())
	}
	return total, nil
}

// DuplicateBytes sums the sizes of the cold-dir copies of hot files: data held on both tiers.
func DuplicateBytes(coldDir string, hot []File) (uint64, error) {
	if coldDir == "" {
		return 0, nil
	}
	var total uint64
	for _, f := range hot {
		info, err := os.Stat(filepath.Join(coldDir, filepath.Base(f.Path)))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, err
		}
		total += uint64(info.Size())
	}
	return total, nil
}

func unversioned(name string) string {
	_, rest, _ := strings.Cut(name, "-")
	return rest
}

// ValidateDemote checks the settings that --tiering.demote needs.
func ValidateDemote(lookback uint64, coldDir, snapDomain string, allowSameFS bool) error {
	if lookback == 0 {
		return errors.New("--tiering.demote requires --tiering.lookback-blocks")
	}
	if coldDir == "" {
		return errors.New("--tiering.demote requires --tiering.cold-dir")
	}
	info, err := os.Stat(coldDir)
	if err != nil {
		return fmt.Errorf("--tiering.cold-dir: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("--tiering.cold-dir %s is not a directory", coldDir)
	}
	if allowSameFS {
		return nil
	}
	same, err := sameFilesystem(coldDir, snapDomain)
	if err != nil {
		return err
	}
	if same {
		return fmt.Errorf("--tiering.cold-dir %s is on the same filesystem as %s (set --tiering.allow-same-fs to allow)", coldDir, snapDomain)
	}
	return nil
}
