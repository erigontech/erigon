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

package snapshot

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
)

// AdoptionReadyMarker is the file the adoption handler writes into a
// staging directory once the batch has passed Stage 1 + Stage 2
// validation. It is the intent journal: a staging directory without
// this marker holds an interrupted or unvalidated fetch and must never
// be cut over; one with it is safe to promote, and a re-run after a
// crash mid-cutover completes the remaining renames.
const AdoptionReadyMarker = ".adoption-ready"

// MoveFileAcrossFS renames src onto dst, falling back to a fsync'd copy
// into a sibling temp file plus an atomic same-directory rename when
// src and dst are on different filesystems. Adoption stages files
// under <datadir>/temp; an operator may symlink the snapshots
// directory onto a separate volume, which makes a direct rename fail
// with EXDEV.
func MoveFileAcrossFS(src, dst string) error {
	if err := os.Rename(src, dst); err == nil {
		return nil
	} else if !errors.Is(err, syscall.EXDEV) {
		return err
	}
	tmp := dst + ".adopting"
	if err := datadir.CopyFile(src, tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = dir.RemoveFile(tmp)
		return err
	}
	return dir.RemoveFile(src)
}

// CutoverStagedDir promotes every file in stagingDir to its live
// location under liveSnapDir, replacing the superseded file where one
// exists. It is the stopped-node cutover: a node restarts and rescans
// the live directory afterwards, so there is no reader barrier and no
// in-memory view to rebuild — just the file swap.
//
// The stale .torrent sidecar of a replaced file is removed so the
// downloader regenerates it for the canonical content at next startup.
// On dryRun nothing is moved; the returned slice still lists what
// would be swapped. A file is removed from stagingDir as it lands, so
// a re-run after an interruption completes the remainder.
func CutoverStagedDir(liveSnapDir, stagingDir string, dryRun bool, logger log.Logger) ([]string, error) {
	entries, err := os.ReadDir(stagingDir)
	if err != nil {
		return nil, fmt.Errorf("read staging dir: %w", err)
	}
	swapped := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || e.Name() == AdoptionReadyMarker {
			continue // the marker is journal metadata, not a snapshot file
		}
		name := e.Name()
		if dryRun {
			swapped = append(swapped, name)
			continue
		}
		dst := PathForName(liveSnapDir, name)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return swapped, fmt.Errorf("create dir for %s: %w", name, err)
		}
		if err := MoveFileAcrossFS(filepath.Join(stagingDir, name), dst); err != nil {
			return swapped, fmt.Errorf("move %s: %w", name, err)
		}
		_ = dir.RemoveFile(dst + ".torrent")
		swapped = append(swapped, name)
	}
	if !dryRun {
		if err := dir.RemoveAll(stagingDir); err != nil && logger != nil {
			logger.Warn("[storage] adopt: staging cleanup failed", "dir", stagingDir, "err", err)
		}
	}
	return swapped, nil
}

// RecoveredBatch records one adoption batch acted on by
// RecoverStagedAdoptions.
type RecoveredBatch struct {
	Name  string   // the adoption-<gen> staging directory's basename
	Files []string // snapshot files cut over (on dryRun: that would be)
}

// RecoverStagedAdoptions promotes every marked, validated adoption batch
// under tmpDir to liveSnapDir. A batch directory (adoption-<gen>) is cut
// over only if it carries the AdoptionReadyMarker; an unmarked batch is
// an interrupted or unvalidated fetch and is logged and skipped. It is
// both the node's startup auto-recovery of a policy=auto cutover that
// crashed between the marker write and the file swap, and the body of
// the `erigon snapshots adopt` command. With dryRun nothing is moved;
// the result still lists what would be swapped. A missing tmpDir is not
// an error — there is simply nothing staged.
//
// Startup callers must run this before any snapshot file is opened: the
// cutover renames live files in place.
func RecoverStagedAdoptions(liveSnapDir, tmpDir string, dryRun bool, logger log.Logger) ([]RecoveredBatch, error) {
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read temp dir: %w", err)
	}
	var recovered []RecoveredBatch
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "adoption-") {
			continue
		}
		batch := filepath.Join(tmpDir, e.Name())
		// Only promote a batch the adoption handler marked as validated.
		// An unmarked directory is an interrupted or in-progress fetch
		// whose files were never checked — cutting it over would publish
		// unverified content.
		if _, err := os.Stat(filepath.Join(batch, AdoptionReadyMarker)); err != nil {
			if logger != nil {
				logger.Warn("[storage] adoption recovery: skipping unvalidated batch (no ready marker)", "batch", e.Name())
			}
			continue
		}
		files, err := CutoverStagedDir(liveSnapDir, batch, dryRun, logger)
		if err != nil {
			return recovered, fmt.Errorf("recover %s: %w", e.Name(), err)
		}
		recovered = append(recovered, RecoveredBatch{Name: e.Name(), Files: files})
	}
	return recovered, nil
}
