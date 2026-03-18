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

package downloader

import (
	"context"
	"crypto/sha1" //nolint:gosec
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/preverified"
)

// SnapshotVerifyIssue is one finding emitted as a JSON line to stdout during verification.
type SnapshotVerifyIssue struct {
	// "error" or "warn"
	Severity string `json:"severity"`
	// .torrent path relative to the snap dir.
	Torrent string `json:"torrent,omitempty"`
	// Data file path relative to the snap dir.
	File string `json:"file,omitempty"`
	// Human-readable description.
	Message string `json:"message"`
}

// SnapshotVerifySummary is printed as a single JSON line to stdout after all checks complete.
type SnapshotVerifySummary struct {
	Torrents int `json:"torrents"`
	Errors   int `json:"errors"`
	Warnings int `json:"warnings"`
}

// issueEmitter is safe for concurrent use.
type issueEmitter struct {
	enc      *json.Encoder
	mu       sync.Mutex
	errors   atomic.Int64
	warnings atomic.Int64
}

func newIssueEmitter(out io.Writer) *issueEmitter {
	return &issueEmitter{enc: json.NewEncoder(out)}
}

func (e *issueEmitter) emit(issue SnapshotVerifyIssue) {
	e.mu.Lock()
	_ = e.enc.Encode(issue) //nolint:errcheck
	e.mu.Unlock()
	switch issue.Severity {
	case "error":
		e.errors.Add(1)
	case "warn":
		e.warnings.Add(1)
	}
}

func (e *issueEmitter) summary(torrents int) SnapshotVerifySummary {
	return SnapshotVerifySummary{
		Torrents: torrents,
		Errors:   int(e.errors.Load()),
		Warnings: int(e.warnings.Load()),
	}
}

// VerifySnapshotDownloads checks snapshot downloads against .torrent files and preverified.toml.
// Findings are written as JSON lines to out as they are discovered. A summary JSON object is
// written last. Torrent files are verified in a pool of workers for concurrency.
//
// Checks performed:
//   - Infohash of each .torrent matches preverified.toml (if present).
//   - File paths within torrents are safe (no directory traversal).
//   - Data files exist, have the right size, and appropriate permissions.
//   - Piece hashes via mmap/sparse-read storage (SEEK_DATA/SEEK_HOLE).
//   - Preverified.toml entries with no corresponding .torrent on disk.
func VerifySnapshotDownloads(ctx context.Context, dirs datadir.Dirs, workers int, out io.Writer) (summary SnapshotVerifySummary, err error) {
	if workers <= 0 {
		workers = max(1, runtime.GOMAXPROCS(0)/2)
	}
	snapDir := dirs.Snap
	emitter := newIssueEmitter(out)

	// Load preverified.toml if present.
	var pvItems preverified.SortedItems
	preverifiedPath := dirs.PreverifiedPath()
	pvData, pvErr := os.ReadFile(preverifiedPath)
	if pvErr == nil {
		var rawMap map[string]string
		if err = toml.Unmarshal(pvData, &rawMap); err != nil {
			err = fmt.Errorf("parsing %s: %w", datadir.PreverifiedFileName, err)
			return
		}
		pvItems = preverified.ItemsFromMap(rawMap)
	} else if !errors.Is(pvErr, fs.ErrNotExist) {
		err = fmt.Errorf("reading %s: %w", datadir.PreverifiedFileName, pvErr)
		return
	}

	// Collect all .torrent names first (walk is fast; this lets us track seen names cleanly).
	var torrentNames []string
	walkErr := fs.WalkDir(os.DirFS(snapDir), ".", func(relPath string, de fs.DirEntry, walkFileErr error) error {
		if walkFileErr != nil {
			emitter.emit(SnapshotVerifyIssue{
				Severity: "warn",
				File:     relPath,
				Message:  fmt.Sprintf("walk error: %v", walkFileErr),
			})
			return nil
		}
		if de.IsDir() {
			return nil
		}
		snapshotName, isTorrent := strings.CutSuffix(relPath, ".torrent")
		if !isTorrent {
			return nil
		}
		torrentNames = append(torrentNames, snapshotName)
		return nil
	})
	if walkErr != nil {
		err = fmt.Errorf("walking snap dir %q: %w", snapDir, walkErr)
		return
	}

	// Report preverified entries with no .torrent file on disk.
	torrentNameSet := make(map[string]bool, len(torrentNames))
	for _, name := range torrentNames {
		torrentNameSet[name] = true
	}
	for _, item := range pvItems {
		if !torrentNameSet[item.Name] {
			emitter.emit(SnapshotVerifyIssue{
				Severity: "warn",
				Torrent:  item.Name + ".torrent",
				Message:  "preverified.toml entry has no corresponding .torrent file on disk",
			})
		}
	}

	// Verify each .torrent file in a worker pool.
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(workers)
	for _, snapshotName := range torrentNames {
		eg.Go(func() error {
			verifyOneTorrent(egCtx, snapDir, snapshotName, pvItems, emitter)
			return nil
		})
	}
	_ = eg.Wait()

	summary = emitter.summary(len(torrentNames))
	e := emitter.enc
	emitter.mu.Lock()
	_ = e.Encode(summary) //nolint:errcheck
	emitter.mu.Unlock()

	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return
}

// verifyOneTorrent performs all checks for one .torrent file and its data files.
// snapshotName is the unix-slash path relative to snapDir, without the ".torrent" suffix.
func verifyOneTorrent(
	ctx context.Context,
	snapDir string,
	snapshotName string,
	pvItems preverified.SortedItems,
	emitter *issueEmitter,
) {
	torrentRel := snapshotName + ".torrent"
	torrentFilePath := filepath.Join(snapDir, filepath.FromSlash(torrentRel))

	mi, err := metainfo.LoadFromFile(torrentFilePath)
	if err != nil {
		emitter.emit(SnapshotVerifyIssue{
			Severity: "error",
			Torrent:  torrentRel,
			Message:  fmt.Sprintf("failed to load torrent file: %v", err),
		})
		return
	}

	infoHash := mi.HashInfoBytes()
	infoHashHex := infoHash.HexString()

	// Check infohash against preverified.toml.
	if pvItems != nil {
		if pvItem, ok := pvItems.Get(snapshotName); ok {
			if pvItem.Hash != infoHashHex {
				emitter.emit(SnapshotVerifyIssue{
					Severity: "error",
					Torrent:  torrentRel,
					Message:  fmt.Sprintf("infohash mismatch: preverified=%s actual=%s", pvItem.Hash, infoHashHex),
				})
			}
		} else {
			emitter.emit(SnapshotVerifyIssue{
				Severity: "warn",
				Torrent:  torrentRel,
				Message:  "not present in preverified.toml",
			})
		}
	}

	info, err := mi.UnmarshalInfo()
	if err != nil {
		emitter.emit(SnapshotVerifyIssue{
			Severity: "error",
			Torrent:  torrentRel,
			Message:  fmt.Sprintf("failed to unmarshal info dict: %v", err),
		})
		return
	}

	// Validate info.Name is a safe path.
	infoName := info.BestName()
	if infoName != metainfo.NoName {
		if _, pathErr := storage.ToSafeFilePath(infoName); pathErr != nil {
			emitter.emit(SnapshotVerifyIssue{
				Severity: "error",
				Torrent:  torrentRel,
				Message:  fmt.Sprintf("info.Name %q is unsafe: %v", infoName, pathErr),
			})
			return
		}
	}

	// info.Name should match the torrent filename.
	baseName := path.Base(snapshotName)
	if infoName != metainfo.NoName && infoName != baseName {
		emitter.emit(SnapshotVerifyIssue{
			Severity: "warn",
			Torrent:  torrentRel,
			Message:  fmt.Sprintf("info.Name %q doesn't match torrent filename %q", infoName, baseName),
		})
	}

	// Check each data file.
	allPresent := true
	for _, fi := range info.UpvertedFiles() {
		if !checkDataFile(snapDir, snapshotName, torrentRel, &info, &fi, pvItems, emitter) {
			allPresent = false
		}
	}

	if allPresent && ctx.Err() == nil {
		verifyPieceHashes(ctx, snapDir, snapshotName, torrentRel, &info, infoHash, emitter)
	}
}

// checkDataFile checks one data file from a torrent for existence, size, and permissions.
// Returns true if the file is present and the right size.
func checkDataFile(
	snapDir string,
	snapshotName string,
	torrentRel string,
	info *metainfo.Info,
	fi *metainfo.FileInfo,
	pvItems preverified.SortedItems,
	emitter *issueEmitter,
) (present bool) {
	infoName := info.BestName()

	var relSlashPath string
	if len(fi.BestPath()) == 0 {
		// Single-file torrent.
		if infoName == metainfo.NoName {
			relSlashPath = snapshotName
		} else {
			relSlashPath = infoName
		}
	} else {
		safePath, pathErr := storage.ToSafeFilePath(fi.BestPath()...)
		if pathErr != nil {
			emitter.emit(SnapshotVerifyIssue{
				Severity: "error",
				Torrent:  torrentRel,
				Message:  fmt.Sprintf("unsafe file path %v in torrent: %v", fi.BestPath(), pathErr),
			})
			return false
		}
		relSlashPath = path.Join(infoName, safePath)
	}

	dataFilePath := filepath.Join(snapDir, filepath.FromSlash(relSlashPath))
	stat, statErr := os.Stat(dataFilePath)

	if errors.Is(statErr, fs.ErrNotExist) {
		severity := "warn"
		msg := "data file missing"
		if pvItems != nil {
			if _, inPV := pvItems.Get(snapshotName); inPV {
				severity = "error"
				msg = "data file missing (listed in preverified.toml)"
			}
		}
		emitter.emit(SnapshotVerifyIssue{
			Severity: severity,
			Torrent:  torrentRel,
			File:     relSlashPath,
			Message:  msg,
		})
		return false
	}
	if statErr != nil {
		emitter.emit(SnapshotVerifyIssue{
			Severity: "error",
			Torrent:  torrentRel,
			File:     relSlashPath,
			Message:  fmt.Sprintf("stat failed: %v", statErr),
		})
		return false
	}

	if stat.Size() != fi.Length {
		emitter.emit(SnapshotVerifyIssue{
			Severity: "error",
			Torrent:  torrentRel,
			File:     relSlashPath,
			Message:  fmt.Sprintf("size mismatch: on-disk=%d torrent=%d", stat.Size(), fi.Length),
		})
		// Size mismatch means piece verification won't be meaningful, treat as absent.
		return false
	}

	perm := stat.Mode().Perm()
	if perm&0o400 == 0 {
		emitter.emit(SnapshotVerifyIssue{
			Severity: "error",
			Torrent:  torrentRel,
			File:     relSlashPath,
			Message:  fmt.Sprintf("not readable by owner (mode=%v)", perm),
		})
	}
	if perm&0o222 != 0 {
		emitter.emit(SnapshotVerifyIssue{
			Severity: "warn",
			Torrent:  torrentRel,
			File:     relSlashPath,
			Message:  fmt.Sprintf("writable (mode=%v); completed snapshots are expected read-only", perm),
		})
	}
	if perm&0o002 != 0 {
		emitter.emit(SnapshotVerifyIssue{
			Severity: "error",
			Torrent:  torrentRel,
			File:     relSlashPath,
			Message:  fmt.Sprintf("world-writable (mode=%v)", perm),
		})
	}

	return true
}

// verifyPieceHashes hashes each piece via the mmap/sparse-read storage (SEEK_DATA/SEEK_HOLE)
// using the same configuration as the main downloader, and reports hash mismatches.
func verifyPieceHashes(
	ctx context.Context,
	snapDir string,
	snapshotName string,
	torrentRel string,
	info *metainfo.Info,
	infoHash metainfo.Hash,
	emitter *issueEmitter,
) {
	client := newSnapStorage(snapDir, nil)
	defer client.Close()

	t, err := client.OpenTorrent(ctx, info, infoHash)
	if err != nil {
		emitter.emit(SnapshotVerifyIssue{
			Severity: "error",
			Torrent:  torrentRel,
			Message:  fmt.Sprintf("failed to open torrent storage for piece verification: %v", err),
		})
		return
	}
	defer t.Close()

	for i := range info.NumPieces() {
		if ctx.Err() != nil {
			return
		}
		p := info.Piece(i)
		expectedHash := p.V1Hash()
		if !expectedHash.Ok {
			// v2 torrents use merkle trees; no v1 piece hash to check.
			continue
		}

		//nolint:gosec
		h := sha1.New()
		piece := t.Piece(p)

		// filePieceImpl implements io.WriterTo, which uses seekDataOrEof for sparse reads.
		var written int64
		var readErr error
		if wt, ok := piece.(io.WriterTo); ok {
			written, readErr = wt.WriteTo(h)
		} else {
			written, readErr = io.Copy(h, io.NewSectionReader(piece, 0, p.Length()))
		}

		if readErr != nil {
			emitter.emit(SnapshotVerifyIssue{
				Severity: "error",
				Torrent:  torrentRel,
				File:     snapshotName,
				Message:  fmt.Sprintf("piece %d: read error: %v", i, readErr),
			})
			continue
		}
		if written < p.Length() {
			emitter.emit(SnapshotVerifyIssue{
				Severity: "error",
				Torrent:  torrentRel,
				File:     snapshotName,
				Message:  fmt.Sprintf("piece %d: short read: got %d of %d bytes (sparse or incomplete)", i, written, p.Length()),
			})
			continue
		}

		var actualHash metainfo.Hash
		h.Sum(actualHash[:0])
		if actualHash != expectedHash.Value {
			emitter.emit(SnapshotVerifyIssue{
				Severity: "error",
				Torrent:  torrentRel,
				File:     snapshotName,
				Message:  fmt.Sprintf("piece %d: hash mismatch: expected %x got %x", i, expectedHash.Value, actualHash),
			})
		}
	}
}
