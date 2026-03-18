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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	g "github.com/anacrolix/generics"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/pelletier/go-toml/v2"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/preverified"
)

// SnapshotVerifyIssue represents a problem found during snapshot download verification.
type SnapshotVerifyIssue struct {
	// "error" or "warn"
	Severity string
	// Relative path within the snap dir, or preverified.toml item name.
	File string
	// Human-readable description.
	Message string
}

func (i SnapshotVerifyIssue) String() string {
	return fmt.Sprintf("[%s] %s: %s", i.Severity, i.File, i.Message)
}

// VerifySnapshotDownloads checks the contents of the snapshot directory against .torrent files and
// an optional preverified.toml. It uses the mixed file/mmap storage from anacrolix/torrent to
// efficiently read piece data with sparse-file support via SEEK_DATA/SEEK_HOLE.
//
// Checks performed:
//   - For each .torrent file: infohash matches preverified.toml (if present).
//   - For each file listed in a .torrent: path is safe (no directory traversal), data file exists,
//     file permissions are appropriate.
//   - Piece hashes of present data files match the torrent metadata.
//   - Preverified.toml entries that have no corresponding .torrent file on disk.
func VerifySnapshotDownloads(ctx context.Context, dirs datadir.Dirs, logger log.Logger) (issues []SnapshotVerifyIssue, err error) {
	snapDir := dirs.Snap

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
		logger.Info("loaded preverified.toml", "items", len(pvItems))
	} else if !errors.Is(pvErr, fs.ErrNotExist) {
		err = fmt.Errorf("reading %s: %w", datadir.PreverifiedFileName, pvErr)
		return
	}

	// Walk the snap dir for .torrent files (they may be in subdirectories).
	torrentNamesSeen := make(map[string]bool)
	walkErr := fs.WalkDir(os.DirFS(snapDir), ".", func(relPath string, de fs.DirEntry, walkFileErr error) error {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if walkFileErr != nil {
			issues = append(issues, SnapshotVerifyIssue{
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
		torrentNamesSeen[snapshotName] = true

		torrentIssues := verifyTorrentFile(ctx, snapDir, snapshotName, pvItems)
		issues = append(issues, torrentIssues...)
		return nil
	})
	if walkErr != nil && !errors.Is(walkErr, context.Canceled) {
		err = fmt.Errorf("walking snap dir %q: %w", snapDir, walkErr)
		return
	}

	// Report preverified items that have no .torrent file on disk.
	for _, item := range pvItems {
		if torrentNamesSeen[item.Name] {
			continue
		}
		issues = append(issues, SnapshotVerifyIssue{
			Severity: "warn",
			File:     item.Name + ".torrent",
			Message:  "preverified.toml entry has no corresponding .torrent file on disk",
		})
	}

	return issues, ctx.Err()
}

// verifyTorrentFile performs all checks for one .torrent file and its associated data files.
// snapshotName is the path relative to snapDir, without the ".torrent" suffix (unix slash style).
func verifyTorrentFile(
	ctx context.Context,
	snapDir string,
	snapshotName string,
	pvItems preverified.SortedItems,
) (issues []SnapshotVerifyIssue) {
	torrentFilePath := filepath.Join(snapDir, filepath.FromSlash(snapshotName+".torrent"))

	mi, err := metainfo.LoadFromFile(torrentFilePath)
	if err != nil {
		return []SnapshotVerifyIssue{{
			Severity: "error",
			File:     snapshotName + ".torrent",
			Message:  fmt.Sprintf("failed to load torrent file: %v", err),
		}}
	}

	infoHash := mi.HashInfoBytes()
	infoHashHex := infoHash.HexString()

	// Check infohash against preverified.toml.
	if pvItems != nil {
		if pvItem, ok := pvItems.Get(snapshotName); ok {
			if pvItem.Hash != infoHashHex {
				issues = append(issues, SnapshotVerifyIssue{
					Severity: "error",
					File:     snapshotName + ".torrent",
					Message:  fmt.Sprintf("infohash mismatch: preverified=%s actual=%s", pvItem.Hash, infoHashHex),
				})
			}
		} else {
			issues = append(issues, SnapshotVerifyIssue{
				Severity: "warn",
				File:     snapshotName + ".torrent",
				Message:  "not present in preverified.toml",
			})
		}
	}

	info, err := mi.UnmarshalInfo()
	if err != nil {
		issues = append(issues, SnapshotVerifyIssue{
			Severity: "error",
			File:     snapshotName + ".torrent",
			Message:  fmt.Sprintf("failed to unmarshal info dict: %v", err),
		})
		return
	}

	// Validate the info.Name safe path (protects against directory traversal in the torrent).
	infoName := info.BestName()
	if infoName != metainfo.NoName {
		if _, pathErr := storage.ToSafeFilePath(infoName); pathErr != nil {
			issues = append(issues, SnapshotVerifyIssue{
				Severity: "error",
				File:     snapshotName + ".torrent",
				Message:  fmt.Sprintf("info.Name %q is unsafe: %v", infoName, pathErr),
			})
			return
		}
	}

	// Check expected name: torrent file basename should match info.Name.
	baseName := path.Base(snapshotName)
	if infoName != metainfo.NoName && infoName != baseName {
		issues = append(issues, SnapshotVerifyIssue{
			Severity: "warn",
			File:     snapshotName + ".torrent",
			Message:  fmt.Sprintf("info.Name %q doesn't match torrent filename %q", infoName, baseName),
		})
	}

	// Check each data file listed in the torrent.
	allFilesPresent := true
	for _, fi := range info.UpvertedFiles() {
		fileIssues, present := checkDataFile(snapDir, snapshotName, &info, &fi, pvItems)
		issues = append(issues, fileIssues...)
		if !present {
			allFilesPresent = false
		}
	}

	// Verify piece hashes using sparse-aware storage (mmap + SEEK_DATA/SEEK_HOLE).
	if allFilesPresent && ctx.Err() == nil {
		pieceIssues := verifyPieceHashes(ctx, snapDir, &info, infoHash)
		issues = append(issues, pieceIssues...)
	}
	return
}

// checkDataFile checks a single file within a torrent for existence, path safety, and permissions.
// Returns whether the file is present.
func checkDataFile(
	snapDir string,
	snapshotName string,
	info *metainfo.Info,
	fi *metainfo.FileInfo,
	pvItems preverified.SortedItems,
) (issues []SnapshotVerifyIssue, present bool) {
	infoName := info.BestName()

	// Compute the data file path using the same logic as the torrent storage client.
	// Single-file: {snapDir}/{info.Name}
	// Multi-file:  {snapDir}/{info.Name}/{file.BestPath()}
	var relSlashPath string
	if len(fi.BestPath()) == 0 {
		// Single-file torrent: info.Name is the file.
		if infoName == metainfo.NoName {
			relSlashPath = snapshotName
		} else {
			relSlashPath = infoName
		}
	} else {
		// Multi-file torrent: validate each path component.
		safePath, pathErr := storage.ToSafeFilePath(fi.BestPath()...)
		if pathErr != nil {
			issues = append(issues, SnapshotVerifyIssue{
				Severity: "error",
				File:     snapshotName,
				Message:  fmt.Sprintf("unsafe file path %v in torrent: %v", fi.BestPath(), pathErr),
			})
			return issues, false
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
		issues = append(issues, SnapshotVerifyIssue{
			Severity: severity,
			File:     relSlashPath,
			Message:  msg,
		})
		return issues, false
	}
	if statErr != nil {
		issues = append(issues, SnapshotVerifyIssue{
			Severity: "error",
			File:     relSlashPath,
			Message:  fmt.Sprintf("stat failed: %v", statErr),
		})
		return issues, false
	}

	// Check file size matches torrent metadata.
	if stat.Size() != fi.Length {
		issues = append(issues, SnapshotVerifyIssue{
			Severity: "error",
			File:     relSlashPath,
			Message:  fmt.Sprintf("size mismatch: on-disk=%d torrent=%d", stat.Size(), fi.Length),
		})
	}

	// Check file permissions.
	perm := stat.Mode().Perm()
	// All files should be readable by owner.
	if perm&0o400 == 0 {
		issues = append(issues, SnapshotVerifyIssue{
			Severity: "error",
			File:     relSlashPath,
			Message:  fmt.Sprintf("file not readable by owner (mode=%v)", perm),
		})
	}
	// Completed snapshot files should not be writable (they're promoted to read-only on completion).
	if perm&0o222 != 0 {
		issues = append(issues, SnapshotVerifyIssue{
			Severity: "warn",
			File:     relSlashPath,
			Message:  fmt.Sprintf("file is writable (mode=%v); completed snapshots are expected to be read-only", perm),
		})
	}
	// World-writable is a stronger concern.
	if perm&0o002 != 0 {
		issues = append(issues, SnapshotVerifyIssue{
			Severity: "error",
			File:     relSlashPath,
			Message:  fmt.Sprintf("file is world-writable (mode=%v)", perm),
		})
	}

	return issues, true
}

// verifyPieceHashes hashes each torrent piece using the mmap/sparse-read storage layer and
// compares to the piece hashes recorded in the torrent info dict.
func verifyPieceHashes(
	ctx context.Context,
	snapDir string,
	info *metainfo.Info,
	infoHash metainfo.Hash,
) (issues []SnapshotVerifyIssue) {
	// NewFileOpts with UsePartFiles=false uses the mmap-based fileIo (defaultFileIo).
	// The mmap layer implements seekDataOrEof via SEEK_DATA/SEEK_HOLE, enabling efficient
	// sparse-file reads that skip holes instead of reading zeroes.
	client := storage.NewFileOpts(storage.NewFileClientOpts{
		ClientBaseDir:   snapDir,
		UsePartFiles:    g.Some(false),
		PieceCompletion: storage.NewMapPieceCompletion(),
	})
	defer client.Close()

	t, err := client.OpenTorrent(ctx, info, infoHash)
	if err != nil {
		return []SnapshotVerifyIssue{{
			Severity: "error",
			File:     info.BestName(),
			Message:  fmt.Sprintf("failed to open torrent storage for piece verification: %v", err),
		}}
	}
	defer t.Close()

	for i := range info.NumPieces() {
		if ctx.Err() != nil {
			break
		}
		p := info.Piece(i)
		// Only v1 torrents carry piece hashes; v2 uses merkle trees (not handled here).
		expectedHash := p.V1Hash()
		if !expectedHash.Ok {
			continue
		}

		//nolint:gosec
		h := sha1.New()
		piece := t.Piece(p)

		// filePieceImpl implements io.WriterTo which uses seekDataOrEof for sparse reads.
		var written int64
		var readErr error
		if wt, ok := piece.(io.WriterTo); ok {
			written, readErr = wt.WriteTo(h)
		} else {
			written, readErr = io.Copy(h, io.NewSectionReader(piece, 0, p.Length()))
		}

		if readErr != nil {
			issues = append(issues, SnapshotVerifyIssue{
				Severity: "error",
				File:     info.BestName(),
				Message:  fmt.Sprintf("piece %d: read error: %v", i, readErr),
			})
			continue
		}
		if written < p.Length() {
			issues = append(issues, SnapshotVerifyIssue{
				Severity: "error",
				File:     info.BestName(),
				Message:  fmt.Sprintf("piece %d: short read: got %d bytes, expected %d (file may be incomplete or sparse)", i, written, p.Length()),
			})
			continue
		}

		var actualHash metainfo.Hash
		h.Sum(actualHash[:0])
		if actualHash != expectedHash.Value {
			issues = append(issues, SnapshotVerifyIssue{
				Severity: "error",
				File:     info.BestName(),
				Message:  fmt.Sprintf("piece %d: hash mismatch: expected %x got %x", i, expectedHash.Value, actualHash),
			})
		}
	}
	return
}
