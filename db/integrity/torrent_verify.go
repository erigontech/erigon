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
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/erigontech/erigon/common/log/v3"
	"golang.org/x/sync/errgroup"
)

// VerifyTorrentFiles verifies that data files match their .torrent piece hashes.
// It scans the given directory recursively for .torrent files and verifies each
// corresponding data file. failFast only governs whether the run stops at the
// first mismatch; either way a mismatch, an unreachable path or a cancelled
// context fails the call, since a partial or failed scan must never pass as a
// complete verification.
func VerifyTorrentFiles(ctx context.Context, dir string, failFast bool, logger log.Logger) error {
	torrentFiles, scanErr := collectTorrentFiles(dir, logger)
	if scanErr != nil {
		scanErr = fmt.Errorf("listing torrent files: %w", scanErr)
	}

	if len(torrentFiles) == 0 {
		logger.Info("[verify] no torrent files found", "dir", dir)
		return scanErr
	}

	var totalBytes int64
	var unreadable int
	var firstUnreadable error
	toVerify := make([]string, 0, len(torrentFiles))
	for _, tf := range torrentFiles {
		dataFile := strings.TrimSuffix(tf, ".torrent")
		info, err := os.Stat(dataFile)
		if os.IsNotExist(err) {
			continue // no data file, skip
		}
		if err != nil {
			logger.Warn("[verify] skipping unreadable data file", "path", dataFile, "err", err)
			unreadable++
			if firstUnreadable == nil {
				firstUnreadable = fmt.Errorf("stat %s: %w", dataFile, err)
			}
			continue
		}
		toVerify = append(toVerify, tf)
		totalBytes += info.Size()
	}
	if unreadable > 0 {
		scanErr = errors.Join(scanErr, fmt.Errorf("%d unreadable data file(s), first: %w", unreadable, firstUnreadable))
	}

	if len(toVerify) == 0 {
		logger.Info("[verify] no data files to verify", "dir", dir)
		return scanErr
	}

	logger.Info("[verify] starting", "files", len(toVerify), "totalGB", totalBytes>>30)

	var completedBytes atomic.Uint64
	var completedFiles atomic.Uint64

	parent := ctx
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.GOMAXPROCS(-1) * 4)

	// Progress logging
	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-logEvery.C:
				logger.Info("[verify] progress",
					"percent", fmt.Sprintf("%.2f%%", 100*float64(completedBytes.Load())/float64(totalBytes)),
					"files", fmt.Sprintf("%d/%d", completedFiles.Load(), len(toVerify)),
				)
			}
		}
	}()

	var failedMu sync.Mutex
	var failed int
	var firstFailure error

	for _, torrentFile := range toVerify {
		g.Go(func() error {
			defer completedFiles.Add(1)
			err := verifyFileFromTorrent(ctx, torrentFile, &completedBytes)
			if err != nil {
				if failFast {
					return err
				}
				logger.Warn("[verify] file failed", "file", torrentFile, "err", err)
				failedMu.Lock()
				failed++
				if firstFailure == nil {
					firstFailure = err
				}
				failedMu.Unlock()
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.Join(scanErr, err)
	}
	var verifyErr error
	if failed > 0 {
		verifyErr = fmt.Errorf("%d file(s) failed verification, first: %w", failed, firstFailure)
	}
	// Without failFast the workers only warn, so an interrupted run would
	// otherwise be indistinguishable from a complete one. The errgroup's own
	// ctx is always cancelled by now, hence the parent.
	if err := parent.Err(); err != nil {
		return errors.Join(scanErr, verifyErr, err)
	}
	if err := errors.Join(scanErr, verifyErr); err != nil {
		return err
	}

	logger.Info("[verify] complete", "files", len(toVerify))
	return nil
}

// collectTorrentFiles lists .torrent files under dir recursively. Symlinked
// directories are followed — a snapshots dir or one of its subtrees living on
// another disk is a supported layout — and the resolved path doubles as the
// cycle guard. Paths that could not be read are reported through the error
// together with the files that were reached: they may hide any number of
// torrents, so the caller verifies what it got and still fails the run.
func collectTorrentFiles(dir string, logger log.Logger) (files []string, err error) {
	visited := map[string]struct{}{}
	var skipped int
	var firstSkipped error
	skip := func(path string, err error) {
		logger.Warn("[verify] skipping unreadable path", "path", path, "err", err)
		skipped++
		if firstSkipped == nil {
			firstSkipped = fmt.Errorf("%s: %w", path, err)
		}
	}

	var walk func(string) error
	walk = func(path string) error {
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil {
			return err
		}
		if _, seen := visited[resolved]; seen {
			return nil
		}
		visited[resolved] = struct{}{}

		entries, err := os.ReadDir(resolved)
		if err != nil {
			return err
		}
		for _, e := range entries {
			child := filepath.Join(resolved, e.Name())
			isDir := e.IsDir()
			if e.Type()&fs.ModeSymlink != 0 {
				info, err := os.Stat(child)
				if err != nil {
					skip(child, err)
					continue
				}
				isDir = info.IsDir()
			}
			if isDir {
				if err := walk(child); err != nil {
					skip(child, err)
				}
				continue
			}
			if strings.HasSuffix(e.Name(), ".torrent") {
				files = append(files, child)
			}
		}
		return nil
	}

	if err := walk(dir); err != nil {
		return nil, err
	}
	if skipped > 0 {
		return files, fmt.Errorf("%d unreadable path(s), first: %w", skipped, firstSkipped)
	}
	return files, nil
}

// verifyFileFromTorrent verifies a single data file against its .torrent piece hashes.
func verifyFileFromTorrent(ctx context.Context, torrentPath string, completedBytes *atomic.Uint64) error {
	mi, err := metainfo.LoadFromFile(torrentPath)
	if err != nil {
		return fmt.Errorf("loading torrent %s: %w", torrentPath, err)
	}

	info, err := mi.UnmarshalInfo()
	if err != nil {
		return fmt.Errorf("unmarshaling torrent info %s: %w", torrentPath, err)
	}

	dataPath := strings.TrimSuffix(torrentPath, ".torrent")
	f, err := os.Open(dataPath)
	if err != nil {
		return fmt.Errorf("opening data file %s: %w", dataPath, err)
	}
	defer f.Close()

	hasher := sha1.New()
	pieceLen := info.PieceLength
	numPieces := info.NumPieces()

	for i := range numPieces {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		hasher.Reset()

		// Calculate piece bounds
		offset := int64(i) * pieceLen
		length := pieceLen
		if i == numPieces-1 {
			// Last piece may be shorter
			length = info.TotalLength() - offset
		}

		_, err := io.Copy(hasher, io.NewSectionReader(f, offset, length))
		if err != nil {
			return fmt.Errorf("reading piece %d of %s: %w", i, dataPath, err)
		}

		expectedHash := info.Pieces[i*20 : (i+1)*20]
		if !bytes.Equal(hasher.Sum(nil), expectedHash) {
			return fmt.Errorf("hash mismatch at piece %d, file: %s", i, filepath.Base(dataPath))
		}

		if completedBytes != nil {
			completedBytes.Add(uint64(length))
		}
	}

	return nil
}
