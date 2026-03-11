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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/erigontech/erigon/common/log/v3"
	"golang.org/x/sync/errgroup"
)

// VerifyTorrentFiles verifies that data files match their .torrent piece hashes.
// It scans the given directory for .torrent files and verifies each corresponding data file.
// If failFast is true, stops on first error. Otherwise logs warnings and continues.
func VerifyTorrentFiles(ctx context.Context, dir string, failFast bool, logger log.Logger) error {
	torrentFiles, err := filepath.Glob(filepath.Join(dir, "*.torrent"))
	if err != nil {
		return fmt.Errorf("listing torrent files: %w", err)
	}

	if len(torrentFiles) == 0 {
		logger.Info("[verify] no torrent files found", "dir", dir)
		return nil
	}

	var totalBytes int64
	toVerify := make([]string, 0, len(torrentFiles))
	for _, tf := range torrentFiles {
		dataFile := tf[:len(tf)-8] // strip .torrent
		info, err := os.Stat(dataFile)
		if os.IsNotExist(err) {
			continue // no data file, skip
		}
		if err != nil {
			return fmt.Errorf("stat %s: %w", dataFile, err)
		}
		toVerify = append(toVerify, tf)
		totalBytes += info.Size()
	}

	if len(toVerify) == 0 {
		logger.Info("[verify] no data files to verify", "dir", dir)
		return nil
	}

	logger.Info("[verify] starting", "files", len(toVerify), "totalGB", totalBytes>>30)

	var completedBytes atomic.Uint64
	var completedFiles atomic.Uint64

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

	for _, torrentFile := range toVerify {
		g.Go(func() error {
			defer completedFiles.Add(1)
			err := verifyFileFromTorrent(ctx, torrentFile, &completedBytes)
			if err != nil {
				if failFast {
					return err
				}
				logger.Warn("[verify] file failed", "file", torrentFile, "err", err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	logger.Info("[verify] complete", "files", len(toVerify))
	return nil
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

	dataPath := torrentPath[:len(torrentPath)-8] // strip .torrent
	f, err := os.Open(dataPath)
	if err != nil {
		return fmt.Errorf("opening data file %s: %w", dataPath, err)
	}
	defer f.Close()

	hasher := sha1.New()
	pieceLen := info.PieceLength
	numPieces := info.NumPieces()

	for i := 0; i < numPieces; i++ {
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
