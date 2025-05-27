// Copyright 2021 The Erigon Authors
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
	"bytes"
	"context"
	"crypto/sha1" //nolint:gosec
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/anacrolix/torrent"
	"github.com/erigontech/erigon-lib/log/v3"
)

func VerifyFileFailFast(ctx context.Context, t *torrent.Torrent, root string, completePieces *atomic.Uint64) error {
	info := t.Info()
	file := info.UpvertedFiles()[0]
	fPath := filepath.Join(append([]string{root, info.Name}, file.Path...)...)
	f, err := os.Open(fPath)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()

	// Bittorrent v1 using `sha1`
	hasher := sha1.New() //nolint:gosec
	for i := 0; i < info.NumPieces(); i++ {
		p := info.Piece(i)
		hasher.Reset()
		_, err := io.Copy(hasher, io.NewSectionReader(f, p.Offset(), p.Length()))
		if err != nil {
			return err
		}
		good := bytes.Equal(hasher.Sum(nil), p.Hash().Bytes())
		if !good {
			err := fmt.Errorf("hash mismatch at piece %d, file: %s", i, t.Name())
			log.Warn("[verify.failfast] ", "err", err)
			return err
		}

		completePieces.Add(1)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}
