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

package l1

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
)

// BlobDir is the BlobSource of a directory of blobs, each in a file named
// by its versioned hash, <hash>.blob. It reads the blobs it lacks from next,
// when set, and stores them: beacon nodes keep blobs for a few weeks only.
type BlobDir struct {
	dir  string
	next BlobSource
}

var _ BlobSource = (*BlobDir)(nil)

// NewBlobDir returns the BlobSource of dir, backed by next when not nil.
func NewBlobDir(dir string, next BlobSource) *BlobDir {
	return &BlobDir{dir: dir, next: next}
}

func (d *BlobDir) path(hash common.Hash) string {
	return filepath.Join(d.dir, hash.Hex()+".blob")
}

// Has reports whether the directory holds a file for hash, without reading
// it.
func (d *BlobDir) Has(hash common.Hash) (bool, error) {
	_, err := os.Stat(d.path(hash))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	case err != nil:
		return false, err
	}
	return true, nil
}

// Load reads the blob of hash, whose KZG versioned hash must be hash.
func (d *BlobDir) Load(hash common.Hash) ([]byte, error) {
	blob, err := os.ReadFile(d.path(hash))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %x", ErrBlobUnavailable, hash)
	}
	if err != nil {
		return nil, err
	}
	got, err := VersionedHash(blob)
	switch {
	case err != nil:
		return nil, fmt.Errorf("%w: %s: %w", ErrInconsistent, d.path(hash), err)
	case got != hash:
		return nil, fmt.Errorf("%w: %s holds blob %x", ErrInconsistent, d.path(hash), got)
	}
	return blob, nil
}

// Blobs returns the blobs of hashes from the directory, and those it lacks
// from next.
func (d *BlobDir) Blobs(ctx context.Context, h *Header, hashes []common.Hash) ([][]byte, error) {
	out := make([][]byte, len(hashes))
	var missing []common.Hash
	for i, hash := range hashes {
		blob, err := d.Load(hash)
		switch {
		case errors.Is(err, ErrBlobUnavailable):
			missing = append(missing, hash)
		case err != nil:
			return nil, err
		}
		out[i] = blob
	}
	if len(missing) == 0 {
		return out, nil
	}
	if d.next == nil {
		return nil, fmt.Errorf("%w: %x of L1 block %d in %s, which no source backs", ErrBlobLost, missing[0], h.Number, d.dir)
	}
	fetched, err := d.next.Blobs(ctx, h, missing)
	if err != nil {
		return nil, err
	}
	for i, j := 0, 0; i < len(out); i++ {
		if out[i] == nil {
			out[i] = fetched[j]
			j++
		}
	}
	for i, blob := range fetched {
		if err := d.store(missing[i], blob); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// store writes blob, which next matched to hash, atomically.
func (d *BlobDir) store(hash common.Hash, blob []byte) error {
	if err := os.MkdirAll(d.dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(d.dir, hash.Hex()+".*.tmp")
	if err != nil {
		return err
	}
	_, err = f.Write(blob)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(f.Name(), d.path(hash))
	}
	if err != nil {
		_ = dir.RemoveFile(f.Name())
		return fmt.Errorf("store blob %x: %w", hash, err)
	}
	return nil
}
