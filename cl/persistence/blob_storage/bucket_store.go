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

package blob_storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/ssz"
)

const (
	subdivisionSlot = 10_000
	rwLocksCount    = 64
	tmpSuffix       = ".tmp"
)

// bucketStore holds sidecars under <slot/subdivisionSlot>/<blockRoot>_<index>.
//
// It never locks, so the caller picks the granularity. Reads need none: a write lands by
// rename, so a reader sees the old file or the new one and never a partial. Writes do —
// see write.
type bucketStore struct {
	fs afero.Fs
}

func (b *bucketStore) init(fs afero.Fs) {
	b.fs = fs
}

func (b *bucketStore) path(slot uint64, root common.Hash, idx uint64) (dir, file string) {
	dir = strconv.FormatUint(slot/subdivisionSlot, 10)
	file = fmt.Sprintf("%s/%s_%d", dir, root.String(), idx)
	return dir, file
}

// write encodes v onto a temp file and renames it onto the target, so a failure
// never leaves a partial file where a reader would take it for a complete one.
// created reports whether the target was absent beforehand.
//
// The temp name is derived from the target alone, so two writes of the same
// (slot, root, idx) would share it and interleave into one file. Callers must
// hold that slot's lock.
func (b *bucketStore) write(slot uint64, root common.Hash, idx uint64, v ssz.Marshaler) (bool, error) {
	dir, file := b.path(slot, root, idx)
	created := false
	if _, err := b.fs.Stat(file); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		created = true
	}
	if err := b.fs.MkdirAll(dir, 0o755); err != nil {
		return false, err
	}
	tmp := file + tmpSuffix
	if err := b.encodeTo(tmp, v); err != nil {
		b.fs.Remove(tmp)
		return false, err
	}
	// Windows replaces via MoveFileEx, which needs delete access to the destination, so
	// this fails while another goroutine holds the target open for reading.
	if err := b.fs.Rename(tmp, file); err != nil {
		b.fs.Remove(tmp)
		return false, err
	}
	return created, nil
}

func (b *bucketStore) encodeTo(path string, v ssz.Marshaler) error {
	fh, err := b.fs.Create(path)
	if err != nil {
		return err
	}
	defer fh.Close()
	// EncodeAndWrite flushes in a defer and discards that error, so a short write is
	// only observable on the writer it was handed.
	w := &errWriter{w: fh}
	if err := ssz_snappy.EncodeAndWrite(w, v); err != nil {
		return err
	}
	if w.err != nil {
		return w.err
	}
	if err := fh.Sync(); err != nil {
		return err
	}
	return fh.Close()
}

// read decodes the stored sidecar into out. A missing file is reported as not found,
// leaving each caller to decide what absence means.
func (b *bucketStore) read(slot uint64, root common.Hash, idx uint64, out ssz.EncodableSSZ, version clparams.StateVersion) (bool, error) {
	_, file := b.path(slot, root, idx)
	fh, err := b.fs.Open(file)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	defer fh.Close()
	if err := ssz_snappy.DecodeAndReadNoForkDigest(fh, out, version); err != nil {
		return false, err
	}
	return true, nil
}

func (b *bucketStore) exists(slot uint64, root common.Hash, idx uint64) (bool, error) {
	_, file := b.path(slot, root, idx)
	if _, err := b.fs.Stat(file); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (b *bucketStore) remove(slot uint64, root common.Hash, idx uint64) error {
	_, file := b.path(slot, root, idx)
	if err := b.fs.Remove(file); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (b *bucketStore) stream(w io.Writer, slot uint64, root common.Hash, idx uint64) error {
	_, file := b.path(slot, root, idx)
	fh, err := b.fs.Open(file)
	if err != nil {
		return err
	}
	defer fh.Close()
	_, err = io.Copy(w, fh)
	return err
}

type errWriter struct {
	w   io.Writer
	err error
}

func (e *errWriter) Write(p []byte) (int, error) {
	n, err := e.w.Write(p)
	if err != nil && e.err == nil {
		e.err = err
	}
	return n, err
}

// pruneBelow removes every bucket that ends before slot. It attempts all of them and
// returns the first failure, so one bucket that cannot be removed — an open file blocks
// its directory on Windows — does not stop the rest.
func (b *bucketStore) pruneBelow(slot uint64) error {
	cutoff := slot / subdivisionSlot
	if cutoff == 0 {
		return nil
	}
	entries, err := afero.ReadDir(b.fs, ".")
	if err != nil {
		return err
	}
	var firstErr error
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		bucket, err := strconv.ParseUint(name, 10, 64)
		// The blob index database is a sibling of the buckets in this directory, so only
		// a name this store could itself have produced may be removed.
		if err != nil || strconv.FormatUint(bucket, 10) != name {
			continue
		}
		if bucket >= cutoff {
			continue
		}
		if err := b.fs.RemoveAll(name); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type slotLocks struct {
	locks []sync.RWMutex
}

func (s *slotLocks) init() {
	s.locks = make([]sync.RWMutex, rwLocksCount)
}

func (s *slotLocks) forSlot(slot uint64) *sync.RWMutex {
	return &s.locks[slot%rwLocksCount]
}
