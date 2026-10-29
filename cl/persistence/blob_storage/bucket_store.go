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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/ssz"
	"github.com/erigontech/erigon/db/kv"
)

const (
	subdivisionSlot = 10_000
	listBatch       = 64
	rwLocksCount    = 64
	tmpSuffix       = ".tmp"
)

// ErrPruneNotStarted reports that pruning failed before any bucket was attempted, so the
// store still holds everything the caller was about to stop advertising.
var ErrPruneNotStarted = errors.New("prune did not start")

// bucketStore holds sidecars under <slot/subdivisionSlot>/<blockRoot>_<index>.
//
// The caller picks the slot granularity. Reads need none: a write lands by rename, so a
// reader sees the old file or the new one and never a partial. Writes do — see write.
type bucketStore struct {
	fs         afero.Fs
	pruneMutex sync.RWMutex
	pruneFloor uint64
	// bucketLocks order a write into a bucket against pruneBelow removing that bucket. The
	// slot stripe locks cannot: one bucket spans subdivisionSlot slots.
	bucketLocks    []sync.RWMutex
	dropRows       func(roots []common.Hash) error
	durableBuckets sync.Map
}

func (b *bucketStore) init(fs afero.Fs) {
	b.fs = fs
	b.bucketLocks = make([]sync.RWMutex, rwLocksCount)
}

func (b *bucketStore) forBucket(bucket uint64) *sync.RWMutex {
	return &b.bucketLocks[bucket%rwLocksCount]
}

func (b *bucketStore) path(slot uint64, root common.Hash, idx uint64) (dir, file string) {
	dir = strconv.FormatUint(slot/subdivisionSlot, 10)
	file = fmt.Sprintf("%s/%s_%d", dir, root.String(), idx)
	return dir, file
}

func (b *bucketStore) startWrite(slot uint64) bool {
	b.pruneMutex.RLock()
	if slot < b.pruneFloor {
		b.pruneMutex.RUnlock()
		return false
	}
	return true
}

func (b *bucketStore) finishWrite() {
	b.pruneMutex.RUnlock()
}

// write encodes v onto a temp file and renames it onto the target, so a failure
// never leaves a partial file where a reader would take it for a complete one.
// created reports whether the target was absent beforehand.
//
// The temp name is derived from the target alone, so two writes of the same
// (slot, root, idx) would share it and interleave into one file. Callers must
// hold that slot's lock.
func (b *bucketStore) write(slot uint64, root common.Hash, idx uint64, v ssz.Marshaler) (bool, error) {
	if !b.startWrite(slot) {
		return false, nil
	}
	defer b.finishWrite()
	return b.writeAdmitted(slot, root, idx, v)
}

func (b *bucketStore) writeAdmitted(slot uint64, root common.Hash, idx uint64, v ssz.Marshaler) (bool, error) {
	l := b.forBucket(slot / subdivisionSlot)
	l.RLock()
	defer l.RUnlock()
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
		b.removeTemp(tmp)
		return false, err
	}
	if err := b.fs.Rename(tmp, file); err != nil {
		b.removeTemp(tmp)
		if created || !isSharingViolation(err) {
			return false, err
		}
		// Windows replaces via MoveFileEx, which needs delete access to the destination,
		// so this fails while another goroutine holds the target open for reading. A
		// sidecar is determined by its (slot, root, idx), so a target still in place is
		// the one this write would have produced; a target that is gone stored nothing.
		if _, statErr := b.fs.Stat(file); statErr != nil {
			return false, err
		}
		return false, nil
	}
	return created, nil
}

func (b *bucketStore) syncBucket(slot uint64) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, _ := b.path(slot, common.Hash{}, 0)
	if err := b.syncDir(dir); err != nil {
		return err
	}
	if _, ok := b.durableBuckets.Load(dir); ok {
		return nil
	}
	if err := b.syncDir("."); err != nil {
		return err
	}
	b.durableBuckets.Store(dir, struct{}{})
	return nil
}

func (b *bucketStore) syncDir(name string) error {
	d, err := b.fs.Open(name)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

// removeTemp best-effort deletes the temp file; the caller already holds the
// real error.
func (b *bucketStore) removeTemp(tmp string) {
	if err := b.fs.Remove(tmp); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Debug("failed to remove temp file after write failure", "path", tmp, "err", err)
	}
}

func (b *bucketStore) encodeTo(path string, v ssz.Marshaler) error {
	fh, err := b.fs.Create(path)
	if err != nil {
		return err
	}
	defer fh.Close()
	if err := ssz_snappy.EncodeAndWrite(fh, v); err != nil {
		return err
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
		// A sidecar that will not decode is a truncated pre-rename write; reporting it absent
		// lets the caller re-fetch and overwrite it, which returning the error never does.
		log.Warn("[blob_storage] discarding undecodable sidecar", "file", file, "err", err)
		return false, nil
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

// EarliestStoredSlot returns where the lowest bucket holding a sidecar starts, which can be
// below the first sidecar actually stored in it, or 0 when nothing is stored. A bucket that
// cannot be read is skipped, since its sidecars cannot be served either, but when no readable
// bucket holds a sidecar the first read error is returned, so an unreadable store is not
// mistaken for an empty one.
func (b *bucketStore) EarliestStoredSlot() (uint64, error) {
	entries, err := afero.ReadDir(b.fs, ".")
	if err != nil {
		return 0, err
	}
	buckets := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		bucket, err := strconv.ParseUint(name, 10, 64)
		if err != nil || strconv.FormatUint(bucket, 10) != name {
			continue
		}
		buckets = append(buckets, bucket)
	}
	slices.Sort(buckets)
	var firstErr error
	for _, bucket := range buckets {
		stored, err := b.holdsSidecar(strconv.FormatUint(bucket, 10))
		if err != nil {
			log.Warn("[blob_storage] skipping unreadable column bucket", "bucket", bucket, "err", err)
			if firstErr == nil {
				firstErr = fmt.Errorf("column bucket %d: %w", bucket, err)
			}
			continue
		}
		if stored {
			return bucket * subdivisionSlot, nil
		}
	}
	return 0, firstErr
}

// holdsSidecar reads the bucket in small batches and stops at the first entry that is a
// stored sidecar, since a bucket holds one file per root and column.
func (b *bucketStore) holdsSidecar(dir string) (bool, error) {
	f, err := b.fs.Open(dir)
	if err != nil {
		return false, err
	}
	defer f.Close()
	for {
		files, err := f.Readdir(listBatch)
		if slices.ContainsFunc(files, isStoredSidecar) {
			return true, nil
		}
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
}

func isStoredSidecar(f os.FileInfo) bool {
	return !f.IsDir() && !strings.HasSuffix(f.Name(), tmpSuffix)
}

// pruneBelow removes every bucket that ends before slot. It attempts all of them and
// returns the first failure, so one bucket that cannot be removed — an open file blocks
// its directory on Windows — does not stop the rest.
func (b *bucketStore) pruneBelow(slot uint64) error {
	if slot == 0 {
		return nil
	}
	b.pruneMutex.Lock()
	entries, err := afero.ReadDir(b.fs, ".")
	if err != nil {
		b.pruneMutex.Unlock()
		return fmt.Errorf("%w: %w", ErrPruneNotStarted, err)
	}
	if slot > b.pruneFloor {
		b.pruneFloor = slot
	}
	effectiveFloor := b.pruneFloor
	b.pruneMutex.Unlock()

	cutoff := effectiveFloor / subdivisionSlot
	if cutoff == 0 {
		return nil
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
		if err := b.removeBucket(bucket, name); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (b *bucketStore) removeBucket(bucket uint64, name string) error {
	if b.dropRows != nil {
		roots, err := b.bucketRoots(name)
		if err != nil {
			return err
		}
		if err := b.dropRows(roots); err != nil {
			return err
		}
	}
	l := b.forBucket(bucket)
	l.Lock()
	defer l.Unlock()
	return b.fs.RemoveAll(name)
}

func (b *bucketStore) bucketRoots(dir string) ([]common.Hash, error) {
	f, err := b.fs.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	seen := map[common.Hash]struct{}{}
	var roots []common.Hash
	for {
		names, err := f.Readdirnames(listBatch)
		for _, name := range names {
			hex, _, _ := strings.Cut(name, "_")
			root := common.HexToHash(hex)
			if _, ok := seen[root]; ok || root.String() != hex {
				continue
			}
			seen[root] = struct{}{}
			roots = append(roots, root)
		}
		if errors.Is(err, io.EOF) {
			return roots, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func deleteRootRows(db kv.RwDB, table string) func(roots []common.Hash) error {
	return func(roots []common.Hash) error {
		if len(roots) == 0 {
			return nil
		}
		return db.Update(context.Background(), func(tx kv.RwTx) error {
			for _, root := range roots {
				if err := tx.Delete(table, root[:]); err != nil {
					return err
				}
			}
			return nil
		})
	}
}

type slotLocks struct {
	locks []sync.RWMutex
}

func (s *slotLocks) initLocks() {
	s.locks = make([]sync.RWMutex, rwLocksCount)
}

func (s *slotLocks) forSlot(slot uint64) *sync.RWMutex {
	return &s.locks[slot%rwLocksCount]
}
