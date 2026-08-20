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
	"fmt"
	"strconv"
	"sync"

	"github.com/spf13/afero"

	"github.com/erigontech/erigon/common"
)

const (
	subdivisionSlot = 10_000
	rwLocksCount    = 64
)

// bucketStore holds sidecars under <slot/subdivisionSlot>/<blockRoot>_<index>.
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
