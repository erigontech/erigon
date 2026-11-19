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

package state

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/datastruct/btindex"
	"github.com/erigontech/erigon/db/datastruct/existence"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

var errRestoreSnapshot = errors.New("restore the unreadable snapshot from a complete source")

// Find the first tx in [from, to) covered only by a rejected file. A readable
// file may cover part or all of that range after a merge.
func (df *DirtyFiles) firstUnreadableData(files visibleFiles, from, to uint64) (uint64, string) {
	first := to
	var path string
	df.unreadable.Range(func(key, value any) bool {
		r := key.([2]uint64)
		start, end := max(from, r[0]), min(first, r[1])
		for _, f := range files {
			if start >= end || f.startTxNum > start {
				break
			}
			start = max(start, f.endTxNum)
		}
		if start < end {
			first, path = start, value.(string)
		}
		return first > from
	})
	return first, path
}

func (dt *DomainRoTx) firstUnreadableTx(from, to uint64) uint64 {
	to, _ = dt.d.dirtyFiles.firstUnreadableData(dt.files, from, to)
	if !dt.d.HistoryDisabled && !dt.d.SnapshotsDisabled {
		to, _ = dt.ht.h.dirtyFiles.firstUnreadableData(dt.ht.files, from, to)
		to = dt.ht.iit.firstUnreadableTx(from, to)
	}
	return to
}

func (iit *InvertedIndexRoTx) firstUnreadableTx(from, to uint64) uint64 {
	first, _ := iit.ii.dirtyFiles.firstUnreadableData(iit.files, from, to)
	return first
}

type stepDataRecovery uint8

const (
	buildMissingData stepDataRecovery = iota
	requireReadableData
	rebuildFromMDBX
)

// A partially pruned step can still contain rows. Check the prune boundary in
// the same MDBX transaction that supplied the collation.
func (df *DirtyFiles) stepDataRecovery(tx kv.Tx, table, path string, txFrom, txTo uint64, count int) (stepDataRecovery, error) {
	rejected, ok := df.unreadable.Load([2]uint64{txFrom, txTo})
	if !ok {
		_, failedPath := df.firstUnreadableData(nil, txFrom, txTo)
		if failedPath == "" {
			return buildMissingData, nil
		}
		// A single-step build cannot replace a merged range.
		if item, ok := df.Get(newFilesItem(txFrom, txTo)); ok && item.decompressor != nil {
			return requireReadableData, nil
		}
		return requireReadableData, fmt.Errorf("%w: %s", errRestoreSnapshot, failedPath)
	}
	if failedPath := rejected.(string); failedPath != path {
		return requireReadableData, fmt.Errorf("%w: %s", errRestoreSnapshot, failedPath)
	}
	if count == 0 {
		return requireReadableData, nil
	}
	progress, err := GetPruneValProgress(tx, []byte(table))
	if err != nil || progress.TxTo > txFrom {
		return requireReadableData, err
	}
	return rebuildFromMDBX, nil
}

func openOrBuildStepData(path string, build func() error, recovery stepDataRecovery, accessorDir string, accessorExts ...string) (*seg.Decompressor, error) {
	data, err := seg.NewDecompressor(path)
	if err == nil {
		return data, nil
	}
	missing := errors.Is(err, os.ErrNotExist)
	if !missing && !errors.Is(err, &seg.ErrCompressedFileCorrupted{}) {
		return nil, err
	}
	switch recovery {
	case buildMissingData:
		if !missing {
			return nil, fmt.Errorf("%w: %w", errRestoreSnapshot, err)
		}
	case requireReadableData:
		return nil, fmt.Errorf("%w: %w", errRestoreSnapshot, err)
	case rebuildFromMDBX:
		// Torrent metadata identifies exact published bytes, including their encoding.
		published, statErr := dir.FileExist(path + ".torrent")
		if statErr != nil {
			return nil, statErr
		}
		if published {
			return nil, fmt.Errorf("%w: %w", errRestoreSnapshot, err)
		}
		if removeErr := removeStepAccessors(path, accessorDir, accessorExts); removeErr != nil {
			return nil, removeErr
		}
	}
	if err := build(); err != nil {
		return nil, err
	}
	return seg.NewDecompressor(path)
}

// Accessors contain offsets into a particular encoding. Remove every version
// for this range before replacing the data so a restart cannot use stale offsets.
func removeStepAccessors(dataPath, accessorDir string, extensions []string) error {
	entries, err := filesFromDir(accessorDir)
	if err != nil {
		return err
	}
	stepName := func(name string) string {
		_, suffix, _ := strings.Cut(name, "-")
		return strings.TrimSuffix(suffix, filepath.Ext(suffix))
	}
	step := stepName(filepath.Base(dataPath))
	var paths []string
	for _, entry := range entries {
		name, published := strings.CutSuffix(entry, ".torrent")
		if stepName(name) == step && slices.Contains(extensions, filepath.Ext(name)) {
			if _, err := version.ParseVersion(name); err != nil {
				continue
			}
			if published {
				return fmt.Errorf("%w: %s has published accessor %s", errRestoreSnapshot, dataPath, name)
			}
			paths = append(paths, filepath.Join(accessorDir, name))
		}
	}
	for _, path := range paths {
		if err := dir.RemoveFile(path); err != nil {
			return err
		}
	}
	return dir.FsyncDir(accessorDir)
}

// Accessors can be regenerated from the data file. Preserve a readable accessor
// because an existing reader may still have it mapped.
func openOrBuildAccessor[T any](path string, open func(string) (*T, error), build func() error) (*T, error) {
	existing, err := openExistingAccessor(path, open)
	if existing != nil || err != nil {
		return existing, err
	}
	if err := build(); err != nil {
		return nil, err
	}
	return open(path)
}

func openExistingAccessor[T any](path string, open func(string) (*T, error)) (*T, error) {
	accessor, err := open(path)
	if err == nil {
		return accessor, nil
	}
	var pathErr *os.PathError
	var errno syscall.Errno
	if !errors.Is(err, os.ErrNotExist) && (errors.As(err, &pathErr) || errors.As(err, &errno)) {
		return nil, err
	}
	return nil, nil
}

func (d *Domain) buildMissingBtIndex(btPath, filterPath string, data *seg.Reader, ps *background.ProgressSet) error {
	bt, err := openExistingAccessor(btPath, func(path string) (*btindex.BtIndex, error) {
		return btindex.OpenBtreeIndexWithDecompressor(path, data)
	})
	if err != nil {
		return err
	}
	bt.Close()
	accessors := d.Accessors
	if accessors.Has(statecfg.AccessorExistence) {
		filter, err := openExistingAccessor(filterPath, existence.OpenFilter)
		if err != nil {
			return err
		}
		if filter != nil {
			filter.Close()
			accessors &^= statecfg.AccessorExistence
		}
	}
	if bt != nil {
		if !accessors.Has(statecfg.AccessorExistence) {
			return nil
		}
		// Building the missing filter also writes a B-tree. Keep the mapped B-tree in place.
		tmp, err := os.MkdirTemp(d.dirs.Tmp, "snapshot-accessors-")
		if err != nil {
			return err
		}
		defer dir.RemoveAll(tmp) //nolint:errcheck
		btPath = filepath.Join(tmp, filepath.Base(btPath))
	}
	return btindex.BuildBtreeIndexWithDecompressor(btPath, filterPath, data, ps, d.dirs.Tmp, *d.salt.Load(), d.logger, d.noFsync, accessors)
}
