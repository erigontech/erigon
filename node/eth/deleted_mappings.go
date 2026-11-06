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

package eth

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/mmap"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

// deletedFileWarnAfter is longer than readers of a retired file normally take to drain.
const deletedFileWarnAfter = 10 * time.Minute

var (
	mxDeletedMappedFiles = metrics.GetOrCreateGauge("mmap_deleted_files")
	mxDeletedMappedBytes = metrics.GetOrCreateGauge("mmap_deleted_bytes")
	mxDeletedOpenFds     = metrics.GetOrCreateGauge("fd_deleted")
)

// deletedFilesWatch remembers when each deleted path was first seen, so files that come and go during a long
// merge do not add up to one warning.
type deletedFilesWatch struct {
	firstSeen map[string]time.Time
	lastWarn  time.Time
}

// warn returns the path deleted longest ago when it has stayed for deletedFileWarnAfter, at most once per that period.
func (w *deletedFilesWatch) warn(now time.Time, paths []string) (oldest string, since time.Time, ok bool) {
	seen := make(map[string]time.Time, len(paths))
	for _, p := range paths {
		t, found := w.firstSeen[p]
		if !found {
			t = now
		}
		seen[p] = t
		if oldest == "" || t.Before(since) {
			oldest, since = p, t
		}
	}
	w.firstSeen = seen
	if oldest == "" || now.Sub(since) < deletedFileWarnAfter || now.Sub(w.lastWarn) < deletedFileWarnAfter {
		return "", time.Time{}, false
	}
	w.lastWarn = now
	return oldest, since, true
}

// watchDeletedMappings exports how many files under datadir stay open or mapped after deletion, and warns when
// one of them does not go away.
func watchDeletedMappings(ctx context.Context, datadir string, logger log.Logger) {
	if resolved, err := filepath.EvalSymlinks(datadir); err == nil {
		datadir = resolved
	}
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	var w deletedFilesWatch
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			mapped, size, err := mmap.DeletedMappings(datadir)
			if errors.Is(err, errors.ErrUnsupported) {
				return
			}
			if err != nil {
				continue
			}
			fds, err := dir.DeletedOpenFiles(datadir)
			if err != nil {
				continue
			}
			mxDeletedMappedFiles.SetUint64(uint64(len(mapped)))
			mxDeletedMappedBytes.SetUint64(size)
			mxDeletedOpenFds.SetUint64(uint64(len(fds)))
			if oldest, since, ok := w.warn(now, append(mapped, fds...)); ok {
				logger.Warn("[files] deleted files are still open or mapped; their disk space is freed only after close and unmap",
					"mapped", len(mapped), "mappedSize", datasize.ByteSize(size).HR(), "deletedFds", len(fds), "oldest", oldest, "since", since.Format(time.DateTime))
			}
		}
	}
}
