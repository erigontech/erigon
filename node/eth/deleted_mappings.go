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
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/mmap"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

// deletedMappingsWarnAfter is longer than readers of a retired file normally take to drain.
const deletedMappingsWarnAfter = 10 * time.Minute

var (
	mxDeletedMappedFiles = metrics.GetOrCreateGauge("mmap_deleted_files")
	mxDeletedMappedBytes = metrics.GetOrCreateGauge("mmap_deleted_bytes")
	mxOpenFds            = metrics.GetOrCreateGauge("fd_open")
	mxDeletedOpenFds     = metrics.GetOrCreateGauge("fd_deleted")
)

type deletedMappingsWatch struct{ since, lastWarn time.Time }

func (w *deletedMappingsWatch) warn(now time.Time, count int) bool {
	if count == 0 {
		w.since = time.Time{}
		return false
	}
	if w.since.IsZero() {
		w.since = now
	}
	if now.Sub(w.since) < deletedMappingsWarnAfter || now.Sub(w.lastWarn) < deletedMappingsWarnAfter {
		return false
	}
	w.lastWarn = now
	return true
}

// watchDeletedMappings exports open fds and how many files under datadir stay open or mapped after deletion,
// and warns when those do not go away.
func watchDeletedMappings(ctx context.Context, datadir string, logger log.Logger) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	var w deletedMappingsWatch
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			mapped, size, err := mmap.DeletedMappings(datadir)
			if err != nil {
				return
			}
			open, deleted, err := dir.OpenFiles(datadir)
			if err != nil {
				return
			}
			mxDeletedMappedFiles.SetUint64(uint64(mapped))
			mxDeletedMappedBytes.SetUint64(size)
			mxOpenFds.SetUint64(uint64(open))
			mxDeletedOpenFds.SetUint64(uint64(deleted))
			if w.warn(now, mapped+deleted) {
				logger.Warn("[files] deleted files are still open or mapped; their disk space is freed only after close and unmap",
					"mapped", mapped, "mappedSize", datasize.ByteSize(size).HR(), "openFds", deleted, "since", w.since.Format(time.DateTime))
			}
		}
	}
}
