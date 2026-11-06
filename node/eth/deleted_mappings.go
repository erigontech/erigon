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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/mmap"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

// deletedMappingsWarnAfter is longer than readers of a retired file normally take to drain.
const deletedMappingsWarnAfter = 10 * time.Minute

var (
	mxDeletedMappedFiles = metrics.GetOrCreateGauge("mmap_deleted_files")
	mxDeletedMappedBytes = metrics.GetOrCreateGauge("mmap_deleted_bytes")
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

// watchDeletedMappings exports how many files under dir stay mapped after deletion, and warns when they do not go away.
func watchDeletedMappings(ctx context.Context, dir string, logger log.Logger) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	var w deletedMappingsWatch
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			n, size, err := mmap.DeletedMappings(dir)
			if err != nil {
				return
			}
			mxDeletedMappedFiles.SetUint64(uint64(n))
			mxDeletedMappedBytes.SetUint64(size)
			if w.warn(now, n) {
				logger.Warn("[mmap] deleted files are still mapped; their disk space is freed only after unmap", "files", n, "size", datasize.ByteSize(size).HR(), "since", w.since.Format(time.DateTime))
			}
		}
	}
}
