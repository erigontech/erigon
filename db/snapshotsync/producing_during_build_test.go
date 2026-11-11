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

package snapshotsync

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

// recordingMarks keeps the names marked and whether each was cleared again.
type recordingMarks struct {
	mu      sync.Mutex
	marked  []string
	open    map[string]int
	everPos map[string]bool
}

func newRecordingMarks() *recordingMarks {
	return &recordingMarks{open: map[string]int{}, everPos: map[string]bool{}}
}

func (r *recordingMarks) MarkProducing(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.marked = append(r.marked, name)
	r.open[name]++
	r.everPos[name] = true
}

func (r *recordingMarks) ClearProducing(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.open[name]--
}

func (r *recordingMarks) stillOpen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for n, c := range r.open {
		if c != 0 {
			out = append(out, n)
		}
	}
	return out
}

// A torrent minted over an index while its build is still running describes
// bytes that will not exist. The build must declare the file as being written,
// and must release it however it ends — a mark that outlives a failed build
// would stop the file ever being advertised.
func TestBuildMissedIndices_MarksItsIndexesWhileWriting(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	_, err := snaptype.LoadSalt(dirs.Snap, true, logger)
	require.NoError(t, err)

	createTestSegmentFile(t, 0, 1_000, snaptype2.Headers.Enum(), dirs.Snap, version.V1_0, logger)
	require.NoError(t, dir.RemoveFile(filepath.Join(dirs.Snap,
		snaptype.IdxFileName(version.V1_0, 0, 1_000, snaptype2.Headers.Enum().String()))))

	s := NewBaseRoSnapshots(
		ethconfig.BlocksFreezing{ChainName: networkname.Mainnet},
		dirs.Snap, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	t.Cleanup(s.Close)
	require.NoError(t, s.OpenFolder())

	marks := newRecordingMarks()
	s.SetProducerMarks(marks)

	_, err = s.buildMissedIndices("test", t.Context(), dirs, nil, 1, logger)
	require.NoError(t, err)

	want := snaptype2.Headers.IdxFileNames(0, 1_000)
	require.Subset(t, marks.marked, want,
		"the index being written must be declared before anything can hash it")
	require.Empty(t, marks.stillOpen(),
		"every mark must be released when the build ends, or the file is never advertised")
}
