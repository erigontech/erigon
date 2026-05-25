// Copyright 2024 The Erigon Authors
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
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/kv"
)

// generateAllDomainsOverlap writes {0,1},{1,2} subset files shadowed by a {0,2}
// covering file for every state domain, so that overlap-cleanup has garbage to
// retire after OpenFolder.
func generateAllDomainsOverlap(t *testing.T, agg *Aggregator) {
	t.Helper()
	ranges := []testFileRange{{0, 1}, {1, 2}, {0, 2}}
	generateAccountsFile(t, agg.Dirs(), ranges)
	generateCodeFile(t, agg.Dirs(), ranges)
	generateStorageFile(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	require.NoError(t, agg.OpenFolder())
}

func mustExist(t *testing.T, path string, want bool) {
	t.Helper()
	got, err := dir.FileExist(path)
	require.NoError(t, err)
	require.Equalf(t, want, got, "file %s exist=%v, want %v", path, got, want)
}

// TestAggregatorReadAfterRetire verifies the bundle-refcount reclamation: a file
// retired (overlap-cleaned) while a reader still pins its generation is NOT
// removed from disk; it is physically deleted only once the last pinning reader
// closes. A reader opened after retirement no longer sees it.
func TestAggregatorReadAfterRetire(t *testing.T) {
	stepSize := uint64(10)
	_, agg := testDbAndAggregatorv3(t, stepSize)
	generateAllDomainsOverlap(t, agg)

	// subset (overlap) files for the accounts history — shadowed by 0-2, non-frozen
	subset01 := filepath.Join(agg.Dirs().SnapHistory, "v1.0-accounts.0-1.v")
	subset12 := filepath.Join(agg.Dirs().SnapHistory, "v1.0-accounts.1-2.v")
	covering := filepath.Join(agg.Dirs().SnapHistory, "v1.0-accounts.0-2.v")
	mustExist(t, subset01, true)
	mustExist(t, subset12, true)
	mustExist(t, covering, true)

	// pin the current generation (overlaps are present in dirtyFiles)
	at := agg.BeginFilesRo()

	// retire the overlap files; they get attached to the generation `at` pins
	require.NoError(t, agg.RemoveOverlapsAfterMerge(t.Context()))

	// still pinned by `at` → must NOT be deleted yet
	mustExist(t, subset01, true)
	mustExist(t, subset12, true)

	// last reader closes → reclaimer deletes the retired files
	at.Close()
	mustExist(t, subset01, false)
	mustExist(t, subset12, false)
	mustExist(t, covering, true) // covering file stays visible

	// a freshly opened reader sees only the covering file
	at2 := agg.BeginFilesRo()
	defer at2.Close()
	hf := at2.d[kv.AccountsDomain].ht.files
	require.Len(t, hf, 1)
	require.Equal(t, uint64(0), hf[0].startTxNum/stepSize)
	require.Equal(t, uint64(2), hf[0].endTxNum/stepSize)
}

// TestAggregatorReclaimConcurrent stresses BeginFilesRo/Close against a
// concurrent retirement under the race detector: no double-free / use-after-free
// of FilesItem regardless of how Close and reclamation interleave.
func TestAggregatorReclaimConcurrent(t *testing.T) {
	stepSize := uint64(10)
	_, agg := testDbAndAggregatorv3(t, stepSize)
	generateAllDomainsOverlap(t, agg)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				tx := agg.BeginFilesRo()
				// touch visible files so a use-after-free would be observed
				_ = tx.d[kv.AccountsDomain].ht.files.EndTxNum()
				tx.Close()
			}
		}()
	}

	require.NoError(t, agg.RemoveOverlapsAfterMerge(t.Context()))
	close(stop)
	wg.Wait()

	mustExist(t, filepath.Join(agg.Dirs().SnapHistory, "v1.0-accounts.0-1.v"), false)
}
