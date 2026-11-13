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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// A deletion must shadow older file values while moving from MDBX to files,
// after MDBX pruning, and after compaction removes the older file layers.
func TestDomain_DeletedKeyNotResurrectedByFiles(t *testing.T) {
	t.Parallel()
	for _, cfg := range []statecfg.DomainCfg{statecfg.Schema.AccountsDomain, statecfg.Schema.CodeDomain} {
		t.Run(cfg.Name.String(), func(t *testing.T) {
			db, d := testDbAndDomainOfStep(t, cfg, 16, log.New())
			tx, err := db.BeginRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			view := d.beginForTests()
			defer view.Close()
			writer := view.NewWriter(db)
			defer writer.Close()
			key, value := []byte("key1"), []byte("value1")
			require.NoError(t, writer.PutWithPrev(key, value, 2, nil))
			require.NoError(t, writer.DeleteWithPrev(key, 18, value))
			require.NoError(t, writer.Flush(t.Context(), tx))
			writer.Close()
			view.Close()
			check := func() {
				t.Helper()
				readView := d.beginForTests()
				defer readView.Close()
				got, _, _, readErr := readView.GetLatest(key, tx)
				require.NoError(t, readErr)
				require.Empty(t, got, "deleted key must not expose the older value")
			}
			progress := background.NewProgressSet()
			require.NoError(t, d.collateBuildIntegrate(t.Context(), 0, tx, progress))
			check()
			require.NoError(t, d.collateBuildIntegrate(t.Context(), 1, tx, progress))
			check()
			view = d.beginForTests()
			logEvery := time.NewTicker(time.Minute)
			defer logEvery.Stop()
			for step := range kv.Step(2) {
				_, pruneErr := view.Prune(t.Context(), tx, step, step.ToTxNum(16), (step + 1).ToTxNum(16), math.MaxUint64, logEvery)
				require.NoError(t, pruneErr)
			}
			_, _, found, err := view.getLatestFromDb(key, tx, kv.NoStepBound)
			require.NoError(t, err)
			require.False(t, found, "the file-only read must not be shadowed by MDBX")
			view.Close()
			check()
			view = d.beginForTests()
			r := view.findMergeRange(32, 32, 32)
			require.True(t, r.values.needMerge)
			values, indexes, history := view.staticFilesInRange(r)
			merged, mergedIndex, mergedHistory, err := view.mergeFiles(t.Context(), values, indexes, history, r, nil, true, progress)
			require.NoError(t, err)
			d.integrateMergedDirtyFiles(merged, mergedIndex, mergedHistory)
			view.Close()
			check()
		})
	}
}
