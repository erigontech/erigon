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
	"bytes"
	"encoding/binary"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// TestDomain_CollationMatchesFullScanOnChaindata compares, for every step still in a real chaindata,
// the collated values file against the full-scan collation it replaces. Run it on a copy of a node's DB:
// COLLATE_COMPARE_CHAINDATA=<dir> COLLATE_COMPARE_STEP_SIZE=<n>.
func TestDomain_CollationMatchesFullScanOnChaindata(t *testing.T) {
	path := os.Getenv("COLLATE_COMPARE_CHAINDATA")
	if path == "" {
		t.Skip("COLLATE_COMPARE_CHAINDATA is not set")
	}
	stepSize, err := strconv.ParseUint(os.Getenv("COLLATE_COMPARE_STEP_SIZE"), 10, 64)
	require.NoError(t, err)
	logger := log.New()
	db := mdbx.New(dbcfg.ChainDB, logger).Path(path).Readonly(true).MustOpen()
	defer db.Close()
	roTx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()

	for _, cfg := range []statecfg.DomainCfg{statecfg.Schema.AccountsDomain, statecfg.Schema.StorageDomain, statecfg.Schema.CommitmentDomain} {
		if cfg.LargeValues {
			continue
		}
		cfg.Hist.SnapshotsDisabled = true
		d, err := NewDomain(cfg, stepSize, config3.DefaultStepsInFrozenFile, datadir.New(t.TempDir()), logger)
		require.NoError(t, err)
		salt := uint32(1)
		d.salt.Store(&salt)
		d.DisableFsync()
		steps, rows := dupSortTableSteps(t, roTx, cfg.ValuesTable)
		t.Logf("%s: %d rows, %d steps %v..%v", cfg.ValuesTable, rows, len(steps), steps[0], steps[len(steps)-1])
		for from := 0; from < len(steps); from += maxCollationBatchSteps {
			to := min(from+maxCollationBatchSteps, len(steps))
			comps, err := d.collateValuesBatch(t.Context(), steps[from], steps[to-1]+1, roTx)
			require.NoError(t, err)
			for _, step := range steps[from:to] {
				batched := comps[step-steps[from]]
				comps[step-steps[from]] = nil
				want := fullScanCollation(t, roTx, cfg.ValuesTable, step)
				for _, values := range []*seg.Compressor{nil, batched} {
					got := collatedValueWords(t, d, step, stepSize, roTx, values)
					require.Equal(t, len(want), len(got), "%s step %d batched=%t: word count", cfg.ValuesTable, step, values != nil)
					for i := range want {
						require.True(t, bytes.Equal(want[i], got[i]), "%s step %d batched=%t: word %d differs", cfg.ValuesTable, step, values != nil, i)
					}
				}
			}
			for _, comp := range comps {
				if comp != nil {
					comp.Close()
				}
			}
		}
		d.Close()
	}
}

func dupSortTableSteps(t *testing.T, tx kv.Tx, table string) ([]kv.Step, int) {
	t.Helper()
	c, err := tx.CursorDupSort(table)
	require.NoError(t, err)
	defer c.Close()
	seen := make(map[kv.Step]struct{})
	rows := 0
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		require.NoError(t, err)
		rows++
		seen[kv.Step(^binary.BigEndian.Uint64(v[:8]))] = struct{}{}
	}
	steps := make([]kv.Step, 0, len(seen))
	for s := range seen {
		steps = append(steps, s)
	}
	slices.Sort(steps)
	require.NotEmpty(t, steps, "%s has no rows", table)
	return steps, rows
}

func fullScanCollation(t *testing.T, tx kv.Tx, table string, step kv.Step) [][]byte {
	t.Helper()
	c, err := tx.CursorDupSort(table)
	require.NoError(t, err)
	defer c.Close()
	stepVal := ^uint64(step)
	var words [][]byte
	for k, v, err := c.First(); k != nil; {
		require.NoError(t, err)
		if binary.BigEndian.Uint64(v[:8]) != stepVal {
			k, v, err = c.Next()
			continue
		}
		words = append(words, bytes.Clone(k), bytes.Clone(v[8:]))
		k, v, err = c.NextNoDup()
	}
	return words
}
