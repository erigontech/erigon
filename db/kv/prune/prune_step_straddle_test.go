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

package prune_test

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
)

// stepKeyedEntry writes a domain value the way a step-keyed values table does:
// the key carries the inverted step, and the entry stands for every write that
// landed anywhere inside that step.
func stepKeyedEntry(tb testing.TB, tx kv.RwTx, key []byte, step uint64) {
	tb.Helper()
	k := make([]byte, 0, len(key)+8)
	k = append(k, key...)
	k = binary.BigEndian.AppendUint64(k, ^step)
	require.NoError(tb, tx.Put(testTxLookupTable, k, []byte("v")))
}

// A step-keyed entry stands for the whole step, so it may only be pruned once
// files cover the step's last txNum. Pruning to a bound inside the step drops
// writes that landed above it and that no file holds: the state is then in
// neither place, and reads fall through to an older file and answer with a
// value from before the bound.
//
// Every prune bound used to be step-aligned, which hid this — a mid-step bound
// is what a boundary file cut at an unwind target produces.
func TestTableScanningPruneKeepsStepStraddlingBound(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	const stepSize = 100
	for step := range uint64(3) {
		stepKeyedEntry(t, tx, makeTxHash(step), step)
	}

	logEvery := time.NewTicker(time.Hour)
	defer logEvery.Stop()
	cur := openPseudoCursor(t, tx)
	defer cur.Close()

	// Files end at 250 — inside step 2, which spans [200, 300).
	stat, err := prune.TableScanningPrune(
		t.Context(), "test", "accounts",
		0, 250, stepSize, logEvery, log.New(),
		nil, cur, false, &prune.Stat{}, prune.StepKeyStorageMode,
	)
	require.NoError(t, err)

	require.EqualValues(t, 2, stat.PruneCountValues, "steps 0 and 1 are covered by files and go")
	require.Equal(t, 1, countTable(t, tx), "step 2 straddles the bound and stays")
}
