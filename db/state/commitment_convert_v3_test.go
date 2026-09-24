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

package state_test

import (
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	v4 "github.com/erigontech/erigon/execution/commitment/v4"
)

func TestConvertCommitmentFiles_V3(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	db, agg := testDbAggregatorWithFiles(t, &testAggConfig{stepSize: 10, disableCommitmentBranchTransform: true})
	legacyTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer legacyTx.Rollback()
	legacyState, found, _, _, err := state.AggTx(legacyTx).DebugGetLatestFromFiles(kv.CommitmentDomain, commitment.KeyCommitmentState, math.MaxUint64)
	require.NoError(t, err)
	require.True(t, found)
	legacyConverted, err := v4.ConvertLegacyState(legacyState)
	legacyTx.Rollback()
	require.NoError(t, err)
	wantBlock, wantTxNum, wantRoot, err := v4.DecodeState(legacyConverted)
	require.NoError(t, err)

	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})

	snapDir := agg.Dirs().SnapDomain
	kvs, err := filepath.Glob(filepath.Join(snapDir, "*-commitment.*.kv"))
	require.NoError(t, err)
	require.NotEmpty(t, kvs)
	for _, p := range kvs {
		name := filepath.Base(p)
		require.Truef(t, strings.HasPrefix(name, "v3.0-"), "%s is not a v3.0 file", name)
		rangeName := strings.TrimSuffix(strings.TrimPrefix(name, "v3.0-"), ".kv")
		for ext, want := range map[string]int{".bt": 1, ".kvei": 1, ".kvi": 0} {
			siblings, globErr := filepath.Glob(filepath.Join(snapDir, "*-"+rangeName+ext))
			require.NoError(t, globErr)
			require.Lenf(t, siblings, want, "%s siblings of %s", ext, name)
		}
	}

	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	stateValue, _, err := tx.GetLatest(kv.CommitmentDomain, v4.StateKey(), kv.GetLatestOptions{})
	require.NoError(t, err)
	blockNum, txNum, root, err := v4.DecodeState(stateValue)
	require.NoError(t, err)
	require.Equal(t, wantRoot, root)
	require.Equal(t, wantBlock, blockNum)
	require.Equal(t, wantTxNum, txNum)
}
