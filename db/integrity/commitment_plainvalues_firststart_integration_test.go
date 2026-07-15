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

package integrity_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
)

// TestFirstStartPlainValuesRegime bootstraps a fresh datadir, resolves erigondb.toml through the
// --commitment.plainValues first-start override, and asserts the merged commitment .kv lands in the
// matching version regime (plain => v2.2, referenced => v2.1) and passes the regime-aware integrity
// checks — i.e. the flag's chosen regime flows resolver -> settings -> schema -> merge.
func TestFirstStartPlainValuesRegime(t *testing.T) {
	t.Run("plain", func(t *testing.T) {
		t.Parallel()
		runFirstStartRegimeCheck(t, true)
	})
	t.Run("referenced", func(t *testing.T) {
		t.Parallel()
		runFirstStartRegimeCheck(t, false)
	})
}

func runFirstStartRegimeCheck(t *testing.T, plainValues bool) {
	t.Helper()
	logger := log.New()
	ctx := t.Context()
	const stepSize = uint64(10)
	const txs = 80 // 8 steps -> merge produces a >= threshold commitment file

	dirs := datadir.New(t.TempDir())

	refs := !plainValues
	settings, err := state.ResolveErigonDBSettingsWithRefsDefault(dirs, logger, true /* noDownloader */, &refs)
	require.NoError(t, err)
	require.Equal(t, refs, settings.RefsInCommitmentBranches(), "resolved regime must match the first-start override")

	// Shrink the step size so a few steps trigger a >= threshold merge; the regime under test is
	// carried by settings.ReferencesInCommitmentBranches, independent of the step size.
	settings.StepSize = stepSize

	db := newTestDBWithSettings(t, ctx, dirs, settings)
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)

	writeAndBuild(t, ctx, db, agg, txs)
	require.NoError(t, agg.MergeLoop(ctx))

	db = reopenAgg(t, db, agg, dirs, stepSize, logger)

	gotReferenced, gotSpan := largestCommitmentFile(t, ctx, db, stepSize)
	require.GreaterOrEqual(t, gotSpan, 2*stepSize, "expected a merged commitment file spanning >= 2 steps")
	require.Equal(t, refs, gotReferenced, "merged commitment file's version regime must match the first-start override")

	require.NoError(t, integrity.CheckStateVerify(ctx, db, true /* failFast */, 0 /* fromStep */, logger))

	// For the plain regime the v2.1 file must be recognised as plain and skipped by the deref
	// validator (the version-aware skip); the referenced regime needs real chain data, so it is
	// covered by CheckStateVerify only, mirroring TestCheckStateVerify_VersionRegimes.
	if plainValues {
		require.NoError(t, integrity.CheckCommitmentKvDeref(ctx, db, nil /* cache */, true /* failFast */, logger))
	}
}

func newTestDBWithSettings(t *testing.T, ctx context.Context, dirs datadir.Dirs, settings *state.ErigonDBSettings) kv.TemporalRwDB {
	t.Helper()
	rawDB := memdb.NewTestDB(t, dbcfg.ChainDB)
	agg := state.NewTest(dirs).WithErigonDBSettings(settings).MustOpen(ctx, rawDB)
	require.NoError(t, agg.OpenFolder())
	t.Cleanup(agg.Close)
	db, err := temporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	t.Cleanup(db.Close)
	return db
}
